#!/usr/bin/env python3
"""Offline-optimal delegator, and the regret of the policy that actually ran (P1-1).

A bandit's learning curve on its own says only that reward went up. Regret says how much
reward was left on the table against the best choice available, which is the axis E2's
delegation-quality figure is plotted on, and which the staleness figure (F6) plots against
the context age P0-4 records.

**Where "the best choice" comes from.** The experiments inject failures at known rates:
`mab.failure_simulation` decides, per (agent, job type, elapsed time), the probability that
a delegated job fails. That profile *is* the ground truth — it is what makes one child group
genuinely better than another for a given job — so the oracle resolves it with the very
functions the agents use (`ResourceAgent._select_failure_phase` / `._resolve_failure_rate`),
imported rather than reimplemented. A second implementation of the ground truth would drift
from the first and make every regret number quietly wrong, with nothing to catch it.

**Three assumptions, all of which change the number, so all of which are stated in the
output rather than buried here:**

1. *A group's failure rate is the mean over its member agents.* Which agent in a group
   actually executes a delegated job is decided by cost-based selection at run time and is
   not knowable offline. The mean assumes any member is equally likely to win it; `--aggregate
   min` scores against the best member instead (an upper bound on how good a group can be)
   and `max` the worst. The three can differ a lot when a group is heterogeneous, so the
   choice is recorded in the output.
2. *A fan-out covering several groups is scored on its best member.* With `top_k > 1` the job
   is delegated to every selected group, so the run gets the best outcome among them.
3. *A decision with nothing to choose between has zero regret.* `bandit_all`/`all` decisions
   take every candidate, so the best one is always among them. They are excluded from routing
   accuracy — a policy cannot be wrong where it was never asked.

**The model is checked, not trusted.** `--validate` compares the failure rate the profile
predicts for the groups that were actually chosen against the exit statuses those jobs really
got. If those disagree, the oracle is scoring against a world the run did not happen in, and
the regret column should not be reported. That check is the difference between a regret
number and a plausible-looking one.

Usage::

    python evaluation/oracle.py --run-dir runs/hier-90/run01 --out regret.csv
    python evaluation/oracle.py --run-dir runs/hier-90/run01 --validate
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
import sys
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from swarm.agents.resource_agent import ResourceAgent  # noqa: E402

#: Decision policies that had no choice to make; see assumption 3.
NO_CHOICE = {"all", "bandit_all"}


class OracleError(RuntimeError):
    """The run cannot be scored — say why rather than emitting a plausible number."""


# --------------------------------------------------------------------------- run loading

def load_run(run_dir: Path) -> dict:
    """Everything needed to score one run, or an OracleError naming what is missing."""
    meta_path = run_dir / "run_meta.json"
    metrics_path = run_dir / "metrics.json"
    agents_path = run_dir / "all_agents.csv"   # JSON despite the name

    if not metrics_path.is_file():
        raise OracleError(f"{metrics_path} not found — nothing to score.")
    meta = _read_json(meta_path, default={})
    metrics = _read_json(metrics_path, default={})
    agents = _read_json(agents_path, default=[])

    truth = meta.get("ground_truth") or {}
    if not truth.get("enabled"):
        raise OracleError(
            "This run has no injected failure profile (mab.failure_simulation disabled, or "
            "run_meta.json predates the ground_truth field). Without one, no group is "
            "genuinely better than another and regret is not defined. Re-run with failure "
            "simulation on, or score a run that had it.")

    decisions = []
    starts = {}
    for agent_id, payload in metrics.items():
        if not isinstance(payload, dict):
            continue
        starts[str(agent_id)] = payload.get("failure_sim_start")
        for row in payload.get("delegation_decisions") or []:
            if isinstance(row, dict):
                decisions.append({**row, "agent_id": str(agent_id)})
    if not decisions:
        raise OracleError(
            "No delegation decisions in metrics.json. Either no coordinator ever delegated "
            "(check --groups-per-coordinator > 1) or this run predates P0-4.")

    return {
        "meta": meta,
        "truth": truth,
        "decisions": sorted(decisions, key=lambda r: r.get("ts") or 0),
        "group_members": _group_members(agents),
        "agent_starts": starts,
        "run_started_at": meta.get("started_at"),
    }


def _read_json(path: Path, default):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return default


def _group_members(agents: list) -> dict[int, list[str]]:
    """`{group_id: [agent_id, ...]}` over the LEAF agents only.

    Coordinators carry a group of their own at level >= 1; counting them as members of the
    group they lead would fold a coordinator's failure rate into the rate of the children it
    routes to, and coordinators do not execute delegated jobs.
    """
    members: dict[int, list[str]] = {}
    for a in agents or []:
        if not isinstance(a, dict) or (a.get("level") or 0) != 0:
            continue
        group = a.get("group")
        if group is None:
            continue
        members.setdefault(int(group), []).append(str(a.get("agent_id")))
    return members


# ------------------------------------------------------------------------ the ground truth

def group_failure_rate(group: int, job_type: Optional[str], at: float,
                       truth: dict, members: dict[int, list[str]],
                       agent_starts: dict[str, float],
                       aggregate: str = "mean") -> Optional[float]:
    """Probability a job of *job_type* delegated to *group* fails, at wall-clock time *at*.

    Resolved per member agent with the agents' own functions, then aggregated (assumption 1).
    Returns None for a group with no known members — scored as unknown, never as zero, since
    a group we know nothing about is not a group we know to be perfect.

    **Each member's failure phase is resolved on that member's own clock.** `after_s` in a
    phase is counted from the agent's construction, and a 30-host remote launch spreads
    construction over a minute; the rate that decides a delegated job's fate is the one the
    EXECUTING agent computes, not the one the coordinator would. Using the coordinator's
    elapsed time misattributes every decision within the launch spread of a phase boundary —
    which is exactly the mid-run flip E2a's non-stationarity arm is built around, so the
    error lands on the decisions that matter most.

    What remains approximate: a job executes some time *after* it is delegated, so a decision
    taken just before a boundary can be executed just after it. That gap is the scheduling
    latency, not the launch spread, and no offline reconstruction can close it.
    """
    agent_ids = members.get(int(group)) or []
    if not agent_ids:
        return None
    phases = truth.get("phases") or []
    base_per_agent = truth.get("per_agent_failure_rates") or {}
    base_per_type = truth.get("per_job_type_failure_rates") or {}
    base_rate = float(truth.get("failure_probability", 0.1))

    rates = []
    for a in agent_ids:
        elapsed = 0.0
        if phases:
            # Only a phased profile depends on when the agent started; a constant one is the
            # same at every instant, so demanding a start there would refuse every run that
            # predates `failure_sim_start` for no gain.
            #
            # There is deliberately NO fallback to the run start. Substituting a different
            # clock is the very error this function exists to avoid: agents start up to a
            # minute apart, so the run's clock places a member in the wrong phase, and a
            # member read as post-flip when it is really pre-flip makes a bad group look
            # good — which scores a bad routing decision as optimal. A clock we do not have
            # is unscoreable, not approximately known.
            #
            # `.get(key, default)` would not help even if a default were wanted:
            # `agent_starts` is built from the metrics payloads, so an agent that reported no
            # start is PRESENT with the value None and the default never applies.
            start = agent_starts.get(str(a))
            if start is None:
                return None
            elapsed = float(at) - float(start)
        per_agent, per_job_type, _ = ResourceAgent._select_failure_phase(
            elapsed, phases, base_per_agent, base_per_type)
        rates.append(ResourceAgent._resolve_failure_rate(
            agent_key=str(a), job_type=job_type, per_agent=per_agent,
            per_job_type=per_job_type, base=base_rate))
    if aggregate == "min":
        return min(rates)
    if aggregate == "max":
        return max(rates)
    return statistics.fmean(rates)


def expected_reward(failure_rate: float, reward_cfg: dict) -> float:
    """Expected bandit reward for a group with this failure probability.

    Mirrors `MABManager._shape_reward` for the two ends it can know offline. The latency
    shaping of a success is deliberately NOT modelled: it depends on queueing at the chosen
    group, which the failure profile says nothing about, and inventing it would make the
    oracle prefer groups for a reason the run never simulated.
    """
    success = 1.0
    if reward_cfg.get("shaped"):
        failure = float(reward_cfg.get("exit_failure", -0.5))
    else:
        failure = -1.0
    return (1.0 - failure_rate) * success + failure_rate * failure


# ------------------------------------------------------------------------------- scoring

def score_decision(row: dict, run: dict, aggregate: str = "mean") -> Optional[dict]:
    """Regret for one delegation. None when the decision cannot be scored."""
    truth, members = run["truth"], run["group_members"]
    reward_cfg = truth.get("reward") or {}

    at = row.get("ts")
    if not at:
        return None
    # The coordinator's own elapsed time, reported for traceability only. It is NOT what the
    # phases are resolved against -- each candidate group's members are placed in a phase on
    # their own clocks inside group_failure_rate, because the executing agent is what decides
    # whether the job fails.
    coord_start = run["agent_starts"].get(row["agent_id"]) or run["run_started_at"]
    elapsed = float(at) - float(coord_start) if coord_start else float("nan")

    job_type = row.get("job_type")
    candidates = [int(g) for g in (row.get("candidates") or [])]
    selected = [int(g) for g in (row.get("selected") or [])]
    if not candidates or not selected:
        return None

    rates = {g: group_failure_rate(g, job_type, float(at), truth, members,
                                   run["agent_starts"], aggregate)
             for g in candidates}
    # EVERY candidate must resolve, or the decision is not scored. Regret is a comparison
    # across the whole candidate set, so dropping the candidates we cannot price and taking
    # the best of what is left biases the result *towards* the policy: if the genuinely best
    # group is the one that vanished, the chosen group becomes the best of the remainder and
    # a bad routing decision is recorded as regret 0, `optimal: True`. A silent bias in the
    # flattering direction is the worst kind for a figure whose whole claim is that one
    # policy routes better than another.
    if any(r is None for r in rates.values()):
        return None
    rewards = {g: expected_reward(r, reward_cfg) for g, r in rates.items()}
    if not all(g in rewards for g in selected):
        return None

    best_reward = max(rewards.values())
    # Assumption 2: a fan-out gets the best of what it was given.
    chosen_reward = max(rewards[g] for g in selected)

    # A decision has no material choice when the policy was not asked (`bandit_all`/`all`),
    # when there was one candidate, OR when every candidate was equally good. The last is not
    # hypothetical: a failure profile whose contrast runs BETWEEN coordinators rather than
    # within each one gives every candidate set a single rate, and grading those as correct
    # reports routing_accuracy 1.0 for a run in which no routing decision could be wrong.
    spread = max(rewards.values()) - min(rewards.values())
    no_choice = (row.get("policy") in NO_CHOICE or len(rewards) <= 1
                 or spread <= 1e-12)
    best_groups = sorted(g for g, v in rewards.items() if v >= best_reward - 1e-12)
    known = rates
    return {
        "ts": row.get("ts"),
        "agent_id": row["agent_id"],
        "job_id": row.get("job_id"),
        "job_type": job_type,
        "policy": row.get("policy"),
        "coordinator_elapsed_s": round(elapsed, 3),
        "candidates": " ".join(str(g) for g in candidates),
        "selected": " ".join(str(g) for g in selected),
        "oracle_groups": " ".join(str(g) for g in best_groups),
        "chosen_failure_rate": round(min(known[g] for g in selected if g in known), 6),
        "oracle_failure_rate": round(min(known.values()), 6),
        "chosen_reward": round(chosen_reward, 6),
        "oracle_reward": round(best_reward, 6),
        "regret": round(max(0.0, best_reward - chosen_reward), 6),
        "optimal": bool(set(selected) & set(best_groups)),
        "no_choice": no_choice,
        # Carried through so regret can be plotted against staleness (F6) from one file.
        "ctx_age_mean": row.get("ctx_age_mean"),
        "ctx_age_chosen": row.get("ctx_age_chosen"),
        "decide_s": row.get("decide_s"),
    }


def score_run(run: dict, aggregate: str = "mean") -> tuple[list[dict], dict]:
    """Per-decision regret rows plus the run-level summary."""
    rows = [r for r in (score_decision(d, run, aggregate) for d in run["decisions"])
            if r is not None]
    if not rows:
        reasons = []
        if not run["group_members"]:
            reasons.append("all_agents.csv is missing or lists no level-0 agents, so no group "
                           "membership is known")
        if (run["truth"].get("phases") or []) and not any(
                v is not None for v in run["agent_starts"].values()):
            reasons.append("the profile has phases but no agent reported failure_sim_start, "
                           "so no member can be placed in a phase (a run from before that "
                           "field existed cannot be scored against a phased profile)")
        raise OracleError(
            "No decision could be scored. " + ("; ".join(reasons) + "." if reasons else
            "Every decision had at least one candidate group that could not be priced."))

    cumulative = 0.0
    for r in rows:
        cumulative += r["regret"]
        r["cumulative_regret"] = round(cumulative, 6)

    decided = [r for r in rows if not r["no_choice"]]
    summary = {
        "decisions_scored": len(rows),
        "decisions_unscored": len(run["decisions"]) - len(rows),
        "decisions_with_a_choice": len(decided),
        # Named so a reader cannot mistake "no candidate was better than another" for
        # "the policy got them all right".
        "decisions_without_a_choice": len(rows) - len(decided),
        "regret_total": round(cumulative, 6),
        "regret_mean": round(cumulative / len(rows), 6),
        # Over decisions that HAD a choice: including the inert ones would report a routing
        # accuracy that rises as a coordinator's fan-out widens, which measures configuration
        # rather than the policy.
        "routing_accuracy": (round(sum(r["optimal"] for r in decided) / len(decided), 6)
                             if decided else None),
        "regret_aggregate": aggregate,
    }
    return rows, summary


# ------------------------------------------------------------------------------ validation

def validate(run: dict, run_dir: Path, aggregate: str = "mean") -> dict:
    """Compare the profile's predicted failure rate against what the run actually observed.

    The oracle scores routing against a model of why jobs fail. If that model does not
    describe the run, every regret number is fiction that looks like data. This compares, per
    job type, the predicted failure rate of the groups that were chosen against the observed
    non-zero-exit fraction of the jobs that ran.
    """
    jobs_path = run_dir / "all_jobs.csv"
    if not jobs_path.is_file():
        return {"validated": False, "reason": "all_jobs.csv not found"}

    # Joined on job_id, not job_type: `all_jobs.csv` carries no job_type column at all, so a
    # type-keyed comparison silently matches nothing and reports a clean bill of health for a
    # check that never ran. The id join is also exact -- each decision is compared against the
    # outcome of the very job it routed, rather than against a type average.
    outcome: dict[str, int] = {}
    try:
        with open(jobs_path, newline="") as fh:
            for record in csv.DictReader(fh):
                status = record.get("exit_status")
                if status in (None, ""):
                    continue
                completed = record.get("completed_at")
                if not completed or float(completed) <= 0:
                    continue  # never finished; its exit status means nothing
                outcome[str(record.get("job_id"))] = 0 if float(status) == 0 else 1
    except (OSError, ValueError) as exc:
        return {"validated": False, "reason": f"could not read all_jobs.csv: {exc}"}

    by_type: dict[str, list[tuple[float, int]]] = {}
    for d in run["decisions"]:
        job_id = str(d.get("job_id"))
        if job_id not in outcome:
            continue  # delegated but never completed, so nothing to compare against
        scored = score_decision(d, run, aggregate)
        if scored is None:
            continue
        by_type.setdefault(d.get("job_type") or "", []).append(
            (scored["chosen_failure_rate"], outcome[job_id]))

    comparison = []
    for job_type, pairs in sorted(by_type.items()):
        n = len(pairs)
        observed = sum(p[1] for p in pairs) / n
        predicted = statistics.fmean(p[0] for p in pairs)
        # A deviation is only evidence against the profile if it is bigger than the sampling
        # noise of these n trials. Some band is necessary or the check is dominated by rare
        # job types: on the slice 2026-09-14 it flagged a run whose profile fitted to 0.018
        # because ONE type with six jobs came in 0.18 off. A validator that cries wolf every
        # run is one nobody reads.
        #
        # The band is the **Poisson-binomial** standard error over the individual predicted
        # rates, not the binomial one at their mean. The trials are not identically
        # distributed — each decision routed to a group with its own rate — and the mean-p
        # binomial systematically overstates the variance, so it accepts deviations the
        # profile says are implausible. Worst at the extreme: a profile under which half the
        # jobs CANNOT fail (p=0) and half CANNOT succeed (p=1) pins the observed rate at
        # exactly 0.5, yet the mean-p band (p=0.5) would accept a swing of +/-0.10 at n=100.
        # The exact band is 0, which is the right answer: nothing else is possible.
        variance = sum(p * (1.0 - p) for p, _ in pairs)
        noise = 2.0 * math.sqrt(variance) / n
        comparison.append({
            "job_type": job_type,
            "n_jobs": n,
            "observed_failure_rate": round(observed, 6),
            "predicted_failure_rate": round(predicted, 6),
            "sampling_noise_2se": round(noise, 6),
            "beyond_noise": bool(abs(observed - predicted) > noise),
        })
    if not comparison:
        return {"validated": False,
                "reason": "no delegated job in the decision log also appears completed in "
                          "all_jobs.csv -- nothing to compare"}
    # Weighted by job count: a type with three jobs should not outvote one with three hundred
    # when deciding whether the profile describes the run.
    total = sum(c["n_jobs"] for c in comparison)
    weighted = sum(abs(c["observed_failure_rate"] - c["predicted_failure_rate"]) * c["n_jobs"]
                   for c in comparison) / total
    worst = max(abs(c["observed_failure_rate"] - c["predicted_failure_rate"])
                for c in comparison)
    beyond = [c["job_type"] for c in comparison if c["beyond_noise"]]
    return {"validated": True, "jobs_compared": total,
            "mean_abs_error": round(weighted, 6), "max_abs_error": round(worst, 6),
            "types_beyond_sampling_noise": beyond,
            "per_job_type": comparison}


# ------------------------------------------------------------------------------------ CLI

def main() -> int:
    ap = argparse.ArgumentParser(
        description="Offline-optimal delegation and regret for one run (P1-1).")
    ap.add_argument("--run-dir", required=True, type=Path,
                    help="Run directory holding run_meta.json, metrics.json, all_agents.csv.")
    ap.add_argument("--out", type=Path, default=None,
                    help="Write per-decision regret rows here as CSV (default: run-dir/regret.csv).")
    ap.add_argument("--aggregate", choices=["mean", "min", "max"], default="mean",
                    help="How a group's failure rate is formed from its members' (assumption "
                         "1 in the module docstring). Changes the numbers; recorded in the "
                         "summary. Default: mean.")
    ap.add_argument("--validate", action="store_true",
                    help="Also check the injected profile against the exit statuses the run "
                         "actually produced. Do this before reporting regret from a new "
                         "configuration.")
    args = ap.parse_args()

    try:
        run = load_run(args.run_dir)
        rows, summary = score_run(run, args.aggregate)
    except OracleError as exc:
        print(f"cannot score {args.run_dir}: {exc}", file=sys.stderr)
        return 2

    out = args.out or (args.run_dir / "regret.csv")
    with open(out, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    print(json.dumps(summary, indent=2))
    print(f"\n{len(rows)} decision(s) -> {out}")

    if args.validate:
        report = validate(run, args.run_dir, args.aggregate)
        print("\nvalidation:", json.dumps(report, indent=2))
        # Gated on the job-weighted error and on deviations that sampling noise cannot
        # explain -- NOT on the raw worst per-type error, which a job type with six jobs
        # trips on noise alone in almost every run.
        if report.get("validated"):
            beyond = report["types_beyond_sampling_noise"]
            if report["mean_abs_error"] > 0.10 or beyond:
                print("\nWARNING: the injected profile does not describe this run "
                      f"(job-weighted error {report['mean_abs_error']:.3f}"
                      + (f"; beyond sampling noise: {', '.join(beyond)}" if beyond else "")
                      + "). The regret column is scored against a world this run did not "
                      "happen in — find out why before reporting it.", file=sys.stderr)
                return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
