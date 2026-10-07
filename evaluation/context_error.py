"""Context ERROR at delegation time: how wrong the coordinator's in-flight view was (T-5).

The staleness figure (F6) was planned against context AGE, `decision - received_at`. That age is
stamped on the 5 s Redis neighbor refresh whichever consensus engine runs, so it measures a
refresh cadence and cannot vary with the protocol. What a faster-finalizing protocol can change
is how WRONG the view is: `GroupSnapshot.inflight` counts the coordinator's delegations to a
group that it has not yet seen resolve, and the truth is the number of delegated copies in that
group that had not finished. This module reconstructs the truth offline and joins it onto each
decision row.

**The truth, per candidate group g at a decision instant t:** the number of delegated copies in
g whose interval contains t. A copy's interval runs from its delegation to its completion:

* start — the decision row's `ts` when a decision row names the copy (same coordinator clock as
  t), else the copy's level-0 `submitted_at` (stamped by the delegating coordinator);
* end — the copy's `completed_at` (the leaf's clock), or open when it never completed;
* a WITHDRAWN copy is deleted from Redis and so is in no export. It is reconstructed from the
  decision rows: its start is the decision, and its end is, in order of preference, the
  coordinator's `delegation_withdrawals` entry for that (job, group) — written when an untaken
  copy is deleted because another group has the job — then the next delegation decision for the
  same job (a withdrawal always precedes the re-delegation), then the coordinator's
  `delegation_reassignments` record when it names the group and falls after the decision.

**Refuse, don't approximate.** A candidate is UNKNOWN at t when any copy that could be in flight
at t in g has no knowable end (a withdrawn copy with no later decision and no reassignment
record), or when a copy's group cannot be attributed. A decision with an unknown candidate is not
scored, and the reason is counted: substituting zero for an unknown copy makes the coordinator's
view look stale by exactly the work that went missing, and substituting "still running" makes it
look fresh. Both bias the one figure this column exists for.

**Known limits, stated rather than corrected.** `completed_at` is on the leaf's clock and t on
the coordinator's, so an unsynchronised fleet shifts each completion by the offset (check
`ctx_skew_max_s` and `fix_slice_clocks.sh --check`). A copy delegated by another coordinator
(co-parents) counts toward g's truth, which is correct — the view's blindness to a co-parent's
work is part of its error. The decision's own job is excluded: its copy is created after the
snapshot the policy saw.
"""
from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

#: Decision-row fields this module adds.
ERROR_FIELDS = ("ctx_err_mean", "ctx_err_max", "ctx_err_chosen", "ctx_err_signed_mean",
                "ctx_err_scored", "ctx_err_reason")

_INF = math.inf


def _f(value) -> Optional[float]:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(v) else v


def _read_csv(path: Path) -> Optional[pd.DataFrame]:
    if not path.is_file():
        return None
    try:
        return pd.read_csv(path)
    except Exception:
        return None


def agent_tiers(run_dir: Path) -> Dict[str, Tuple[int, Optional[int]]]:
    """`{agent_id: (level, group)}` from `all_agents.csv` (JSON despite the name)."""
    path = run_dir / "all_agents.csv"
    try:
        agents = json.loads(path.read_text())
    except (OSError, ValueError):
        return {}
    out: Dict[str, Tuple[int, Optional[int]]] = {}
    for a in agents or []:
        if not isinstance(a, dict) or a.get("agent_id") is None:
            continue
        group = a.get("group")
        out[str(a["agent_id"])] = (int(a.get("level") or 0),
                                   int(group) if group is not None else None)
    return out


def _as_list(value) -> List[int]:
    if value is None:
        return []
    if isinstance(value, str):
        return [int(x) for x in value.split() if x.strip().lstrip("-").isdigit()]
    return [int(x) for x in value]


def _view(row: dict) -> Dict[str, dict]:
    v = row.get("ctx_view")
    if isinstance(v, str):
        try:
            v = json.loads(v)
        except ValueError:
            return {}
    return v if isinstance(v, dict) else {}


class _Copy:
    __slots__ = ("job_id", "group", "start", "end")

    def __init__(self, job_id: str, group: Optional[int], start: Optional[float],
                 end: Optional[float]):
        self.job_id, self.group, self.start, self.end = job_id, group, start, end


def _leaf_copies(run_dir: Path, tiers: Dict[str, Tuple[int, Optional[int]]]
                 ) -> Tuple[Dict[Tuple[str, int], dict], List[str]]:
    """Surviving level-0 copies keyed `(job_id, group)`, plus the job ids of copies whose group
    could not be attributed."""
    copies: Dict[Tuple[str, int], dict] = {}
    unattributed: List[str] = []
    for name in ("level0_jobs.csv", "pending_level0_jobs.csv"):
        df = _read_csv(run_dir / name)
        if df is None or df.empty or "job_id" not in df.columns:
            continue
        for rec in df.to_dict("records"):
            job = str(rec.get("job_id"))
            group = _f(rec.get("group")) if "group" in rec else None
            if group is None:
                leader = rec.get("leader_id")
                lf = _f(leader)
                if lf is not None:
                    tier = tiers.get(str(int(lf)))
                    if tier and tier[0] == 0 and tier[1] is not None:
                        group = float(tier[1])
            if group is None:
                unattributed.append(job)
                continue
            completed = _f(rec.get("completed_at"))
            copies[(job, int(group))] = {
                "submitted_at": _f(rec.get("submitted_at")),
                "completed_at": completed if completed and completed > 0 else None,
            }
    return copies, unattributed


def _reassign_records(agents: Dict[str, dict]) -> Dict[Tuple[str, str], dict]:
    """`{(agent_id, job_id): record}` from each coordinator's `delegation_reassignments`."""
    out: Dict[Tuple[str, str], dict] = {}
    for agent_id, payload in (agents or {}).items():
        if not isinstance(payload, dict):
            continue
        for job_id, rec in (payload.get("delegation_reassignments") or {}).items():
            if isinstance(rec, dict):
                out[(str(agent_id), str(job_id))] = rec
    return out


def _withdrawals(agents: Dict[str, dict]) -> Dict[Tuple[str, int], List[float]]:
    """`{(job_id, group): [at, ...]}` from each coordinator's `delegation_withdrawals` — the
    untaken copies it deleted once another group had the job."""
    out: Dict[Tuple[str, int], List[float]] = {}
    for payload in (agents or {}).values():
        if not isinstance(payload, dict):
            continue
        for entry in payload.get("delegation_withdrawals") or []:
            try:
                job, group, at = entry[0], int(entry[1]), float(entry[2])
            except (TypeError, ValueError, IndexError):
                continue
            out.setdefault((str(job), group), []).append(at)
    for times in out.values():
        times.sort()
    return out


def build_copies(rows: List[dict], run_dir: Path, agents: Dict[str, dict],
                 tiers: Dict[str, Tuple[int, Optional[int]]]
                 ) -> Tuple[List[_Copy], List[str]]:
    """Every delegated copy with its interval; an end of None means UNKNOWN, +inf still running."""
    surviving, unattributed = _leaf_copies(run_dir, tiers)
    reassigned = _reassign_records(agents)
    withdrawn = _withdrawals(agents)

    by_job: Dict[str, List[dict]] = {}
    for r in rows:
        if r.get("ts") is None or not r.get("job_id"):
            continue
        # Only a LEVEL-1 coordinator's decision creates a level-0 copy. A level-2 decision
        # selects level-1 groups, whose ids collide with level-0 ones, and counting it here
        # would put upper-tier delegations into a leaf group's truth (stop-time review). A
        # decider whose tier is unknown could be either, so its copies are unattributed.
        tier = tiers.get(str(r.get("agent_id")))
        if tier is None:
            unattributed.append(str(r["job_id"]))
            continue
        if tier[0] != 1:
            continue
        by_job.setdefault(str(r["job_id"]), []).append(r)
    for decisions in by_job.values():
        decisions.sort(key=lambda r: float(r["ts"]))

    copies: List[_Copy] = []
    named: set = set()
    for job, decisions in by_job.items():
        for i, d in enumerate(decisions):
            ts = float(d["ts"])
            nxt = float(decisions[i + 1]["ts"]) if i + 1 < len(decisions) else None
            for g in _as_list(d.get("selected")):
                later_same_group = any(g in _as_list(e.get("selected")) for e in decisions[i + 1:])
                leaf = surviving.get((job, g))
                if leaf is not None and not later_same_group:
                    # The copy the export holds is this decision's.
                    named.add((job, g))
                    end = leaf["completed_at"]
                    copies.append(_Copy(job, g, ts, end if end is not None else _INF))
                    continue
                # Withdrawn: in no export. The coordinator records the deletion of an
                # untaken copy once another group has the job; failing that, the end is
                # bounded by the re-delegation that followed it, or recorded by the
                # coordinator that pulled the job back.
                at = next((t for t in withdrawn.get((job, g), []) if t >= ts), None)
                if at is not None and (nxt is None or at <= nxt):
                    copies.append(_Copy(job, g, ts, at))
                    continue
                if nxt is not None:
                    copies.append(_Copy(job, g, ts, nxt))
                    continue
                rec = reassigned.get((str(d.get("agent_id")), job))
                at = _f(rec.get("reassigned_at")) if rec else None
                if at is not None and at >= ts and g in _as_list(rec.get("child_groups")):
                    copies.append(_Copy(job, g, ts, at))
                else:
                    copies.append(_Copy(job, g, ts, None))
    # Copies no decision row names (a ring-dropped row, a coordinator whose metrics are
    # missing): real load in the group, so they count, from their own delegation stamp.
    for (job, g), leaf in surviving.items():
        if (job, g) in named or job in by_job and any(
                g in _as_list(d.get("selected")) for d in by_job[job]):
            continue
        end = leaf["completed_at"]
        copies.append(_Copy(job, g, leaf["submitted_at"], end if end is not None else _INF))
    return copies, unattributed


def _truth(copies_in_group: List[_Copy], at: float, own_job: str) -> Optional[int]:
    n = 0
    for c in copies_in_group:
        if c.job_id == own_job or c.start is None or c.start > at:
            continue
        if c.end is None:
            return None        # could be in flight now, and nothing says when it ended
        if c.end > at:
            n += 1
    return n


def annotate(rows: List[dict], run_dir: Path, agents: Dict[str, dict]) -> List[dict]:
    """Add the ERROR_FIELDS to every decision row, in place. Returns the rows."""
    tiers = agent_tiers(run_dir)
    copies, unattributed = build_copies(rows, run_dir, agents, tiers)
    per_group: Dict[int, List[_Copy]] = {}
    for c in copies:
        per_group.setdefault(int(c.group), []).append(c)
    have_export = (run_dir / "level0_jobs.csv").is_file()

    for r in rows:
        for k in ERROR_FIELDS:
            r.pop(k, None)
        reason = None
        view = _view(r)
        candidates = _as_list(r.get("candidates"))
        decider = tiers.get(str(r.get("agent_id")))
        if not view:
            reason = "no_view"            # the row predates T-5, or the snapshot build failed
        elif not have_export:
            reason = "no_level0_export"
        elif decider is None:
            reason = "decider_tier_unknown"    # not in all_agents.csv: tier cannot be told
        elif decider[0] != 1:
            reason = "not_a_level1_decision"   # candidates are not level-0 groups
        elif unattributed:
            reason = "unattributed_copies"
        elif r.get("ts") is None:
            reason = "no_timestamp"
        if reason is None:
            at = float(r["ts"])
            errors: Dict[int, float] = {}
            for g in candidates:
                seen = (view.get(str(g)) or {}).get("inflight")
                if seen is None:
                    reason = "candidate_not_in_view"
                    break
                true = _truth(per_group.get(g, []), at, str(r.get("job_id")))
                if true is None:
                    reason = "copy_with_unknown_end"
                    break
                errors[g] = float(seen) - float(true)
            if reason is None and errors:
                absolute = [abs(e) for e in errors.values()]
                selected = [g for g in _as_list(r.get("selected")) if g in errors]
                r["ctx_err_mean"] = round(sum(absolute) / len(absolute), 6)
                r["ctx_err_max"] = round(max(absolute), 6)
                r["ctx_err_chosen"] = (round(sum(abs(errors[g]) for g in selected)
                                             / len(selected), 6) if selected else None)
                # Positive: the coordinator counted work as in flight that had finished (or
                # never knew it had been withdrawn) — the stale-resolution direction.
                r["ctx_err_signed_mean"] = round(sum(errors.values()) / len(errors), 6)
                r["ctx_err_scored"] = 1
                r["ctx_err_reason"] = ""
                continue
            if reason is None:
                reason = "no_candidates"
        r["ctx_err_scored"] = 0
        r["ctx_err_reason"] = reason
    return rows


def _spearman(x: pd.Series, y: pd.Series) -> Optional[float]:
    if len(x) < 3 or x.nunique() < 2 or y.nunique() < 2:
        return None
    return round(float(x.rank().corr(y.rank())), 6)


def run_metrics(rows: List[dict], agents: Dict[str, dict], metrics_complete: bool
                ) -> Dict[str, Any]:
    """Run-level context-error columns from rows already passed through `annotate`.

    Absent entirely when the run made no decision at all. When it did, the counts are emitted
    as 0 when measured clean, so `aggregate()` never averages a cell over the runs where the
    column happened to fire.
    """
    if not rows:
        return {}
    frame = pd.DataFrame(rows)
    out: Dict[str, Any] = {}
    scored = frame[frame.get("ctx_err_scored", pd.Series(dtype=float)) == 1] \
        if "ctx_err_scored" in frame.columns else frame.iloc[0:0]
    out["ctx_err_decisions_scored"] = int(len(scored))
    out["ctx_err_decisions_unscored"] = int(len(frame) - len(scored))
    if "ctx_err_reason" in frame.columns:
        for reason, count in frame.loc[frame["ctx_err_reason"].fillna("") != "",
                                       "ctx_err_reason"].value_counts().items():
            out[f"ctx_err_unscored_{reason}"] = int(count)
    def _col(name):
        if name not in scored.columns:
            return pd.Series(dtype=float)
        return pd.to_numeric(scored[name], errors="coerce").dropna()

    err = _col("ctx_err_mean")
    if not err.empty:
        out["ctx_err_mean"] = round(float(err.mean()), 6)
        out["ctx_err_p50"] = round(float(err.quantile(0.5)), 6)
        out["ctx_err_p95"] = round(float(err.quantile(0.95)), 6)
        out["ctx_err_max"] = round(float(err.max()), 6)
    chosen = _col("ctx_err_chosen")
    if not chosen.empty:
        out["ctx_err_chosen_mean"] = round(float(chosen.mean()), 6)
    signed = _col("ctx_err_signed_mean")
    if not signed.empty:
        out["ctx_err_signed_mean"] = round(float(signed.mean()), 6)
    # The truth is a lower bound when some coordinator's decisions are missing: a withdrawn copy
    # only a missing row could have named is in no export at all.
    dropped = 0
    for payload in (agents or {}).values():
        if isinstance(payload, dict):
            summary = (payload.get("instrumentation") or {}).get("delegation") or {}
            dropped += int(summary.get("records_dropped") or 0)
    out["ctx_err_truth_complete"] = bool(metrics_complete and dropped == 0)
    return out


def regret_correlation(regret_rows: List[dict], annotated: List[dict]) -> Dict[str, Any]:
    """Spearman correlation of regret against context error, over decisions with a choice."""
    key = lambda r: (str(r.get("agent_id")), str(r.get("job_id")), round(float(r["ts"]), 6))
    errors = {key(r): r.get("ctx_err_mean") for r in annotated
              if r.get("ctx_err_scored") == 1 and r.get("ts") is not None}
    pairs = [(errors[key(r)], r["regret"]) for r in regret_rows
             if not r.get("no_choice") and r.get("ts") is not None and key(r) in errors]
    if len(pairs) < 3:
        return {}
    corr = _spearman(pd.Series([p[0] for p in pairs], dtype=float),
                     pd.Series([p[1] for p in pairs], dtype=float))
    out = {"regret_ctx_err_pairs": len(pairs)}
    if corr is not None:
        out["regret_ctx_err_spearman"] = corr
    return out
