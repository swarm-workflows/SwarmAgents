#!/usr/bin/env python3
"""Post-hoc metric extraction over archived SwarmAgents run trees.

Walks one or more run-tree roots, finds every leaf run directory (identified by
``all_jobs.csv``), derives the experiment factors from the directory path, computes the
metric set from ``docs/FGCS_EVAL_PLAN.md`` section 7, and writes:

* ``runs_wide.csv``   -- one row per run, one column per metric
* ``runs_tidy.csv``   -- one row per (run, metric); convenient for seaborn/ggplot
* ``config_agg.csv``  -- per-configuration mean/std/n/ci95 across repeats
* ``decisions.csv``   -- one row per delegation decision (P0-4), when any run recorded them:
  the per-decision context age F6 needs, and the rows the oracle (P1-1) labels offline

Every paper figure should be regenerable from these three files, so numbers never get
hand-copied out of logs.

Factors come from three sources, later ones winning:

1. Path segments, scanned for known patterns (``hier-110``, ``run-mesh-30-500``,
   ``demo-mesh120-snowloc``, ``qwen3``, ``linucb``, ``run03``, ...).
2. ``collect_meta.json`` files anywhere on the path from a root down to the run; keys are
   merged top-down so a campaign-wide file can be overridden per run.
3. ``--label key=value`` on the command line.

Usage::

    python evaluation/collect.py --out analysis/banked \\
        --root ../swarmplus-evaluation-data/runs/pegasus-llm \\
        --root ../swarmplus-evaluation-data/runs/pegasus-workloads/snow-gossip
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

# Import path so the script runs from anywhere in the repo.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from plotting.stats import jains_fairness  # noqa: E402

# A run directory is anything holding this file.
RUN_MARKER = "all_jobs.csv"

#: Bids a run must have made before a 100% failure rate is called an outage rather than noise.
#: Mirrors `LlmAgent._LLM_DEAD_MIN_CALLS`, which raises the same alarm while the run is live.
LLM_DEAD_MIN_CALLS = 20

#: Fraction of a run's bidding a failure counter must cover before it may speak for the whole
#: fleet. Below this the rate is still reported — over the subset it actually measures — but no
#: verdict is drawn from it, because the two possible errors are symmetric and both wrong: a
#: counter present on a few healthy agents hides a fleet-wide outage, and a counter present
#: only on one broken agent condemns a fleet that was 29/30 fine.
LLM_COVERAGE_MIN = 0.9
META_FILE = "collect_meta.json"

TOPOLOGY_ALIASES = {
    "hier": "hierarchical",
    "hierarchical": "hierarchical",
    "mesh": "mesh",
    "ring": "ring",
    "star": "star",
}

# <topology><sep><agents>[<sep><jobs>], with or without a run-/demo- prefix and with or
# without separators: "hier-110", "run-mesh-30-500", "demo-mesh120-snowloc".
_TOPO_SCALE_RE = re.compile(
    r"(?:^|[-_])(hier|hierarchical|mesh|ring|star)[-_]?(\d+)(?:[-_](\d+))?(?:$|[-_])",
    re.IGNORECASE,
)
_RUN_IDX_RE = re.compile(r"^run[-_]?(\d+)$", re.IGNORECASE)

# Token -> (factor, value). Scanned against '-'/'_'-split path segments.
TOKEN_FACTORS: dict[str, tuple[str, Any]] = {
    # consensus engine
    "pbft": ("engine", "pbft"),
    "snow": ("engine", "snow"),
    "hybrid": ("engine", "hybrid"),
    "snowloc": ("engine", "snow"),
    "snowtuned": ("engine", "snow"),
    # delegation policy
    "linucb": ("policy", "linucb"),
    "lin_ts": ("policy", "lin_ts"),
    "lints": ("policy", "lin_ts"),
    "ucb1": ("policy", "ucb1"),
    "epsilon": ("policy", "epsilon_greedy"),
    "eps": ("policy", "epsilon_greedy"),
    "greedy": ("policy", "greedy"),
    "random": ("policy", "random"),
    "roundrobin": ("policy", "round_robin"),
    # LLM model family
    "qwen3": ("llm_model", "qwen3"),
    "qwen2.5": ("llm_model", "qwen2.5"),
    "gpt-oss": ("llm_model", "gpt-oss"),
    "gptoss": ("llm_model", "gpt-oss"),
    "glm-4.7": ("llm_model", "glm-4.7"),
    "glm": ("llm_model", "glm-4.7"),
    "gpt-4o-mini": ("llm_model", "gpt-4o-mini"),
    "llama3.1": ("llm_model", "llama3.1"),
}

# Multi-word tokens that only make sense as a whole segment.
SEGMENT_FACTORS: dict[str, tuple[str, Any]] = {
    "snowloc": ("snow_locality", True),
    "snowtuned": ("snow_tuned", True),
    "fixed": ("code_rev", "post-fix"),
    "baseline": ("arm", "baseline"),
}

FACTOR_COLUMNS = [
    "campaign", "topology", "agents", "jobs_planned", "engine", "policy",
    "llm_model", "snow_locality", "snow_tuned", "code_rev", "arm", "run",
]


# --------------------------------------------------------------------------- discovery


def discover_runs(root: Path) -> list[Path]:
    """Every directory under ``root`` (inclusive) that contains the run marker."""
    if (root / RUN_MARKER).is_file():
        return [root]
    return sorted(p.parent for p in root.rglob(RUN_MARKER))


def _split_tokens(segment: str) -> list[str]:
    return [t for t in re.split(r"[-_]", segment) if t]


def parse_factors(run_dir: Path, root: Path) -> dict[str, Any]:
    """Derive experiment factors from the path of ``run_dir`` relative to ``root``."""
    factors: dict[str, Any] = {"campaign": root.name}
    try:
        rel_parts = run_dir.relative_to(root).parts
    except ValueError:
        rel_parts = run_dir.parts

    for segment in rel_parts:
        match = _TOPO_SCALE_RE.search(segment)
        if match:
            factors["topology"] = TOPOLOGY_ALIASES[match.group(1).lower()]
            factors["agents"] = int(match.group(2))
            if match.group(3):
                factors["jobs_planned"] = int(match.group(3))

        run_match = _RUN_IDX_RE.match(segment)
        if run_match:
            factors["run"] = int(run_match.group(1))

        seg_lower = segment.lower()
        for token, (key, value) in SEGMENT_FACTORS.items():
            if token in seg_lower:
                factors[key] = value

        # Whole segment first: hyphenated names such as "gpt-oss" or "glm-4.7" must not be
        # split before lookup. Then fall back to individual tokens.
        candidates = [seg_lower] + _split_tokens(seg_lower)
        for token in candidates:
            if token in TOKEN_FACTORS:
                key, value = TOKEN_FACTORS[token]
                factors[key] = value

    factors.setdefault("run", 1)
    return factors


def load_meta_chain(run_dir: Path, root: Path) -> dict[str, Any]:
    """Merge every ``collect_meta.json`` from ``root`` down to ``run_dir`` (deepest wins)."""
    merged: dict[str, Any] = {}
    try:
        rel = run_dir.relative_to(root)
    except ValueError:
        return merged
    current = root
    for part in ("",) + rel.parts:
        current = current / part if part else current
        meta_path = current / META_FILE
        if meta_path.is_file():
            try:
                merged.update(json.loads(meta_path.read_text()))
            except (json.JSONDecodeError, OSError) as exc:
                print(f"  warn: unreadable {meta_path}: {exc}", file=sys.stderr)
    return merged


# ----------------------------------------------------------------------------- loading


def _numeric(df: pd.DataFrame, column: str) -> pd.Series:
    """Column as floats, or an all-NaN series when the column is absent."""
    if column not in df.columns:
        return pd.Series(np.nan, index=df.index, dtype=float)
    return pd.to_numeric(df[column], errors="coerce")


def _positive(series: pd.Series) -> pd.Series:
    return series[series.notna() & (series > 0)]


def read_jobs_csv(path: Path) -> pd.DataFrame | None:
    """Parse a jobs CSV. ``None`` means absent/unreadable; an EMPTY frame is a real result.

    A header-only jobs file is what a livelocked run produces -- zero jobs ever assigned.
    That is the single most important data point in a PBFT-at-scale comparison, so it must
    survive as a 0%-completion row rather than being dropped as "no data".
    """
    if not path.is_file():
        return None
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()  # zero-byte file: still a run that completed nothing
    except (pd.errors.ParserError, OSError) as exc:
        print(f"  warn: unreadable {path}: {exc}", file=sys.stderr)
        return None


def dedup_jobs(df: pd.DataFrame) -> pd.DataFrame:
    """One row per job_id, preferring the row that shows completion.

    Reselection and re-proposal leave several records per job; the completed one is the
    outcome of record. Sorting by completed_at puts it last, so keep='last' picks it.
    """
    if "job_id" not in df.columns:
        return df
    ordered = df.assign(_completed=_numeric(df, "completed_at").fillna(0.0))
    ordered = ordered.sort_values("_completed", kind="stable")
    return ordered.drop_duplicates(subset="job_id", keep="last").drop(columns="_completed")


# ----------------------------------------------------------------------------- metrics


def _dist(prefix: str, values: pd.Series) -> dict[str, float]:
    """mean/std/p50/p95/p99 for one distribution, NaN when empty."""
    clean = values.dropna()
    if clean.empty:
        return {f"{prefix}_{k}": float("nan")
                for k in ("mean", "std", "p50", "p95", "p99", "n")}
    return {
        f"{prefix}_mean": float(clean.mean()),
        f"{prefix}_std": float(clean.std(ddof=1)) if len(clean) > 1 else 0.0,
        f"{prefix}_p50": float(clean.quantile(0.50)),
        f"{prefix}_p95": float(clean.quantile(0.95)),
        f"{prefix}_p99": float(clean.quantile(0.99)),
        f"{prefix}_n": float(len(clean)),
    }


def selection_metrics(df: pd.DataFrame, prefix: str) -> dict[str, float]:
    """Selection time = assigned_at - selection_started_at, over jobs that got both."""
    started = _numeric(df, "selection_started_at")
    assigned = _numeric(df, "assigned_at")
    valid = started.notna() & assigned.notna() & (started > 0) & (assigned > 0)
    return _dist(prefix, (assigned - started)[valid])


# ------------------------------------------------------------------ per-agent instrumentation

def read_agent_metrics(run_dir: Path) -> dict[str, dict]:
    """`{agent_id: payload}` from a run's metrics.json, or {} when it is absent/unreadable.

    This is the file run_test.py only fills once every agent has reported for THIS run, so a
    run carrying metrics_shortfall.json yields aggregates over a subset — already flagged as
    `metrics_complete` on the same row.
    """
    path = run_dir / "metrics.json"
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text())
    except (OSError, ValueError):
        print(f"  warn: {run_dir.name}/metrics.json is unreadable", file=sys.stderr)
        return {}
    if not isinstance(payload, dict):
        return {}
    return {str(k): v for k, v in payload.items() if isinstance(v, dict)}


def read_agent_levels(run_dir: Path) -> dict[str, int]:
    """`{agent_id: level}` from all_agents.csv (JSON despite the name), or {}."""
    try:
        payload = json.loads((run_dir / "all_agents.csv").read_text())
    except (OSError, ValueError):
        return {}
    if not isinstance(payload, list):
        return {}
    return {str(a.get("agent_id")): int(a.get("level") or 0)
            for a in payload if isinstance(a, dict) and a.get("agent_id") is not None}


def read_run_meta(run_dir: Path) -> dict:
    """`run_meta.json`, or {} — it carries the declared agent type, which is what says whether
    an agent with no LLM block was analytic by design or simply unmeasured."""
    try:
        payload = json.loads((run_dir / "run_meta.json").read_text())
    except (OSError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def launched_agent_count(meta: dict | None) -> int:
    """How many agents the run launched, from run_meta.json (`agents` + `dynamic_agents`);
    0 when the run predates those fields. Ids are 1..N."""
    meta = meta or {}
    try:
        return int(meta.get("agents") or 0) + int(meta.get("dynamic_agents") or 0)
    except (TypeError, ValueError):
        return 0


def expected_llm_agent_ids(agents: dict[str, dict], meta: dict | None,
                           levels: dict[str, int], observed: set[str]) -> set[str] | None:
    """Ids of the agents that ran the LLM plane, or None if unknowable.

    The union of what the config DECLARED and what was OBSERVED. Observed matters because an
    agent that bid ran the LLM plane whatever the run was labelled — that is how a hierarchical
    run with LLM coordinators over analytic workers is handled, since its declared type names
    neither. Declared matters because an agent that reported nothing cannot be observed at all,
    and those are exactly the unmeasured ones the coverage gate exists to catch.

    Per level, because the roles differ by level: `--agent-type` sets the leaves and
    `--hierarchical-level1-agent-type` sets the coordinators, and a run with LLM workers under
    analytic coordinators (or the reverse) is supported. Treating every agent as LLM because
    the run declared `agent_type: llm` rejects such a run outright — 27 of 30 agents reporting
    is 0.90 coverage, and a coordinator tier that is analytic on purpose looks like a hole.

    None when the run predates `hierarchical_level1_agent_type` in run_meta.json and is
    hierarchical, because the coordinators' role is then genuinely unknown; the caller falls
    back to observed activity rather than guessing in either direction.
    """
    meta = meta or {}
    leaf_type = meta.get("agent_type")
    if leaf_type is None:
        return None
    # The population is every agent the run LAUNCHED, not every agent that reported. Until
    # 2026-09-18 this iterated the payload dict, so an agent that wrote no metrics.json was
    # not in the denominator at all — 10 silent agents out of 30 read as coverage 1.00, the
    # exact blindness the docstring says this set exists to catch. run_meta.json records the
    # launched counts (ids are 1..agents+dynamic); without them the payloads are all there is.
    population = set(map(str, agents))
    launched = launched_agent_count(meta)
    if launched > 0:
        population |= {str(i) for i in range(1, launched + 1)}
    hierarchical = (meta.get("topology") == "hierarchical")
    if not hierarchical:
        return (population if leaf_type == "llm" else set()) | observed

    coord_type = meta.get("hierarchical_level1_agent_type")
    if coord_type is None or not levels:
        # Pre-dates the field, so the coordinators' role is genuinely unknown. Fall back to
        # what was observed rather than guessing: guessing "llm" rejects a supported run and
        # guessing "resource" hides unmeasured agents.
        return None
    declared = set()
    for agent_id in sorted(population, key=lambda a: (len(a), a)):
        level = levels.get(str(agent_id))
        if level is None:
            # A launched agent whose level nobody knows — usually one that reported nothing,
            # since levels come from the payloads. Its role cannot be attributed, so no
            # verdict; `metrics_complete` on the same row says why.
            return None
        if (leaf_type if level == 0 else coord_type) == "llm":
            declared.add(str(agent_id))
    return declared | observed


def selection_by_tier(agents: dict[str, dict], levels: dict[str, int]) -> dict[str, Any]:
    """Proposal fan-out per tier, from the per-agent `instrumentation.selection` blocks.

    Fleet **sums** per tier, for the same reason the rest of this function sums: a mean over
    agents divides by how many reported, which varies with metrics completeness.

    The tier of an agent is taken from the counter block itself, which the agent stamped from
    its own topology, and only then from all_agents.csv. An agent whose tier cannot be
    established at all is counted in `sel_unattributed_agents` rather than folded into level
    0 — putting a coordinator's proposals in the leaf tier is precisely the confusion the
    measurement exists to remove.

    The denominator (jobs at that tier) is not here: it comes from the per-level job CSVs,
    which only `run_metrics` reads. This returns the numerators.
    """
    out: dict[str, Any] = {}
    per_level: dict[int, dict[str, float]] = {}
    unattributed = 0
    for agent_id, payload in agents.items():
        instr = payload.get("instrumentation") or {}
        block = instr.get("selection")
        if not isinstance(block, dict) or not block.get("proposals_issued"):
            continue
        level = block.get("level")
        if level is None:
            level = levels.get(str(agent_id))
        if level is None:
            unattributed += 1
            continue
        acc = per_level.setdefault(int(level), {
            "agents": 0, "proposals": 0, "jobs": 0, "reproposals": 0,
            "width_sum": 0.0, "width_n": 0, "width_max": 0})
        acc["agents"] += 1
        acc["proposals"] += int(block.get("proposals_issued") or 0)
        acc["jobs"] += int(block.get("jobs_proposed") or 0)
        acc["reproposals"] += int(block.get("reproposals") or 0)
        width = block.get("matrix_assignees_mean")
        if width is not None:
            acc["width_sum"] += float(width)
            acc["width_n"] += 1
        width_max = block.get("matrix_assignees_max")
        if width_max is not None:
            acc["width_max"] = max(acc["width_max"], int(width_max))
    for level, acc in sorted(per_level.items()):
        out[f"sel_agents_l{level}"] = acc["agents"]
        out[f"sel_proposals_l{level}"] = acc["proposals"]
        out[f"sel_proposer_pairs_l{level}"] = acc["jobs"]
        out[f"sel_reproposals_l{level}"] = acc["reproposals"]
        if acc["width_n"]:
            out[f"sel_matrix_width_l{level}"] = round(acc["width_sum"] / acc["width_n"], 4)
            out[f"sel_matrix_width_max_l{level}"] = acc["width_max"]
    if unattributed:
        out["sel_unattributed_agents"] = unattributed
    return out


def instrumentation_metrics(agents: dict[str, dict], meta: dict | None = None,
                            levels: dict[str, int] | None = None) -> dict[str, Any]:
    """P0-4 columns: message cost, finalization, delegation context age, LLM token cost.

    Per-agent counters are **summed**, not averaged: the quantity the message-complexity
    figure predicts is fleet total per job, and a mean over agents silently divides by a
    denominator (how many agents reported) that varies with metrics completeness.

    The context-age distribution is recomputed from the per-decision rows rather than
    averaging the per-agent summaries — a coordinator that made 400 decisions and one that
    made 4 would otherwise weigh the same.
    """
    out: dict[str, Any] = {}
    if not agents:
        return out

    sent = sent_bytes = recv = recv_bytes = dropped = 0
    finalized = abandoned = 0
    protocols: set[str] = set()
    llm_calls = llm_failures = llm_in = llm_out = 0
    bid_jobs = bid_calls = designated = forced = claimed_jobs = 0
    bid_failures = bid_calls_paired = llm_calls_paired = 0
    have_messages = have_consensus = have_llm = False
    have_bidding = designate_on = have_bid_failures = have_llm_failures = False
    # Counted per AGENT, not per call: call coverage is blind to an agent that reported no
    # instrumentation at all, because such an agent contributes nothing to either side of the
    # ratio. A mixed-revision fleet where a third of the agents carry the new blocks reads as
    # 1.00 call coverage while two thirds of the bidding was never measured.
    levels = levels or {}
    agents_with_llm = agents_with_failure_counter = 0
    reported_failure_ids: set[str] = set()
    observed_llm_ids: set[str] = set()
    finalize_s: list[float] = []
    rounds: list[float] = []

    # Keyed on the MAP KEY, which is the agent id `read_agent_metrics` indexed by, not on
    # `payload["id"]`. The two normally agree, but the key is the one the rest of this
    # function counts with, and an id field that disagrees with it would silently shrink every
    # coverage intersection.
    for agent_id, payload in agents.items():
        agent_id = str(agent_id)
        instr = payload.get("instrumentation") or {}
        llm_block = instr.get("llm") if isinstance(instr.get("llm"), dict) else {}
        if llm_block:
            agents_with_llm += 1
            observed_llm_ids.add(agent_id)
            if any(isinstance(v, dict) and ("failures" in v or "bid_failures" in v)
                   for v in llm_block.values()):
                agents_with_failure_counter += 1
                reported_failure_ids.add(agent_id)
        messages = instr.get("messages")
        if isinstance(messages, dict):
            have_messages = True
            sent += int(messages.get("sent_msgs", 0) or 0)
            sent_bytes += int(messages.get("sent_bytes", 0) or 0)
            recv += int(messages.get("recv_msgs", 0) or 0)
            recv_bytes += int(messages.get("recv_bytes", 0) or 0)
            dropped += int(messages.get("dropped_msgs", 0) or 0)
        consensus = instr.get("consensus")
        if isinstance(consensus, dict):
            have_consensus = True
            protocols.add(str(consensus.get("protocol", "")))
            finalized += int(consensus.get("finalized", 0) or 0)
            abandoned += int(consensus.get("abandoned", 0) or 0)
            # Per-agent p50s, weighted equally on purpose: each is one agent's typical
            # finalize, and the figure compares agents' experience across protocols.
            for key, sink in (("finalize_s_p50", finalize_s), ("rounds_p50", rounds)):
                value = consensus.get(key)
                if value is not None:
                    sink.append(float(value))
        llm = instr.get("llm")
        if isinstance(llm, dict):
            for key, site in llm.items():
                if not isinstance(site, dict):
                    continue
                if key == "bidding":       # P0-8 counters, not a per-call-site usage block
                    have_bidding = True
                    bid_jobs += int(site.get("bid_jobs", 0) or 0)
                    calls_here = int(site.get("bid_calls", 0) or 0)
                    bid_calls += calls_here
                    # Presence is tracked, not defaulted. `bid_failures` was added after the
                    # first payloads that carried a `bidding` block, and reading its absence
                    # as 0 makes a run where EVERY bid failed report a 0.0 failure rate — a
                    # perfectly healthy plane. Absent is unknown, never zero.
                    #
                    # The denominator is PAIRED with the numerator: only calls from agents
                    # that also reported their failures. A fleet on mixed revisions — a
                    # partial push, dynamic agents from an older image — would otherwise
                    # divide one agent's failures by thirty agents' calls. With 1 of 30
                    # reporting, a fleet in which every single bid fails reads as a 3.3%
                    # failure rate.
                    if "bid_failures" in site:
                        have_bid_failures = True
                        bid_failures += int(site.get("bid_failures", 0) or 0)
                        bid_calls_paired += calls_here
                    designated += int(site.get("designate_mine", 0) or 0)
                    forced += int(site.get("designate_forced", 0) or 0)
                    # The agent already resolved the mine/forced overlap into a union; the
                    # collector must use that, not re-derive it by adding the two back up.
                    claimed_jobs += int(site.get("designate_claimed", 0) or 0)
                    if site.get("designate_bidder"):
                        designate_on = True
                    continue
                have_llm = True
                site_calls = int(site.get("calls", 0) or 0)
                llm_calls += site_calls
                # Paired for the same reason as the bidding counters above.
                if "failures" in site:
                    have_llm_failures = True
                    llm_failures += int(site.get("failures", 0) or 0)
                    llm_calls_paired += site_calls
                llm_in += int(site.get("input_tokens", 0) or 0)
                llm_out += int(site.get("output_tokens", 0) or 0)

    out["agents_reporting"] = len(agents)
    if have_messages:
        out["msgs_sent"] = sent
        out["msgs_recv"] = recv
        out["msg_bytes_sent"] = sent_bytes
        out["msg_bytes_recv"] = recv_bytes
        out["msgs_dropped"] = dropped
    if have_consensus:
        out["consensus_protocol"] = "/".join(sorted(p for p in protocols if p)) or float("nan")
        out["consensus_finalized"] = finalized
        out["consensus_abandoned"] = abandoned
        out.update(_dist("finalize_s", pd.Series(finalize_s, dtype=float)))
        out.update(_dist("rounds", pd.Series(rounds, dtype=float)))
    if have_llm:
        out["llm_calls"] = llm_calls
        out["llm_failures"] = llm_failures
        out["llm_input_tokens"] = llm_in
        out["llm_output_tokens"] = llm_out
        # Derived HERE and not only from the `bidding` block, because that block exists only
        # in payloads written after P0-8. Every LlmAgent run has this one, so a run archived
        # before then showed `llm_calls` and `llm_failures` as two unremarkable integers among
        # eighty columns with nothing computing the ratio — a wholly dead LLM plane reading as
        # an ordinary row.
        if have_llm_failures:
            out["llm_failure_rate"] = (round(llm_failures / llm_calls_paired, 6)
                                       if llm_calls_paired else float("nan"))
    if have_bidding:
        # P0-8. `llm_bid_jobs` is the fleet sum of distinct jobs each agent paid an LLM bid
        # for; over the jobs in the run it gives bidders-per-job, the ~3.7 that designated
        # bidding exists to cut to ~1. The denominator is added by the caller, which knows how
        # many jobs the run actually had.
        out["designate_bidder"] = designate_on
        out["llm_bid_jobs"] = bid_jobs
        out["llm_bid_calls"] = bid_calls
        # Read this before anything else on an LLM row. A run whose bids all failed completes
        # normally and looks healthy in every other column, but it is neither an LLM-plane
        # measurement nor a clean analytic baseline — a failed bid returns in ~0s, which is
        # the race-to-propose regime. Measured on the slice 2026-09-14: 924 of 924 bids
        # returned 403 for a model the gateway key cannot access, and the run completed
        # 197/197 jobs. Omitted entirely when the payload never carried the counter, rather
        # than reported as 0.
        if have_bid_failures:
            out["llm_bid_failures"] = bid_failures
            out["llm_bid_failure_rate"] = (round(bid_failures / bid_calls_paired, 6)
                                           if bid_calls_paired else float("nan"))
            # How much of the fleet's bidding the rate above actually covers. Below 1.0 the
            # rate describes a subset of the agents, so a healthy-looking value says nothing
            # about the rest.
            out["llm_failure_coverage"] = (round(bid_calls_paired / bid_calls, 6) if bid_calls
                                           else float("nan"))
        if designate_on:
            out["designate_designated"] = designated
            out["designate_forced"] = forced
            out["designate_claimed"] = claimed_jobs
            # A run whose deadline fires on most jobs has designation in name only: every
            # agent bids anyway and bidders-per-job returns to its undesignated value. Without
            # this the two runs are indistinguishable in every artefact.
            #
            # Denominator is the agents' own `designate_claimed` — the union of the jobs they
            # were designated and the jobs they took on the deadline. Adding `designated` and
            # `forced` back together would double-count every job that took both routes across
            # reselection rounds and dilute the share in the flattering direction.
            out["designate_forced_share"] = (round(forced / claimed_jobs, 6) if claimed_jobs
                                             else float("nan"))

    # One flag, computed from whichever failure signal the payload actually carries, so the
    # answer does not depend on how old the run is. A run that trips it is not an LLM-plane
    # measurement AND not a clean analytic baseline: a failed bid returns in ~0s, which is the
    # race-to-propose regime. The minimum call count keeps a handful of bids from tripping it.
    #
    # Only signals that EXIST are consulted, and when none does the flag is omitted and
    # `llm_plane_unchecked` says so. "We checked and it is fine" and "we could not check" must
    # not be the same value: reading a missing counter as zero is what let a run where every
    # one of 924 bids failed report a 0.0 failure rate.
    # (covered calls, failures, total calls) for each signal the payload actually carries.
    signals = []
    if have_bid_failures:
        signals.append((bid_calls_paired, bid_failures, bid_calls))
    if have_llm_failures:
        signals.append((llm_calls_paired, llm_failures, llm_calls))
    # Only signals that cover enough of the fleet may decide. A verdict drawn from a thin
    # slice is wrong in both directions and the two errors are symmetric: a counter that
    # happens to sit on a few healthy agents hides a fleet-wide outage, and one that sits on a
    # single broken agent condemns a fleet that was 29 of 30 fine.
    # How many agents that ran the LLM plane actually reported a failure counter. Which
    # agents those are is a PER-LEVEL question: `--agent-type llm
    # --hierarchical-level1-agent-type resource` is a supported mixed-role run whose
    # coordinators are analytic by design, and counting them as unmeasured blocks a verdict
    # on a run that is perfectly measurable.
    expected_ids = expected_llm_agent_ids(agents, meta, levels, observed_llm_ids)
    if expected_ids is None:
        # Roles cannot be attributed. Falling back to the observed set here is NOT safe: it
        # makes every agent that reported nothing vanish from the denominator, which is the
        # exact blindness this gate exists to prevent — 10 instrumented agents out of 30 would
        # read as 1.00 coverage. The observed set is only trustworthy when there is nothing
        # unattributed left over, i.e. every reporting agent carried an LLM block.
        # ... AND only when every launched agent is among the reporters. An agent that died
        # at startup is in neither the payloads nor all_agents.csv, so it has no level and lands
        # here; if the survivors all carried LLM blocks this branch used to certify the fleet
        # at 1.00 — three LLM coordinators dead on hosts without the API key, 27 LLM leaves
        # reporting, coverage 1.00. That is the startup-death case the run-level completeness
        # gate exists for, and this gate must not contradict it.
        if agents_with_llm == len(agents) and len(agents) >= launched_agent_count(meta):
            expected_llm_agents = agents_with_llm
            covered = agents_with_failure_counter
        else:
            # Some agents are unaccounted for and we cannot say whether they should have bid.
            # Unknown, so no verdict — an honest "unchecked" beats a verdict over a subset.
            expected_llm_agents = 0
            covered = 0
    else:
        expected_llm_agents = len(expected_ids)
        covered = len(reported_failure_ids & expected_ids)
    agent_coverage = (covered / expected_llm_agents
                      if expected_llm_agents else float("nan"))
    if expected_llm_agents:
        out["llm_failure_agent_coverage"] = round(agent_coverage, 6)
    elif have_llm or have_bidding:
        # The roles could not be attributed and agents are unaccounted for. Report it as 0
        # rather than omitting it: an absent coverage column is what a reader skims past.
        out["llm_failure_agent_coverage"] = 0.0

    # BOTH coverages must hold. Call coverage catches an agent that bid without reporting its
    # failures; agent coverage catches an agent that reported nothing at all.
    agent_ok = bool(expected_llm_agents) and agent_coverage >= LLM_COVERAGE_MIN
    deciding = [(c, f) for c, f, total in signals
                if total and c / total >= LLM_COVERAGE_MIN and c and agent_ok]
    if not deciding:
        # `expected_llm_agents` is in this condition on purpose: a run whose DECLARED LLM tier
        # reported nothing at all carries no LLM block anywhere, so `have_llm`/`have_bidding`
        # are both False and, gated on those alone, the collector said nothing — an entirely
        # silent LLM coordinator tier read as a clean analytic run rather than as unchecked.
        if have_bidding or have_llm or expected_llm_agents:
            out["llm_plane_unchecked"] = True
    else:
        out["llm_plane_unchecked"] = False
        out["llm_plane_dead"] = any(
            calls >= LLM_DEAD_MIN_CALLS and failures >= calls for calls, failures in deciding)

    out.update(selection_by_tier(agents, levels))

    rows = decision_rows(agents)
    if rows:
        frame = pd.DataFrame(rows)
        out["delegations"] = int(len(frame))
        # The headline series: both terms on the coordinator's own clock, so inter-host
        # offset cancels. This is what the staleness figure plots.
        out.update(_dist("ctx_age", _numeric(frame, "ctx_age_mean")))
        out.update(_dist("ctx_age_chosen", _numeric(frame, "ctx_age_chosen")))
        # End-to-end, including propagation from the child, but straddling two clocks.
        out.update(_dist("ctx_age_remote", _numeric(frame, "ctx_age_remote_mean")))
        out.update(_dist("decide_s", _numeric(frame, "decide_s")))
        # Validity columns, not results — and an ABSENT validity column is not a clean one.
        # `_numeric` returns all-NaN for a missing column and NaN sums to 0, so decision rows
        # written without these fields (an older agent revision, an instrumentation gap) read
        # as "0 unknown groups, 0 skewed ages", i.e. as a run whose staleness axis is
        # trustworthy. Unknown is recorded as None, which is what "we could not check" looks
        # like in the CSV; a reader that sees 0 assumes the check ran.
        def _validity_sum(column: str):
            col = _numeric(frame, column)
            return int(col.sum()) if col.notna().any() else None
        out["ctx_unknown_groups"] = _validity_sum("ctx_age_unknown")
        # Non-zero skew means child and coordinator clocks disagree, so ctx_age_remote_* from
        # this run is biased by up to ctx_skew_max_s; the headline ctx_age_* is taken on one
        # clock and is unaffected. Reported as a MAX, not a sum: a count cannot separate a
        # 1 ms artefact from a 1.1 s free-running clock.
        out["ctx_skewed_ages"] = _validity_sum("ctx_age_skewed")
        skew_max = _numeric(frame, "ctx_age_skew_max_s").max()
        out["ctx_skew_max_s"] = float(skew_max) if pd.notna(skew_max) else None
        for policy, count in frame["policy"].value_counts().items():
            out[f"delegations_{policy}"] = int(count)
    return out


def regret_metrics(run_dir: Path) -> dict[str, Any]:
    """Delegation regret against the offline optimum (P1-1), when the run can be scored.

    Silently absent rather than NaN for a run with no injected failure profile: with every
    group equally good there is no optimum to be short of, and a zero-filled regret column
    would read as a perfect policy instead of as no measurement. `evaluation/oracle.py`
    refuses those runs for the same reason, and this mirrors it.
    """
    try:
        from evaluation.oracle import OracleError, load_run, score_run
    except ImportError:
        return {}
    try:
        rows, summary = score_run(load_run(run_dir))
    except OracleError:
        return {}
    except Exception as exc:  # a broken run must not take the whole collection down
        print(f"  warn: regret scoring failed for {run_dir.name}: {exc}", file=sys.stderr)
        return {}

    out = {
        "regret_total": summary["regret_total"],
        "regret_mean": summary["regret_mean"],
        "routing_accuracy": summary["routing_accuracy"],
        "regret_decisions_scored": summary["decisions_scored"],
        "regret_aggregate": summary["regret_aggregate"],
    }
    # The staleness figure (F6) is regret against context age, so the correlation between
    # them belongs on the same row as both — otherwise every plot of it starts by rejoining
    # two files.
    paired = [(r["ctx_age_mean"], r["regret"]) for r in rows
              if r.get("ctx_age_mean") is not None]
    if len(paired) > 2:
        ages = pd.Series([p[0] for p in paired], dtype=float)
        regrets = pd.Series([p[1] for p in paired], dtype=float)
        if ages.std(ddof=1) > 0 and regrets.std(ddof=1) > 0:
            out["regret_ctx_age_corr"] = round(float(ages.corr(regrets)), 6)
    return out


def decision_rows(agents: dict[str, dict]) -> list[dict[str, Any]]:
    """Flatten every agent's `delegation_decisions` into rows tagged with the agent id."""
    rows: list[dict[str, Any]] = []
    for agent_id, payload in agents.items():
        for record in payload.get("delegation_decisions") or []:
            if not isinstance(record, dict):
                continue
            row = dict(record)
            row["agent_id"] = agent_id
            row["candidates"] = " ".join(str(g) for g in row.get("candidates") or [])
            row["selected"] = " ".join(str(g) for g in row.get("selected") or [])
            rows.append(row)
    return rows


def run_metrics(run_dir: Path, expected_jobs: int | None) -> dict[str, Any]:
    """Compute the full metric set for one run directory."""
    raw = read_jobs_csv(run_dir / RUN_MARKER)
    if raw is None:
        return {}
    if raw.empty:
        print(f"  note: {run_dir.name} assigned zero jobs -- recording as 0% completion",
              file=sys.stderr)

    jobs = dedup_jobs(raw)
    submitted = _numeric(jobs, "submitted_at")
    completed_at = _numeric(jobs, "completed_at")
    started_at = _numeric(jobs, "started_at")
    assigned_at = _numeric(jobs, "assigned_at")
    exit_status = _numeric(jobs, "exit_status")

    is_complete = completed_at.notna() & (completed_at > 0)
    n_unique = int(len(jobs))
    n_completed = int(is_complete.sum())

    # Per-agent metrics (load, utilisation, fairness, MAB and delegation counts) come from
    # metrics.json, which run_test.py only fills once every agent has reported for THIS run.
    # A shortfall file means some agents never did, so any per-agent aggregate from this cell
    # is computed over a subset — recorded as a column rather than left for a reader to notice.
    shortfall_path = run_dir / "metrics_shortfall.json"
    metrics_complete = not shortfall_path.exists()
    missing_agents = 0
    if not metrics_complete:
        try:
            missing_agents = len(json.loads(shortfall_path.read_text()).get("missing_agents", []))
        except (OSError, ValueError):
            missing_agents = -1  # present but unreadable
        print(f"  WARNING: {run_dir.name} has metrics_shortfall.json "
              f"({missing_agents} agents silent) -- per-agent aggregates are partial",
              file=sys.stderr)

    metrics: dict[str, Any] = {
        "metrics_complete": metrics_complete,
        "agents_missing_metrics": missing_agents,
        "job_records": int(len(raw)),
        "jobs_seen": n_unique,
        "jobs_completed": n_completed,
        # Reselection/re-proposal churn: >1.0 means jobs were scheduled more than once.
        "reselection_multiplier": round(len(raw) / n_unique, 4) if n_unique else float("nan"),
        "exit_failures": int((exit_status.notna() & (exit_status != 0) & is_complete).sum()),
    }

    # Completion% is only meaningful against the number of jobs *submitted*. Falling back
    # to the number of jobs the run happened to see hides exactly the failure we care
    # about: a run that assigned 206 of 5000 jobs would otherwise report 100%.
    if expected_jobs:
        metrics["jobs_expected"] = expected_jobs
        metrics["completion_basis"] = "declared"
        metrics["completion_pct"] = round(100.0 * n_completed / expected_jobs, 4)
    else:
        metrics["jobs_expected"] = float("nan")
        metrics["completion_basis"] = "unknown"
        metrics["completion_pct"] = float("nan")
    # Always available: completion among the jobs this run actually touched.
    metrics["completion_pct_of_seen"] = (
        round(100.0 * n_completed / n_unique, 4) if n_unique else float("nan")
    )

    # Latency decomposition. Each stage is only defined for jobs that reached it.
    # NOTE: 'selection' and 'sched_latency' are DIFFERENT quantities and both have been
    # called "selection time" in past write-ups. selection = assigned_at -
    # selection_started_at (consensus/selection only); sched_latency = the agent-reported
    # scheduling_latency column (includes queueing ahead of selection). Pick one
    # definition per figure and say which.
    metrics.update(selection_metrics(jobs, "selection"))
    metrics.update(_dist("sched_latency", _positive(_numeric(jobs, "scheduling_latency"))))
    metrics.update(_dist("reasoning", _positive(_numeric(jobs, "reasoning_time"))))

    complete_rows = is_complete & submitted.notna() & (submitted > 0)
    metrics.update(_dist("job_latency", (completed_at - submitted)[complete_rows]))

    wait_valid = started_at.notna() & assigned_at.notna() & (started_at > 0) & (assigned_at > 0)
    metrics.update(_dist("wait", (started_at - assigned_at)[wait_valid]))

    exec_valid = is_complete & started_at.notna() & (started_at > 0)
    metrics.update(_dist("exec", (completed_at - started_at)[exec_valid]))

    # Makespan and throughput over the completed set.
    if n_completed and complete_rows.any():
        first_submit = float(submitted[complete_rows].min())
        last_complete = float(completed_at[complete_rows].max())
        makespan = last_complete - first_submit
        metrics["makespan_s"] = round(makespan, 4)
        metrics["throughput_jobs_per_s"] = (
            round(n_completed / makespan, 4) if makespan > 0 else float("nan")
        )
    else:
        metrics["makespan_s"] = float("nan")
        metrics["throughput_jobs_per_s"] = float("nan")

    # Load balance across executing agents (Jain's over completed jobs per leader).
    if "leader_id" in jobs.columns and n_completed:
        per_leader = jobs.loc[is_complete, "leader_id"].value_counts()
        metrics["active_leaders"] = int(len(per_leader))
        metrics["fairness_jain"] = round(float(jains_fairness(per_leader.to_numpy(dtype=float))), 4)
    else:
        metrics["active_leaders"] = 0
        metrics["fairness_jain"] = float("nan")

    # Per-level selection time -- hierarchical runs report level-0 as the headline number.
    for level in (0, 1, 2):
        level_df = read_jobs_csv(run_dir / f"level{level}_jobs.csv")
        if level_df is None:
            continue
        level_jobs = dedup_jobs(level_df)
        metrics[f"l{level}_jobs"] = int(len(level_jobs))
        metrics.update(selection_metrics(level_jobs, f"l{level}_selection"))

    # Jobs still queued at teardown.
    for level_name, filename in (
        ("pending", "pending_jobs.csv"),
        ("pending_l0", "pending_level0_jobs.csv"),
        ("pending_l1", "pending_level1_jobs.csv"),
    ):
        pending_df = read_jobs_csv(run_dir / filename)
        # None means the file is absent or unreadable, which is not "no backlog": a run that
        # died before dumping its pending set would otherwise read as one that drained it.
        metrics[f"{level_name}_count"] = None if pending_df is None else int(len(pending_df))

    # P0-4 instrumentation, from the per-agent payloads rather than the job CSVs.
    metrics.update(instrumentation_metrics(
        read_agent_metrics(run_dir), read_run_meta(run_dir), read_agent_levels(run_dir)))
    # P0-8: bidders per job needs the fleet's bid count over the run's job count, and only
    # this scope knows the latter. Computed over jobs the run actually SAW, not the declared
    # count: a job never distributed was never available to bid on, and including it would
    # make a stalled run look like a well-partitioned one.
    if metrics.get("llm_bid_jobs") is not None and n_unique:
        metrics["bidders_per_job"] = round(metrics["llm_bid_jobs"] / n_unique, 6)
    # Proposal fan-out per tier. The numerators are fleet sums from the agent payloads; the
    # denominator is the jobs that reached that tier, which only this scope knows.
    #
    # `proposers_per_job` is the quantity E5 needs: how many DISTINCT agents proposed a given
    # job at that tier. 1.0 is "the tier ran one consensus decision per job"; the coordinator
    # count is "every coordinator holding the job proposed itself", which is what the
    # self-only cost matrix produces and what a messages-per-job curve cannot tell from
    # PBFT's own cost. `proposals_per_job` adds re-proposals on top, so the gap between the
    # two is reselection churn and nothing to do with tier width.
    #
    # Level 0 falls back to the run's distinct job count when no level0_jobs.csv was written
    # (flat runs before that file existed); a higher tier has no such fallback, because
    # all_jobs.csv is every tier at once and dividing by it would understate the fan-out by
    # exactly the factor being measured.
    for level in (0, 1, 2):
        pairs = metrics.get(f"sel_proposer_pairs_l{level}")
        if pairs is None:
            continue
        denom = metrics.get(f"l{level}_jobs")
        if not denom and level == 0:
            denom = n_unique
        if not denom:
            continue
        metrics[f"proposers_per_job_l{level}"] = round(pairs / denom, 6)
        metrics[f"proposals_per_job_l{level}"] = round(
            metrics.get(f"sel_proposals_l{level}", 0) / denom, 6)
    # P1-1 regret, when the run archived the failure profile it was scored against.
    metrics.update(regret_metrics(run_dir))

    return metrics


# --------------------------------------------------------------------------- aggregate


def aggregate(wide: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    """Mean/std/n/ci95 per configuration across repeats."""
    metric_cols = [
        c for c in wide.columns
        if c not in set(FACTOR_COLUMNS) | {"run_dir", "root"}
        and pd.api.types.is_numeric_dtype(wide[c])
    ]
    if not group_cols or not metric_cols:
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []
    for keys, group in wide.groupby(group_cols, dropna=False):
        key_tuple = keys if isinstance(keys, tuple) else (keys,)
        row: dict[str, Any] = dict(zip(group_cols, key_tuple))
        row["n_runs"] = int(len(group))
        for metric in metric_cols:
            values = group[metric].dropna()
            n = len(values)
            mean = float(values.mean()) if n else float("nan")
            std = float(values.std(ddof=1)) if n > 1 else 0.0
            row[f"{metric}_mean"] = mean
            row[f"{metric}_std"] = std
            # 95% CI half-width; 1.96 is fine given >=5 repeats per the stats protocol.
            row[f"{metric}_ci95"] = 1.96 * std / math.sqrt(n) if n > 1 else float("nan")
        rows.append(row)
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------------- CLI


def parse_labels(pairs: list[str]) -> dict[str, str]:
    labels = {}
    for pair in pairs:
        if "=" not in pair:
            raise SystemExit(f"--label expects key=value, got {pair!r}")
        key, value = pair.split("=", 1)
        labels[key.strip()] = value.strip()
    return labels


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Extract per-run metrics from archived SwarmAgents run trees.")
    ap.add_argument("--root", action="append", required=True, type=Path,
                    help="Run-tree root to walk (repeatable).")
    ap.add_argument("--out", required=True, type=Path,
                    help="Output directory for runs_wide.csv / runs_tidy.csv / config_agg.csv.")
    ap.add_argument("--expected-jobs", type=int, default=None,
                    help="Denominator for completion%%. Defaults to jobs_planned from the "
                         "path, else the number of distinct jobs seen in the run.")
    ap.add_argument("--label", action="append", default=[], metavar="KEY=VALUE",
                    help="Extra factor applied to every run in this invocation (repeatable).")
    ap.add_argument("--group-by", default=None,
                    help="Comma-separated factors for config_agg.csv. Default: every factor "
                         "that varies across the collected runs, excluding 'run'.")
    args = ap.parse_args()

    labels = parse_labels(args.label)
    records: list[dict[str, Any]] = []
    decisions: list[dict[str, Any]] = []

    for root in args.root:
        root = root.expanduser().resolve()
        if not root.is_dir():
            print(f"skip: {root} is not a directory", file=sys.stderr)
            continue
        run_dirs = discover_runs(root)
        print(f"{root}: {len(run_dirs)} run(s)")
        for run_dir in run_dirs:
            factors = parse_factors(run_dir, root)
            factors.update(load_meta_chain(run_dir, root))
            factors.update(labels)

            expected = args.expected_jobs or factors.get("jobs_planned")
            metrics = run_metrics(run_dir, expected)
            if not metrics:
                print(f"  warn: no usable job data in {run_dir}", file=sys.stderr)
                continue

            if metrics.get("llm_plane_unchecked"):
                cov = metrics.get("llm_failure_coverage")
                detail = ("carries no failure counter" if not cov or cov != cov else
                          f"has a failure counter covering only {cov:.1%} of its bidding")
                print(f"  WARNING: {run_dir.name} — this run used the LLM plane but {detail}, "
                      f"so whether its bids worked CANNOT be determined for the fleet. Any "
                      f"rate on this row describes only the covered agents; do not read it, "
                      f"or its absence, as health.", file=sys.stderr)
            if metrics.get("llm_plane_dead"):
                print(f"  WARNING: {run_dir.name} — every LLM call failed. This run is 100% "
                      f"analytic fallback: not an LLM-plane measurement, and not a clean "
                      f"analytic baseline either (a failed bid returns in ~0s, which is the "
                      f"race-to-propose regime). Check the model name against the provider "
                      f"key's allowed list.", file=sys.stderr)

            record = {"root": str(root), "run_dir": os.path.relpath(run_dir, root)}
            record.update(factors)
            record.update(metrics)
            records.append(record)

            # Per-decision rows for F6 and for the oracle's offline labelling (P1-1). Carried
            # here rather than derived later because the factors that identify the run live
            # in this loop and the rows are useless without them.
            for row in decision_rows(read_agent_metrics(run_dir)):
                row.update({"root": str(root),
                            "run_dir": os.path.relpath(run_dir, root)})
                row.update(factors)
                decisions.append(row)

    if not records:
        print("No runs collected.", file=sys.stderr)
        return 1

    wide = pd.DataFrame(records)
    ordered = (["root", "run_dir"]
               + [c for c in FACTOR_COLUMNS if c in wide.columns]
               + [c for c in wide.columns
                  if c not in set(FACTOR_COLUMNS) | {"root", "run_dir"}])
    wide = wide[ordered]

    args.out.mkdir(parents=True, exist_ok=True)
    wide.to_csv(args.out / "runs_wide.csv", index=False)

    id_cols = ["root", "run_dir"] + [c for c in FACTOR_COLUMNS if c in wide.columns]
    tidy = wide.melt(id_vars=id_cols, var_name="metric", value_name="value")
    tidy.to_csv(args.out / "runs_tidy.csv", index=False)

    if decisions:
        decision_frame = pd.DataFrame(decisions)
        lead = [c for c in ("root", "run_dir", "agent_id", "ts", "job_id")
                if c in decision_frame.columns]
        decision_frame = decision_frame[
            lead + [c for c in decision_frame.columns if c not in lead]]
        decision_frame.to_csv(args.out / "decisions.csv", index=False)

    if args.group_by:
        group_cols = [c.strip() for c in args.group_by.split(",") if c.strip() in wide.columns]
    else:
        # Everything that actually varies -- avoids a per-run "aggregate" of 1.
        group_cols = [c for c in FACTOR_COLUMNS
                      if c in wide.columns and c != "run" and wide[c].nunique(dropna=False) > 1]
    agg = aggregate(wide, group_cols)
    if not agg.empty:
        agg.to_csv(args.out / "config_agg.csv", index=False)

    print(f"\n{len(wide)} runs -> {args.out}")
    if decisions:
        print(f"  delegation decisions: {len(decisions)} -> decisions.csv")
    print(f"  grouped by: {', '.join(group_cols) if group_cols else '(nothing varies)'}")
    print(f"  configurations: {len(agg)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
