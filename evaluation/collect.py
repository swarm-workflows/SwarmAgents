#!/usr/bin/env python3
"""Post-hoc metric extraction over archived SwarmAgents run trees.

Walks one or more run-tree roots, finds every leaf run directory (identified by
``all_jobs.csv``), derives the experiment factors from the directory path, computes the
metric set from ``docs/CCGRID_EVAL_PLAN.md`` section 7, and writes:

* ``runs_wide.csv``   -- one row per run, one column per metric
* ``runs_tidy.csv``   -- one row per (run, metric); convenient for seaborn/ggplot
* ``config_agg.csv``  -- per-configuration mean/std/n/ci95 across repeats

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

    metrics: dict[str, Any] = {
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
        metrics[f"{level_name}_count"] = 0 if pending_df is None else int(len(pending_df))

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

            record = {"root": str(root), "run_dir": os.path.relpath(run_dir, root)}
            record.update(factors)
            record.update(metrics)
            records.append(record)

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
    print(f"  grouped by: {', '.join(group_cols) if group_cols else '(nothing varies)'}")
    print(f"  configurations: {len(agg)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
