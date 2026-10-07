"""Per-job RTT attribution and RTT-bin metrics (T-2, E3a / F3).

Joins `<run>/rtt_matrix.json` (captured by run_test.py before launch) onto the job exports:

* **Hierarchical run:** a job's RTT is between the host of the agent that EXECUTED it (the
  leaf's `leader_id` in `all_jobs.csv`) and the host of the coordinator that DELEGATED it (the
  `leader_id` of its record in `level1_jobs.csv`). `rtt_kind = coordinator`. This is the WAN the
  coordinator tier's consensus and the delegation cross.
* **Flat run:** there is no delegating coordinator — the executor is the proposer — so the RTT is
  the executor's MEDIAN RTT to every other agent of the run, the distance its consensus
  messages travel. `rtt_kind = peer_median`. Never mixed with the hierarchical kind in one bin
  table: the two are different quantities.

Bins, in ms, contiguous: `lan` < 5 <= `regional` < 40 <= `continental` < 90 <= `transatlantic`.

**Refuse, don't approximate.** A job is unattributed — no RTT, no bin, counted in
`rtt_jobs_unattributed` — when its executor or coordinator is unknown, the agent is not in the
matrix, or the pair was not measured. A pair measured in one direction only uses that direction
(ICMP RTT is symmetric up to noise). A run without a matrix gets `rtt_matrix_status` and nothing
else: absent, never zero.
"""
from __future__ import annotations

import json
import statistics
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

BIN_EDGES_MS = (5.0, 40.0, 90.0)
BINS = ("lan", "regional", "continental", "transatlantic")


def rtt_bin(rtt_ms: Optional[float]) -> Optional[str]:
    if rtt_ms is None:
        return None
    for edge, name in zip(BIN_EDGES_MS, BINS):
        if rtt_ms < edge:
            return name
    return BINS[-1]


def load_matrix(run_dir: Path) -> Optional[dict]:
    try:
        return json.loads((run_dir / "rtt_matrix.json").read_text())
    except (OSError, ValueError):
        return None


class Matrix:
    def __init__(self, raw: dict):
        self.agent_host = {str(a): h for a, h in (raw.get("agents") or {}).items()}
        self.rtt = raw.get("rtt_ms") or {}

    def between_hosts(self, a: Optional[str], b: Optional[str]) -> Optional[float]:
        if not a or not b:
            return None
        if a == b:
            return 0.0
        forward = (self.rtt.get(a) or {}).get(b)
        if forward is not None:
            return float(forward)
        backward = (self.rtt.get(b) or {}).get(a)
        return float(backward) if backward is not None else None

    def between_agents(self, a, b) -> Optional[float]:
        return self.between_hosts(self.agent_host.get(_aid(a)), self.agent_host.get(_aid(b)))

    def peer_median(self, agent, peers: List[str]) -> Optional[float]:
        values = [self.between_agents(agent, p) for p in peers if _aid(p) != _aid(agent)]
        if not values or any(v is None for v in values):
            return None
        return float(statistics.median(values))


def _aid(value) -> Optional[str]:
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if f != f:      # NaN
        return None
    return str(int(f))


def _read(path: Path) -> Optional[pd.DataFrame]:
    if not path.is_file():
        return None
    try:
        return pd.read_csv(path)
    except Exception:
        return None


def _selection(df: pd.DataFrame) -> pd.Series:
    started = pd.to_numeric(df.get("selection_started_at"), errors="coerce")
    assigned = pd.to_numeric(df.get("assigned_at"), errors="coerce")
    out = assigned - started
    return out.where((started > 0) & (assigned > 0) & (out >= 0))


def job_rows(run_dir: Path, jobs: pd.DataFrame) -> List[Dict[str, Any]]:
    """One row per job with its RTT attribution. *jobs* is the deduplicated all_jobs frame."""
    raw = load_matrix(run_dir)
    if not raw or raw.get("status") == "failed" or jobs is None or jobs.empty:
        return []
    m = Matrix(raw)
    level1 = _read(run_dir / "level1_jobs.csv")
    hierarchical = level1 is not None and not level1.empty
    coord_of: Dict[str, Optional[str]] = {}
    l1_sel: Dict[str, Optional[float]] = {}
    if hierarchical:
        l1 = level1.copy()
        l1["_sel"] = _selection(l1)
        for rec in l1.to_dict("records"):
            jid = str(rec.get("job_id"))
            coord_of[jid] = _aid(rec.get("leader_id"))
            sel = rec.get("_sel")
            l1_sel[jid] = None if sel is None or sel != sel else float(sel)
    peers = list(m.agent_host)

    frame = jobs.copy()
    frame["_sel"] = _selection(frame)
    completed = pd.to_numeric(frame.get("completed_at"), errors="coerce")
    out = []
    for rec, done in zip(frame.to_dict("records"), (completed > 0).tolist()):
        jid = str(rec.get("job_id"))
        executor = _aid(rec.get("leader_id"))
        if hierarchical:
            kind = "coordinator"
            coordinator = coord_of.get(jid)
            rtt = m.between_agents(executor, coordinator) if executor and coordinator else None
        else:
            kind, coordinator = "peer_median", None
            rtt = m.peer_median(executor, peers) if executor else None
        sel = rec.get("_sel")
        out.append({
            "job_id": jid, "executor": executor, "coordinator": coordinator,
            "rtt_kind": kind,
            "rtt_ms": None if rtt is None else round(rtt, 3),
            "rtt_bin": rtt_bin(rtt),
            "completed": bool(done),
            "selection_s": None if sel is None or sel != sel else float(sel),
            "l1_selection_s": l1_sel.get(jid) if hierarchical else None,
        })
    return out


def run_metrics(run_dir: Path, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Run-level RTT columns. `rtt_matrix_status` alone when there is no usable matrix."""
    raw = load_matrix(run_dir)
    if raw is None:
        return {}
    out: Dict[str, Any] = {"rtt_matrix_status": raw.get("status")}
    if raw.get("status") == "failed":
        return out
    out["rtt_pairs_missing"] = int(raw.get("pairs_missing") or 0)
    if not rows:
        return out
    frame = pd.DataFrame(rows)
    out["rtt_kind"] = frame["rtt_kind"].iloc[0]
    attributed = frame[frame["rtt_bin"].notna()]
    out["rtt_jobs_attributed"] = int(len(attributed))
    out["rtt_jobs_unattributed"] = int(len(frame) - len(attributed))
    if not attributed.empty:
        out["rtt_ms_p50"] = round(float(attributed["rtt_ms"].median()), 3)
    for name in BINS:
        part = attributed[attributed["rtt_bin"] == name]
        # Emitted for every bin, 0 when empty: a missing column would let aggregate() average
        # a bin over the runs that happened to populate it.
        out[f"rtt_{name}_jobs"] = int(len(part))
        if part.empty:
            continue
        out[f"rtt_{name}_completed_share"] = round(float(part["completed"].mean()), 6)
        for col, prefix in (("selection_s", "selection"), ("l1_selection_s", "l1_selection")):
            vals = pd.to_numeric(part[col], errors="coerce").dropna()
            if not vals.empty:
                out[f"rtt_{name}_{prefix}_p50"] = round(float(vals.quantile(0.5)), 6)
                out[f"rtt_{name}_{prefix}_p95"] = round(float(vals.quantile(0.95)), 6)
    return out
