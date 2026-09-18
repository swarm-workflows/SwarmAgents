"""Sizing a fleet from the jobs it will be given.

A converted Pegasus workflow carries the resources its jobs really needed and the DTNs their
data sits on. A generated fleet is drawn from a fixed flavour pool that knows nothing about
them, and when the two do not meet `is_job_feasible` is false for every agent: the job is never
proposed, never fails, and shows up only as still-pending at the end. This module is the one
place that reconciles them, used by both `pegasus_to_swarm_converter.py --generate-agent-configs`
and `generate_configs.py --size-to-jobs`.

Two rules, and each was a real defect before it was written down:

* **Raise, never lower.** Fitting means every agent can host the largest job; an agent that was
  already bigger stays as it is, or sizing a fleet to a small workflow would shrink it.
* **One agent must satisfy every dimension at once.** The requirement is the per-dimension
  maximum over jobs, applied to each flavour as a whole — a fleet with a big-CPU agent and a
  big-RAM agent still cannot run a job needing both.

**Universal feasibility is the goal, not a side effect.** Every agent ends able to host every
job, which is what makes a failure test mean anything: an agent goes down and another can pick
its work up, rather than the work being stranded because it fitted only the agent that died.

**That does not mean a uniform fleet, and two dimensions carry the variability.** Capacity is
raised to a *floor*, never levelled — an agent already larger than the largest job keeps its
flavour, so the spread above that floor survives. Locality is carried by `connectivity_score`,
which every agent has for every DTN but at its *own* value: feasibility tests DTN **names**
only (`ResourceAgent.is_job_feasible`), while the score feeds the cost model, so varying it
differentiates agents without ever making a job unrunnable. Pinning it at 1.0 was wrong twice
over — it erased locality, and it did so in the flattering direction, since an agent's
organically assigned DTNs score 0.6-0.95 and the bolted-on ones would have scored better than
any of them.
"""
from __future__ import annotations

import glob
import json
import math
import os
import random
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Set


@dataclass
class JobRequirements:
    """What one agent must have for every job in a set to be feasible somewhere."""

    core: float = 0.0
    ram: float = 0.0
    disk: float = 0.0
    gpu: float = 0.0
    dtns: Set[str] = field(default_factory=set)
    job_count: int = 0

    @property
    def empty(self) -> bool:
        return self.job_count == 0


def load_job_records(jobs_dir: str) -> List[dict]:
    """Every `job_*.json` in *jobs_dir* — the same set `job_distributor.py` publishes.

    A record that will not parse is skipped rather than raising: a directory can hold a
    half-written file, and refusing to size a whole fleet over one of them helps nobody.
    """
    records = []
    for path in sorted(glob.glob(os.path.join(jobs_dir, "job_*.json"))):
        try:
            with open(path) as fh:
                records.append(json.load(fh))
        except (json.JSONDecodeError, OSError):
            continue
    return records


def job_requirements(jobs: Iterable[dict]) -> JobRequirements:
    """The per-dimension maximum over *jobs*, plus every DTN they reference.

    `local` is excluded because it means the local filesystem, not a transfer node —
    `ResourceAgent.is_job_feasible` excludes it too, and attaching it to an agent would be
    meaningless.
    """
    req = JobRequirements()
    for job in jobs:
        caps = job.get("capacities") or {}
        req.core = max(req.core, float(caps.get("core", 0) or 0))
        req.ram = max(req.ram, float(caps.get("ram", 0) or 0))
        req.disk = max(req.disk, float(caps.get("disk", 0) or 0))
        req.gpu = max(req.gpu, float(caps.get("gpu", 0) or 0))
        for node in (job.get("data_in") or []) + (job.get("data_out") or []):
            name = node.get("name") if isinstance(node, dict) else None
            if name and name != "local":
                req.dtns.add(str(name))
        req.job_count += 1
    return req


def fit_flavor(flavor: Dict, req: JobRequirements) -> Dict:
    """*flavor* raised so it can host the largest job. Whole units, never lowered."""
    fitted = dict(flavor)
    fitted["core"] = max(int(fitted.get("core", 0) or 0), int(math.ceil(req.core)))
    fitted["ram"] = max(int(fitted.get("ram", 0) or 0), int(math.ceil(req.ram)))
    fitted["disk"] = max(int(fitted.get("disk", 0) or 0), int(math.ceil(req.disk)))
    fitted["gpu"] = max(int(fitted.get("gpu", 0) or 0), int(math.ceil(req.gpu)))
    return fitted


# Mirrors generate_configs.generate_global_dtn_pool / assign_agent_dtns, so a sized DTN and a
# drawn one are scored on the same scale. A sized fleet otherwise mixes two distributions and
# the difference reads as locality rather than as bookkeeping.
DTN_BASE_RANGE = (0.6, 0.95)
DTN_JITTER = 0.05


def dtn_base_scores(names: Iterable[str], rng=random) -> Dict[str, float]:
    """One base connectivity score per DTN name, shared by every agent.

    A DTN that is well connected is well connected for everyone; what differs per agent is the
    jitter around it. Drawing independently per agent instead would make the *name* meaningless
    and leave no stable locality for selection to learn.
    """
    return {name: round(rng.uniform(*DTN_BASE_RANGE), 2) for name in sorted(names)}


def dtn_entries(names: Iterable[str], subnet: str = "192.168.200",
                base_scores: Dict[str, float] = None, rng=random) -> List[dict]:
    """DTN records for *names*, in the shape agent configs expect — one agent's worth.

    Call it once per agent, passing the same *base_scores* each time: every agent then holds
    every name (so no job is infeasible anywhere) with its own `connectivity_score` around that
    name's base (so agents still differ). `base_scores` omitted means draw them here, which is
    right only when there is a single agent or the caller does not care that each one gets an
    unrelated draw.
    """
    names = sorted(names)
    bases = base_scores if base_scores is not None else dtn_base_scores(names, rng=rng)
    entries = []
    for i, name in enumerate(names, 1):
        base = bases.get(name, sum(DTN_BASE_RANGE) / 2)
        score = min(1.0, max(0.0, base + rng.uniform(-DTN_JITTER, DTN_JITTER)))
        entries.append({"name": name, "ip": f"{subnet}.{i}",
                        "user": f"dtn_user_{name}", "connectivity_score": round(score, 2)})
    return entries
