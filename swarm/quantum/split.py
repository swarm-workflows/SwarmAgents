# MIT License
#
# Copyright (c) 2024 swarm-workflows

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# Author: Komal Thareja(kthare10@renci.org)
"""
Splitting hybrid jobs into co-schedulable sub-jobs, and building classical
post-processing jobs that quantum agents push back into the pool (Phase 2 of
docs/QUANTUM_HYBRID_DESIGN.md).

Placement model: the quantum sub-job (the scarce resource) is placed first
through normal consensus. The classical sub-job is data-triggered — its data
predicate keeps it out of selection until the producer's first snapshot batch
exists, and the cross-site communication penalty (below) steers it toward the
producer's site. This is a distributed approximation of joint pair placement;
true placement-pair consensus remains future work.

All builders are pure dict->dict functions so they are trivially testable and
usable from both the job distributor and the agents.
"""
from typing import Dict, Optional, Tuple

# Classical footprint of the quantum sub-job: circuit compilation and state
# preparation need little more than a core and a few GB
_PREP_CORE = 1.0
_PREP_RAM = 2.0
_PREP_DISK = 5.0
# Wall-time share of the classical component attributed to prep vs. the
# iterative update loop
_PREP_WALL_FRACTION = 0.2


def experiment_id_for(job_id: str) -> str:
    return f"exp-{job_id}"


def split_hybrid_job(job_dict: Dict) -> Optional[Tuple[Dict, Dict]]:
    """
    Split a hybrid job (quantum spec with hybrid=True) into:
      <id>-q : quantum sub-job — full quantum spec + a small classical prep
               footprint; produces one snapshot batch per iteration
      <id>-c : classical sub-job — the original classical demand; consumes the
               snapshot stream, gated by a data predicate

    Returns None for jobs that are not splittable (classical, one-shot
    quantum, or already split).
    """
    quantum = job_dict.get("quantum")
    if not quantum or not quantum.get("hybrid") or job_dict.get("sub_role"):
        return None

    job_id = str(job_dict["id"])
    exp = experiment_id_for(job_id)
    iterations = max(1, int(quantum.get("iterations", 1)))
    caps = dict(job_dict.get("capacities") or {})
    wall_time = float(job_dict.get("wall_time") or 0.0)

    quantum_sub = {
        "id": f"{job_id}-q",
        "wall_time": round(wall_time * _PREP_WALL_FRACTION, 2),
        "capacities": {
            "core": min(_PREP_CORE, caps.get("core", _PREP_CORE)),
            "ram": min(_PREP_RAM, caps.get("ram", _PREP_RAM)),
            "disk": min(_PREP_DISK, caps.get("disk", _PREP_DISK)),
            "gpu": 0,
            "qubits": quantum.get("qubits", 0),
        },
        "quantum": dict(quantum),
        "sub_role": "quantum",
        "linked_job_id": f"{job_id}-c",
        "experiment_id": exp,
        "should_fail": job_dict.get("should_fail", False),
    }

    classical_caps = {k: v for k, v in caps.items() if k != "qubits"}
    classical_sub = {
        "id": f"{job_id}-c",
        "wall_time": round(wall_time * (1.0 - _PREP_WALL_FRACTION), 2),
        "capacities": classical_caps,
        "sub_role": "classical",
        "linked_job_id": f"{job_id}-q",
        "experiment_id": exp,
        "data_predicate": {
            "experiment_id": exp,
            "min_snapshots": 1,
            "total_snapshots": iterations,
        },
        "data_in": job_dict.get("data_in"),
        "data_out": job_dict.get("data_out"),
        "should_fail": False,
    }
    return quantum_sub, classical_sub


def build_post_process_job(job_id: str, experiment_id: str,
                           output_type: str = "") -> Dict:
    """
    Classical post-processing job a quantum agent pushes to the pool after a
    one-shot quantum job completes (the first concrete instance of the
    self-expanding DAG: the job exists only because measurement data does).
    """
    return {
        "id": f"{job_id}-post",
        "wall_time": 2.0,
        "capacities": {"core": _PREP_CORE, "ram": _PREP_RAM, "disk": _PREP_DISK, "gpu": 0},
        "sub_role": "classical",
        "linked_job_id": job_id,
        "experiment_id": experiment_id,
        "data_predicate": {
            "experiment_id": experiment_id,
            "min_snapshots": 1,
            "total_snapshots": 1,
        },
        "state_data": {"output_type": output_type},
    }


def split_comm_penalty(factor: float, total_snapshots: int,
                       agent_site: Optional[str], producer_site: Optional[str]) -> float:
    """
    Multiplicative cost penalty for placing a stream consumer away from its
    producer. Same site (or unknown sites) -> 1.0; cross-site grows with the
    volume of snapshot traffic, saturating at 1 + factor.
    """
    if factor <= 0 or not agent_site or not producer_site or agent_site == producer_site:
        return 1.0
    volume = min(1.0, max(1, total_snapshots) / 50.0)
    return 1.0 + factor * (0.5 + 0.5 * volume)
