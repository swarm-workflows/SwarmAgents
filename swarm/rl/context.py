# MIT License
#
# Copyright (c) 2024 swarm-workflows
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# Author: Komal Thareja(kthare10@renci.org)
"""Feature extraction for contextual bandit delegation.

Builds one fixed-length, [0, 1]-normalized feature vector per
(job, child group) candidate pair for LinUCBPolicy. See
docs/CONTEXTUAL_BANDIT_DESIGN.md section 4.2.
"""
import hashlib
import json
import math
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import numpy as np


@dataclass
class GroupSnapshot:
    """Coordinator's current view of one child group (from heartbeats and
    local delegation tracking). All headroom values are fractions in [0, 1]
    (1.0 = fully idle). ``type_failure_rates`` holds the group's recent
    failure rate per job type (sliding window, maintained by MABManager);
    types absent from the dict fall back to the aggregate ``failure_rate``."""
    active_children: int = 0
    cpu_headroom: float = 1.0
    ram_headroom: float = 1.0
    gpu_headroom: float = 1.0
    inflight: int = 0
    failure_rate: float = 0.0
    type_failure_rates: Dict[str, float] = field(default_factory=dict)
    # Time-decayed delegation-timeout signal in [0, 1] (maintained by
    # MABManager). Unlike the failure windows, it fades with wall-clock time
    # even when the arm is never tried — no refresh hysteresis after an
    # outage ends (design doc section 8.3).
    timeout_rate: float = 0.0
    # When the freshest / stalest child record behind this snapshot was stamped, on the
    # *child's* clock (P0-4). Timestamps rather than ages, because the age that matters is
    # taken at the instant of the decision and an LLM delegation decides seconds after the
    # snapshot is built. ``None`` means no observation at all — an unknown age, which is not
    # the same as a large one and must never be substituted for one.
    observed_at: Optional[float] = None
    oldest_observed_at: Optional[float] = None
    # The same two, but stamped on the COORDINATOR's clock when it took delivery of the
    # record. `observed_at` minus a decision time is one term per clock, so any inter-host
    # offset lands in the measurement; these two share a clock with the decision and so are
    # immune to it. This is the pair the staleness figure is computed from.
    received_at: Optional[float] = None
    oldest_received_at: Optional[float] = None


def _headroom(total: float, used: float) -> float:
    """Free fraction of a capacity dimension; unknown capacity reads as idle."""
    if total <= 0:
        return 1.0
    return float(min(1.0, max(0.0, 1.0 - used / total)))


def snapshots_from_children(children, delegation_infos,
                            seen_at: Optional[Dict[int, float]] = None
                            ) -> Dict[int, GroupSnapshot]:
    """Aggregate per-group GroupSnapshots from a coordinator's view.

    *children*: iterable of AgentInfo-like records (``group``, ``capacities``,
    ``capacity_allocations``). *delegation_infos*: iterable of delegated-job
    tracking dicts (``{'groups': [...]}``) for in-flight counts.
    *seen_at*: ``{agent_id: local_timestamp}`` of when the caller last took delivery of a
    fresher record for each child, on the caller's own clock.

    Failure-rate fields are left at defaults — MABManager owns those and
    overwrites them from its outcome windows.
    """
    per_group: Dict[int, list] = {}
    for child in children:
        group = child.group if child.group is not None else 0
        per_group.setdefault(group, []).append(child)

    inflight: Dict[int, int] = {}
    for info in delegation_infos:
        for group in info.get("groups", []):
            inflight[group] = inflight.get(group, 0) + 1

    snapshots = {}
    for group, members in per_group.items():
        total_core = total_ram = total_gpu = 0.0
        used_core = used_ram = used_gpu = 0.0
        stamps = []
        local_stamps = []
        for child in members:
            caps = child.capacities
            alloc = child.capacity_allocations
            total_core += getattr(caps, "core", 0) or 0
            total_ram += getattr(caps, "ram", 0) or 0
            total_gpu += getattr(caps, "gpu", 0) or 0
            used_core += getattr(alloc, "core", 0) or 0
            used_ram += getattr(alloc, "ram", 0) or 0
            used_gpu += getattr(alloc, "gpu", 0) or 0
            stamp = getattr(child, "last_updated", None)
            if stamp:
                stamps.append(float(stamp))
            local = (seen_at or {}).get(getattr(child, "agent_id", None))
            if local:
                local_stamps.append(float(local))
        snapshots[group] = GroupSnapshot(
            active_children=len(members),
            cpu_headroom=_headroom(total_core, used_core),
            ram_headroom=_headroom(total_ram, used_ram),
            gpu_headroom=_headroom(total_gpu, used_gpu),
            inflight=inflight.get(group, 0),
            # A group's load view is only as fresh as the child records it was aggregated
            # from. Both ends are kept: the newest says how recently the coordinator heard
            # anything about the group at all, the oldest how stale the worst contributor to
            # these headroom numbers is. A group of nine where one child went quiet reads as
            # fresh on the first and stale on the second, and that is the honest answer.
            observed_at=max(stamps) if stamps else None,
            oldest_observed_at=min(stamps) if stamps else None,
            received_at=max(local_stamps) if local_stamps else None,
            oldest_received_at=min(local_stamps) if local_stamps else None,
        )
    return snapshots


class ContextExtractor:
    """Produces per-(job, group) feature vectors for contextual bandits.

    The feature layout (and thus the model dimension) is fixed by config at
    construction time. ``schema_version`` fingerprints the layout so persisted
    model state trained under a different layout is discarded on load.
    """

    def __init__(self, config: Optional[dict] = None):
        cfg = config or {}
        self.job_types: List[str] = list(cfg.get("job_types", []))
        self.max_group_size = float(cfg.get("max_group_size", 10))
        self.max_inflight = float(cfg.get("max_inflight", 32))
        self.max_dtns = float(cfg.get("max_dtns", 4))
        self.long_job_threshold = float(cfg.get("long_job_threshold", 20.0))
        max_caps = cfg.get("max_caps", {})
        self.max_core = float(max_caps.get("core", 16))
        self.max_ram = float(max_caps.get("ram", 64))      # G, matching Capacities units
        self.max_disk = float(max_caps.get("disk", 500))   # G
        self.max_gpu = float(max_caps.get("gpu", 4))

    @property
    def feature_names(self) -> List[str]:
        # A linear model over concat(job, group) alone is additive: the job
        # part is identical for every candidate arm, so job-dependent routing
        # requires explicit interaction terms — the fit_* products and the
        # group's failure rate *for this job's type*.
        return (
            ["job_core", "job_ram", "job_disk", "job_gpu",
             "job_wall_time", "job_dtn_count"]
            + [f"job_type:{t}" for t in self.job_types]
            + ["grp_children", "grp_cpu_headroom", "grp_ram_headroom",
               "grp_gpu_headroom", "grp_inflight", "grp_failure_rate",
               "grp_timeout_rate",
               "fit_core", "fit_ram", "fit_gpu", "grp_type_failure_rate",
               "bias"]
        )

    @property
    def dim(self) -> int:
        return len(self.feature_names)

    @property
    def schema_version(self) -> str:
        """Stable fingerprint of the feature layout and normalization caps."""
        payload = json.dumps({
            "features": self.feature_names,
            "caps": [self.max_core, self.max_ram, self.max_disk, self.max_gpu,
                     self.max_group_size, self.max_inflight, self.max_dtns,
                     self.long_job_threshold],
        }, sort_keys=True)
        return hashlib.md5(payload.encode()).hexdigest()[:12]

    @staticmethod
    def _clip01(value: float) -> float:
        return float(min(1.0, max(0.0, value)))

    @staticmethod
    def _dtn_count(job) -> int:
        try:
            if hasattr(job, "get_data_in"):
                data_in = job.get_data_in() or []
                data_out = job.get_data_out() or []
            else:
                data_in = getattr(job, "data_in", None) or []
                data_out = getattr(job, "data_out", None) or []
            return len(data_in) + len(data_out)
        except Exception:
            return 0

    def _job_norms(self, job) -> Dict[str, float]:
        caps = getattr(job, "capacities", None)
        core = getattr(caps, "core", 0) if caps else 0
        ram = getattr(caps, "ram", 0) if caps else 0
        disk = getattr(caps, "disk", 0) if caps else 0
        gpu = getattr(caps, "gpu", 0) if caps else 0
        wall_time = getattr(job, "wall_time", None) or 0.0
        return {
            "core": self._clip01(core / self.max_core),
            "ram": self._clip01(ram / self.max_ram),
            "disk": self._clip01(disk / self.max_disk),
            "gpu": self._clip01(gpu / self.max_gpu),
            "wall": self._clip01(
                math.log1p(wall_time) / math.log1p(self.long_job_threshold)),
            "dtn": self._clip01(self._dtn_count(job) / self.max_dtns),
        }

    def job_features(self, job) -> np.ndarray:
        norms = self._job_norms(job)
        features = [norms["core"], norms["ram"], norms["disk"], norms["gpu"],
                    norms["wall"], norms["dtn"]]
        job_type = getattr(job, "job_type", None)
        features.extend(1.0 if job_type == t else 0.0 for t in self.job_types)
        return np.array(features, dtype=float)

    def group_features(self, snapshot: GroupSnapshot) -> np.ndarray:
        return np.array([
            self._clip01(snapshot.active_children / self.max_group_size),
            self._clip01(snapshot.cpu_headroom),
            self._clip01(snapshot.ram_headroom),
            self._clip01(snapshot.gpu_headroom),
            self._clip01(snapshot.inflight / self.max_inflight),
            self._clip01(snapshot.failure_rate),
            self._clip01(snapshot.timeout_rate),
        ], dtype=float)

    def interaction_features(self, job, snapshot: GroupSnapshot) -> np.ndarray:
        norms = self._job_norms(job)
        job_type = getattr(job, "job_type", None)
        type_failure = snapshot.type_failure_rates.get(
            job_type, snapshot.failure_rate)
        return np.array([
            self._clip01(norms["core"] * snapshot.cpu_headroom),
            self._clip01(norms["ram"] * snapshot.ram_headroom),
            self._clip01(norms["gpu"] * snapshot.gpu_headroom),
            self._clip01(type_failure),
        ], dtype=float)

    def build(self, job, group_ids: List[int],
              snapshots: Optional[Dict[int, GroupSnapshot]] = None
              ) -> Dict[int, np.ndarray]:
        """One feature vector per candidate group:
        concat(job, group, job-x-group interactions, bias).

        Groups missing from *snapshots* get default (idle, unknown-history)
        group features — this is how a freshly added child group is scored.
        """
        snapshots = snapshots or {}
        job_vec = self.job_features(job)
        default = GroupSnapshot()
        vectors = {}
        for g in group_ids:
            snap = snapshots.get(g, default)
            vectors[g] = np.concatenate([
                job_vec,
                self.group_features(snap),
                self.interaction_features(job, snap),
                [1.0],
            ])
        return vectors
