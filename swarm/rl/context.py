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
