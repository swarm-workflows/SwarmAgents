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
Quantum Measurement Data Layer (Phase 2 of docs/QUANTUM_HYBRID_DESIGN.md).

Implements the runtime paper's measurement collection component on Redis
streams: producers (agents executing the quantum sub-job) publish timestamped
measurement batches ("snapshots") per experiment; consumers (classical
sub-jobs / post-processing jobs) read the stream incrementally. Data
predicates of the form "at least N snapshots from experiment X" gate job
selection, steering computation by quantum data availability.

One stream entry = one snapshot batch (e.g. the shots of one hybrid-loop
iteration). Keys carry a TTL so finished experiments age out of Redis.
"""
import json
import threading
import time
from typing import List, Optional, Tuple


class MeasurementLayer:
    """
    Thin, thread-safe wrapper over Redis streams for quantum measurement data.

    Key layout:
      measurements:<experiment_id>       stream of snapshot batches
      measurements:<experiment_id>:site  site label of the producing agent
                                         (consumed by the communication penalty)
    """

    def __init__(self, redis_client, ttl_s: int = 3600, predicate_cache_s: float = 1.0):
        self.redis = redis_client
        self.ttl_s = int(ttl_s)
        # Small snapshot-count cache so per-tick predicate gating doesn't
        # hammer Redis when many agents watch the same experiment
        self._cache_ttl = float(predicate_cache_s)
        self._count_cache: dict[str, Tuple[float, int]] = {}
        self._site_cache: dict[str, str] = {}
        self._lock = threading.Lock()

    @staticmethod
    def stream_key(experiment_id: str) -> str:
        return f"measurements:{experiment_id}"

    @staticmethod
    def site_key(experiment_id: str) -> str:
        return f"measurements:{experiment_id}:site"

    # ------------------------------------------------------------------
    # Producer side
    # ------------------------------------------------------------------
    def announce_producer(self, experiment_id: str, site: Optional[str]) -> None:
        """Record which site produces this experiment's data (used by the
        cross-site communication penalty). Empty site still writes a marker
        so consumers can distinguish 'not started' from 'no site label'."""
        key = self.site_key(experiment_id)
        self.redis.set(key, site or "", ex=self.ttl_s)

    def publish(self, experiment_id: str, payload: dict) -> int:
        """
        Append one snapshot batch to the experiment stream. Returns the new
        snapshot count.
        """
        key = self.stream_key(experiment_id)
        pipe = self.redis.pipeline(transaction=False)
        pipe.xadd(key, {"payload": json.dumps(payload), "ts": repr(time.time())})
        pipe.expire(key, self.ttl_s)
        pipe.xlen(key)
        results = pipe.execute()
        count = int(results[-1])
        with self._lock:
            self._count_cache[experiment_id] = (time.time(), count)
        return count

    # ------------------------------------------------------------------
    # Consumer side
    # ------------------------------------------------------------------
    def snapshot_count(self, experiment_id: str) -> int:
        """Cached XLEN of the experiment stream (0 if it doesn't exist)."""
        now = time.time()
        with self._lock:
            cached = self._count_cache.get(experiment_id)
            if cached and now - cached[0] < self._cache_ttl:
                return cached[1]
        try:
            count = int(self.redis.xlen(self.stream_key(experiment_id)))
        except Exception:
            count = 0
        with self._lock:
            self._count_cache[experiment_id] = (now, count)
        return count

    def predicate_satisfied(self, experiment_id: str, min_snapshots: int) -> bool:
        """Data predicate: 'at least min_snapshots snapshots from experiment X'."""
        return self.snapshot_count(experiment_id) >= max(1, int(min_snapshots))

    def producer_site(self, experiment_id: str) -> Optional[str]:
        """Site of the producing agent, or None if the producer hasn't started.
        Cached forever once seen — the producer never moves mid-experiment."""
        with self._lock:
            if experiment_id in self._site_cache:
                return self._site_cache[experiment_id]
        val = self.redis.get(self.site_key(experiment_id))
        if val is None:
            return None
        site = val.decode() if isinstance(val, bytes) else str(val)
        with self._lock:
            self._site_cache[experiment_id] = site
        return site

    def read_from(self, experiment_id: str, last_id: str = "0-0",
                  block_ms: int = 1000, count: int = 16) -> List[Tuple[str, dict]]:
        """
        Read snapshot batches after `last_id`, blocking up to block_ms when the
        stream is dry. Returns [(entry_id, payload_dict), ...] (possibly empty).
        """
        try:
            resp = self.redis.xread({self.stream_key(experiment_id): last_id},
                                    count=count, block=block_ms)
        except Exception:
            return []
        out: List[Tuple[str, dict]] = []
        for _stream, entries in (resp or []):
            for entry_id, fields in entries:
                eid = entry_id.decode() if isinstance(entry_id, bytes) else str(entry_id)
                raw = fields.get(b"payload") if isinstance(next(iter(fields), None), bytes) \
                    else fields.get("payload")
                try:
                    payload = json.loads(raw if isinstance(raw, str) else raw.decode())
                except Exception:
                    payload = {}
                out.append((eid, payload))
        return out
