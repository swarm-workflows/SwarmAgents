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
"""Sparrow-style decentralized scheduling baseline: batch sampling with late binding.

What it reproduces from Sparrow (Ousterhout et al., SOSP 2013), and how:

* **Several independent schedulers, no shared scheduler state.** Each job belongs to exactly one
  scheduler, by a stable hash of its id, so no two schedulers ever probe for the same job and
  none of them needs to agree with any other. They run as separate processes, on agent hosts
  in a remote run, so each pays its own WAN distance to the store.
* **Probes, not placements.** For a job the owning scheduler samples ``probe_ratio`` distinct
  workers among those that could ever run it (static capacity and DTNs — Sparrow's
  constraint-aware sampling) and live (fresh heartbeat), and places a *reservation* in each
  one's queue. It does not look at load: the queues are where load shows up.
* **Late binding.** A worker serves its queue in order. When it has a free slot that fits the
  job at the head, it asks for the task by claiming the job record: a compare-and-swap from
  PENDING to READY naming itself. The first worker to claim gets the task; every later
  reservation for the same job finds it taken and is dropped (Sparrow's no-op reply).
* **Re-probing.** A job still unclaimed after ``reprobe_s`` gets a fresh round of probes, so
  reservations stranded behind a long job or on a dead worker do not strand the job.

What it does NOT reproduce, stated so the comparison is read correctly:

* Probes and claims go through the shared Redis store on `database` — the same store SWARM's
  job pool lives in — rather than scheduler-to-worker RPC. Each probe and each claim therefore
  costs one round trip to `database` from wherever the scheduler or worker runs: the same WAN
  as SWARM, but a star rather than direct peer links.
* No task failure handling beyond what Sparrow has (none at the scheduler): a worker that dies
  holding a claimed job leaves it READY/RUNNING, as Sparrow leaves it to the framework.
* Jobs are single-task (m = 1), so ``probe_ratio`` is the number of probes per job.

The job records, the readiness registry for workflow DAGs, and the execution path
(`Job.execute`, the wall-time clamp, exit statuses) are SWARM's own, so the per-job export is
directly comparable through `evaluation/collect.py` and `plotting/comparison.py`.
"""
from __future__ import annotations

import json
import logging
import random
import threading
import time
import zlib
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Dict, Iterable, List, Optional

from baselines.agent_sim import SimulatedAgent
from swarm.database.repository import Repository
from swarm.models.capacities import Capacities
from swarm.models.job import Job, ObjectState

logger = logging.getLogger("sparrow")

LEVEL, GROUP = 0, 0
PENDING = ObjectState.PENDING.value


def owner_of(job_id: str, n_schedulers: int) -> int:
    """The scheduler that owns *job_id*. Stable across processes (not Python's `hash`, which
    is salted per process) so every scheduler computes the same partition."""
    return zlib.crc32(str(job_id).encode()) % max(1, int(n_schedulers))


class Keys:
    """Every Sparrow key is run-scoped, so a leftover node from an earlier run cannot serve
    or claim this run's work."""

    def __init__(self, run_id: str):
        self.base = f"sparrow:{run_id}"

    def queue(self, worker_id: int) -> str:
        return f"{self.base}:q:{int(worker_id)}"

    def heartbeat(self, worker_id: int) -> str:
        return f"{self.base}:hb:{int(worker_id)}"

    def scheduler_heartbeat(self, index: int) -> str:
        return f"{self.base}:shb:{int(index)}"

    def stats(self, role: str, node_id) -> str:
        return f"{self.base}:stats:{role}:{node_id}"

    @property
    def fleet(self) -> str:
        return f"{self.base}:fleet"

    @property
    def shutdown(self) -> str:
        return f"{self.base}:shutdown"


def publish_fleet(redis_client, keys: Keys, agents: Iterable[SimulatedAgent]) -> None:
    """Put the fleet's static profiles in the store, so nodes on other hosts need no file."""
    fleet = []
    for a in agents:
        fleet.append({
            "agent_id": a.agent_id,
            "capacities": a.capacities.to_dict() or {},
            "dtns": sorted(a.dtns.keys()) if isinstance(a.dtns, dict) else list(a.dtns or []),
        })
    redis_client.set(keys.fleet, json.dumps(fleet))


def load_fleet(redis_client, keys: Keys) -> List[SimulatedAgent]:
    raw = redis_client.get(keys.fleet)
    if not raw:
        raise RuntimeError(f"No fleet published at {keys.fleet}; start the orchestrator first.")
    from swarm.models.data_node import DataNode
    agents = []
    for entry in json.loads(raw):
        caps = Capacities.from_dict(entry.get("capacities") or {}) or Capacities()
        dtns = {name: DataNode(name=name) for name in entry.get("dtns") or []}
        agents.append(SimulatedAgent(agent_id=int(entry["agent_id"]), capacities=caps,
                                     dtns=dtns))
    return agents


def statically_feasible(job: Job, agent: SimulatedAgent) -> bool:
    """Could *agent* ever run *job*: total capacity and DTNs, never current load."""
    need = job.capacities or Capacities()
    have = agent.capacities
    for dim in ("core", "ram", "disk", "gpu"):
        if float(getattr(need, dim, 0) or 0) > float(getattr(have, dim, 0) or 0):
            return False
    required = job.required_dtns()
    return not required or required.issubset(set(agent.dtns or {}))


def _data_ready(repo: Repository, job: Job) -> bool:
    """SWARM's workflow DAG gate, file-dependency form. A failure to check is not readiness."""
    pred = job.data_predicate
    if not pred:
        return True
    if pred.get("kind") == "files" or "files" in pred:
        try:
            return repo.data_available(list(pred.get("files") or []))
        except Exception:
            return False
    # The quantum measurement-stream predicate has no Sparrow analogue; never release it.
    return False


class SparrowScheduler:
    """One Sparrow scheduler: owns a hash partition of the job ids and probes for them."""

    def __init__(self, index: int, n_schedulers: int, repo: Repository, redis_client,
                 keys: Keys, fleet: List[SimulatedAgent], probe_ratio: int = 2,
                 reprobe_s: float = 30.0, heartbeat_ttl_s: int = 30,
                 seed: Optional[int] = None, clock: Callable[[], float] = time.time):
        self.index, self.n = int(index), int(n_schedulers)
        self.repo, self.redis, self.keys = repo, redis_client, keys
        self.fleet = list(fleet)
        self.probe_ratio = max(1, int(probe_ratio))
        self.reprobe_s = float(reprobe_s)
        self.heartbeat_ttl_s = int(heartbeat_ttl_s)
        self.rng = random.Random(seed if seed is None else seed * 1000 + index)
        self.clock = clock
        self.probed: Dict[str, dict] = {}       # job_id -> {"at": t, "rounds": n}
        self.stats = {"jobs_probed": 0, "probes_sent": 0, "reprobes": 0,
                      "no_feasible_worker": 0, "no_live_worker": 0, "gated": 0}

    def live_workers(self) -> set:
        ids = [a.agent_id for a in self.fleet]
        if not ids:
            return set()
        alive = self.redis.mget([self.keys.heartbeat(i) for i in ids])
        return {i for i, v in zip(ids, alive) if v}

    def tick(self) -> int:
        """One pass over this scheduler's pending jobs. Returns the probes sent."""
        self.redis.set(self.keys.scheduler_heartbeat(self.index), "1",
                       ex=self.heartbeat_ttl_s)
        state_map = self.repo.get_all_ids_multi(key_prefix=Repository.KEY_JOB, level=LEVEL,
                                                group=GROUP, states=[PENDING])
        pending = [j for j in state_map.get(PENDING, []) if owner_of(j, self.n) == self.index]
        pending_set = set(pending)
        for gone in [j for j in self.probed if j not in pending_set]:
            del self.probed[gone]

        now = self.clock()
        due = [j for j in pending
               if j not in self.probed or now - self.probed[j]["at"] >= self.reprobe_s]
        if not due:
            return 0
        records = self.repo.get_many(due, key_prefix=Repository.KEY_JOB, level=LEVEL,
                                     group=GROUP)
        live = self.live_workers()
        sent = 0
        for job_id in due:
            rec = records.get(job_id)
            if not rec or rec.get("state") != PENDING:
                continue
            job = Job()
            job.from_dict(rec)
            if not _data_ready(self.repo, job):
                self.stats["gated"] += 1
                continue
            feasible = [a.agent_id for a in self.fleet if statically_feasible(job, a)]
            if not feasible:
                self.stats["no_feasible_worker"] += 1
                continue
            candidates = [w for w in feasible if w in live]
            if not candidates:
                self.stats["no_live_worker"] += 1
                continue
            first = job_id not in self.probed
            if first:
                # Stamped once, before any reservation exists, so no worker can have claimed
                # the job yet; the precondition still refuses a record that moved.
                job.mark_selection_started()
                if not self.repo.save(obj=job.to_dict(), key_prefix=Repository.KEY_JOB,
                                      level=LEVEL, group=GROUP,
                                      precondition=lambda cur: bool(cur)
                                      and cur.get("state") == PENDING):
                    continue
            targets = self.rng.sample(candidates, min(self.probe_ratio, len(candidates)))
            reservation = json.dumps({"job": job_id, "sched": self.index, "at": now})
            pipe = self.redis.pipeline()
            for w in targets:
                pipe.rpush(self.keys.queue(w), reservation)
            pipe.execute()
            entry = self.probed.setdefault(job_id, {"at": now, "rounds": 0})
            entry["at"] = now
            entry["rounds"] += 1
            sent += len(targets)
            self.stats["probes_sent"] += len(targets)
            if first:
                self.stats["jobs_probed"] += 1
            else:
                self.stats["reprobes"] += 1
        return sent

    def write_stats(self) -> None:
        self.redis.set(self.keys.stats("scheduler", self.index), json.dumps(self.stats))


class SparrowWorker:
    """One worker: serves its reservation queue in order and late-binds by claiming the job."""

    def __init__(self, agent: SimulatedAgent, repo: Repository, redis_client, keys: Keys,
                 max_concurrent: int = 10, heartbeat_ttl_s: int = 30):
        self.agent = agent
        self.repo, self.redis, self.keys = repo, redis_client, keys
        self.max_concurrent = max(1, int(max_concurrent))
        self.heartbeat_ttl_s = int(heartbeat_ttl_s)
        self._running = 0
        self._lock = threading.Lock()
        self._slot_freed = threading.Event()
        self.pool = ThreadPoolExecutor(max_workers=self.max_concurrent)
        self.executed_jobs: List[str] = []
        self.stats = {"reservations": 0, "claims": 0, "noops": 0, "lost_races": 0,
                      "head_of_line_waits": 0, "completed": 0, "persist_refused": 0}

    # -- queue service -------------------------------------------------------------------

    def heartbeat(self) -> None:
        self.redis.set(self.keys.heartbeat(self.agent.agent_id), "1", ex=self.heartbeat_ttl_s)

    def _has_slot(self) -> bool:
        with self._lock:
            return self._running < self.max_concurrent

    def serve_one(self, block_s: float = 1.0) -> str:
        """Take the reservation at the head of the queue and act on it.

        Returns what happened: ``empty``, ``noop`` (the job was already taken — Sparrow's no-op
        reply), ``claimed``, ``lost`` (another worker's claim landed first), or ``wait`` (no free
        slot that fits the job; the reservation is put back at the head, keeping queue order).
        """
        if not self._has_slot():
            return "wait"
        q = self.keys.queue(self.agent.agent_id)
        item = self.redis.blpop([q], timeout=max(1, int(block_s))) if block_s else \
            self._lpop(q)
        if not item:
            return "empty"
        raw = item[1] if isinstance(item, (list, tuple)) else item
        self.stats["reservations"] += 1
        job_id = json.loads(raw)["job"]
        rec = self.repo.get(job_id, key_prefix=Repository.KEY_JOB, level=LEVEL, group=GROUP)
        if not rec or rec.get("state") != PENDING:
            self.stats["noops"] += 1
            return "noop"
        job = Job()
        job.from_dict(rec)
        caps = job.capacities or Capacities()
        if not self.agent.can_fit(caps):
            # Head-of-line: the reservation keeps its place and the worker waits for a slot.
            self.redis.lpush(q, raw)
            self.stats["head_of_line_waits"] += 1
            return "wait"
        job.state = ObjectState.READY
        job.leader_id = self.agent.agent_id
        job.mark_assigned()
        claimed = self.repo.save(obj=job.to_dict(), key_prefix=Repository.KEY_JOB,
                                 level=LEVEL, group=GROUP,
                                 precondition=lambda cur: bool(cur)
                                 and cur.get("state") == PENDING)
        if not claimed:
            self.stats["lost_races"] += 1
            return "lost"
        self.stats["claims"] += 1
        self.agent.allocate(caps)
        with self._lock:
            self._running += 1
        self.pool.submit(self._run, job, caps)
        return "claimed"

    def _lpop(self, q):
        return self.redis.lpop(q)

    # -- execution -----------------------------------------------------------------------

    def _ours(self, cur) -> bool:
        return bool(cur) and str(cur.get("leader_id")) == str(self.agent.agent_id) \
            and cur.get("state") != ObjectState.COMPLETE.value

    def _run(self, job: Job, caps) -> None:
        try:
            self.executed_jobs.append(str(job.job_id))
            job.state = ObjectState.RUNNING
            job.mark_started()
            self.repo.save(obj=job.to_dict(), key_prefix=Repository.KEY_JOB, level=LEVEL,
                           group=GROUP, precondition=self._ours)
            try:
                job.execute()
            except Exception as exc:          # an execution failure is the job's outcome
                logger.error("job %s raised: %s", job.job_id, exc)
                job.exit_status = 1
            job.state = ObjectState.COMPLETE
            # The same publication rule as ResourceAgent.execute_job: outputs exist only when
            # the job succeeded, and the names ride the completion write so a DAG's children
            # are released by the same transaction that records their parent done.
            produced = ([d.file for d in (job.data_out or []) if getattr(d, "file", None)]
                        if job.exit_status == 0 else None)
            if not self.repo.save(obj=job.to_dict(), key_prefix=Repository.KEY_JOB,
                                  level=LEVEL, group=GROUP, produced_data=produced or None,
                                  precondition=self._ours):
                self.stats["persist_refused"] += 1
            else:
                self.stats["completed"] += 1
        finally:
            self.agent.release(caps)
            with self._lock:
                self._running -= 1
            self._slot_freed.set()

    def wait_for_slot(self, timeout: float) -> None:
        self._slot_freed.wait(timeout)
        self._slot_freed.clear()

    def run_forever(self, stop: threading.Event, block_s: float = 1.0,
                    stats_every_s: float = 10.0) -> None:
        """Serve the queue until *stop* or the run's shutdown key. Stats are written on a
        cadence as well as at the end, so a node killed before it could flush still left a
        recent count behind."""
        last_stats = time.monotonic()
        while not stop.is_set():
            self.heartbeat()
            if self.redis.get(self.keys.shutdown):
                break
            outcome = self.serve_one(block_s=block_s)
            if outcome == "wait":
                self.wait_for_slot(timeout=0.5)
            if time.monotonic() - last_stats >= stats_every_s:
                self.write_stats()
                last_stats = time.monotonic()

    def shutdown(self, wait: bool = True) -> None:
        """Stop taking work; with *wait*, let running jobs finish and persist first."""
        self.pool.shutdown(wait=wait, cancel_futures=not wait)

    def write_stats(self) -> None:
        payload = dict(self.stats)
        payload["executed_jobs"] = list(self.executed_jobs)
        self.redis.set(self.keys.stats("worker", self.agent.agent_id), json.dumps(payload))
