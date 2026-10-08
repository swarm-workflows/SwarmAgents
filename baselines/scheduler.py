"""
Centralized baseline schedulers for the E7 comparison: Greedy (min analytic cost),
Round-Robin and Random. One process decides every placement; execution happens on the SWARM
cell's own level-0 fleet — remote workers on the agent hosts in a slice run
(`baselines/run_baseline_remote.py`, `baselines/baseline_worker.py`), per-agent thread pools
in a local smoke run (`baselines/run_baseline.py`).

Everything that is not the placement decision is SWARM's own and comes from
`baselines/common.py`: the level-0 fleet, the wall-time clamp, `executor_workers` per agent,
the workflow DAG gate and the completion write that publishes outputs. Fixed 2026-10-07 —
before then the remote worker ran one job at a time while the scheduler admitted several per
agent, coordinator slots executed at Hier-N, Round-Robin and Random could over-commit an agent
inside one batch, the wall-time clamp and the DAG were ignored, and `metrics.json` was not keyed
by agent (the collector read `agent_job_counts` as an agent).

The scheduler learns about a completion the way it would in a real deployment: through the
store, one poll after the worker wrote it. Assignments reach a worker through a run-scoped
Redis list (`baseline:<run>:q:<agent>`), which is the same star through `database` that
SWARM's job pool and the Sparrow arm's probes use.
"""
import json
import logging
import os
import random
import threading
import time
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import redis

from swarm.database.repository import Repository
from swarm.models.job import Job, ObjectState
from swarm.models.capacities import Capacities
from baselines.agent_sim import SimulatedAgent
from baselines.common import (GROUP, LEVEL, data_ready, keyed_metrics, load_level0_fleet,
                              read_reports, run_job)
from baselines.sparrow import Keys

logger = logging.getLogger(__name__)

PENDING = ObjectState.PENDING.value
COMPLETE = ObjectState.COMPLETE.value

# A store unreadable for this long ends the run (exit 4), as for SWARM and Sparrow.
REDIS_UNREADABLE_S = 60.0


class BaselineScheduler(ABC):
    """Base class for centralized baseline schedulers.

    Handles the fleet, the store, dispatch, execution (local mode) and result collection.
    Subclasses implement ``assign_jobs()`` only, and must reserve capacity on the agent they
    pick (``self._reserve``) before considering the next job of the batch.
    """

    POLICY = ""          # collect_meta.json `policy`

    def __init__(
        self,
        db_host: str,
        db_port: int,
        agent_profiles_path: str,
        jobs_dir: str,
        jobs_per_interval: int,
        run_dir: str,
        total_jobs: int,
        interval: float = 1.0,
        executor_workers: int = 10,
        level: int = LEVEL,
        group: int = GROUP,
        cost_weights: dict | None = None,
        long_job_threshold: float = 20.0,
        connectivity_penalty_factor: float = 1.0,
        timeout: float = 600.0,
        remote: bool = False,
        run_id: str | None = None,
        agents: int | None = None,
        redis_client=None,
    ):
        self.db_host = db_host
        self.db_port = db_port
        self.agent_profiles_path = agent_profiles_path
        self.jobs_dir = jobs_dir
        self.jobs_per_interval = jobs_per_interval
        self.run_dir = run_dir
        self.total_jobs = total_jobs
        self.interval = interval
        self.executor_workers = max(1, int(executor_workers))
        self.level = level
        self.group = group
        self.timeout = timeout
        self.remote = remote
        self.run_id = run_id or os.environ.get("SWARM_RUN_ID") or ""
        self.fleet_size = agents
        self.keys = Keys(self.run_id, prefix="baseline")

        # Cost function parameters (match ResourceAgent defaults)
        cw = cost_weights or {}
        self.cpu_weight = cw.get("cpu", 0.4)
        self.ram_weight = cw.get("ram", 0.3)
        self.disk_weight = cw.get("disk", 0.2)
        self.gpu_weight = cw.get("gpu", 0.1)
        self.long_job_threshold = long_job_threshold
        self.connectivity_penalty_factor = connectivity_penalty_factor

        # State
        self.agents: list[SimulatedAgent] = []
        self.redis_client = redis_client
        self.repo: Repository | None = None
        self.completed_count = 0
        self.assigned_count = 0
        self.start_time: float = 0.0
        self._completed_lock = threading.Lock()
        self._released: set[str] = set()      # remote: completions whose capacity is freed
        self._unconfirmed: dict = {}          # job_id -> (job, agent): write outcome unknown
        self._undispatched: dict = {}         # job_id -> agent_id: assigned, not yet pushed
        self._executed: dict[int, list] = {}  # local: job ids each agent started
        self._agent_stats: dict[int, dict] = {}
        self.stats = {"assigned": 0, "gated": 0, "unplaced_passes": 0,
                      "assign_refused": 0, "dispatched": 0}
        self.drain: dict = {"status": None}
        self.exit_code = 0

        os.makedirs(self.run_dir, exist_ok=True)

    # ── Agent Loading ──────────────────────────────────────────────

    def load_agents(self):
        """The SWARM cell's level-0 fleet from agent_profiles.json — never coordinator slots.

        With ``agents`` given, ids 1..agents (as run_test.py launches them) and the level-0
        subset of those; without it, every level-0 profile."""
        if self.fleet_size:
            self.agents = load_level0_fleet(self.agent_profiles_path, int(self.fleet_size))
        else:
            with open(self.agent_profiles_path, "r") as f:
                profiles = json.load(f)
            self.agents = [SimulatedAgent.from_profile(int(aid), prof)
                           for aid, prof in profiles.items()
                           if int(prof.get("level") or 0) == 0]
        self.agents.sort(key=lambda a: a.agent_id)
        self._executed = {a.agent_id: [] for a in self.agents}
        logger.info("Loaded %d level-0 agents from %s", len(self.agents), self.agent_profiles_path)

    # ── Feasibility & Cost (ported from ResourceAgent) ─────────────

    @staticmethod
    def is_feasible(job: Job, agent: SimulatedAgent) -> bool:
        """Check if job can run on agent: capacity check + DTN connectivity."""
        # Capacity check against available resources
        if not agent.can_fit(job.capacities):
            return False

        # DTN connectivity check. `Job.required_dtns()` is the one definition of that set and
        # excludes `local`, which is a Pegasus *site* rather than a data transfer node. This
        # was ported from `ResourceAgent` without the exclusion, so a `--dtn-names local`
        # bundle was infeasible everywhere and E7 could not run a converted workflow at all
        # (code review §11). Campaign jobs carry real DTN names, so it bit only workflow input.
        required_dtns = job.required_dtns()

        if required_dtns:
            agent_dtn_names = set(agent.dtns.keys())
            if not required_dtns.issubset(agent_dtn_names):
                return False

        return True

    def compute_cost(self, job: Job, agent: SimulatedAgent) -> float:
        """Compute scheduling cost — identical formula to ResourceAgent.compute_job_cost."""
        total = agent.capacities

        if total.core <= 0 or total.ram <= 0 or total.disk <= 0:
            return float("inf")

        # Load base weights
        cpu_weight = self.cpu_weight
        ram_weight = self.ram_weight
        disk_weight = self.disk_weight
        gpu_weight = self.gpu_weight
        long_job_threshold = self.long_job_threshold
        conn_penalty_factor = self.connectivity_penalty_factor

        # Dynamic tuning based on job type
        job_type = job.job_type or ""

        if "cpu_bound" in job_type:
            cpu_weight *= 1.5
            ram_weight *= 0.7
            disk_weight *= 0.7
            gpu_weight *= 0.7
        elif "ram_bound" in job_type:
            ram_weight *= 1.5
            cpu_weight *= 0.7
            disk_weight *= 0.7
            gpu_weight *= 0.7
        elif "disk_bound" in job_type:
            disk_weight *= 1.5
            cpu_weight *= 0.7
            ram_weight *= 0.7
            gpu_weight *= 0.7
        elif "gpu_bound" in job_type:
            gpu_weight *= 1.5
            cpu_weight *= 0.7
            ram_weight *= 0.7
            disk_weight *= 0.7

        # Normalize weights to sum to 1.0
        w_sum = cpu_weight + ram_weight + disk_weight + gpu_weight
        if w_sum > 0:
            cpu_weight /= w_sum
            ram_weight /= w_sum
            disk_weight /= w_sum
            gpu_weight /= w_sum

        if "long" in job_type:
            long_job_threshold = max(5.0, long_job_threshold * 0.75)
        elif "short" in job_type:
            long_job_threshold *= 1.5

        if "dtn_heavy" in job_type:
            conn_penalty_factor *= 1.5
        elif "dtn_light" in job_type:
            conn_penalty_factor *= 0.5

        # Resource ratios
        core_ratio = job.capacities.core / total.core
        ram_ratio = job.capacities.ram / total.ram
        disk_ratio = job.capacities.disk / total.disk

        total_gpu = total.gpu if total.gpu > 0 else 1
        job_gpu = job.capacities.gpu if job.capacities.gpu else 0
        gpu_ratio = job_gpu / total_gpu

        # Base score (weighted sum)
        base_score = (
            cpu_weight * core_ratio
            + ram_weight * ram_ratio
            + disk_weight * disk_ratio
            + gpu_weight * gpu_ratio
        )

        # Bottleneck penalty
        bottleneck_penalty = max(core_ratio, ram_ratio, disk_ratio, gpu_ratio) ** 2

        # Time penalty
        wall_time = job.wall_time or 0.0
        if wall_time > long_job_threshold:
            time_penalty = 1.5 + (wall_time - long_job_threshold) / long_job_threshold
        else:
            time_penalty = 1 + (wall_time / long_job_threshold) ** 2

        # DTN connectivity penalty. Same single definition as feasibility above — this block
        # had the agent's §8 defect too, scoring `local` at 0.0 on every agent and doubling
        # every cost, which would have made the baseline's costs incomparable with the
        # scheduler's on exactly the workflow cells E7 compares them on.
        required_dtns = job.required_dtns()

        if required_dtns:
            agent_dtn_scores = {
                name: dn.connectivity_score for name, dn in agent.dtns.items()
            }
            scores = [agent_dtn_scores.get(dtn, 0.0) for dtn in required_dtns]
            avg_conn = sum(scores) / len(scores)
        else:
            avg_conn = 1.0

        connectivity_penalty = 1 + conn_penalty_factor * (1 - avg_conn)

        # Final cost
        cost = (base_score + bottleneck_penalty) * time_penalty * connectivity_penalty * 100
        return round(cost, 2)

    # ── Strategy helpers ───────────────────────────────────────────

    @staticmethod
    def _reserve(agent: SimulatedAgent, job: Job) -> None:
        """Commit *job*'s capacity on *agent* now, so the next job of the same batch sees it.
        Without this a batch could place several jobs on one agent that each fit alone."""
        agent.allocate(job.capacities)

    # ── Abstract Strategy ──────────────────────────────────────────

    @abstractmethod
    def assign_jobs(self, jobs: list[Job]) -> list[tuple[Job, SimulatedAgent]]:
        """Assign a batch of jobs to agents, reserving capacity for each pick.

        Returns list of (job, agent) pairs for jobs that were assigned.
        Unassigned jobs stay in the waiting queue for the next pass.
        """

    # ── Execution (local mode) ─────────────────────────────────────

    def _execute_local(self, job: Job, agent: SimulatedAgent):
        """Run one job as *agent* in its own pool (local smoke runs)."""
        try:
            run_job(self.repo, job, agent.agent_id, self._executed[agent.agent_id])
        except Exception as e:
            logger.warning("Job %s could not be persisted: %s", job.job_id, e)
        finally:
            agent.release(job.capacities)
            with self._completed_lock:
                self.completed_count += 1

    # ── Completion tracking (remote mode) ──────────────────────────

    def _poll_completions(self) -> int:
        """COMPLETE count from the store; frees capacity for completions not seen before."""
        ids = self.repo.get_all_ids_multi(key_prefix=Repository.KEY_JOB, level=self.level,
                                          group=self.group, states=[COMPLETE]).get(COMPLETE, [])
        new = [j for j in ids if j not in self._released]
        if new:
            agent_map = {a.agent_id: a for a in self.agents}
            for jid, rec in self.repo.get_many(new, key_prefix=Repository.KEY_JOB,
                                               level=self.level, group=self.group).items():
                self._released.add(jid)
                leader, caps = rec.get("leader_id"), rec.get("capacities")
                agent = agent_map.get(int(leader)) if leader is not None else None
                if agent is not None and caps:
                    agent.release(Capacities.from_dict(caps))
        return len(ids)

    # ── Main Run Loop ──────────────────────────────────────────────

    def _fetch_new_pending(self, waiting: dict, seen: set) -> None:
        ids = self.repo.get_all_ids_multi(key_prefix=Repository.KEY_JOB, level=self.level,
                                          group=self.group, states=[PENDING]).get(PENDING, [])
        new = [j for j in ids if j not in seen and j not in waiting]
        if not new:
            return
        for jid, rec in self.repo.get_many(new, key_prefix=Repository.KEY_JOB, level=self.level,
                                           group=self.group).items():
            if rec.get("state") != PENDING:
                continue
            job = Job()
            job.from_dict(rec)
            job.level = self.level
            waiting[jid] = job

    def _commit(self, job: Job, agent: SimulatedAgent, executors) -> str:
        """Persist the assignment, then hand the job to its agent. Returns:

        * ``ok`` — written; the job is dispatched, or owed a dispatch (`_undispatched`);
        * ``refused`` — the record is no longer PENDING, the job is not ours to place; the
          reservation is freed;
        * ``uncertain`` — the store raised. That does NOT mean nothing was written: EXEC can
          commit and its reply be lost. The job keeps its reservation and goes to
          `_unconfirmed`, and `_reconcile` reads the record to settle it.
        """
        job.mark_assigned()
        job.leader_id = agent.agent_id
        job.state = ObjectState.READY
        try:
            ok = self.repo.save(obj=job.to_dict(), key_prefix=Repository.KEY_JOB,
                                level=self.level, group=self.group,
                                precondition=lambda cur: bool(cur)
                                and cur.get("state") == PENDING)
        except (redis.RedisError, RuntimeError) as exc:
            logger.warning("assignment of %s unconfirmed: %s", job.job_id, exc)
            self._unconfirmed[job.job_id] = (job, agent)
            return "uncertain"
        if not ok:
            # A refusal is not proof either: Repository.save retries a WatchError, which the
            # client also raises when the connection drops after EXEC landed — the retry then
            # finds the record READY (our own write) and refuses. Read it before giving up.
            try:
                rec = self.repo.get(job.job_id, key_prefix=Repository.KEY_JOB,
                                    level=self.level, group=self.group)
            except redis.RedisError:
                self._unconfirmed[job.job_id] = (job, agent)
                return "uncertain"
            if self._is_our_write(rec, job, agent):
                self.stats["assign_reply_lost"] = self.stats.get("assign_reply_lost", 0) + 1
                self._confirmed(job, agent, executors)
                return "ok"
            agent.release(job.capacities, completed=False)
            self.stats["assign_refused"] += 1
            return "refused"
        self._confirmed(job, agent, executors)
        return "ok"

    @staticmethod
    def _is_our_write(rec, job: Job, agent: SimulatedAgent) -> bool:
        """Whether *rec* is the assignment this attempt wrote: same agent, past PENDING, and
        carrying this attempt's own `assigned_at` stamp — so an older assignment of the same
        job to the same agent is never mistaken for it."""
        if not rec or str(rec.get("leader_id")) != str(agent.agent_id) \
                or rec.get("state") == PENDING:
            return False
        stamp = job.assigned_at
        stored = rec.get("assigned_at")
        values = stored.values() if isinstance(stored, dict) else [stored]
        return any(v is not None and abs(float(v) - float(stamp)) < 1e-6 for v in values)

    def _confirmed(self, job: Job, agent: SimulatedAgent, executors) -> None:
        """The assignment is in the store: count it and owe its agent the job."""
        self.assigned_count += 1
        self.stats["assigned"] += 1
        if self.remote:
            self._undispatched[job.job_id] = agent.agent_id
            self._flush_dispatches()
        else:
            executors[agent.agent_id].submit(self._execute_local, job, agent)

    def _flush_dispatches(self) -> None:
        """Push every owed dispatch. One that fails stays owed (and raises, so the caller's
        unreadable-store clock runs) — a READY job its worker never hears of is stranded, and
        dropping it would let the run end looking healthy."""
        for jid, aid in list(self._undispatched.items()):
            self.redis_client.rpush(self.keys.queue(aid), str(jid))
            del self._undispatched[jid]
            self.stats["dispatched"] += 1

    def _reconcile(self, waiting: dict, seen: set, executors) -> None:
        """Settle assignments whose write raised, by reading what the store holds. Raises on a
        store error; the jobs stay unconfirmed and keep their reservations meanwhile."""
        if not self._unconfirmed:
            return
        ids = list(self._unconfirmed)
        records = self.repo.get_many(ids, key_prefix=Repository.KEY_JOB, level=self.level,
                                     group=self.group)
        for jid in ids:
            job, agent = self._unconfirmed.pop(jid)
            rec = records.get(jid) or {}
            if rec.get("state") == PENDING:
                # The write never landed: the job is placeable again.
                agent.release(job.capacities, completed=False)
                seen.discard(jid)
                fresh = Job()
                fresh.from_dict(rec)
                fresh.level = self.level
                waiting[jid] = fresh
                self.stats["assign_retried"] = self.stats.get("assign_retried", 0) + 1
            elif self._is_our_write(rec, job, agent):
                # It landed (the reply was lost): this is our assignment.
                self._confirmed(job, agent, executors)
                self.stats["assign_reply_lost"] = self.stats.get("assign_reply_lost", 0) + 1
            else:
                agent.release(job.capacities, completed=False)
                self.stats["assign_refused"] += 1

    # ── Main Run Loop ──────────────────────────────────────────────

    def run(self, start_distributor: bool = True):
        """Main scheduling loop.

        Distribute jobs; each pass, read new PENDING jobs, hold back those whose DAG inputs
        are not ready, let the strategy place the rest, persist and dispatch. Ends when every
        job is COMPLETE (`all_terminal`), on the timeout (`timer`), or when the store has been
        unreadable for a minute (`redis_unreadable`, exit 4). The outcome is in ``self.drain``.
        """
        if self.redis_client is None:
            self.redis_client = redis.StrictRedis(
                host=self.db_host, port=self.db_port, decode_responses=True)
        self.repo = Repository(redis_client=self.redis_client, run_id=self.run_id or None)

        distributor = None
        if start_distributor:
            from job_distributor import JobDistributor
            distributor = JobDistributor(
                redis_host=self.db_host, redis_port=self.db_port, jobs_dir=self.jobs_dir,
                jobs_per_interval=self.jobs_per_interval, interval=self.interval,
                level=self.level, group=self.group)
            distributor.daemon = True
            distributor.start()

        self.start_time = time.time()
        self.completed_count = 0
        self.assigned_count = 0
        waiting: dict[str, Job] = {}
        seen: set[str] = set()
        gated_once: set[str] = set()
        executors = {}
        if not self.remote:
            executors = {a.agent_id: ThreadPoolExecutor(max_workers=self.executor_workers)
                         for a in self.agents}

        logger.info("Starting %s [%s] with %d agents, %d jobs, %d concurrent per agent "
                    "(timeout=%.0fs)", self.__class__.__name__,
                    "REMOTE" if self.remote else "LOCAL", len(self.agents), self.total_jobs,
                    self.executor_workers, self.timeout)

        unreadable_since = None
        try:
            while True:
                if time.time() - self.start_time > self.timeout:
                    logger.warning("Timeout reached (%.0fs). Stopping.", self.timeout)
                    self.drain = {"status": "timer"}
                    break
                try:
                    if self.remote:
                        self.completed_count = self._poll_completions()
                    if self.completed_count >= self.total_jobs:
                        self.drain = {"status": "all_terminal"}
                        break
                    self._reconcile(waiting, seen, executors)
                    self._flush_dispatches()
                    self._fetch_new_pending(waiting, seen)
                    unreadable_since = None
                except redis.RedisError as exc:
                    unreadable_since = unreadable_since or time.time()
                    if time.time() - unreadable_since > REDIS_UNREADABLE_S:
                        logger.error("Store unreadable for %.0f s: %s", REDIS_UNREADABLE_S, exc)
                        self.drain = {"status": "redis_unreadable"}
                        self.exit_code = 4
                        break
                    time.sleep(1.0)
                    continue

                eligible = []
                for jid, job in waiting.items():
                    if not data_ready(self.repo, job):
                        if jid not in gated_once:
                            gated_once.add(jid)
                            self.stats["gated"] += 1
                        continue
                    if not job.selection_started_at_dict:
                        job.mark_selection_started()
                    eligible.append(job)

                if eligible:
                    assignments = self.assign_jobs(eligible)
                    store_failed = False
                    for job, agent in assignments:
                        if store_failed:            # free what this pass reserved, retry later
                            agent.release(job.capacities, completed=False)
                            continue
                        try:
                            outcome = self._commit(job, agent, executors)
                        except redis.RedisError:
                            outcome = "ok"          # written; its dispatch is owed and kept
                            store_failed = True
                        if outcome == "uncertain":
                            store_failed = True
                        waiting.pop(job.job_id, None)
                        seen.add(job.job_id)
                    if store_failed:
                        unreadable_since = unreadable_since or time.time()
                    if len(assignments) < len(eligible):
                        self.stats["unplaced_passes"] += 1
                    if self.assigned_count and self.assigned_count % 50 < len(assignments):
                        logger.info("Progress: %d assigned, %d/%d completed (%.1fs elapsed)",
                                    self.assigned_count, self.completed_count, self.total_jobs,
                                    time.time() - self.start_time)
                time.sleep(0.5)
        finally:
            if distributor is not None:
                distributor.shutdown_flag.set()
            for ex in executors.values():
                ex.shutdown(wait=True)

        if self.remote:
            try:
                self.completed_count = self._poll_completions()
            except redis.RedisError:
                pass
        if self.remote and (self._unconfirmed or self._undispatched):
            try:
                self._reconcile(waiting, seen, executors)
                self._flush_dispatches()
            except redis.RedisError:
                pass
        owed = len(self._unconfirmed) + len(self._undispatched)
        if owed:
            # Jobs the run could not hand to a worker: not a clean ending, whatever the timer
            # says. Exit 4 — a store failure — rather than an `ok_on_cap` that hides them.
            self.drain["undispatched_at_stop"] = len(self._undispatched)
            self.drain["unconfirmed_at_stop"] = len(self._unconfirmed)
            self.exit_code = self.exit_code or 4
            logger.error("%d job(s) never reached a worker (store errors)", owed)
        self.drain["completed_at_stop"] = self.completed_count
        self.drain["inflight_at_stop"] = max(0, self.assigned_count - self.completed_count)
        self.drain["jobs"] = self.total_jobs
        logger.info("Finished: %d/%d completed, %d assigned in %.1fs (%s)",
                    self.completed_count, self.total_jobs, self.assigned_count,
                    time.time() - self.start_time, self.drain["status"])

    # ── Result Collection ──────────────────────────────────────────

    def worker_stats(self) -> dict[int, dict]:
        """Per-agent `{executed_jobs, final, ...}`: the workers' latest stats in a remote run
        (only a `final: true` one is a report — see `keyed_metrics`), this process's own lists
        in a local one, where every pool has been joined by the time results are written."""
        if not self.remote:
            return {a.agent_id: {"executed_jobs": list(self._executed.get(a.agent_id, [])),
                                 "final": True}
                    for a in self.agents}
        return read_reports(self.redis_client,
                            {a.agent_id: self.keys.stats("worker", a.agent_id)
                             for a in self.agents})

    def save_results(self, extra_meta: dict | None = None) -> int:
        """Write what the collector and the comparison plot read, in SWARM's shapes:
        `all_jobs.csv`, `metrics.json` keyed by agent id with each agent's `executed_jobs` (so
        `jobs_executed_twice` is measured), `all_agents.csv` (the level-0 fleet, so fairness
        counts idle agents), `run_meta.json`, `collect_meta.json` and `drain.json`.

        `drain.json` is written first and needs no store; an unreadable store while exporting
        is exit 4 with every file that does not need it still written. Otherwise the exit
        status is the run's own (4 for an unreadable store), else 3 when some agent sent no
        final report, else 0."""
        from plotting.data import save_jobs

        run_dir = Path(self.run_dir)
        ended = time.time()
        drain = dict(self.drain)
        drain.setdefault("ended_at", ended)
        (run_dir / "drain.json").write_text(json.dumps(drain, indent=2))
        exit_code = self.exit_code

        reported = {}
        try:
            save_jobs(self.repo.get_all_objects(key_prefix=Repository.KEY_JOB,
                                                level=self.level, group=self.group, state=None),
                      self.run_dir)
            reported = self.worker_stats()
        except redis.RedisError as exc:
            logger.error("Store unreadable while collecting results: %s", exc)
            exit_code = exit_code or 4

        metrics, missing = keyed_metrics([a.agent_id for a in self.agents], reported,
                                         self.run_id, "baseline")
        (run_dir / "metrics.json").write_text(json.dumps(metrics, indent=2))
        if missing:
            (run_dir / "metrics_shortfall.json").write_text(json.dumps(
                {"missing_agents": missing, "expected": len(self.agents)}, indent=2))
            logger.warning("%d agent(s) sent no final report: %s", len(missing), missing[:10])
            exit_code = exit_code or 3

        (run_dir / "all_agents.csv").write_text(json.dumps([
            {"agent_id": a.agent_id, "level": 0, "group": 0,
             "capacities": a.capacities.to_dict() or {}, "dtns": sorted(a.dtns or {})}
            for a in self.agents], indent=2))
        meta = {
            "run_id": self.run_id, "scheduler": self.POLICY,
            "mode": "remote" if self.remote else "local",
            "agents": self.fleet_size or len(self.agents), "level0_agents": len(self.agents),
            "jobs": self.total_jobs, "executor_workers": self.executor_workers,
            "wall_time": {"scale": Job._WALL_TIME_SCALE, "min_s": Job._WALL_TIME_MIN_S,
                          "max_s": Job._WALL_TIME_MAX_S},
            "profiles": str(Path(self.agent_profiles_path).resolve()),
            "jobs_dir": str(Path(self.jobs_dir).resolve()) if self.jobs_dir else None,
            "started_at": self.start_time, "ended_at": ended,
            "started_at_utc": datetime.fromtimestamp(self.start_time or ended,
                                                     timezone.utc).isoformat(),
            "makespan_seconds": round(ended - self.start_time, 2) if self.start_time else None,
            "completed_jobs": self.completed_count, "assigned_jobs": self.assigned_count,
            "scheduler_stats": dict(self.stats),
        }
        meta.update(extra_meta or {})
        (run_dir / "run_meta.json").write_text(json.dumps(meta, indent=2, default=str))
        (run_dir / "collect_meta.json").write_text(json.dumps(
            {"arm": "baseline", "policy": self.POLICY}, indent=2))
        logger.info("Results saved to %s (exit %d)", self.run_dir, exit_code)
        return exit_code


class GreedyScheduler(BaselineScheduler):
    """Greedy min-cost scheduler: assigns each job to the lowest-cost feasible agent."""

    POLICY = "greedy"

    def assign_jobs(self, jobs: list[Job]) -> list[tuple[Job, SimulatedAgent]]:
        assignments = []
        for job in jobs:
            best_agent = None
            best_cost = float("inf")

            for agent in self.agents:
                if not self.is_feasible(job, agent):
                    continue
                cost = self.compute_cost(job, agent)
                if cost < best_cost:
                    best_cost = cost
                    best_agent = agent

            if best_agent is not None:
                self._reserve(best_agent, job)
                assignments.append((job, best_agent))

        return assignments


class RoundRobinScheduler(BaselineScheduler):
    """Round-robin scheduler: rotates through agents, picking the first feasible one."""

    POLICY = "round_robin"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._rr_index = 0

    def assign_jobs(self, jobs: list[Job]) -> list[tuple[Job, SimulatedAgent]]:
        assignments = []
        n_agents = len(self.agents)

        for job in jobs:
            for _ in range(n_agents):
                agent = self.agents[self._rr_index % n_agents]
                self._rr_index += 1

                if self.is_feasible(job, agent):
                    self._reserve(agent, job)
                    assignments.append((job, agent))
                    break
            # If no agent is feasible, the job stays waiting for the next pass.

        return assignments


class RandomScheduler(BaselineScheduler):
    """Random scheduler: picks a random feasible agent for each job."""

    POLICY = "random"

    def assign_jobs(self, jobs: list[Job]) -> list[tuple[Job, SimulatedAgent]]:
        assignments = []

        for job in jobs:
            feasible = [a for a in self.agents if self.is_feasible(job, a)]
            if feasible:
                agent = random.choice(feasible)
                self._reserve(agent, job)
                assignments.append((job, agent))

        return assignments


SCHEDULER_MAP = {
    "greedy": GreedyScheduler,
    "round-robin": RoundRobinScheduler,
    "random": RandomScheduler,
}
