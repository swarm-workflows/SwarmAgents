"""
Baseline schedulers for CCGrid paper comparison.

Provides GreedyScheduler, RoundRobinScheduler, and RandomScheduler — all
centralized, single-process schedulers that use the same workloads, job model,
and Redis infrastructure as SWARM+ for fair comparison.
"""
import csv
import json
import logging
import os
import random
import time
import threading
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, Future

import redis

from swarm.database.repository import Repository
from swarm.models.job import Job, ObjectState
from swarm.models.capacities import Capacities
from swarm.models.data_node import DataNode
from baselines.agent_sim import SimulatedAgent

logger = logging.getLogger(__name__)


class BaselineScheduler(ABC):
    """Base class for centralized baseline schedulers.

    Handles agent loading, Redis interaction, job execution, and result
    collection.  Subclasses only implement ``assign_jobs()``.
    """

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
        level: int = 0,
        group: int = 0,
        cost_weights: dict | None = None,
        long_job_threshold: float = 20.0,
        connectivity_penalty_factor: float = 1.0,
        timeout: float = 600.0,
    ):
        self.db_host = db_host
        self.db_port = db_port
        self.agent_profiles_path = agent_profiles_path
        self.jobs_dir = jobs_dir
        self.jobs_per_interval = jobs_per_interval
        self.run_dir = run_dir
        self.total_jobs = total_jobs
        self.interval = interval
        self.executor_workers = executor_workers
        self.level = level
        self.group = group
        self.timeout = timeout

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
        self.redis_client: redis.StrictRedis | None = None
        self.repo: Repository | None = None
        self.completed_count = 0
        self.assigned_count = 0
        self.start_time: float = 0.0
        self._futures: list[Future] = []

        os.makedirs(self.run_dir, exist_ok=True)

    # ── Agent Loading ──────────────────────────────────────────────

    def load_agents(self):
        """Load agents from agent_profiles.json."""
        with open(self.agent_profiles_path, "r") as f:
            profiles = json.load(f)

        self.agents = []
        for agent_id_str, profile in profiles.items():
            agent = SimulatedAgent.from_profile(int(agent_id_str), profile)
            self.agents.append(agent)

        self.agents.sort(key=lambda a: a.agent_id)
        logger.info("Loaded %d agents from %s", len(self.agents), self.agent_profiles_path)

    # ── Feasibility & Cost (ported from ResourceAgent) ─────────────

    @staticmethod
    def is_feasible(job: Job, agent: SimulatedAgent) -> bool:
        """Check if job can run on agent: capacity check + DTN connectivity."""
        # Capacity check against available resources
        if not agent.can_fit(job.capacities):
            return False

        # DTN connectivity check
        required_dtns = set()
        for dn in (job.data_in or []):
            required_dtns.add(dn.name)
        for dn in (job.data_out or []):
            required_dtns.add(dn.name)

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

        # DTN connectivity penalty
        required_dtns = set()
        for dn in (job.data_in or []):
            required_dtns.add(dn.name)
        for dn in (job.data_out or []):
            required_dtns.add(dn.name)

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

    # ── Job Execution ──────────────────────────────────────────────

    def _execute_on_agent(self, job: Job, agent: SimulatedAgent):
        """Execute a job on a simulated agent (runs in thread pool)."""
        try:
            job.execute()
        except Exception:
            job._exit_status = 1
            job.state = ObjectState.COMPLETE
            job.mark_completed()
        finally:
            agent.release(job.capacities)

        # Save completed job to Redis
        self.repo.save(
            obj=job.to_dict(),
            key_prefix="job",
            key=str(job.job_id),
            level=self.level,
            group=self.group,
        )
        self.completed_count += 1
        if self.completed_count % 50 == 0:
            logger.info(
                "Progress: %d/%d jobs completed (%.1fs elapsed)",
                self.completed_count,
                self.total_jobs,
                time.time() - self.start_time,
            )

    # ── Abstract Strategy ──────────────────────────────────────────

    @abstractmethod
    def assign_jobs(self, jobs: list[Job]) -> list[tuple[Job, SimulatedAgent]]:
        """Assign a batch of jobs to agents.

        Returns list of (job, agent) pairs for jobs that were assigned.
        Unassigned jobs are returned to the waiting queue by the caller.
        """

    # ── Main Run Loop ──────────────────────────────────────────────

    def run(self):
        """Main scheduling loop.

        1. Start JobDistributor thread for rate-controlled submission
        2. Poll Redis for PENDING jobs
        3. Call assign_jobs() — strategy-specific
        4. Submit assigned jobs to ThreadPoolExecutor
        5. Re-queue unassigned jobs (waiting queue)
        6. Exit when all jobs COMPLETE or timeout
        """
        self.redis_client = redis.StrictRedis(
            host=self.db_host, port=self.db_port, decode_responses=True
        )
        self.repo = Repository(redis_client=self.redis_client)

        # Start job distributor thread
        from job_distributor import JobDistributor

        distributor = JobDistributor(
            redis_host=self.db_host,
            redis_port=self.db_port,
            jobs_dir=self.jobs_dir,
            jobs_per_interval=self.jobs_per_interval,
            interval=self.interval,
            level=self.level,
            group=self.group,
        )
        distributor.daemon = True
        distributor.start()

        self.start_time = time.time()
        self.completed_count = 0
        self.assigned_count = 0
        waiting_queue: list[Job] = []

        executor = ThreadPoolExecutor(max_workers=self.executor_workers)

        logger.info(
            "Starting %s with %d agents, %d jobs (timeout=%.0fs)",
            self.__class__.__name__,
            len(self.agents),
            self.total_jobs,
            self.timeout,
        )

        try:
            while self.completed_count < self.total_jobs:
                elapsed = time.time() - self.start_time
                if elapsed > self.timeout:
                    logger.warning("Timeout reached (%.0fs). Stopping.", elapsed)
                    break

                # Poll Redis for PENDING jobs
                pending_dicts = self.repo.get_all_objects(
                    key_prefix="job",
                    level=self.level,
                    group=self.group,
                    state=ObjectState.PENDING.value,
                )

                new_jobs = []
                for jd in pending_dicts:
                    if isinstance(jd, str):
                        jd = json.loads(jd)
                    job = Job()
                    job.from_dict(jd)
                    job.level = self.level
                    new_jobs.append(job)

                # Combine with waiting queue
                batch = waiting_queue + new_jobs
                waiting_queue = []

                if not batch:
                    time.sleep(0.5)
                    continue

                # Mark selection started for all jobs in batch
                now = time.time()
                for job in batch:
                    if not job.selection_started_at_dict:
                        job.mark_selection_started()

                # Strategy-specific assignment
                assignments = self.assign_jobs(batch)
                assigned_ids = {job.job_id for job, _ in assignments}

                # Jobs that weren't assigned go back to waiting queue
                for job in batch:
                    if job.job_id not in assigned_ids:
                        waiting_queue.append(job)

                # Execute assigned jobs
                for job, agent in assignments:
                    job.mark_assigned()
                    job.leader_id = agent.agent_id
                    job.state = ObjectState.READY

                    # Save assignment to Redis
                    self.repo.save(
                        obj=job.to_dict(),
                        key_prefix="job",
                        key=str(job.job_id),
                        level=self.level,
                        group=self.group,
                    )

                    # Allocate capacity and submit to thread pool
                    agent.allocate(job.capacities)
                    self.assigned_count += 1
                    executor.submit(self._execute_on_agent, job, agent)

                time.sleep(0.5)

        finally:
            distributor.shutdown_flag.set()
            executor.shutdown(wait=True)

        elapsed = time.time() - self.start_time
        logger.info(
            "Finished: %d/%d completed, %d assigned in %.1fs (makespan)",
            self.completed_count,
            self.total_jobs,
            self.assigned_count,
            elapsed,
        )

    # ── Result Collection ──────────────────────────────────────────

    def save_results(self):
        """Save all jobs to CSV and metrics to JSON."""
        from plotting.data import save_jobs

        # Fetch all completed jobs from Redis
        all_dicts = self.repo.get_all_objects(
            key_prefix="job", level=self.level, group=self.group, state=None
        )
        save_jobs(all_dicts, self.run_dir)

        # Save metrics summary
        elapsed = time.time() - self.start_time
        metrics = {
            "scheduler": self.__class__.__name__,
            "agents": len(self.agents),
            "total_jobs": self.total_jobs,
            "completed_jobs": self.completed_count,
            "assigned_jobs": self.assigned_count,
            "makespan_seconds": round(elapsed, 2),
            "agent_job_counts": {
                a.agent_id: a.jobs_completed for a in self.agents
            },
        }
        metrics_path = os.path.join(self.run_dir, "metrics.json")
        with open(metrics_path, "w") as f:
            json.dump(metrics, f, indent=2)

        logger.info("Results saved to %s", self.run_dir)


class GreedyScheduler(BaselineScheduler):
    """Greedy min-cost scheduler: assigns each job to the lowest-cost feasible agent."""

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
                assignments.append((job, best_agent))
                # Greedy-commit: allocate immediately so next job sees updated capacity
                best_agent.allocate(job.capacities)

        # Undo the pre-allocations — they'll be re-done in the run loop
        for job, agent in assignments:
            agent.release(job.capacities)

        return assignments


class RoundRobinScheduler(BaselineScheduler):
    """Round-robin scheduler: rotates through agents, picking first feasible."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._rr_index = 0

    def assign_jobs(self, jobs: list[Job]) -> list[tuple[Job, SimulatedAgent]]:
        assignments = []
        n_agents = len(self.agents)

        for job in jobs:
            assigned = False
            for _ in range(n_agents):
                agent = self.agents[self._rr_index % n_agents]
                self._rr_index += 1

                if self.is_feasible(job, agent):
                    assignments.append((job, agent))
                    assigned = True
                    break

            # If no agent is feasible, job stays unassigned (goes to waiting queue)

        return assignments


class RandomScheduler(BaselineScheduler):
    """Random scheduler: picks a random feasible agent for each job."""

    def assign_jobs(self, jobs: list[Job]) -> list[tuple[Job, SimulatedAgent]]:
        assignments = []

        for job in jobs:
            feasible = [a for a in self.agents if self.is_feasible(job, a)]
            if feasible:
                agent = random.choice(feasible)
                assignments.append((job, agent))

        return assignments
