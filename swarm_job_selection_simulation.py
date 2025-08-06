import csv
import os
import statistics
import threading
import time
from collections import defaultdict
from typing import Any

import numpy as np
from matplotlib import pyplot as plt

import swarm.utils.queues
from swarm.models.capacities import Capacities
import random

from swarm.models.job import Job, JobState

completed_jobs = []  # global or pass into algo
load_history = defaultdict(list)  # {agent_id: [(timestamp, load), ...]}

output_dir = "../output"
os.makedirs(output_dir, exist_ok=True)


class JobGenerator:
    """
    Generates random jobs and stores them in Redis or as JSON files.
    """

    def __init__(self, job_count: int = 0) -> None:
        """
        Initialize the JobGenerator.

        :param host: Redis host
        :param port: Redis port
        :param job_count: Number of jobs to generate
        """
        self.job_count = job_count

    def generate_job(self, x: int) -> dict[str, Any]:
        """
        Generate a single job dictionary with randomized fields.

        :param x: Job index (used as job ID)
        :return: Dictionary representing a job
        """
        job_id = str(x)
        execution_time = round(random.uniform(0.1, 10.0), 2)
        core = round(random.uniform(0.1, 4.0), 2)
        ram = round(random.uniform(0.1, 4.0), 2)
        disk = round(random.uniform(0.1, 4.0), 2)
        status = random.choice([0, -1])

        remote_ips = ['192.158.2.1', '192.158.1.2', '192.158.4.2']
        input_files = ['/var/tmp/outgoing/file100M.txt', '/var/tmp/outgoing/file500M.txt', '/var/tmp/outgoing/file1G.txt']
        output_files = ['/var/tmp/outgoing/file100M.txt', '/var/tmp/outgoing/file500M.txt', '/var/tmp/outgoing/file1G.txt']

        data_in = [
            {'remote_ip': random.choice(remote_ips),
             'remote_file': random.choice(input_files),
             'remote_user': 'root'}
            for _ in range(random.randint(0, 3))
        ]
        data_out = [
            {'remote_ip': random.choice(remote_ips),
             'remote_file': random.choice(output_files),
             'remote_user': 'root'}
            for _ in range(random.randint(0, 3))
        ]

        return {
            'id': job_id,
            'execution_time': execution_time,
            'capacities': {'core': core, 'ram': ram, 'disk': disk},
            'data_in': data_in,
            'data_out': data_out,
            'status': status
        }


class Agent:
    def __init__(self, agent_id: int, capacities: Capacities):
        self.agent_id = agent_id
        self.capacities = capacities
        self.select_queue = swarm.utils.queues.SimpleJobQueue()
        self.load = 0

    @property
    def capacity_allocations(self):
        return self.select_queue.capacities(self.select_queue.get_jobs())


class SelectionAlgo:
    def __init__(self, agent_count: int):
        self.agents = {}
        for x in range(1, agent_count):
            capacity = Capacities(core=self.random_capacity(1,8),
                                  gpu=self.random_capacity(1, 8),
                                  ram=self.random_capacity(8, 64),
                                  disk=self.random_capacity(100, 500))
            agent = Agent(agent_id=x,
                          capacities=capacity)
            self.agents[x] = agent
        self.jobs = swarm.utils.queues.SimpleJobQueue()

    @staticmethod
    def random_capacity(min_val, max_val):
        return random.randint(min_val, max_val)

    @staticmethod
    def compute_overall_load(agent: Agent):
        allocated_caps = Capacities()
        allocated_caps += agent.select_queue.capacities(agent.select_queue.get_jobs())

        return SelectionAlgo.resource_usage_score(allocated=allocated_caps, total=agent.capacities)

    @staticmethod
    def resource_usage_score(allocated: Capacities, total: Capacities):
        if allocated == total:
            return 0
        core = (allocated.core / total.core) * 100
        ram = (allocated.ram / total.ram) * 100
        disk = (allocated.disk / total.disk) * 100

        return round((core + ram + disk) / 3, 2)

    @staticmethod
    def compute_cost_matrix(agents: dict[int, Agent], jobs: list[Job]) -> np.ndarray:
        """
        Compute the cost matrix where rows represent agents and columns represent jobs.
        Each entry [i, j] represents the cost of agent i executing job j, scaled by projected load.
        """
        agent_ids = list(agents.keys())  # Not guaranteed to be [0,1,2,...]
        num_agents = len(agent_ids)
        num_jobs = len(jobs)

        cost_matrix = np.full((num_agents, num_jobs), float('inf'))

        for row_idx, agent_id in enumerate(agent_ids):
            agent = agents[agent_id]
            projected_load = agent.load
            for j, job in enumerate(jobs):
                feasibility = SelectionAlgo.is_job_feasible(
                    total=agent.capacities,
                    job=job,
                    allocated_caps=agent.capacity_allocations,
                    projected_load=projected_load
                )
                if feasibility:
                    job_cost = SelectionAlgo.compute_job_cost(job, total=agent.capacities)
                    cost_matrix[row_idx, j] = round(job_cost * (1 + projected_load / 100), 2)
                    projected_load += job_cost

        return cost_matrix

    @staticmethod
    def find_min_cost_agents(agents: dict[int, Agent], cost_matrix: np.ndarray) -> list[tuple[int, float]]:
        """
        Find the agents with the minimum cost for each job.
        In case of ties, select the agent with the smallest (agent_id, cost) tuple.
        """
        min_cost_agents = []
        agent_ids = list(agents.keys())

        for j in range(cost_matrix.shape[1]):
            valid_costs = cost_matrix[:, j]
            finite_mask = np.isfinite(valid_costs)
            if not np.any(finite_mask):
                continue

            min_cost = np.min(valid_costs[finite_mask])
            threshold = min_cost + 5.0  # Tunable
            candidates = [(agent_ids[i], valid_costs[i]) for i in np.where(valid_costs <= threshold)[0]]
            selected_agent = min(candidates, key=lambda x: (x[1], x[0]))
            min_cost_agents.append(selected_agent)

        return min_cost_agents

    def distribute_jobs(self):
        """
        Select feasible agents and assign jobs to their select queues based on cost.
        This function clears the central job queue once jobs are assigned.
        """
        jobs = self.jobs.get_jobs(states=[JobState.PENDING], count=100)
        if not jobs:
            return

        cost_matrix = SelectionAlgo.compute_cost_matrix(self.agents, jobs)
        assignments = SelectionAlgo.find_min_cost_agents(self.agents, cost_matrix)

        for j, (agent_id, cost) in enumerate(assignments):
            job = jobs[j]
            agent = self.agents.get(agent_id)
            if agent:
                job.set_leader(agent.agent_id)
                job.change_state(JobState.READY)
                completed_jobs.append(job)
                agent.select_queue.add_job(job)
                self.jobs.remove_job(job.get_job_id())

    @staticmethod
    def is_job_feasible(job: Job, total: Capacities, projected_load: float,
                        proposed_caps: Capacities = Capacities(),
                        allocated_caps: Capacities = Capacities()) -> bool:
        if projected_load >= 300:
            return False
        allocated_caps += proposed_caps
        available = total - allocated_caps
        return SelectionAlgo._has_sufficient_capacity(job, available)

    @staticmethod
    def _has_sufficient_capacity(job: Job, available: Capacities) -> bool:
        """
        Returns True if the job can be accommodated by the available capacity.
        Also checks data transfer feasibility if enabled.
        """
        # Check if job fits into available capacities
        residual = available - job.get_capacities()
        negative_fields = residual.negative_fields()
        if len(negative_fields) > 0:
            return False
        return True

    @staticmethod
    def compute_job_cost(job: Job, total: Capacities) -> float:
        """
        Computes job cost as time-weighted resource usage across core, RAM, and disk.

        Cost = Avg[(usage % of each resource) × execution_time]

        :param job: The job whose cost is to be computed.
        :param total: The total available resources on the agent.
        :return: Cost score (float)
        """
        if not total or total.core == 0 or total.ram == 0 or total.disk == 0:
            return float('inf')  # Avoid divide-by-zero or uninitialized capacities

        time_factor = max(1, getattr(job, "execution_time", 1))  # default to 1 if not set

        core_load = (job.capacities.core / total.core) * 100 * time_factor
        ram_load = (job.capacities.ram / total.ram) * 100 * time_factor
        disk_load = (job.capacities.disk / total.disk) * 100 * time_factor

        cost = round((core_load + ram_load + disk_load) / 3, 2)

        # Add a tunable penalty for longer jobs to prioritize short jobs when feasible
        #penalty_factor = math.log(1 + job.execution_time)
        #cost *= penalty_factor
        return round(cost, 2)


def job_producer_thread(algo: SelectionAlgo, job_rate: int = 20):
    generator = JobGenerator()
    job_index = 0
    sleep_time = 1.0 / job_rate

    while True:
        job_dict = generator.generate_job(job_index)
        job = Job()
        job.from_dict(job_dict)
        algo.jobs.add_job(job)
        job_index += 1
        time.sleep(sleep_time)


def distributor_thread(algo: SelectionAlgo, interval: float = 1.0):
    while True:
        algo.distribute_jobs()
        time.sleep(interval)


def metrics_printer_thread(algo: SelectionAlgo, interval: float = 5.0):
    while True:
        print("\n[METRICS]")
        now = time.time()
        for agent_id, agent in algo.agents.items():
            queue_size = agent.select_queue.size()
            load_history[agent_id].append((now, agent.load))
            print(f"  Agent-{agent_id} | Load: {agent.load:.2f} | Queue Size: {queue_size} | Capacities: {agent.capacities}")
        print("-" * 40)
        time.sleep(interval)


def agent_executor_thread(agent: Agent):
    while True:
        jobs = agent.select_queue.get_jobs()
        if not jobs:
            time.sleep(0.5)
            continue

        job = jobs[0]
        agent.select_queue.remove_job(job.get_job_id())
        agent.load = SelectionAlgo.compute_overall_load(agent=agent)
        exec_time = getattr(job, "execution_time", 1)
        time.sleep(exec_time)

        #agent.load -= SelectionAlgo.compute_job_cost(job, total=agent.capacities)
        #agent.load = max(agent.load, 0)  # Avoid negatives


# TODO instead of centralized distribute_jobs, each agent checks pending jobs, has access to global agents and jobs
# Each agent computes cost matrix and if identifies itself as the lowest cost, adds entries to job_proposals,
# rest of the agents should check the proposal, if they accept, incremente prepare count by 1
# once prepare count > agent_count/2 +1, leader_agent should increment commit 0,
# rest of agents if they accept should increment commit count
# once commit count > agent_count/2 +1, leader_agent changes job to READY and removes from jobs queuue

proposal_lock = threading.RLock()
job_proposals = defaultdict(lambda: {"leader": None, "prepares": set(), "commits": set()})


def agent_selection_thread(agent: Agent, algo: SelectionAlgo):
    agent_ids = list(algo.agents.keys())
    n_agents = len(agent_ids)
    quorum_threshold = (n_agents // 2) + 1

    while True:
        pending_jobs = algo.jobs.get_jobs(states=[JobState.PENDING], count=20)
        if not pending_jobs:
            time.sleep(0.5)
            continue

        # Step 1: Compute cost matrix ONCE for all agents and jobs
        cost_matrix = SelectionAlgo.compute_cost_matrix(algo.agents, pending_jobs)

        # Step 2: Use existing helper to get the best agent per job
        assignments = SelectionAlgo.find_min_cost_agents(algo.agents, cost_matrix)

        # Step 3: If this agent is assigned, start proposal
        for job_idx, (selected_agent_id, _) in enumerate(assignments):
            if selected_agent_id == agent.agent_id:
                job = pending_jobs[job_idx]
                with proposal_lock:
                    if job.get_job_id() not in job_proposals:
                        job_proposals[job.get_job_id()] = {
                            "leader": agent.agent_id,
                            "prepares": {agent.agent_id},
                            "commits": set()
                        }

        # Step 4: Handle prepare/commit as before
        with proposal_lock:
            for job_id, proposal in list(job_proposals.items()):
                leader_id = proposal["leader"]
                job = algo.jobs.get_job(job_id)
                if not job:
                    continue

                # Accept prepare if feasible
                if leader_id != agent.agent_id:
                    #if SelectionAlgo.is_job_feasible(job, agent.capacities, agent.load):
                    proposal["prepares"].add(agent.agent_id)

                # Leader moves to commit on quorum
                if leader_id == agent.agent_id and len(proposal["prepares"]) >= quorum_threshold:
                    proposal["commits"].add(agent.agent_id)

                # Others commit if leader commits
                if leader_id != agent.agent_id and leader_id in proposal["commits"]:
                    proposal["commits"].add(agent.agent_id)

                # Finalize on commit quorum
                if leader_id == agent.agent_id and len(proposal["commits"]) >= quorum_threshold:
                    job.set_leader(leader_id)
                    job.change_state(JobState.READY)
                    completed_jobs.append(job)
                    agent.select_queue.add_job(job)
                    algo.jobs.remove_job(job.get_job_id())
                    del job_proposals[job_id]

        time.sleep(0.5)


if __name__ == '__main__':
    output_dir = "../output"
    algo = SelectionAlgo(agent_count=10)

    producer = threading.Thread(target=job_producer_thread, args=(algo, 20), daemon=True)
    #distributor = threading.Thread(target=distributor_thread, args=(algo, 1.0), daemon=True)
    metrics = threading.Thread(target=metrics_printer_thread, args=(algo, 5.0), daemon=True)

    producer.start()
    #distributor.start()
    metrics.start()

    # One selection thread per agent
    for agent in algo.agents.values():
        t = threading.Thread(target=agent_selection_thread, args=(agent, algo), daemon=True)
        t.start()

    # One executor thread per agent
    for agent in algo.agents.values():
        t = threading.Thread(target=agent_executor_thread, args=(agent,), daemon=True)
        t.start()

    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        print("Shutting down.")

    # --- Ensure output directory ---
    output_dir = "../output"
    os.makedirs(output_dir, exist_ok=True)

    # --- Save Completed Jobs CSV ---
    completed_jobs_csv = os.path.join(output_dir, "completed_jobs.csv")
    with open(completed_jobs_csv, mode="w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "job_id", "leader_agent_id", "created_at", "selected_by_agent_at",
            "scheduled_at", "completed_at", "selection_latency", "execution_latency", "total_latency"
        ])
        for job in completed_jobs:
            selection_latency = (job.selected_by_agent_at - job.created_at) if job.selected_by_agent_at else None
            execution_latency = (job.completed_at - job.scheduled_at) if job.completed_at and job.scheduled_at else None
            total_latency = (job.completed_at - job.created_at) if job.completed_at else None
            writer.writerow([
                job.get_job_id(), job.get_leader_agent_id(),
                job.created_at, job.selected_by_agent_at, job.scheduled_at, job.completed_at,
                selection_latency, execution_latency, total_latency
            ])

    # --- Save Agent Load History CSV ---
    load_history_csv = os.path.join(output_dir, "agent_load_history.csv")
    with open(load_history_csv, mode="w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "agent_id", "load"])
        for agent_id, series in load_history.items():
            for ts, load in series:
                writer.writerow([ts, agent_id, load])

    # --- Calculate latency stats ---
    latencies = [
        job.selected_by_agent_at - job.created_at
        for job in completed_jobs if job.selected_by_agent_at
    ]
    if latencies:
        avg_latency = statistics.mean(latencies)
        max_latency = max(latencies)
        print(f"Average selection latency: {avg_latency:.2f} s")
        print(f"Maximum selection latency: {max_latency:.2f} s")
    else:
        print("No selection latencies recorded.")

    # --- Plot Job Distribution ---
    job_counts = defaultdict(int)
    for job in completed_jobs:
        job_counts[job.get_leader_agent_id()] += 1
    plt.figure(figsize=(10, 6))
    plt.bar([f"Agent-{aid}" for aid in job_counts.keys()], job_counts.values())
    plt.xlabel("Leader Agent ID")
    plt.ylabel("Jobs Completed")
    plt.title("Job Distribution by Leader Agent")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "job_distribution.png"))
    plt.close()

    # --- Plot Latency Histogram ---
    if latencies:
        plt.figure(figsize=(10, 6))
        plt.hist(latencies, bins=20)
        plt.xlabel("Selection Latency (s)")
        plt.ylabel("Frequency")
        plt.title(f"Job Selection Latency Distribution Avg: {avg_latency:.2f} s Max: {max_latency:.2f} s")
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, "selection_latency_histogram.png"))
        plt.close()

    # --- Plot Agent Load Over Time ---
    plt.figure(figsize=(10, 6))
    for aid, series in load_history.items():
        times, loads = zip(*series)
        plt.plot([t - times[0] for t in times], loads, label=f"Agent-{aid}")
    plt.xlabel("Time (s)")
    plt.ylabel("Load (%)")
    plt.title("Agent Load Over Time")
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "agent_load_over_time.png"))
    plt.close()

    print(f"Saved CSVs and plots in {output_dir}")