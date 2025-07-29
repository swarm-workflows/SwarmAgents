import csv
import json
from typing import Any

from swarm.models.job import Job


class Metrics:
    def __init__(self):
        self.load_per_agent = {}
        self.idle_time = []
        self.idle_start_time = None
        self.total_idle_time = 0
        self.restart_job_selection_cnt = 0
        self.conflicts = 0

    def save_load_metric(self, agent_id: int, load: float):
        if agent_id not in self.load_per_agent:
            self.load_per_agent[agent_id] = []
        self.load_per_agent[agent_id].append(load)

    def save_idle_time(self, path: str):
        with open(path, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['Idle Time'])
            for t in self.idle_time:
                writer.writerow([t])

    def save_misc(self, path: str):
        with open(path, 'w') as file:
            json.dump({
                "restarts": self.restart_job_selection_cnt,
                "conflicts": self.conflicts
            }, file, indent=4)

    def save_jobs_per_agent(self, jobs: list[Any], path: str):
        jobs_per_agent = {}
        for job_data in jobs:
            if isinstance(job_data, dict):
                job = Job()
                job.from_dict(job_data)
            else:
                job = job_data
            if job.leader_agent_id is not None:
                jobs_per_agent.setdefault(job.leader_agent_id, {"jobs": [], "job_count": 0})
                jobs_per_agent[job.leader_agent_id]["job_count"] += 1
                jobs_per_agent[job.leader_agent_id]["jobs"].append(job.job_id)

        with open(path, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['Agent ID', 'Number of Jobs Selected', 'Jobs'])
            for agent_id, info in jobs_per_agent.items():
                writer.writerow([agent_id, info['job_count'], info['jobs']])

    def save_scheduling_latency(self, jobs: list[Any], path: str):
        wait_times = {}
        selection_times = {}
        scheduling_latency = {}
        detailed_latency = []  # For combined output

        for job_data in jobs:
            if isinstance(job_data, dict):
                job = Job()
                job.from_dict(job_data)
            else:
                job = job_data

            job_id = job.job_id
            leader_id = getattr(job, "leader_agent_id", "N/A")
            wait_time = selection_time = latency = None

            if job.selection_started_at and job.created_at:
                wait_time = job.selection_started_at - job.created_at
                wait_times[job_id] = wait_time

            if job.selected_by_agent_at and job.selection_started_at:
                selection_time = job.selected_by_agent_at - job.selection_started_at
                selection_times[job_id] = selection_time

            if wait_time is not None and selection_time is not None:
                latency = wait_time + selection_time
                scheduling_latency[job_id] = latency

            # Store all in one row
            detailed_latency.append([
                job_id,
                wait_time if wait_time is not None else 0,
                selection_time if selection_time is not None else 0,
                latency if latency is not None else 0,
                leader_id
            ])

        # Save combined CSV with leader_agent_id
        with open(path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['job_id', 'wait_time', 'selection_time', 'scheduling_latency', 'leader_agent_id'])
            writer.writerows(detailed_latency)

    def save_load_trace(self, path: str):
        max_len = max((len(l) for l in self.load_per_agent.values()), default=0)
        with open(path, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['Time Interval'] + [f'Agent {aid}' for aid in self.load_per_agent])
            for i in range(max_len):
                row = [i] + [self.load_per_agent[aid][i] if i < len(self.load_per_agent[aid]) else ''
                             for aid in self.load_per_agent]
                writer.writerow(row)