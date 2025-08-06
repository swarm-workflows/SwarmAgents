import csv
import json
import time
from typing import Any

from swarm.models.job import Job, JobState


class Metrics:
    def __init__(self):
        self.load = []
        self.idle_time = []
        self.idle_start_time = None
        self.total_idle_time = 0
        self.restart_job_selection_cnt = 0
        self.conflicts = 0

    def save_load_metric(self, load: float):
        """
        Save the load metric along with the current timestamp for an agent.

        :param load: Current load value of the agent
        """
        self.load.append((time.time(), load))

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

    def save_jobs(self, jobs: list[Any], path: str):
        detailed_latency = []  # For combined output
        for job_data in jobs:
            if isinstance(job_data, dict):
                job = Job()
                job.from_dict(job_data)
            else:
                job = job_data
            job_id = job.job_id
            leader_id = getattr(job, "leader_agent_id", None)
            if leader_id is None:
                continue
            if job.state not in [JobState.READY, JobState.RUNNING, JobState.COMPLETE]:
                continue

            # Store all in one row
            detailed_latency.append([
                job_id,
                job.created_at if job.created_at is not None else 0,
                job.selection_started_at if job.selection_started_at is not None else 0,
                job.selected_by_agent_at if job.selected_by_agent_at is not None else 0,
                job.scheduled_at if job.scheduled_at is not None else 0,
                job.completed_at if job.completed_at is not None else 0,
                job.status if job.status is not None else 0,
                leader_id
            ])

        # Save combined CSV with leader_agent_id
        with open(path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['job_id', 'created_at', 'selection_started_at', 'selected_by_agent_at', 'scheduled_at',
                             'completed_at', 'exit_status', 'leader_agent_id'])
            writer.writerows(detailed_latency)

    def save_load_trace(self, agent_id: int, path: str):
        with open(path, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["timestamp", "agent_id", "load"])
            for ts, load in self.load:
                writer.writerow([ts, agent_id, load])