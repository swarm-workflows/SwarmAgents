import csv
import json

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

    def save_jobs_per_agent(self, jobs: list[Job], path: str):
        jobs_per_agent = {}
        for j in jobs:
            if j.leader_agent_id is not None:
                jobs_per_agent.setdefault(j.leader_agent_id, {"jobs": [], "job_count": 0})
                jobs_per_agent[j.leader_agent_id]["job_count"] += 1
                jobs_per_agent[j.leader_agent_id]["jobs"].append(j.job_id)

        with open(path, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['Agent ID', 'Number of Jobs Selected', 'Jobs'])
            for agent_id, info in jobs_per_agent.items():
                writer.writerow([agent_id, info['job_count'], info['jobs']])

    def save_scheduling_latency(self, jobs: list[Job], wait_path: str, sel_path: str, lat_path: str):
        wait_times, selection_times, scheduling_latency = {}, {}, {}

        for j in jobs:
            if j.selection_started_at and j.created_at:
                wait_times[j.job_id] = j.selection_started_at - j.created_at
            if j.selected_by_agent_at and j.selection_started_at:
                selection_times[j.job_id] = j.selected_by_agent_at - j.selection_started_at
            if j.job_id in wait_times and j.job_id in selection_times:
                scheduling_latency[j.job_id] = wait_times[j.job_id] + selection_times[j.job_id]

        with open(wait_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['job_id', 'wait_time'])
            for k, v in wait_times.items():
                writer.writerow([k, v])

        with open(sel_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['job_id', 'selection_time'])
            for k, v in selection_times.items():
                writer.writerow([k, v])

        with open(lat_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['job_id', 'scheduling_latency'])
            for k, v in scheduling_latency.items():
                writer.writerow([k, v])

    def save_load_trace(self, path: str):
        max_len = max((len(l) for l in self.load_per_agent.values()), default=0)
        with open(path, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['Time Interval'] + [f'Agent {aid}' for aid in self.load_per_agent])
            for i in range(max_len):
                row = [i] + [self.load_per_agent[aid][i] if i < len(self.load_per_agent[aid]) else ''
                             for aid in self.load_per_agent]
                writer.writerow(row)