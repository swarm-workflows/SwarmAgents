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
import csv
import json
import time
from typing import Any

from swarm.models.job import Job, ObjectState


class Metrics:
    def __init__(self):
        self.load = []
        self.idle_time = []
        self.idle_start_time = None
        self.total_idle_time = 0
        self.restarts = {}
        self.conflicts = {}
        self.agent_failures = {}
        self.reassignments = {}
        self.quorum_changes = []
        self.delegation_reassignments = {}

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
                "restarts": self.restarts,
                "conflicts": self.conflicts
            }, file, indent=4)

    def save_agents(self, agents: list[Any], path: str):
        # Save combined CSV with leader_agent_id
        with open(path, 'w', newline='') as f:
            json.dump(agents, f, indent=2)

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
            if job.state not in [ObjectState.READY, ObjectState.RUNNING, ObjectState.COMPLETE]:
                continue

            # Store all in one row
            detailed_latency.append([
                job_id,
                job._submission_time if job._submission_time is not None else 0,
                job._selection_at if job._selection_at is not None else 0,
                job._selected_at if job._selected_at is not None else 0,
                job._enqueued_at if job._enqueued_at is not None else 0,
                job._completion_time if job._completion_time is not None else 0,
                job.exit_status if job.exit_status is not None else 0,
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