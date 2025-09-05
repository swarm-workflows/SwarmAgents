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
import threading
from itertools import islice

from swarm.models.job import Job, JobState
from swarm.queue.job_queue import JobQueue


class SimpleJobQueue(JobQueue):
    def __init__(self):
        self.jobs = {}
        self.lock = threading.RLock()

    def size(self):
        with self.lock:
            return len(self.jobs)

    def get_jobs(self, states: list[JobState] = None, count: int = None) -> list[Job]:
        # Copy references while holding the lock, so iteration happens outside
        with self.lock:
            all_jobs = list(self.jobs.values())

        if states:
            states_set = set(states)
            all_jobs = (j for j in all_jobs if j.get_state() in states_set)

        if count:
            all_jobs = islice(all_jobs, count)

        return list(all_jobs)

    def add_job(self, job: Job):
        with self.lock:
            self.jobs[job.get_job_id()] = job

    def update_job(self, job: Job):
        with self.lock:
            self.jobs[job.get_job_id()] = job

    def remove_job(self, job_id: str):
        with self.lock:
            if job_id in self.jobs:
                self.jobs.pop(job_id)

    def get_job(self, job_id: str) -> Job:
        with self.lock:
            return self.jobs.get(job_id)

    def __contains__(self, job_id):
        with self.lock:
            return job_id in self.jobs
