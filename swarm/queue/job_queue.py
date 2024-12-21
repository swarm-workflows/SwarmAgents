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
from abc import ABC, abstractmethod

from swarm.models.capacities import Capacities
from swarm.models.job import JobState, Job


class JobQueue(ABC):
    @abstractmethod
    def get_jobs(self, states: list[JobState] = None) -> list[Job]:
        pass

    @abstractmethod
    def add_job(self, job: Job):
        pass

    @abstractmethod
    def remove_job(self, job_id: str):
        pass

    @abstractmethod
    def update_job(self, job: Job):
        pass

    @staticmethod
    def capacities(jobs: list[Job]):
        allocated_caps = Capacities()
        for t in jobs:
            allocated_caps += t.get_capacities()
        return allocated_caps

    @abstractmethod
    def get_job(self, job_id: str) -> Job:
        pass

    @abstractmethod
    def __contains__(self, job_id: str):
        pass

