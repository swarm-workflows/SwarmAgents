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
import logging
import threading
import time
import riak
from swarm.models.job import Job, JobState
from swarm.queue.job_queue import JobQueue


class RiakJobQueue(JobQueue):
    _instance = None  # Class-level variable to hold the singleton instance
    _instance_lock = threading.Lock()  # Lock to ensure thread-safe singleton access

    def __new__(cls, *args, **kwargs):
        """
        Ensure thread-safe singleton instance creation.

        :return: The singleton instance of RiakJobQueue
        :rtype: RiakJobQueue
        """
        if not cls._instance:
            with cls._instance_lock:
                if not cls._instance:
                    cls._instance = super(RiakJobQueue, cls).__new__(cls)
        return cls._instance

    def __init__(self, logger: logging.Logger = None, host: str = "127.0.0.1", port: int = 8098):
        """
        Initialize the RiakJobQueue with a connection to the Riak database.

        :param logger: Logger instance for logging operations
        :type logger: logging.Logger
        :param host: Hostname or IP address of the Riak server, defaults to "127.0.0.1"
        :type host: str, optional
        :param port: Port of the Riak server, defaults to 8098
        :type port: int, optional
        """
        if hasattr(self, '_initialized') and self._initialized:
            return
        self.host = host
        self.port = port
        self.client = riak.RiakClient(nodes=[{'host': self.host, 'http_port': self.port}])
        self.bucket = self.client.bucket('jobs')
        self.lock = threading.Lock()
        self.logger = logger if logger else logging.getLogger(self.__class__.__name__)
        self._initialized = True

    def add_job(self, job: Job):
        """
        Adds a job to the Riak database.

        :param job: The Job instance to add
        :type job: Job
        :return: The added job
        :rtype: Job
        """
        with self.lock:
            job_data = job.to_dict()
            if job.job_id is None:
                job.job_id = str(int(time.time()))  # Generate job_id if not set
            job_object = self.bucket.new(job.job_id, data=job_data)
            job_object.store()
            self.logger.info(f"Job {job.job_id} added to Riak.")
        return job

    def get_job(self, job_id: str) -> Job:
        """
        Retrieves a job from the Riak database by job ID.

        :param job_id: The ID of the job to retrieve
        :type job_id: str
        :return: The Job instance if found, otherwise None
        :rtype: Job
        """
        job_object = self.bucket.get(job_id)
        if job_object.exists:
            job_data = job_object.data
            job = Job()
            job.from_dict(job_data)
            self.logger.info(f"Job {job_id} retrieved from Riak.")
            return job
        else:
            self.logger.error(f"Job {job_id} not found in Riak.")

    def get_jobs(self, states: list[JobState] = None) -> list[Job]:
        """
        Retrieve a list of jobs from the Riak database that match specified states.

        :param states: A list of job states to filter by. If None, all jobs are retrieved.
        :type states: list[JobState], optional
        :return: A list of Job instances in the specified states
        :rtype: list[Job]
        """
        jobs = []
        for key in self.bucket.get_keys():  # Get all job keys in the bucket
            job_object = self.bucket.get(key)
            if job_object.exists:
                job_data = job_object.data
                job_instance = Job()
                job_instance.from_dict(job_data)
                if states is None or job_instance.state in states:
                    jobs.append(job_instance)
        self.logger.info(f"Retrieved {len(jobs)} jobs from Riak with states {states}.")
        return jobs

    def remove_job(self, job_id: str):
        """
        Removes a job from the Riak database by job ID.

        :param job_id: The ID of the job to remove
        :type job_id: str
        """
        with self.lock:
            if not job_id:
                self.logger.error("Job ID is not set. Cannot delete.")
                return
            job_object = self.bucket.get(job_id)
            if job_object.exists:
                job_object.delete()
                self.logger.info(f"Job {job_id} removed from Riak.")
            else:
                self.logger.error(f"Job {job_id} does not exist in Riak.")

    def __contains__(self, item):
        """
        Checks if a job exists in the Riak database.

        :param item: The job ID or Job instance to check for existence
        :type item: str or Job
        :return: True if the job exists in the database, False otherwise
        :rtype: bool
        """
        job_id = item if isinstance(item, str) else item.job_id if isinstance(item, Job) else None
        if job_id is None:
            self.logger.error("Invalid item type for __contains__. Expected str or Job instance.")
            return False
        job_object = self.bucket.get(job_id)
        return job_object.exists

    def update_job(self, job: Job):
        """
        Updates an existing job in the Riak database.

        :param job: The Job instance with updated data
        :type job: Job
        """
        with self.lock:
            if not job.job_id:
                self.logger.error("Job ID is not set. Cannot update.")
                return
            job_object = self.bucket.get(job.job_id)
            if job_object.exists:
                job_object.data = job.to_dict()
                job_object.store()
                self.logger.info(f"Job {job.job_id} updated in Riak.")
            else:
                self.logger.error(f"Job {job.job_id} does not exist in Riak.")
