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
import threading
import time
import redis
import json
import os
import argparse
from typing import Iterator, List

from swarm.database.repository import Repository
from swarm.models.capacities import Capacities
from swarm.models.data_node import DataNode
from swarm.models.job import Job
from swarm.quantum.split import split_hybrid_job


class JobDistributor(threading.Thread):
    """
    A threaded job distributor that pushes job definitions from a directory
    into a Redis-backed job repository at a fixed interval.
    """

    def __init__(self, redis_host: str, redis_port: int, jobs_dir: str,
                 jobs_per_interval: int, interval: float, level: int,
                 group: int, split_hybrid: bool = False) -> None:
        """
        Initialize the JobDistributor.

        :param redis_host: Hostname or IP address of the Redis server.
        :param redis_port: Port number of the Redis server.
        :param jobs_dir: Directory containing JSON job files.
        :param jobs_per_interval: Number of jobs to submit per interval.
        :param interval: Interval in seconds between batches.
        :param split_hybrid: Split hybrid jobs into quantum/classical sub-jobs
                             for data-triggered co-scheduling.
        """
        super().__init__()
        self.split_hybrid = split_hybrid
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.jobs_dir = jobs_dir
        self.jobs_per_interval = jobs_per_interval
        self.interval = interval
        self.shutdown_flag = threading.Event()
        self.redis_client = redis.StrictRedis(host=self.redis_host, port=self.redis_port, decode_responses=True)
        self.job_repo = Repository(self.redis_client)
        self.file_iter = self._job_file_generator()
        self.level = level
        self.group = group

    def _job_file_generator(self) -> Iterator[str]:
        """
        Lazily yields full paths to JSON job files in order of their creation time.
        Uses os.scandir() and a generator to reduce memory use.
        """

        def file_iter():
            with os.scandir(self.jobs_dir) as it:
                for entry in it:
                    # Only actual job files (job_*.json) — never sidecars like
                    # conversion_summary.json / pegasus_baseline.json, which are
                    # metadata, not jobs.
                    if entry.name.startswith("job_") and entry.name.endswith(".json") and entry.is_file():
                        yield (entry.stat().st_ctime, entry.path)

        # Lazily sort and yield just file paths
        for _, path in sorted(file_iter()):
            yield path

    def _load_jobs_from_file(self, file_path: str) -> List[Job]:
        """
        Load Job(s) from the given JSON file. A hybrid job becomes two linked
        sub-jobs when split_hybrid is enabled; anything else yields one Job.

        :param file_path: Path to the job JSON file.
        :return: List of parsed Job objects (empty for non-job files).
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                job_data = json.load(f)
        except (UnicodeDecodeError, json.JSONDecodeError, OSError) as e:
            # Skip unreadable / non-JSON files (e.g. conversion_summary.json,
            # sidecar files with non-UTF-8 bytes). One bad file must not abort
            # the whole distribution batch.
            print(f"Skipping unreadable job file {file_path}: {e}")
            return []

        if not isinstance(job_data, dict) or 'id' not in job_data:
            return []  # Skip non-job / non-dict files (metadata sidecars, etc.)

        if self.split_hybrid:
            split = split_hybrid_job(job_data)
            if split is not None:
                jobs = []
                for sub in split:
                    job = Job()
                    job.from_dict({**sub, "state": 1})
                    jobs.append(job)
                return jobs

        job = Job()
        job.job_id = job_data['id']
        job.capacities = Capacities.from_dict(job_data['capacities'])
        job.wall_time = job_data['wall_time']
        if job_data.get('data_in'):
            for data_in in job_data['data_in']:
                job.add_incoming_data_dep(DataNode.from_dict(data_in))
        if job_data.get('data_out'):
            for data_out in job_data['data_out']:
                job.add_outgoing_data_dep(DataNode.from_dict(data_out))
        if job_data.get('quantum'):
            job.quantum = job_data['quantum']
        return [job]

    def run(self) -> None:
        """
        Start the job distribution process. Jobs are read from the directory
        and submitted to the job repository in batches at each interval.
        """
        #self.job_repo.delete_all()
        while not self.shutdown_flag.is_set():
            batch: List[Job] = []
            try:
                for _ in range(self.jobs_per_interval):
                    file_path = next(self.file_iter)
                    batch.extend(self._load_jobs_from_file(file_path))
            except StopIteration:
                if not batch:
                    break
            for job in batch:
                job.level = self.level
                job.mark_submitted()
                self.job_repo.save(obj=job.to_dict(), key_prefix=Repository.KEY_JOB, level=self.level,
                                   group=self.group)
            time.sleep(self.interval)

    def stop(self) -> None:
        """
        Signal the thread to shut down gracefully.
        """
        self.shutdown_flag.set()


def parse_args() -> argparse.Namespace:
    """
    Parse CLI arguments for the job distributor script.

    :return: Namespace with parsed arguments.
    """
    parser = argparse.ArgumentParser(description="Distribute job JSON files to Redis at a controlled rate.")
    parser.add_argument("--redis-host", required=True, help="Redis host address")
    parser.add_argument("--redis-port", type=int, default=6379, help="Redis port (default: 6379)")
    parser.add_argument("--jobs-dir", required=True, help="Directory containing job JSON files")
    parser.add_argument("--jobs-per-interval", type=int, required=True, help="Number of jobs to push per interval")
    parser.add_argument("--interval", type=float, default=1.0, help="Interval duration in seconds (default: 1.0)")
    parser.add_argument("--level", type=int, default=0, help="Topology level for which to add Jobs")
    parser.add_argument("--group", type=int, default=0, help="Topology group at a level for which to add Jobs")
    parser.add_argument("--split-hybrid", action="store_true",
                        help="Split hybrid jobs into quantum/classical sub-jobs for "
                             "data-triggered co-scheduling on different agents")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    distributor = JobDistributor(
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        jobs_dir=args.jobs_dir,
        jobs_per_interval=args.jobs_per_interval,
        interval=args.interval,
        level=args.level,
        group=args.group,
        split_hybrid=args.split_hybrid
    )
    distributor.start()

    try:
        while distributor.is_alive():
            distributor.join(1)
    except KeyboardInterrupt:
        distributor.stop()
        distributor.join()
