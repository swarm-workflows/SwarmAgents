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
import math
import os
import random
import argparse
from typing import Iterator, List, Optional, Sequence, Tuple

from swarm.database.repository import Repository
from swarm.models.capacities import Capacities
from swarm.models.data_node import DataNode
from swarm.models.job import Job
from swarm.quantum.split import split_hybrid_job


ARRIVAL_MODES = ("batch", "poisson")


def load_rate_schedule(path: str) -> List[Tuple[float, float]]:
    """A piecewise-constant arrival rate: JSON ``[[start_s, jobs_per_s], ...]`` (or
    ``{"segments": [...]}``), starting at 0, strictly increasing, rates >= 0. The last segment
    runs until the jobs run out, so its rate must be positive or the rest would never arrive."""
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    if isinstance(raw, dict):
        raw = raw.get("segments")
    if not isinstance(raw, list) or not raw:
        raise ValueError(f"{path}: expected a non-empty list of [start_s, rate] segments")
    segments = []
    for item in raw:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            raise ValueError(f"{path}: segment {item!r} is not [start_s, rate]")
        start, rate = float(item[0]), float(item[1])
        if not (math.isfinite(start) and math.isfinite(rate)) or rate < 0:
            raise ValueError(f"{path}: segment {item!r} has a non-finite start or negative rate")
        segments.append((start, rate))
    if segments[0][0] != 0:
        raise ValueError(f"{path}: the first segment must start at 0, not {segments[0][0]}")
    if any(b[0] <= a[0] for a, b in zip(segments, segments[1:])):
        raise ValueError(f"{path}: segment starts must be strictly increasing")
    if segments[-1][1] <= 0:
        raise ValueError(f"{path}: the last segment's rate must be > 0, or the remaining jobs "
                         f"never arrive")
    return segments


def poisson_offsets(schedule: Sequence[Tuple[float, float]],
                    seed: Optional[int] = None) -> Iterator[float]:
    """Arrival times (seconds from the start) of a Poisson process whose rate is the
    piecewise-constant `schedule`. A constant rate is a one-segment schedule.

    Exact for a time-varying rate: unit-rate exponential gaps are mapped through the inverse of
    the cumulative rate, so a gap that straddles a segment boundary is finished at the next
    segment's rate rather than drawn again. Seeded, so every repeat of a cell gets the same
    arrival times and repeats differ only in what the system does with them."""
    rng = random.Random(seed)
    seg, t = 0, 0.0
    while True:
        remaining = rng.expovariate(1.0)
        while remaining > 0:
            start, rate = schedule[seg]
            end = schedule[seg + 1][0] if seg + 1 < len(schedule) else math.inf
            room = rate * (end - t)
            if rate > 0 and remaining <= room:
                t += remaining / rate
                remaining = 0.0
            else:
                remaining -= room if rate > 0 else 0.0
                t, seg = end, seg + 1
        yield t


class JobDistributor(threading.Thread):
    """
    A threaded job distributor that pushes job definitions from a directory
    into a Redis-backed job repository at a fixed interval.
    """

    def __init__(self, redis_host: str, redis_port: int, jobs_dir: str,
                 jobs_per_interval: int, interval: float, level: int,
                 group: int, split_hybrid: bool = False, arrival: str = "batch",
                 schedule: Optional[Sequence[Tuple[float, float]]] = None,
                 arrival_seed: Optional[int] = None,
                 arrivals_out: Optional[str] = None) -> None:
        """
        Initialize the JobDistributor.

        :param redis_host: Hostname or IP address of the Redis server.
        :param redis_port: Port number of the Redis server.
        :param jobs_dir: Directory containing JSON job files.
        :param jobs_per_interval: Number of jobs to submit per interval.
        :param interval: Interval in seconds between batches.
        :param split_hybrid: Split hybrid jobs into quantum/classical sub-jobs
                             for data-triggered co-scheduling.
        :param arrival: ``batch`` (default: `jobs_per_interval` jobs every `interval` s, what
                        every run before 2026-10-14 used) or ``poisson`` (one job at a time at
                        the times of a Poisson process with rate `schedule`).
        :param arrivals_out: Where to write each job's planned and actual submission time.
        """
        if arrival not in ARRIVAL_MODES:
            raise ValueError(f"arrival {arrival!r} is not one of {', '.join(ARRIVAL_MODES)}")
        if (arrival == "poisson") != bool(schedule):
            raise ValueError("a rate schedule is required for poisson arrivals and refused for "
                             "batch arrivals")
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
        self.arrival = arrival
        self.schedule = list(schedule or [])
        self.arrival_seed = arrival_seed
        self.arrivals_out = arrivals_out
        self.arrivals: List[dict] = []

    def _job_file_generator(self) -> Iterator[str]:
        """
        Yields full paths to JSON job files in JOB-NUMBER order (`job_2` before `job_10`).

        It used to sort by creation time, which is the order files were COPIED in — after an
        rsync, lexicographic (`job_1`, `job_10`, `job_100`, ...) — so the arrival order of a
        workload depended on how its directory was transferred (code review 2026-10-05 §58).
        """
        def key(entry_name: str):
            stem = entry_name[len("job_"):-len(".json")]
            return (0, int(stem), "") if stem.isdigit() else (1, 0, stem)

        with os.scandir(self.jobs_dir) as it:
            # Only actual job files (job_*.json) — never sidecars like conversion_summary.json /
            # pegasus_baseline.json, which are metadata, not jobs.
            names = [e.name for e in it
                     if e.name.startswith("job_") and e.name.endswith(".json") and e.is_file()]
        for name in sorted(names, key=key):
            yield os.path.join(self.jobs_dir, name)

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

        # `Job.from_dict` rather than copying fields across by hand. The hand-written version
        # carried id, capacities, wall_time, data_in, data_out and quantum — and silently
        # dropped every field added since, which by now meant:
        #
        #   data_predicate  -> DAG gating never reached the agents through this path
        #   should_fail     -> a replayed failure profile was discarded
        #   execution       -> real execution could not fire; jobs simulated instead
        #
        # None of these announced themselves. A converted workflow published to Redis simply
        # arrived without the halves that make it a workflow, and the run looked healthy.
        # The model already knows how to read its own serialisation; a second partial copy of
        # that knowledge can only drift from it, and did.
        job = Job()
        job.from_dict(job_data)
        return [job]

    def _submit(self, jobs: List[Job], planned_s: Optional[float], t0: float) -> None:
        for job in jobs:
            job.level = self.level
            job.mark_submitted()
            self.job_repo.save(obj=job.to_dict(), key_prefix=Repository.KEY_JOB, level=self.level,
                               group=self.group)
            self.arrivals.append({"job_id": job.job_id, "planned_s": planned_s,
                                  "submitted_s": round(time.monotonic() - t0, 4)})

    def _write_arrivals(self) -> None:
        if not self.arrivals_out:
            return
        payload = {"arrival": self.arrival, "seed": self.arrival_seed,
                   "schedule": self.schedule if self.arrival == "poisson" else None,
                   "jobs_per_interval": self.jobs_per_interval if self.arrival == "batch" else None,
                   "interval": self.interval if self.arrival == "batch" else None,
                   "jobs": self.arrivals}
        tmp = self.arrivals_out + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f)
        os.replace(tmp, self.arrivals_out)

    def run(self) -> None:
        try:
            if self.arrival == "poisson":
                self._run_poisson()
            else:
                self._run_batch()
        finally:
            self._write_arrivals()

    def _run_poisson(self) -> None:
        """One job file at a time, at the arrival times of a (possibly time-varying) Poisson
        process. Times are planned from the start, not from the previous submission, so a slow
        Redis write delays one job without shifting every later arrival."""
        t0 = time.monotonic()
        for planned, file_path in zip(poisson_offsets(self.schedule, self.arrival_seed),
                                      self.file_iter):
            delay = t0 + planned - time.monotonic()
            if delay > 0 and self.shutdown_flag.wait(delay):
                return
            if self.shutdown_flag.is_set():
                return
            self._submit(self._load_jobs_from_file(file_path), round(planned, 4), t0)

    def _run_batch(self) -> None:
        """
        Jobs are read from the directory and submitted to the job repository in batches at
        each interval.
        """
        #self.job_repo.delete_all()
        t0 = time.monotonic()
        while not self.shutdown_flag.is_set():
            batch: List[Job] = []
            try:
                for _ in range(self.jobs_per_interval):
                    file_path = next(self.file_iter)
                    batch.extend(self._load_jobs_from_file(file_path))
            except StopIteration:
                if not batch:
                    break
            self._submit(batch, None, t0)
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
    parser.add_argument("--jobs-per-interval", type=int, default=None,
                        help="Number of jobs to push per interval (batch arrivals; required there)")
    parser.add_argument("--interval", type=float, default=1.0, help="Interval duration in seconds (default: 1.0)")
    parser.add_argument("--level", type=int, default=0, help="Topology level for which to add Jobs")
    parser.add_argument("--group", type=int, default=0, help="Topology group at a level for which to add Jobs")
    parser.add_argument("--split-hybrid", action="store_true",
                        help="Split hybrid jobs into quantum/classical sub-jobs for "
                             "data-triggered co-scheduling on different agents")
    parser.add_argument("--arrival", choices=ARRIVAL_MODES, default="batch",
                        help="batch: --jobs-per-interval every --interval s (the default, and what "
                             "every run before 2026-10-14 used). poisson: one job at a time at "
                             "Poisson arrival times, rate from --rate or --rate-schedule")
    parser.add_argument("--rate", type=float, default=None,
                        help="Poisson arrivals: constant rate in jobs per second")
    parser.add_argument("--rate-schedule", default=None,
                        help="Poisson arrivals: JSON file of [[start_s, jobs_per_s], ...] "
                             "(piecewise-constant rate, e.g. a compressed work day)")
    parser.add_argument("--arrival-seed", type=int, default=None,
                        help="Seed for Poisson arrival times (same seed = same arrival times)")
    parser.add_argument("--arrivals-out", default=None,
                        help="Write each job's planned and actual submission time here (JSON)")
    args = parser.parse_args()
    if args.arrival == "batch":
        if args.jobs_per_interval is None:
            parser.error("--jobs-per-interval is required for batch arrivals")
        if args.rate is not None or args.rate_schedule:
            parser.error("--rate/--rate-schedule apply to --arrival poisson only")
    else:
        if (args.rate is None) == (not args.rate_schedule):
            parser.error("poisson arrivals take exactly one of --rate or --rate-schedule")
        if args.rate is not None and not (math.isfinite(args.rate) and args.rate > 0):
            parser.error(f"--rate must be a positive number, got {args.rate}")
    return args


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
        split_hybrid=args.split_hybrid,
        arrival=args.arrival,
        schedule=([(0.0, args.rate)] if args.rate is not None
                  else load_rate_schedule(args.rate_schedule) if args.rate_schedule else None),
        arrival_seed=args.arrival_seed,
        arrivals_out=args.arrivals_out,
    )
    distributor.start()

    try:
        while distributor.is_alive():
            distributor.join(1)
    except KeyboardInterrupt:
        distributor.stop()
        distributor.join()
