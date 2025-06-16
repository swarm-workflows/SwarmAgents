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
import hashlib
import json
import logging
import os
import queue
import random
import socket
import string
import threading
import time
import traceback
from abc import abstractmethod
from logging.handlers import RotatingFileHandler

import redis
import yaml

from swarm.agents.data.queues import AgentQueues
from swarm.comm.message_service_grpc import MessageServiceGrpc
from swarm.comm.messages.message import MessageType
from swarm.comm.message_service_kafka import Observer
from swarm.database.etcd_repository import EtcdRepository
from swarm.database.repository import Repository
from swarm.models.capacities import Capacities
from swarm.models.agent_info import AgentInfo
from swarm.models.job import Job


class IterableQueue:
    def __init__(self, source_queue: queue.Queue):
        self.source_queue = source_queue

    def __iter__(self):
        while True:
            try:
                yield self.source_queue.get_nowait()
            except queue.Empty:
                return


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


class Agent(Observer):
    def __init__(self, agent_id: int, config_file: str):
        self.agent_id = agent_id
        self.neighbor_map = {}
        self.neighbor_map_lock = threading.Lock()

        with open(config_file, 'r') as f:
            self.config = yaml.safe_load(f)

        self.queues = AgentQueues(self.config.get("queue", {}))
        self.grpc_config = self.config.get("grpc", {})
        self.log_config = self.config.get("logging", {})
        self.runtime_config = self.config.get("runtime", {})
        self.etcd_config = self.config.get("etcd", {"host": "127.0.0.1", "port": 2379})
        self.redis_config = self.config.get("redis", {"host": "127.0.0.1", "port": 6379})
        self.peer_agents = self.config.get("topology", {}).get("peer_agents", [])
        self.topology_type = self.config.get("topology", {}).get("type", "mesh")

        self._capacities = Capacities().from_dict(self.config.get("capacities", {}))
        self.logger = self._setup_logger()

        self.redis_client = redis.StrictRedis(host=self.redis_config["host"],
                                              port=self.redis_config["port"],
                                              decode_responses=True)
        #self.repo = Repository(redis_client=self.redis_client)
        self.repo = EtcdRepository(host=self.etcd_config["host"], port=self.etcd_config["port"])

        self.condition = threading.Condition()
        self.shutdown = False
        self.shutdown_path = "./shutdown"

        self.completed_lock = threading.Lock()
        self.completed_jobs_set = set()
        self.pre_prepare_lock = threading.Lock()
        self.pre_prepare_jobs_set = set()
        self.prepare_lock = threading.Lock()
        self.prepare_jobs_set = set()
        self.commit_lock = threading.Lock()
        self.commit_jobs_set = set()
        self.metrics = Metrics()

        self.grpc_thread = MessageServiceGrpc(port=self.grpc_port + self.agent_id, logger=self.logger)

        self.threads = {
            "periodic": threading.Thread(target=self._do_periodic, daemon=True),
            "inbound": threading.Thread(target=self._process_inbound_messages, daemon=True),
            "selection": threading.Thread(target=self.job_selection_main, daemon=True),
            "scheduling": threading.Thread(target=self.job_scheduling_main, daemon=True)
        }
        self.last_non_empty_time = time.time()

    @property
    def empty_timeout_seconds(self):
        return self.runtime_config.get("max_time_load_zero", 3)

    @property
    def proposal_job_batch_size(self):
        return self.runtime_config.get("jobs_per_proposal", 10)

    @property
    def max_pending_elections(self):
        return self.runtime_config.get("max_pending_elections", 50)

    @property
    def live_agent_count(self) -> int:
        """Returns the count of all known agents including self."""
        return 1 + len(self.neighbor_map)

    @property
    def configured_agent_count(self) -> int:
        """Returns the expected total number of agents from the runtime configuration."""
        return int(self.runtime_config.get("total_agents", 0))

    @property
    def projected_queue_threshold(self) -> float:
        return self.runtime_config.get("projected_queue_threshold", 300.00)

    @property
    def ready_queue_threshold(self) -> float:
        return self.runtime_config.get("ready_queue_threshold", 100.00)

    @property
    def results_dir(self) -> str:
        return self.runtime_config.get("results_dir", "results_dir")

    @property
    def data_transfer(self) -> bool:
        return self.runtime_config.get("data_transfer", False)

    @property
    def peer_expiry_seconds(self) -> int:
        return self.runtime_config.get("peer_heartbeat_timeout", 300)

    @property
    def grpc_port(self) -> int:
        return self.grpc_config.get("port", 50051)

    @property
    def grpc_host(self):
        return self.grpc_config.get("host", "localhost")

    @property
    def capacities(self) -> Capacities:
        return self._capacities

    def start_idle(self):
        if self.metrics.idle_start_time is None:
            self.metrics.idle_start_time = time.time()

    def end_idle(self):
        if self.metrics.idle_start_time is not None:
            idle_duration = time.time() - self.metrics.idle_start_time
            self.metrics.idle_time.append(idle_duration)
            self.metrics.total_idle_time += idle_duration
            self.metrics.idle_start_time = None

    def get_total_idle_time(self):
        self.end_idle()
        return self.metrics.total_idle_time

    def compute_overall_load(self, proposed_jobs: list[str] = None):
        proposed_jobs = proposed_jobs or []
        allocated_caps = Capacities()

        allocated_caps += self.queues.ready_queue.capacities(self.queues.ready_queue.get_jobs())
        allocated_caps += self.queues.selected_queue.capacities(self.queues.selected_queue.get_jobs())

        for job_id in proposed_jobs:
            if job_id not in self.queues.ready_queue and job_id not in self.queues.selected_queue:
                job = self.queues.job_queue.get_job(job_id=job_id)
                allocated_caps += job.capacities

        core = (allocated_caps.core / self.capacities.core) * 100
        ram = (allocated_caps.ram / self.capacities.ram) * 100
        disk = (allocated_caps.disk / self.capacities.disk) * 100

        return round((core + ram + disk) / 3, 2)

    def _setup_logger(self):
        log_path = f"{self.log_config['log-directory']}/{self.log_config['log-file']}-{self.agent_id}.log"
        logger = logging.getLogger(f"{self.log_config['logger']}-{self.agent_id}")
        logger.setLevel(self.log_config.get("log-level", logging.INFO))

        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        handler = RotatingFileHandler(log_path,
                                      backupCount=int(self.log_config.get("log-retain", 5)),
                                      maxBytes=int(self.log_config.get("log-size", 10**6)))
        formatter = logging.Formatter(
            self.log_config.get("log-format",
                                '%(asctime)s - %(name)s - {%(filename)s:%(lineno)d} - [%(threadName)s]- %(levelname)s - %(message)s'))
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.CRITICAL)
        logger.addHandler(stream_handler)

        return logger

    def start(self):
        self.grpc_thread.register_observers(agent=self)
        self.grpc_thread.start()

        for thread in self.threads.values():
            thread.start()

        for thread in self.threads.values():
            thread.join()

    def stop(self):
        self.shutdown = True
        self.grpc_thread.stop()
        with self.condition:
            self.condition.notify_all()
        self.save_results()

    def _add_peer(self, peer: AgentInfo):
        if peer.agent_id is None or peer.agent_id == self.agent_id:
            return
        with self.neighbor_map_lock:
            current = self.neighbor_map.get(peer.agent_id)
            if current is None or peer.last_updated > current.last_updated:
                self.neighbor_map[peer.agent_id] = peer

    def _remove_peer(self, agent_id: str):
        with self.neighbor_map_lock:
            self.neighbor_map.pop(agent_id, None)

    def generate_agent_info(self):
        agent_info = AgentInfo(
            agent_id=self.agent_id,
            capacities=self.capacities,
            capacity_allocations=self.queues.ready_queue.capacities(self.queues.ready_queue.get_jobs()),
            load=self.compute_overall_load(),
            last_updated=time.time()
        )
        return agent_info

    def _do_periodic(self):
        interval = 5
        while not self.shutdown:
            try:
                agent_info = self.generate_agent_info()
                self.repo.save(agent_info.to_dict(), key_prefix=Repository.KEY_AGENT)

                current_time = int(time.time())
                peers = self.repo.get_all_objects(key_prefix=Repository.KEY_AGENT)
                active_peer_ids = set()

                for p in peers:
                    peer = AgentInfo.from_dict(p)
                    if peer.agent_id == self.agent_id:
                        continue
                    self._add_peer(peer)
                    active_peer_ids.add(peer.agent_id)

                stale_peers = [
                    peer.agent_id for peer in self.neighbor_map.values()
                    if (current_time - peer.last_updated) >= self.peer_expiry_seconds and peer.agent_id not in active_peer_ids
                ]

                for agent_id in stale_peers:
                    self._remove_peer(agent_id)

                for _ in range(int(interval * 10)):
                    if self.shutdown:
                        break
                    time.sleep(0.1)
            except Exception as e:
                self.logger.error(f"Periodic update error: {e}\n{traceback.format_exc()}")

    @abstractmethod
    def job_selection_main(self):
        pass

    def _process_inbound_messages(self):
        self.logger.info("Inbound Message Handler - Start")
        while not self.shutdown:
            try:
                messages = list(IterableQueue(self.queues.message_queue))
                if messages:
                    self._process(messages=messages)
                time.sleep(0.01)
            except Exception as e:
                self.logger.error(f"Inbound processing error: {e}\n{traceback.format_exc()}")
        self.logger.info("Inbound Message Handler - Stopped")

    @abstractmethod
    def _process(self, messages: list[dict]):
        pass

    @staticmethod
    def is_reachable(*, hostname: str, port: int = 22):
        try:
            # Attempt to resolve the hostname to an IP address
            ip_address = socket.gethostbyname(hostname)

            # Attempt to create a socket connection to the IP address and port 80
            with socket.create_connection((ip_address, port), timeout=5):
                return True
        except (socket.gaierror, socket.timeout, OSError):
            return False

    def _has_sufficient_capacity(self, job: Job, available: Capacities) -> bool:
        """
        Returns True if the job can be accommodated by the available capacity.
        Also checks data transfer feasibility if enabled.
        """
        # Check if job fits into available capacities
        residual = available - job.get_capacities()
        negative_fields = available.negative_fields()
        if len(negative_fields) > 0:
            return False

        # Check data transfer endpoints if required
        if self.data_transfer:
            for dn in job.get_data_in() + job.get_data_out():
                if not self.is_reachable(hostname=dn.get_remote_ip()):
                    return False

        return True

    def can_schedule_job(self, job: Job) -> bool:
        ready_caps = self.queues.ready_queue.capacities(self.queues.ready_queue.get_jobs())
        available = self.capacities - ready_caps
        return self._has_sufficient_capacity(job, available)

    def can_accommodate_job(self, job: Job, only_total: bool = False) -> bool:
        if only_total:
            available = self.capacities
        else:
            caps = self.queues.ready_queue.capacities(self.queues.ready_queue.get_jobs())
            caps += self.queues.selected_queue.capacities(self.queues.selected_queue.get_jobs())
            available = self.capacities - caps
        return self._has_sufficient_capacity(job, available)

    def is_job_feasible(self, job: Job, total: Capacities, projected_load: float,
                        proposed_caps: Capacities = Capacities(),
                        allocated_caps: Capacities = Capacities()) -> bool:
        if projected_load >= self.projected_queue_threshold:
            return False
        allocated_caps += proposed_caps
        available = total - allocated_caps
        return self._has_sufficient_capacity(job, available)

    def execute_job(self, job: Job):
        job_id = job.get_job_id()
        self.logger.info(f"[EXECUTE] Starting job {job_id} on agent {self.agent_id}")
        try:
            job.execute(data_transfer=self.data_transfer)
            self.queues.done_queue.add_job(job=job)
            self.queues.ready_queue.remove_job(job_id=job_id)
            self.logger.info(f"[COMPLETE] Job {job_id} completed on agent {self.agent_id}")
            if not self.queues.ready_queue.get_jobs():
                self.start_idle()
        except Exception as e:
            self.logger.error(f"[ERROR] Job {job_id} failed on agent {self.agent_id}: {e}")
            self.logger.error(traceback.format_exc())

    def save_results(self, jobs: list[Job] = None):
        self.logger.info("Saving Results")
        jobs = jobs or self.queues.job_queue.get_jobs()
        completed_jobs = [j for j in jobs if j.leader_agent_id is not None]

        self.metrics.save_misc(f"{self.results_dir}/misc_{self.agent_id}.json")
        self.metrics.save_jobs_per_agent(completed_jobs, f"{self.results_dir}/jobs_per_agent_{self.agent_id}.csv")
        self.metrics.save_scheduling_latency(
            completed_jobs,
            wait_path=f"{self.results_dir}/wait_time_{self.agent_id}.csv",
            sel_path=f"{self.results_dir}/selection_time_{self.agent_id}.csv",
            lat_path=f"{self.results_dir}/scheduling_latency_{self.agent_id}.csv"
        )
        self.metrics.save_load_trace(f"{self.results_dir}/agent_loads_{self.agent_id}.csv")
        self.metrics.save_idle_time(f"{self.results_dir}/idle_time_per_agent_{self.agent_id}.csv")
        self.logger.info("Results saved")

    def job_scheduling_main(self):
        self.logger.info(f"Starting Job Scheduling Thread: {self}")
        while not self.shutdown:
            try:
                for job in self.queues.selected_queue.get_jobs():
                    ready_queue_load = self.compute_ready_queue_load()
                    if ready_queue_load < self.ready_queue_threshold and self.can_schedule_job(job):
                        self.queues.selected_queue.remove_job(job.get_job_id())
                        self.schedule_job(job)
                    else:
                        time.sleep(0.5)

                time.sleep(1)
            except Exception as e:
                self.logger.error(f"Error occurred while executing e: {e}")
                self.logger.error(traceback.format_exc())
        self.logger.info(f"Stopped Job Scheduling Thread!")

    def compute_ready_queue_load(self):
        allocated_caps = self.queues.ready_queue.capacities(jobs=self.queues.ready_queue.get_jobs())

        core_load = (allocated_caps.core / self.capacities.core) * 100
        ram_load = (allocated_caps.ram / self.capacities.ram) * 100
        disk_load = (allocated_caps.disk / self.capacities.disk) * 100

        overall_load = (core_load + ram_load + disk_load) / 3
        return round(overall_load, 2)

    def schedule_job(self, job: Job):
        self.end_idle()
        print(f"SCHEDULED: {job.get_job_id()} on agent: {self.agent_id}")
        self.logger.info(f"[SCHEDULED]: {job.get_job_id()} on agent: {self.agent_id}")
        # Add the job to the list of allocated jobs
        self.queues.ready_queue.add_job(job=job)

        # Launch a thread to execute the job
        thread = threading.Thread(target=self.execute_job, args=(job,))
        thread.start()

    def update_jobs(self, jobs: list[str], job_set: set, lock: threading.Lock):
        with lock:
            job_set.update(jobs)

    def is_job_in_state(self, job_id: str, job_set: set, redis_key_prefix: str, update_fn):
        if job_id in job_set:
            return True
        update_fn(self.repo.get_all_ids(key_prefix=redis_key_prefix))
        return job_id in job_set

    # Unified update methods
    def update_completed_jobs(self, jobs: list[str]):
        self.update_jobs(jobs, self.completed_jobs_set, self.completed_lock)

    def update_pre_prepare_jobs(self, jobs: list[str]):
        self.update_jobs(jobs, self.pre_prepare_jobs_set, self.pre_prepare_lock)

    def update_prepare_jobs(self, jobs: list[str]):
        self.update_jobs(jobs, self.prepare_jobs_set, self.prepare_lock)

    def update_commit_jobs(self, jobs: list[str]):
        self.update_jobs(jobs, self.commit_jobs_set, self.commit_lock)

    # Unified check methods
    def is_job_completed(self, job_id: str) -> bool:
        return self.is_job_in_state(
            job_id,
            self.completed_jobs_set,
            Repository.KEY_JOB,
            self.update_completed_jobs
        )

    def is_job_pre_prepare(self, job_id: str) -> bool:
        return self.is_job_in_state(
            job_id,
            self.pre_prepare_jobs_set,
            Repository.KEY_PRE_PREPARE,
            self.update_pre_prepare_jobs
        )

    def is_job_prepare(self, job_id: str) -> bool:
        return self.is_job_in_state(
            job_id,
            self.prepare_jobs_set,
            Repository.KEY_PREPARE,
            self.update_prepare_jobs
        )

    def is_job_commit(self, job_id: str) -> bool:
        return self.is_job_in_state(
            job_id,
            self.commit_jobs_set,
            Repository.KEY_COMMIT,
            self.update_commit_jobs
        )

    def dispatch_message(self, message: str):
        try:
            # Parse message
            payload = json.loads(message) if isinstance(message, str) else message

            source_agent_id = payload.get("source") or payload.get("agent_id")
            if source_agent_id == self.agent_id:
                return  # Skip self-messages

            message_type = payload.get("message_type")
            msg_name = MessageType(message_type)
            fwd = payload.get("forwarded_by")

            # Log message
            log_msg = f"[IN] [{msg_name}] [SRC: {source_agent_id}]"
            if fwd:
                log_msg += f" [FWD: {fwd}]"
            log_msg += f", Payload: {json.dumps(payload)}"
            self.logger.debug(log_msg)

            # Queue message
            if message_type in (MessageType.HeartBeat.name, MessageType.HeartBeat.value):
                self.queues.hb_message_queue.put_nowait(payload)
            else:
                self.queues.message_queue.put_nowait(payload)

            with self.condition:
                self.condition.notify_all()

        except Exception as e:
            self.logger.debug(f"Failed to enqueue message: {message}, error: {e}")

    def _send_message(self, json_message: dict, excluded_peers: list[int] = None,
                       src: int = None, fwd: int = None):
        src = src or self.agent_id
        excluded = set(excluded_peers) if excluded_peers else set()

        for peer_id in self.peer_agents:
            if peer_id in excluded:
                continue

            peer_host = f"agent-{peer_id}" if self.grpc_host != "localhost" else "localhost"
            topic = f"{peer_host}:{self.grpc_port + peer_id}"

            self.grpc_thread.produce_message(
                json_message=json_message,
                topic=topic,
                dest=peer_id,
                src=src,
                fwd=fwd
            )

    @staticmethod
    def generate_id() -> str:
        return hashlib.sha256(''.join(random.choices(string.ascii_lowercase, k=8)).encode()).hexdigest()[:8]

    def select_job(self, job: Job):
        #print(f"Adding: {job.get_job_id()} on agent: {self.agent_id} to Select Queue")
        self.logger.info(f"[SELECTED]: {job.get_job_id()} on agent: {self.agent_id} to Select Queue")
        # Add the job to the list of allocated jobs
        self.queues.selected_queue.add_job(job=job)

    def calculate_quorum(self) -> int:
        # Simple majority quorum calculation
        return (self.live_agent_count // 2) + 1

    def check_queue(self):
        """Call this periodically to monitor the queue."""
        candidate_jobs = [
            job for job in self.queues.job_queue.get_jobs()
            if job.is_pending() and not self.is_job_completed(job_id=job.get_job_id())
        ]
        if candidate_jobs:
            self.last_non_empty_time = time.time()

    def should_shutdown(self):
        """
        Returns True if queue has been empty for empty_timeout_seconds.
        """
        return (time.time() - self.last_non_empty_time) >= self.empty_timeout_seconds
