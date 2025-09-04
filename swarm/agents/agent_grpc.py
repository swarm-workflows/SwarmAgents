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

import json
import logging
import os
import threading
import time
import traceback
from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor
from logging.handlers import RotatingFileHandler

import redis
import yaml

from swarm.utils.queues import AgentQueues
from swarm.comm.message_service_grpc import MessageServiceGrpc
from swarm.comm.messages.message import MessageType
from swarm.comm.message_service_kafka import Observer
from swarm.database.repository import Repository
from swarm.models.capacities import Capacities
from swarm.models.agent_info import AgentInfo
from swarm.models.job import Job, JobState
from swarm.utils.thread_safe_dict import ThreadSafeDict
from swarm.utils.topology import Topology, TopologyType
from swarm.utils.metrics import Metrics
from swarm.utils.iterable_queue import IterableQueue
from swarm.utils.utils import timed


class Agent(Observer):
    def __init__(self, agent_id: int, config_file: str):
        self.agent_id = agent_id
        self.neighbor_map = ThreadSafeDict[int, AgentInfo]()
        self.children = ThreadSafeDict[int, AgentInfo]()
        self.parents = ThreadSafeDict[int, AgentInfo]()

        with open(config_file, 'r') as f:
            self.config = yaml.safe_load(f)

        self.queues = AgentQueues()
        self.grpc_config = self.config.get("grpc", {})
        self.log_config = self.config.get("logging", {})
        self.runtime_config = self.config.get("runtime", {})
        self.etcd_config = self.config.get("etcd", {"host": "127.0.0.1", "port": 2379})
        self.redis_config = self.config.get("redis", {"host": "127.0.0.1", "port": 6379})
        self.topology = Topology(topo=self.config.get("topology", {}))

        self._capacities = Capacities().from_dict(self.config.get("capacities", {}))
        self._load = 0
        self.logger = self._setup_logger()

        self.redis_client = redis.StrictRedis(host=self.redis_config["host"],
                                              port=self.redis_config["port"],
                                              decode_responses=True)
        self.repo = Repository(redis_client=self.redis_client)

        #self.repo = EtcdRepository(host=self.etcd_config["host"], port=self.etcd_config["port"])

        self.condition = threading.Condition()
        self.shutdown = False
        self.shutdown_path = "./shutdown"

        self.metrics = Metrics()
        self.completed_lock = threading.RLock()
        self.completed_jobs_set = set()

        self.grpc_thread = MessageServiceGrpc(port=self.grpc_port + self.agent_id, logger=self.logger)

        self.threads = {
            "periodic": threading.Thread(target=self._do_periodic, daemon=True),
            "inbound": threading.Thread(target=self._process_inbound_messages, daemon=True),
            "selection": threading.Thread(target=self.job_selection_main, daemon=True),
            "scheduling": threading.Thread(target=self.job_scheduling_main, daemon=True)
        }
        self.last_non_empty_time = time.time()
        self.executor = ThreadPoolExecutor(max_workers=self.runtime_config.get("executor_workers", 3))

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
        #return 1 + len(self.neighbor_map)
        return len(self.neighbor_map)

    @property
    def configured_agent_count(self) -> int:
        """Returns the expected total number of agents from the runtime configuration."""
        if self.topology.type == TopologyType.Ring:
            return int(self.runtime_config.get("total_agents", 0))

        return 1 + len(self.topology.peers)

    @property
    def restart_job_selection(self) -> float:
        return self.runtime_config.get("restart_job_selection", 60.00)

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

    @property
    def capacity_allocations(self) -> Capacities:
        return self.queues.selected_queue.capacities(self.queues.ready_queue.get_jobs())

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
        try:
            self.grpc_thread.register_observers(agent=self)
            self.grpc_thread.start()

            for thread in self.threads.values():
                thread.start()

            for thread in self.threads.values():
                thread.join()
        except Exception as e:
            self.logger.error(f"Exception occurred in startup: {e}")
            self.logger.error(traceback.format_exc())
            self.stop()

    def stop(self):
        try:
            self.shutdown = True
            self.executor.shutdown(wait=True)
            self.grpc_thread.stop()
            with self.condition:
                self.condition.notify_all()
            self.save_results()
        except Exception as e:
            self.logger.error(f"Exception occurred in shutdown: {e}")
            self.logger.error(traceback.format_exc())

    @abstractmethod
    def _do_periodic(self):
        pass

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
                time.sleep(0.5)
            except Exception as e:
                self.logger.error(f"Inbound processing error: {e}\n{traceback.format_exc()}")
        self.logger.info("Inbound Message Handler - Stopped")

    @abstractmethod
    def _process(self, messages: list[dict]):
        pass

    def select_job(self, job: Job):
        #print(f"Adding: {job.get_job_id()} on agent: {self.agent_id} to Select Queue")
        self.logger.info(f"[SELECTED]: {job.get_job_id()} on agent: {self.agent_id} to Select Queue")
        job.change_state(new_state=JobState.READY)
        self.repo.save(obj=job.to_dict(), key_prefix=Repository.KEY_JOB,
                       level=self.topology.level, group=self.topology.group)
        # Add the job to the list of allocated jobs
        self.queues.selected_queue.add_job(job=job)

    def can_schedule_job(self, job: Job) -> bool:
        ready_caps = self.queues.ready_queue.capacities(self.queues.ready_queue.get_jobs())
        available = self.capacities - ready_caps
        return self._has_sufficient_capacity(job, available)

    def execute_job(self, job: Job):
        try:
            job_id = job.get_job_id()
            self.logger.info(f"[EXECUTE] Starting job {job_id} on agent {self.agent_id}")
            job.execute(data_transfer=self.data_transfer)
            self.queues.ready_queue.remove_job(job_id=job_id)
            self.logger.info(f"[COMPLETE] Job {job_id} completed on agent {self.agent_id}")
            if not self.queues.ready_queue.get_jobs():
                self.start_idle()
        except Exception as e:
            self.logger.error(f"[ERROR] Job {job} failed on agent {self.agent_id}: {e}")
            self.logger.error(traceback.format_exc())
        #print(f"EXECUTED: {job.get_job_id()} on agent: {self.agent_id}")

    def job_scheduling_main(self):
        """
        Main job scheduling loop. If this agent has children, forward the job to them.
        Otherwise, schedule it locally if load permits.
        """
        self.logger.info(f"Starting Job Scheduling Thread: {self}")

        while not self.shutdown:
            try:
                selected_jobs = self.queues.selected_queue.get_jobs()
                if not selected_jobs:
                    time.sleep(0.5)
                    continue

                for job in selected_jobs:
                    job_id = job.get_job_id()

                    # Multi-level: delegate job to children
                    if self.topology.children:
                        self.queues.selected_queue.remove_job(job_id)
                        job.change_state(JobState.PENDING)

                        for child_group in self.topology.children:
                            self.repo.save(
                                obj=job.to_dict(),
                                key_prefix=Repository.KEY_JOB,
                                level=self.topology.level - 1,
                                group=child_group
                            )
                            self.logger.info(
                                f"Delegated job {job_id} to level {self.topology.level - 1}, group {child_group}")

                    # Leaf agent: schedule job if load allows
                    else:
                        if self.can_schedule_job(job):
                            self.queues.selected_queue.remove_job(job_id)
                            self.schedule_job(job)

                time.sleep(0.5)

            except Exception as e:
                self.logger.error(f"Error in job_scheduling_main: {e}")
                self.logger.error(traceback.format_exc())

        self.logger.info(f"Stopped Job Scheduling Thread!")

    def schedule_job(self, job: Job):
        self.end_idle()
        #print(f"SCHEDULED: {job.get_job_id()} on agent: {self.agent_id}")
        self.logger.info(f"[SCHEDULED]: {job.get_job_id()} on agent: {self.agent_id}")
        # Add the job to the list of allocated jobs
        self.queues.ready_queue.add_job(job=job)

        # Launch a thread to execute the job
        #thread = threading.Thread(target=self.execute_job, args=(job,))
        #thread.start()
        self._update_completed_jobs(jobs=[job.get_job_id()])
        job.change_state(JobState.COMPLETE)
        self.repo.save(obj=job.to_dict(), level=self.topology.level, group=self.topology.group)
        self.executor.submit(self.execute_job, job)

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
            self.queues.message_queue.put_nowait(payload)

            with self.condition:
                self.condition.notify_all()

        except Exception as e:
            self.logger.debug(f"Failed to enqueue message: {message}, error: {e}")

    def _send_message(self, json_message: dict, excluded_peers: list[int] = None, src: int = None, fwd: int = None):
        src = src or self.agent_id
        excluded = set(excluded_peers) if excluded_peers else set()

        for peer_id in self.topology.peers:
            if peer_id in excluded:
                continue

            #peer_host = f"agent-{peer_id}" if self.grpc_host != "localhost" else "localhost"
            peer_host = self.grpc_host
            topic = f"{peer_host}:{self.grpc_port + peer_id}"

            self.grpc_thread.produce_message(
                json_message=json_message,
                topic=topic,
                dest=peer_id,
                src=src,
                fwd=fwd
            )
    '''
    def _send_message(
            self,
            json_message: dict,
            excluded_peers: list[int] = None,
            src: int = None,
            fwd: int = None
    ):
        src = src or self.agent_id
        excluded = set(excluded_peers) if excluded_peers else set()

        with timed(self.logger, "_send_message.total",
                         src=src,
                         fwd=fwd,
                         excluded=list(excluded) if excluded else None,
                         num_peers=len(self.topology.peers)):
            for peer_id in self.topology.peers:
                if peer_id in excluded:
                    continue

                with timed(self.logger, "_send_message.per_peer", dest=peer_id, src=src, fwd=fwd):
                    peer_host = (
                        f"agent-{peer_id}"
                        if self.grpc_host != "localhost"
                        else "localhost"
                    )
                    topic = f"{peer_host}:{self.grpc_port + peer_id}"

                    self.grpc_thread.produce_message(
                        json_message=json_message,
                        topic=topic,
                        dest=peer_id,
                        src=src,
                        fwd=fwd
                    )

    '''
    @staticmethod
    def update_jobs(jobs: list[str], job_set: set, lock: threading.RLock):
        with lock:
            job_set.update(jobs)

    def is_job_in_state(self, job_id: str, job_set: set, redis_key_prefix: str, update_fn, state):
        return job_id in job_set
        # deliberate skip updates for now
        if job_id in job_set:
            return True
        update_fn(self.repo.get_all_ids(key_prefix=redis_key_prefix, level=self.topology.level,
                                        group=self.topology.group, state=state))
        return job_id in job_set

    # Unified update methods
    def _update_completed_jobs(self, jobs: list[str]):
        self.update_jobs(jobs, self.completed_jobs_set, self.completed_lock)

    # Unified check methods
    def is_job_completed(self, job_id: str) -> bool:
        return self.is_job_in_state(
            job_id,
            self.completed_jobs_set,
            Repository.KEY_JOB,
            self._update_completed_jobs,
            state=JobState.COMPLETE.value
        )

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
        if os.path.exists("./shutdown"):
            return True
        return (time.time() - self.last_non_empty_time) >= self.empty_timeout_seconds

    @staticmethod
    def _has_sufficient_capacity(job: Job, available: Capacities) -> bool:
        """
        Returns True if the job can be accommodated by the available capacity.
        Also checks data transfer feasibility if enabled.
        """
        # Check if job fits into available capacities
        residual = available - job.get_capacities()
        negative_fields = residual.negative_fields()
        if len(negative_fields) > 0:
            return False
        return True

    @staticmethod
    def resource_usage_score(allocated: Capacities, total: Capacities):
        if allocated == total:
            return 0
        core = (allocated.core / total.core) * 100
        ram = (allocated.ram / total.ram) * 100
        disk = (allocated.disk / total.disk) * 100

        return round((core + ram + disk) / 3, 2)

    def save_results(self):
        self.logger.info("Saving Results")
        agent_metrics = {
            "id": self.agent_id,
            "restarts": self.metrics.restarts,
            "conflicts": self.metrics.conflicts,
            "idle_time": self.metrics.idle_time,
            "load_trace": self.metrics.load
        }
        self.repo.save(obj=agent_metrics, key=f"metrics:{self.agent_id}")
        '''
        self.metrics.save_misc(f"{self.results_dir}/misc_{self.agent_id}.json")
        self.metrics.save_load_trace(self.agent_id, f"{self.results_dir}/agent_loads_{self.agent_id}.csv")
        self.metrics.save_idle_time(f"{self.results_dir}/idle_time_per_agent_{self.agent_id}.csv")

        if self.topology.level == 0:
            lock_path = f"{self.results_dir}/result.lock"

            try:
                # Try to create the lock file atomically
                fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.close(fd)  # Lock acquired
            except FileExistsError:
                self.logger.info("Another agent has started saving results")
                return

            all_jobs = self.repo.get_all_objects(key_prefix=Repository.KEY_JOB, level=0)

            self.metrics.save_jobs(all_jobs, path=f"{self.results_dir}/all_jobs.csv")

            all_agents = self.repo.get_all_objects(key_prefix=Repository.KEY_AGENT, level=None)
            self.metrics.save_agents(all_agents, path=f"{self.results_dir}/all_agents.csv")
        '''
        self.logger.info("Results saved")

