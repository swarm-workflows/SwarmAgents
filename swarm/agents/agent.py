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
from typing import Union

import psutil as psutil
import redis
import yaml
from matplotlib import pyplot as plt

from swarm.agents.data.messaging_config import MessagingConfig
from swarm.agents.data.queues import AgentQueues
from swarm.comm.message_service_grpc import MessageServiceGrpc
from swarm.comm.message_service_nats import MessageServiceNats
from swarm.comm.messages.heart_beat import HeartBeat
from swarm.comm.messages.message import MessageType
from swarm.comm.message_service_kafka import MessageServiceKafka, Observer
from swarm.database.repository import Repository
from swarm.models.capacities import Capacities
from swarm.models.agent_info import AgentInfo
from swarm.models.profile import ProfileType, PROFILE_MAP
from swarm.models.job import Job


class IterableQueue:
    def __init__(self, *, source_queue: queue.Queue):
        self.source_queue = source_queue

    def __iter__(self):
        while True:
            try:
                yield self.source_queue.get_nowait()
            except queue.Empty:
                return


class Agent(Observer):
    def __init__(self, agent_id: int, config_file: str):
        self.agent_id = agent_id
        self.neighbor_map = {}  # Store neighbor information
        self.neighbor_map_lock = threading.Lock()

        self.config = {}
        with open(config_file, 'r') as f:
            self.config = yaml.safe_load(f)
        self.queues = AgentQueues(self.config.get("queue", {}))
        self.messaging = self._build_messaging_config(config=self.config)
        self.log_config = self.config.get("logging", {})
        self.runtime_config = self.config.get("runtime", {})
        self.redis_config = self.config.get("redis", {"host": "127.0.0.1", "port": 6379})
        self.last_updated = time.time()
        self.logger = self.make_logger(log_dir=self.log_config.get("log-directory"),
                                       log_file=f'{self.log_config.get("log-file")}-{self.agent_id}.log',
                                       logger=f'{self.log_config.get("logger")}-{self.agent_id}',
                                       log_retain=self.log_config.get("log-retain"),
                                       log_size=self.log_config.get("log-size"),
                                       log_level=self.log_config.get("log-level"))

        self.redis_client = redis.StrictRedis(host=self.redis_config.get("host"),
                                              port=self.redis_config.get("port"),
                                              decode_responses=True)

        self.job_repo = Repository(redis_client=self.redis_client)
        self.agent_repo = Repository(redis_client=self.redis_client)

        # Message Processors
        self.ctrl_msg_srv = self._create_control_message_service()
        self.hrt_msg_srv, self.heartbeat_receiver_thread = self._create_heartbeat_message_service()

        self.load_per_agent = {}
        self.condition = threading.Condition()
        self.shutdown = False
        self.shutdown_path = "./shutdown"
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_main,
                                                 daemon=True, name="HeartBeatThread")
        self.msg_receiver_thread = threading.Thread(target=self._message_processor_main,
                                                    daemon=True, name="MsgReceiverThread")
        self.job_selection_thread = threading.Thread(target=self.job_selection_main,
                                                     daemon=True, name="JobSelectionThread")
        self.job_scheduling_thread = threading.Thread(target=self.job_scheduling_main,
                                                      daemon=True, name="JobSchedulingThread")
        # Load for self and the peers is 0.0 for upto 5 minutes; trigger shutdown
        self.load_check_counter = 0
        self.idle_time = []
        self.idle_start_time = None
        self.total_idle_time = 0
        self.restart_job_selection_cnt = 0
        self.conflicts = 0
        self.plot_figures = False
        self.completed_lock = threading.Lock()
        self.completed_jobs_set = set()

    @property
    def grpc_port(self):
        return self.messaging.grpc_config.get("port", 50051)

    @property
    def grpc_host(self):
        return self.messaging.grpc_config.get("host", "localhost")

    @property
    def total_agents(self):
        return self.runtime_config.get("total_agents", 5)

    @property
    def agent_count(self):
        return len(self.neighbor_map) + 1

    @property
    def topic(self):
        return self.messaging.kafka_config.get("topic")

    @property
    def hb_topic(self):
        return self.messaging.kafka_config.get("hb_topic")

    @property
    def capacities(self):
        return self.get_system_info()

    @property
    def profile(self):
        return PROFILE_MAP.get(self.runtime_config.get("profile", str(ProfileType.BalancedProfile)))

    @property
    def data_transfer(self):
        return self.runtime_config.get("data_transfer", False)

    @property
    def message_service_type(self):
        return self.runtime_config.get("message_service_type", "kafka")

    @property
    def projected_queue_threshold(self):
        return self.runtime_config.get("projected_queue_threshold", 300.0)

    @property
    def ready_queue_threshold(self):
        return self.runtime_config.get("ready_queue_threshold", 100.0)

    @property
    def max_time_load_zero(self):
        return self.runtime_config.get("max_time_load_zero", 30)

    @property
    def restart_job_selection(self):
        return self.runtime_config.get("restart_job_selection", 300)

    @property
    def peer_heartbeat_timeout(self):
        return self.runtime_config.get("peer_heartbeat_timeout", 300)

    @property
    def results_dir(self):
        return self.runtime_config.get("results_dir", ".")

    @property
    def shutdown_mode(self):
        return self.runtime_config.get("shutdown_mode", "manual")

    @property
    def heartbeat_mode(self):
        return self.runtime_config.get("heartbeat_mode", "kafka")

    @property
    def jobs_per_proposal(self):
        return self.runtime_config.get("jobs_per_proposal", 3)

    def _build_messaging_config(self, config: dict) -> MessagingConfig:
        topic_suffix = ""
        peer_agents = []

        # Handle topology config
        topology_config = config.get("topology", {})
        if "peer_agents" in topology_config:
            peer_agents = topology_config["peer_agents"]
            if isinstance(peer_agents, list):
                topic_suffix = self.agent_id

        # Load individual configs
        kafka_config_raw = config.get("kafka", {})
        nat_config_raw = config.get("nats", {})
        grpc_config = config.get("grpc", {})

        topic = kafka_config_raw.get("topic", "agent")
        topic_hb = kafka_config_raw.get("hb_topic", "agent-hb")
        cg = kafka_config_raw.get("consumer_group_id", "cg")
        bootstrap_servers = kafka_config_raw.get("bootstrap_servers", "localhost:19092")
        batch_size = kafka_config_raw.get("batch_size")

        # Construct Kafka configs
        kafka_config = {
            'kafka_bootstrap_servers': bootstrap_servers,
            'kafka_topic': f'{topic}-{topic_suffix}',
            'consumer_group_id': f'{cg}-{topic}-{self.agent_id}',
            'batch_size': batch_size
        }

        kafka_config_hb = {
            'kafka_bootstrap_servers': bootstrap_servers,
            'kafka_topic': f'{topic_hb}-{topic_suffix}',
            'consumer_group_id': f'{cg}-{topic_hb}-{self.agent_id}',
            'batch_size': batch_size
        }

        # Construct NATS configs
        nat_config = {
            'nats_servers': nat_config_raw.get('nats_servers', 'nats://127.0.0.1:4222'),
            'nats_topic': nat_config_raw.get('nats_topic', 'job.consensus')
        }

        nat_config_hb = {
            'nats_servers': nat_config_raw.get('nats_servers', 'nats://127.0.0.1:4222'),
            'nats_topic': nat_config_raw.get('hb_nats_topic', 'job.consensus')
        }

        return MessagingConfig(
            kafka_config=kafka_config,
            kafka_config_hb=kafka_config_hb,
            nat_config=nat_config,
            nat_config_hb=nat_config_hb,
            grpc_config=grpc_config,
            topology_peer_agent_list=peer_agents
        )

    def _create_control_message_service(self):
        if self.message_service_type == "nats":
            return MessageServiceNats(config=self.messaging.nat_config, logger=self.logger)
        elif self.message_service_type == "grpc":
            port = self.grpc_port + self.agent_id
            return MessageServiceGrpc(port=port, logger=self.logger)
        else:  # Default to Kafka
            return MessageServiceKafka(config=self.messaging.kafka_config, logger=self.logger)

    def _create_heartbeat_message_service(self) -> tuple[Union[MessageServiceNats, MessageServiceKafka, None],
                                                         Union[threading.Thread, None]]:
        if self.message_service_type == "nats":
            return (
                MessageServiceNats(config=self.messaging.nat_config_hb, logger=self.logger),
                None
            )
        elif self.message_service_type == "grpc":
            return (None, None)  # No heartbeat service for gRPC
        else:  # Kafka
            if self.heartbeat_mode == "kafka":
                thread = threading.Thread(target=self._heartbeat_processor_main,
                                          daemon=True, name="HeartBeatReceiverThread")
                return (
                    MessageServiceKafka(config=self.messaging.kafka_config_hb, logger=self.logger),
                    thread
                )
            else:
                return (None, None)

    def start_idle(self):
        if self.idle_start_time is None:
            self.idle_start_time = time.time()

    def end_idle(self):
        if self.idle_start_time is not None:
            current_idle_time = time.time() - self.idle_start_time
            self.idle_time.append(current_idle_time)
            self.total_idle_time += current_idle_time
            self.idle_start_time = None

    def get_total_idle_time(self):
        # End current idle period before getting the total idle time
        self.end_idle()
        return self.total_idle_time

    def __enqueue(self, incoming: str):
        try:
            if isinstance(incoming, str):
                message = json.loads(incoming)
            else:
                message = incoming
            source_agent_id = message.get("source")
            fwd = message.get("forwarded_by")

            if source_agent_id is None:
                source_agent_id = message.get("agent_id")

            if source_agent_id == self.agent_id:
                return

            message_type = message.get('message_type')
            msg_name = MessageType(message_type)

            if fwd is not None:
                self.logger.debug(f"[IN] [{str(msg_name)}] [SRC: {source_agent_id}], "
                                  f"Payload:  {json.dumps(message)}")
            else:
                self.logger.debug(f"[IN] [{str(msg_name)}] [SRC: {source_agent_id}] [FWD: {fwd}], "
                                  f"Payload:  {json.dumps(message)}")

            if message_type == MessageType.HeartBeat.name or message_type == MessageType.HeartBeat.value:
                self.queues.hb_message_queue.put_nowait(message)
            else:
                self.queues.message_queue.put_nowait(message)
            with self.condition:
                self.condition.notify_all()
        except Exception as e:
            self.logger.debug(f"Failed to add incoming message to queue: {incoming}: e: {e}")

    def __dequeue(self, queue_obj: queue.Queue) -> list[dict]:
        events = []
        if not queue_obj.empty():
            try:
                for event in IterableQueue(source_queue=queue_obj):
                    events.append(event)
            except Exception as e:
                self.logger.error(f"Error while adding message to queue! e: {e}")
                self.logger.error(traceback.format_exc())
        return events

    def process_message(self, message: str):
        self.__enqueue(incoming=message)

    def _receive_heartbeat(self, incoming: HeartBeat):
        for peer in incoming.agents:
            if peer.agent_id == self.agent_id:
                continue
            self.__add_peer(peer=peer)
            self._save_load_metric(peer.agent_id, peer.load)

    def _save_load_metric(self, agent_id: int, load: float):
        if agent_id not in self.load_per_agent:
            self.load_per_agent[agent_id] = []
        self.load_per_agent[agent_id].append(load)

    def _build_heart_beat(self, only_self: bool = False) -> dict:
        agents = {}
        my_load = self.compute_overall_load()
        agent = AgentInfo(agent_id=self.agent_id,
                          capacities=self.capacities,
                          capacity_allocations=self.queues.ready_queue.capacities(jobs=self.queues.ready_queue.get_jobs()),
                          load=my_load,
                          last_updated=time.time())
        self._save_load_metric(self.agent_id, my_load)
        if not only_self and isinstance(self.messaging.topology_peer_agent_list, list) and len(self.neighbor_map.values()):
            for peer_agent_id, peer in self.neighbor_map.items():
                agents[peer_agent_id] = peer
        agents[self.agent_id] = agent

        return agents

    def _heartbeat_main(self):
        while not self.shutdown:
            agents = {}
            try:
                agents = self._build_heart_beat()
                if self.heartbeat_mode != "kafka":
                    agent_info = agents[self.agent_id]
                    self.agent_repo.save(obj=agent_info.to_dict(), key_prefix="agent")

                    # Update Peer info
                    peers = self.agent_repo.get_all_objects(key_prefix="agent")
                    self.logger.debug(f"Fetched peers: {peers}")
                    for p in peers:
                        agent_info = AgentInfo.from_dict(p)
                        self.__add_peer(peer=agent_info)
                else:
                    # If broadcasting is enabled, send one message for all
                    if self.messaging.topology_peer_agent_list == "all":
                        hb = HeartBeat(source=self.agent_id, agents=list(agents.values()))
                        self.hrt_msg_srv.produce_message(json_message=hb.to_dict())

                    else:
                        # Send individually only if required
                        for peer_agent_id in self.messaging.topology_peer_agent_list:
                            hb_agents = {k: v for k, v in agents.items() if k != peer_agent_id}
                            hb = HeartBeat(source=self.agent_id, agents=list(hb_agents.values()))
                            self.hrt_msg_srv.produce_message(json_message=hb.to_dict(),
                                                             topic=f"{self.hb_topic}-{peer_agent_id}",
                                                             dest=peer_agent_id,
                                                             src=self.agent_id)
                time.sleep(5)
            except Exception as e:
                self.logger.error(f"Error occurred while sending heartbeat e: {e}")
                self.logger.error(traceback.format_exc())

            if self._can_shutdown(agents=agents):
                self.stop()

    def _send_message(self, json_message: dict, excluded_peers: list[int] = [], src: int = None, fwd: int = None):
        if src is None:
            src = self.agent_id

        if excluded_peers is None:
            excluded_peers = set()

        if isinstance(self.messaging.topology_peer_agent_list, list):
            for peer_agent_id in self.messaging.topology_peer_agent_list:
                if peer_agent_id in excluded_peers:
                    continue
                if self.message_service_type == "grpc":
                    peer_host = "localhost"
                    if self.grpc_host != peer_host:
                        peer_host = f"agent-{peer_agent_id}"

                    topic = f"{peer_host}:{self.grpc_port + peer_agent_id}"
                else:
                    topic = f"{self.topic}-{peer_agent_id}"
                self.ctrl_msg_srv.produce_message(json_message=json_message,
                                                  topic=topic,
                                                  dest=peer_agent_id,
                                                  src=src, fwd=fwd)
        else:
            self.ctrl_msg_srv.produce_message(json_message=json_message,
                                              src=src)

    def _message_processor_main(self):
        self.logger.info("Message Processor Started")

        while not self.shutdown:
            try:
                messages = []
                while not self.queues.message_queue.empty():
                    messages.extend(self.__dequeue(self.queues.message_queue))

                if messages:
                    self._process_messages(messages=messages)

                time.sleep(0.01)  # Short sleep to prevent CPU overuse
            except Exception as e:
                self.logger.error(f"Error occurred while processing message e: {e}")
                self.logger.error(traceback.format_exc())

        self.logger.info("Message Processor Stopped")

    def _heartbeat_processor_main(self):
        self.logger.info("Heartbeat Processor Started")

        while not self.shutdown:
            try:
                messages = []
                while not self.queues.hb_message_queue.empty():
                    messages.extend(self.__dequeue(self.queues.hb_message_queue))

                if messages:
                    self._process_messages(messages=messages)

                time.sleep(0.01)  # Short sleep to avoid tight loop consuming CPU
            except Exception as e:
                self.logger.error(f"Error occurred while processing heartbeat messages e: {e}")
                self.logger.error(traceback.format_exc())

        self.logger.info("Heartbeat Processor Stopped")

    def _process_messages(self, *, messages: list[dict]):
        for message in messages:
            try:
                begin = time.time()
                message_type = message.get('message_type')
                if message_type == str(MessageType.HeartBeat):
                    incoming = HeartBeat.from_dict(message)
                    self._receive_heartbeat(incoming=incoming)
                else:
                    self.logger.info(f"Ignoring unsupported message: {message}")
                diff = int(time.time() - begin)
                if diff > 1:
                    self.logger.info(f"Event {message.get('message_type')} TIME: {diff}")
            except Exception as e:
                self.logger.error(f"Error while processing message {type(message)}, {e}")
                self.logger.error(traceback.format_exc())

    @staticmethod
    def get_system_info():
        # Get CPU information
        cpu_count = psutil.cpu_count()

        # Get RAM information
        total_ram = round(psutil.virtual_memory().total / (1024.0 ** 3), 2)  # Total RAM in GB
        available_ram = round(psutil.virtual_memory().available / (1024.0 ** 3), 2)  # Available RAM in GB

        # Get disk information
        disk_usage = psutil.disk_usage('/')
        total_disk = round(disk_usage.total / (1024.0 ** 3), 2)  # Total disk space in GB
        used_disk = round(disk_usage.used / (1024.0 ** 3), 2)  # Used disk space in GB
        free_disk = round(disk_usage.free / (1024.0 ** 3), 2)  # Free disk space in GB

        return Capacities(core=float(cpu_count), ram=float(available_ram), disk=float(free_disk))

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

    def is_job_feasible(self, job: Job, total: Capacities, projected_load: float,
                        proposed_caps: Capacities = Capacities(),
                        allocated_caps: Capacities = Capacities()):
        if projected_load >= self.projected_queue_threshold:
            return 0
        allocated_caps += proposed_caps
        available = total - allocated_caps

        # Check if the agent can accommodate the given job based on its capacities
        # Compare the requested against available
        available = available - job.get_capacities()
        negative_fields = available.negative_fields()
        if len(negative_fields) > 0:
            return 0

        if self.data_transfer:
            for data_node in job.get_data_in():
                if not self.is_reachable(hostname=data_node.get_remote_ip()):
                    return 0

            for data_node in job.get_data_out():
                if not self.is_reachable(hostname=data_node.get_remote_ip()):
                    return 0
        return 1

    def can_accommodate_job(self, job: Job, only_total: bool = False):
        if only_total:
            available = self.capacities
        else:
            allocated_caps = self.queues.ready_queue.capacities(jobs=self.queues.ready_queue.get_jobs())
            allocated_caps += self.queues.selected_queue.capacities(jobs=self.queues.selected_queue.get_jobs())
            available = self.capacities - allocated_caps

        # Check if the agent can accommodate the given job based on its capacities
        # Compare the requested against available
        available = available - job.get_capacities()
        negative_fields = available.negative_fields()
        if len(negative_fields) > 0:
            return False

        if self.data_transfer:
            for data_node in job.get_data_in():
                if not self.is_reachable(hostname=data_node.get_remote_ip()):
                    return False

            for data_node in job.get_data_out():
                if not self.is_reachable(hostname=data_node.get_remote_ip()):
                    return False

        return True

    def can_schedule_job(self, job: Job):
        allocated_caps = self.queues.ready_queue.capacities(jobs=self.queues.ready_queue.get_jobs())
        available = self.capacities - allocated_caps

        # Check if the agent can accommodate the given job based on its capacities
        # Compare the requested against available
        available = available - job.get_capacities()
        negative_fields = available.negative_fields()
        if len(negative_fields) > 0:
            return False

        if self.data_transfer:
            for data_node in job.get_data_in():
                if not self.is_reachable(hostname=data_node.get_remote_ip()):
                    return False

            for data_node in job.get_data_out():
                if not self.is_reachable(hostname=data_node.get_remote_ip()):
                    return False

        return True

    def compute_overall_load(self, proposed_jobs: list[str] = None):
        if proposed_jobs is None:
            proposed_jobs = []

        allocated_caps = Capacities()
        allocated_caps += self.queues.ready_queue.capacities(jobs=self.queues.ready_queue.get_jobs())
        allocated_caps += self.queues.selected_queue.capacities(jobs=self.queues.selected_queue.get_jobs())

        for j in proposed_jobs:
            if j not in self.queues.ready_queue and j not in self.queues.selected_queue:
                job = self.queues.job_queue.get_job(job_id=j)
                allocated_caps += job.capacities

        core_load = (allocated_caps.core / self.capacities.core) * 100
        ram_load = (allocated_caps.ram / self.capacities.ram) * 100
        disk_load = (allocated_caps.disk / self.capacities.disk) * 100

        overall_load = (core_load * self.profile.core_weight +
                        ram_load * self.profile.ram_weight +
                        disk_load * self.profile.disk_weight) / (self.profile.core_weight +
                                                                 self.profile.ram_weight +
                                                                 self.profile.disk_weight)
        return round(overall_load, 2)

    def compute_ready_queue_load(self):
        allocated_caps = self.queues.ready_queue.capacities(jobs=self.queues.ready_queue.get_jobs())

        core_load = (allocated_caps.core / self.capacities.core) * 100
        ram_load = (allocated_caps.ram / self.capacities.ram) * 100
        disk_load = (allocated_caps.disk / self.capacities.disk) * 100

        overall_load = (core_load * self.profile.core_weight +
                        ram_load * self.profile.ram_weight +
                        disk_load * self.profile.disk_weight) / (self.profile.core_weight +
                                                                 self.profile.ram_weight +
                                                                 self.profile.disk_weight)
        return round(overall_load, 2)

    def select_job(self, job: Job):
        #print(f"Adding: {job.get_job_id()} on agent: {self.agent_id} to Select Queue")
        self.logger.info(f"[SELECTED]: {job.get_job_id()} on agent: {self.agent_id} to Select Queue")
        # Add the job to the list of allocated jobs
        self.queues.selected_queue.add_job(job=job)

    def schedule_job(self, job: Job):
        self.end_idle()
        print(f"SCHEDULED: {job.get_job_id()} on agent: {self.agent_id}")
        self.logger.info(f"[SCHEDULED]: {job.get_job_id()} on agent: {self.agent_id}")
        # Add the job to the list of allocated jobs
        self.queues.ready_queue.add_job(job=job)

        # Launch a thread to execute the job
        thread = threading.Thread(target=self.execute_job, args=(job,))
        thread.start()

    def execute_job(self, job: Job):
        # Function to execute the job (similar to the previous implementation)
        # Once the job is completed, move it to the completed_jobs list and remove it from the allocated_jobs list
        # Assuming execute_job function performs the actual execution of the job
        try:
            job.execute(data_transfer=self.data_transfer)
            self.queues.done_queue.add_job(job=job)
            self.queues.ready_queue.remove_job(job_id=job.get_job_id())
            if not len(self.queues.ready_queue.get_jobs()):
                self.start_idle()
        except Exception as e:
            self.logger.error(f"Execution error: {e}")
            self.logger.error(traceback.format_exc())

    def __str__(self):
        return f"agent_id: {self.agent_id} capacities: {self.capacities} load: {self.compute_overall_load()}"

    @abstractmethod
    def job_selection_main(self):
        """
        Job selection main loop
        """

    @staticmethod
    def make_logger(*, log_dir: str, log_file: str, log_level, log_retain: int, log_size: int, logger: str,
                    log_format: str = None):
        """
        Detects the path and level for the log file from the actor config and sets
        up a logger. Instead of detecting the path and/or level from the
        config, a custom path and/or level for the log file can be passed as
        optional arguments.

       :param log_dir: Log directory
       :param log_file
       :param log_level
       :param log_retain
       :param log_size
       :param logger
       :param log_format
       :return: logging.Logger object
        """
        log_path = f"{log_dir}/{log_file}"

        if log_path is None:
            raise RuntimeError('The log file path must be specified in config or passed as an argument')

        if log_level is None:
            log_level = logging.INFO

        # Set up the root logger
        log = logging.getLogger(logger)
        log.setLevel(log_level)
        default_log_format = \
            '%(asctime)s - %(name)s - {%(filename)s:%(lineno)d} - [%(threadName)s]- %(levelname)s - %(message)s'
        if log_format is not None:
            default_log_format = log_format

        os.makedirs(os.path.dirname(log_path), exist_ok=True)

        file_handler = RotatingFileHandler(log_path, backupCount=int(log_retain), maxBytes=int(log_size))
        file_handler.setFormatter(logging.Formatter(default_log_format))
        log.addHandler(file_handler)

        console_log = logging.getLogger()
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.CRITICAL)
        console_log.addHandler(console_handler)

        # Disable console logging to prevent /var partition from filling up with container logs
        console_log = logging.getLogger()
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.CRITICAL)
        console_log.addHandler(console_handler)
        return log

    @staticmethod
    def generate_id() -> str:
        return hashlib.sha256(''.join(random.choices(string.ascii_lowercase, k=8)).encode()).hexdigest()[:8]

    def start(self):
        self.ctrl_msg_srv.register_observers(agent=self)
        if self.hrt_msg_srv:
            self.hrt_msg_srv.register_observers(agent=self)
        if self.heartbeat_receiver_thread:
            self.heartbeat_receiver_thread.start()
        self.msg_receiver_thread.start()
        self.ctrl_msg_srv.start()
        if self.hrt_msg_srv:
            self.hrt_msg_srv.start()
        self.heartbeat_thread.start()
        self.job_selection_thread.start()
        self.job_scheduling_thread.start()

        if self.heartbeat_thread.is_alive():
            self.heartbeat_thread.join()
        if self.heartbeat_receiver_thread and self.heartbeat_receiver_thread.is_alive():
            self.heartbeat_receiver_thread.join()
        if self.msg_receiver_thread.is_alive():
            self.msg_receiver_thread.join()
        if self.job_selection_thread.is_alive():
            self.job_selection_thread.join()
        if self.job_scheduling_thread.is_alive():
            self.job_scheduling_thread.join()

    def stop(self):
        self.shutdown = True
        self.ctrl_msg_srv.stop()
        if self.hrt_msg_srv:
            self.hrt_msg_srv.stop()
        with self.condition:
            self.condition.notify_all()
        self.plot_results()

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

    def save_idle_time_per_agent(self):
        # Save jobs_per_agent to CSV
        with open(f'{self.results_dir}/idle_time_per_agent_{self.agent_id}.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['Idle Time'])
            for idle_time in self.idle_time:
                writer.writerow([idle_time])

    def save_miscellaneous(self):
        # Save misc to JSON
        with open(f'{self.results_dir}/misc_{self.agent_id}.json', 'w') as file:
            data = {
                "restarts": self.restart_job_selection_cnt,
                "conflicts": self.conflicts
            }
            json.dump(data, file, indent=4)

    def plot_jobs_per_agent(self, jobs: list[Job] = None):
        if not jobs:
            jobs = self.queues.job_queue.get_jobs()
        completed_jobs = [j for j in jobs if j.leader_agent_id is not None]
        jobs_per_agent = {}

        for j in completed_jobs:
            if j.leader_agent_id is not None:
                if j.leader_agent_id not in jobs_per_agent:
                    jobs_per_agent[j.leader_agent_id] = {"jobs": [], "job_count": 0}
                jobs_per_agent[j.leader_agent_id]["job_count"] += 1
                jobs_per_agent[j.leader_agent_id]["jobs"].append(j.job_id)

        # Save jobs_per_agent to CSV
        with open(f'{self.results_dir}/jobs_per_agent_{self.agent_id}.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['Agent ID', 'Number of Jobs Selected', "Jobs"])
            for agent_id, info in jobs_per_agent.items():
                writer.writerow([agent_id, info["job_count"], info["jobs"]])

        if self.plot_figures:
            # Extracting agent IDs and job counts for plotting
            agent_ids = list(jobs_per_agent.keys())
            job_counts = [info["job_count"] for info in jobs_per_agent.values()]

            # Plotting the jobs per agent as a bar chart
            plt.figure(figsize=(10, 6))
            plt.bar(agent_ids, job_counts, color='blue')
            plt.xlabel('Agent ID')
            plt.ylabel('Number of Jobs Selected')

            # Title with SWARM and number of agents
            num_agents = len(jobs_per_agent)
            plt.title(f'SWARM: Number of Jobs Selected by Each Agent (Total Agents: {num_agents})')

            plt.grid(axis='y', linestyle='--', linewidth=0.5)

            # Save the plot
            plt.savefig(f'{self.results_dir}/jobs_per_agent_{self.agent_id}.png')
            plt.close()

    def plot_scheduling_latency(self, jobs: list[Job] = None):
        if not jobs:
            jobs = self.queues.job_queue.get_jobs()
        completed_jobs = [j for j in jobs if j.leader_agent_id is not None]

        wait_times = {}
        selection_times = {}
        scheduling_latency = {}

        # Calculate scheduling latency
        for j in completed_jobs:
            if j.selection_started_at and j.created_at:
                wait_times[j.job_id] = j.selection_started_at - j.created_at
            if j.selected_by_agent_at and j.selection_started_at:
                selection_times[j.job_id] = j.selected_by_agent_at - j.selection_started_at
            if j.job_id in wait_times and j.job_id in selection_times:
                scheduling_latency[j.job_id] = wait_times[j.job_id] + selection_times[j.job_id]

        with open(f'{self.results_dir}/wait_time_{self.agent_id}.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['job_id', 'wait_time'])
            for key, value in wait_times.items():
                writer.writerow([key, value])

        with open(f'{self.results_dir}/selection_time_{self.agent_id}.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['job_id', 'selection_time'])
            for key, value in selection_times.items():
                writer.writerow([key, value])

        with open(f'{self.results_dir}/scheduling_latency_{self.agent_id}.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['job_id', 'scheduling_latency'])
            for key, value in scheduling_latency.items():
                writer.writerow([key, value])

        if self.plot_figures:
            # Plotting scheduling latency in red
            plt.plot(list(scheduling_latency.values()),
                     'ro-', label='Scheduling Latency: Combined Wait Time and Selection Time '
                                  '(Job Ready to be scheduled on Agent)')

            # Plotting wait time in blue
            if wait_times:
                plt.plot(list(wait_times.values()),
                         'bo-', label='Wait Time: Duration from Job Creation to Start of Selection')

            # Plotting leader election time in green
            if selection_times:
                plt.plot(list(selection_times.values()),
                         'go-', label='Job Selection Time: Time Taken for Agents to Reach Consensus on Job Selection')

            # Title with SWARM and number of agents
            num_agents = len(set([t.leader_agent_id for t in completed_jobs]))
            plt.title(f'SWARM: Scheduling Latency (Total Agents: {num_agents})')

            plt.xlabel('Task Index')
            plt.ylabel('Time Units (seconds)')
            plt.grid(True)

            # Adjusting legend position to avoid overlapping the graph
            plt.legend(loc='upper left', bbox_to_anchor=(1, 1))  # This places the legend outside the plot area

            # Save the plot
            plt.savefig(f'{self.results_dir}/job_latency_{self.agent_id}.png', bbox_inches='tight')  # bbox_inches='tight' ensures that the entire plot is saved
            plt.close()

    def plot_load_per_agent(self, load_dict: dict, threshold: float, title_prefix: str = ""):
        csv_filename = f'{self.results_dir}/agent_loads_{self.agent_id}.csv'
        plot_filename = f'{self.results_dir}/agent_loads_plot_{self.agent_id}.png'
        # Find the maximum length of the load lists
        max_intervals = max(len(loads) for loads in load_dict.values())

        # Save the data to a CSV file
        with open(csv_filename, mode='w', newline='') as file:
            writer = csv.writer(file)

            # Write the header (agent IDs)
            header = ['Time Interval'] + [f'Agent {agent_id}' for agent_id in load_dict.keys()]
            writer.writerow(header)

            # Write the load data row by row
            for i in range(max_intervals):
                row = [i]  # Start with the time interval
                for agent_id in load_dict.keys():
                    # Add load or empty string if not available
                    row.append(load_dict[agent_id][i] if i < len(load_dict[agent_id]) else '')
                writer.writerow(row)

        if self.plot_figures:
            # Plot the data
            plt.figure(figsize=(10, 6))

            for agent_id, loads in load_dict.items():
                plt.plot(loads, label=f'Agent {agent_id}')

            plt.xlabel('Time Interval')
            plt.ylabel('Load')
            plt.title(f'{title_prefix} Agent Load Over Time [Max Threshold: {threshold}]')
            plt.legend()
            plt.grid(True)

            # Save the plot to a file
            plt.savefig(plot_filename)

    def plot_results(self, jobs: list[Job] = None):
        self.logger.info("Plotting Results")
        self.save_miscellaneous()
        self.plot_jobs_per_agent(jobs=jobs)
        self.plot_scheduling_latency(jobs=jobs)
        self.plot_load_per_agent(self.load_per_agent, self.projected_queue_threshold,
                                 title_prefix="Projected")
        self.save_idle_time_per_agent()
        self.logger.info("Plot completed")

    def _can_shutdown(self, agents: dict):
        if not agents or len(agents) == 0:
            return False

        if self.shutdown_mode != "auto":
            # Remove stale peers
            peers_to_remove = []
            for peer in self.neighbor_map.values():
                diff = int(time.time() - peer.last_updated)
                if diff >= self.peer_heartbeat_timeout:
                    peers_to_remove.append(peer.agent_id)
            for p in peers_to_remove:
                self.__remove_peer(agent_id=p)
            if not os.path.exists(self.shutdown_path):
                return False
            return True

        for peer in agents.values():
            if peer.load != 0.0:
                self.load_check_counter = 0
                return False
        self.logger.info(f"Can Shutdown; Neighbor Map: {self.neighbor_map}")
        # Remove stale peers
        peers_to_remove = []
        for peer in self.neighbor_map.values():
            diff = int(time.time() - peer.last_updated)
            if diff >= self.peer_heartbeat_timeout:
                peers_to_remove.append(peer.agent_id)
        for p in peers_to_remove:
            self.__remove_peer(agent_id=p)
        for peer in self.neighbor_map.values():
            if peer.load != 0.0:
                self.load_check_counter = 0
                return False
        self.load_check_counter += 1
        if self.load_check_counter < self.max_time_load_zero:
            return False

        return True

    def __add_peer(self, peer: AgentInfo):
        """
        Adds or updates a peer in the neighbor map, ensuring only the latest update is stored.
        """
        if peer.agent_id is None or peer.agent_id == self.agent_id:
            return  # Ignore invalid agent_id

        with self.neighbor_map_lock:
            current_peer_info = self.neighbor_map.get(peer.agent_id)

            # Only update if the new peer info is newer
            if current_peer_info is None or peer.last_updated > current_peer_info.last_updated:
                self.neighbor_map[peer.agent_id] = peer

    def __remove_peer(self, agent_id: str):
        with self.neighbor_map_lock:
            self.neighbor_map.pop(agent_id)

    def update_completed_jobs(self, jobs: list[str]):
        # Update completed_jobs_set from job repository
        with self.completed_lock:
            self.completed_jobs_set.update(jobs)

    def is_job_completed(self, job_id: str) -> bool:
        """
        Checks if a job is completed by looking it up in the completed_jobs_set.
        If not found, updates the set from the job repository and checks again.
        """
        if job_id in self.completed_jobs_set:
            return True

        # Update completed_jobs_set from job repository
        self.update_completed_jobs(self.job_repo.get_all_ids())

        return job_id in self.completed_jobs_set
