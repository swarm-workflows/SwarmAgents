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
from typing import List

import psutil as psutil
import yaml
from matplotlib import pyplot as plt

from swarm.comm.message_service_nats import MessageServiceNats
from swarm.comm.messages.heart_beat import HeartBeat
from swarm.comm.messages.message import MessageType
from swarm.comm.message_service_kafka import MessageServiceKafka, Observer
from swarm.models.capacities import Capacities
from swarm.models.agent_info import AgentInfo
from swarm.models.profile import ProfileType, PROFILE_MAP
from swarm.models.job import Job
from swarm.queue.simple_job_queue import SimpleJobQueue


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
    def __init__(self, agent_id: str, config_file: str, cycles: int):
        self.agent_id = agent_id
        self.job_queue = SimpleJobQueue()
        self.selected_queue = SimpleJobQueue()
        self.ready_queue = SimpleJobQueue()
        self.done_queue = SimpleJobQueue()
        self.last_updated = time.time()
        self.neighbor_map = {}  # Store neighbor information
        self.profile = None
        self.data_transfer = True
        self.kafka_config = {}
        self.kafka_config_hb = {}
        self.nat_config = {}
        self.nat_config_hb = {}
        self.message_service_type = "kafka"
        self.logger = None
        self.projected_queue_threshold = 300.00
        self.ready_queue_threshold = 100.00
        self.max_time_load_zero = 30
        self.restart_job_selection = 300
        self.peer_heartbeat_timeout = 300
        self.results_dir = "."
        self.load_config(config_file)
        self.capacities = self.get_system_info()
        if self.message_service_type == "nats":
            self.ctrl_msg_srv = MessageServiceNats(config=self.nat_config, logger=self.logger)
            self.hrt_msg_srv = MessageServiceNats(config=self.nat_config_hb, logger=self.logger)
        else:
            self.ctrl_msg_srv = MessageServiceKafka(config=self.kafka_config, logger=self.logger)
            self.hrt_msg_srv = MessageServiceKafka(config=self.kafka_config_hb, logger=self.logger)
        self.load_per_agent = {}
        self.cycles = cycles
        self.message_queue = queue.Queue()
        self.hb_message_queue = queue.Queue()
        self.condition = threading.Condition()
        self.shutdown = False
        self.neighbor_map_lock = threading.Lock()
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_main,
                                                 daemon=True, name="HeartBeatThread")
        self.heartbeat_processor_thread = threading.Thread(target=self._heartbeat_processor_main,
                                                           daemon=True, name="HeartBeatProcessorThread")
        self.msg_processor_thread = threading.Thread(target=self._message_processor_main,
                                                     daemon=True, name="MsgProcessorThread")
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
            message = json.loads(incoming)

            if "agent" in message:
                source_agent_id = message.get("agent").get("agent_id")
            else:
                source_agent_id = message.get("agent_id")

            if source_agent_id == self.agent_id:
                return

            message_type = message.get('message_type')
            if message_type == MessageType.HeartBeat.name or message_type == MessageType.HeartBeat.value:
                self.hb_message_queue.put_nowait(message)
            else:
                self.message_queue.put_nowait(message)
            with self.condition:
                self.condition.notify_all()
            self.logger.debug(f"Added incoming message to queue: {incoming}")
        except Exception as e:
            self.logger.debug(f"Failed to add incoming message to queue: {incoming}: e: {e}")

    def __dequeue(self, queue_obj: queue.Queue) -> List[dict]:
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
        incoming.agent.last_updated = time.time()
        self.__add_peer(peer=incoming.agent)
        self._save_load_metric(incoming.agent.agent_id, incoming.agent.load)
        temp = ""
        for p in self.neighbor_map.values():
            temp += f"[{p}],"
        self.logger.info(f"Received Heartbeat from Agent: MAP:: {temp}")

    def _save_load_metric(self, agent_id: str, load: float):
        if agent_id not in self.load_per_agent:
            self.load_per_agent[agent_id] = []
        self.load_per_agent[agent_id].append(load)

    def _build_heart_beat(self) -> HeartBeat:
        my_load = self.compute_overall_load()
        agent = AgentInfo(agent_id=self.agent_id,
                          capacities=self.capacities,
                          capacity_allocations=self.ready_queue.capacities(jobs=self.ready_queue.get_jobs()),
                          load=my_load)
        self._save_load_metric(self.agent_id, my_load)
        return HeartBeat(agent=agent)

    def _heartbeat_main(self):
        heart_beat = None
        while not self.shutdown:
            try:
                heart_beat = self._build_heart_beat()
                self.hrt_msg_srv.produce_message(heart_beat.to_dict())
                time.sleep(5)
            except Exception as e:
                self.logger.error(f"Error occurred while sending heartbeat e: {e}")
                self.logger.error(traceback.format_exc())
            if self._can_shutdown(heart_beat=heart_beat):
                self.stop()

    def _message_processor_main(self):
        self.logger.info("Message Processor Started")
        while True:
            try:
                with self.condition:
                    while not self.shutdown and self.message_queue.empty():
                        self.condition.wait()

                if self.shutdown:
                    break

                messages = self.__dequeue(self.message_queue)
                self._process_messages(messages=messages)
            except Exception as e:
                self.logger.error(f"Error occurred while processing message e: {e}")
                self.logger.error(traceback.format_exc())
        self.logger.info("Message Processor Stopped")

    def _heartbeat_processor_main(self):
        self.logger.info("Heartbeat Processor Started")
        while True:
            try:
                with self.condition:
                    while not self.shutdown and self.hb_message_queue.empty():
                        self.condition.wait()

                if self.shutdown:
                    break

                messages = self.__dequeue(self.hb_message_queue)
                self._process_messages(messages=messages)
            except Exception as e:
                self.logger.error(f"Error occurred while processing message e: {e}")
                self.logger.error(traceback.format_exc())
        self.logger.info("Heartbeat Processor Stopped")

    def _process_messages(self, *, messages: List[dict]):
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
                if diff > 0:
                    self.logger.info(f"Event {message.get('message_type')} TIME: {diff}")
            except Exception as e:
                self.logger.error(f"Error while processing message {type(message)}, {e}")
                self.logger.error(traceback.format_exc())

    def load_config(self, config_file):
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)

            queue_config = config.get("queue", {})
            queue_type = queue_config.get("type", "simple")
            if queue_type == "riak":
                from swarm.queue.riak_job_queue import RiakJobQueue
                self.job_queue = RiakJobQueue()
                self.selected_queue = self.job_queue
                self.ready_queue = self.job_queue
                self.done_queue = self.job_queue

            kafka_config = config.get("kafka", {})
            nat_config = config.get("nats", {})
            self.kafka_config = {
                'kafka_bootstrap_servers': kafka_config.get("bootstrap_servers", "localhost:19092"),
                'kafka_topic': kafka_config.get("topic", "agent_load"),
                'consumer_group_id': f'{kafka_config.get("consumer_group_id", "swarm_agent")}-{self.agent_id}'
            }

            self.kafka_config_hb = {
                'kafka_bootstrap_servers': kafka_config.get("bootstrap_servers", "localhost:19092"),
                'kafka_topic': kafka_config.get("hb_topic", "agent_load_hb"),
                'consumer_group_id': f'{kafka_config.get("consumer_group_id", "swarm_agent")}-{self.agent_id}'
            }

            self.nat_config = {
                'nats_servers': nat_config.get('nats_servers', 'nats://127.0.0.1:4222'),
                'nats_topic': nat_config.get('nats_topic', 'job.consensus'),
            }

            self.nat_config_hb = {
                'nats_servers': nat_config.get('nats_servers', 'nats://127.0.0.1:4222'),
                'nats_topic': nat_config.get('hb_nats_topic', 'job.consensus'),
            }
            runtime_config = config.get("runtime", {})
            self.message_service_type = runtime_config.get("message_service", "kafka")
            profile_name = runtime_config.get("profile", str(ProfileType.BalancedProfile))
            self.profile = PROFILE_MAP.get(profile_name)
            self.data_transfer = runtime_config.get("data_transfer", True)
            self.projected_queue_threshold = runtime_config.get("projected_queue_threshold", 300.00)
            self.ready_queue_threshold = runtime_config.get("ready_queue_threshold", 100.00)
            self.max_time_load_zero = runtime_config.get("max_time_load_zero", 30)
            self.restart_job_selection = runtime_config.get("restart_job_selection", 300)
            self.peer_heartbeat_timeout = runtime_config.get("peer_heartbeat_timeout", 300)
            self.results_dir = runtime_config.get("results_dir", ".")

            log_config = config.get("logging")
            self.logger = self.make_logger(log_dir=log_config.get("log-directory"),
                                           log_file=f'{log_config.get("log-file")}-{self.agent_id}.log',
                                           logger=f'{log_config.get("logger")}-{self.agent_id}',
                                           log_retain=log_config.get("log-retain"),
                                           log_size=log_config.get("log-size"),
                                           log_level=log_config.get("log-level"))

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
            allocated_caps = self.ready_queue.capacities(jobs=self.ready_queue.get_jobs())
            allocated_caps += self.selected_queue.capacities(jobs=self.selected_queue.get_jobs())
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
        allocated_caps = self.ready_queue.capacities(jobs=self.ready_queue.get_jobs())
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

    def compute_overall_load(self, proposed_jobs: List[str] = None):
        if proposed_jobs is None:
            proposed_jobs = []

        allocated_caps = Capacities()
        allocated_caps += self.ready_queue.capacities(jobs=self.ready_queue.get_jobs())
        allocated_caps += self.selected_queue.capacities(jobs=self.selected_queue.get_jobs())

        for j in proposed_jobs:
            if j not in self.ready_queue and j not in self.selected_queue:
                job = self.job_queue.get_job(job_id=j)
                allocated_caps += job.capacities

        core_load = (allocated_caps.core / self.capacities.core) * 100
        ram_load = (allocated_caps.ram / self.capacities.ram) * 100
        disk_load = (allocated_caps.disk / self.capacities.disk) * 100

        overall_load = (core_load * self.profile.core_weight +
                        ram_load * self.profile.ram_weight +
                        disk_load * self.profile.disk_weight) / (self.profile.core_weight +
                                                                 self.profile.ram_weight +
                                                                 self.profile.disk_weight)
        return overall_load

    def compute_ready_queue_load(self):
        allocated_caps = self.ready_queue.capacities(jobs=self.ready_queue.get_jobs())

        core_load = (allocated_caps.core / self.capacities.core) * 100
        ram_load = (allocated_caps.ram / self.capacities.ram) * 100
        disk_load = (allocated_caps.disk / self.capacities.disk) * 100

        overall_load = (core_load * self.profile.core_weight +
                        ram_load * self.profile.ram_weight +
                        disk_load * self.profile.disk_weight) / (self.profile.core_weight +
                                                                 self.profile.ram_weight +
                                                                 self.profile.disk_weight)
        return overall_load

    def select_job(self, job: Job):
        print(f"Adding: {job.get_job_id()} on agent: {self.agent_id} to Select Queue")
        self.logger.info(f"Adding: {job.get_job_id()} on agent: {self.agent_id} to Select Queue")
        # Add the job to the list of allocated jobs
        self.selected_queue.add_job(job=job)

    def schedule_job(self, job: Job):
        self.end_idle()
        print(f"Executing: {job.get_job_id()} on agent: {self.agent_id}")
        self.logger.info(f"Executing: {job.get_job_id()} on agent: {self.agent_id}")
        # Add the job to the list of allocated jobs
        self.ready_queue.add_job(job=job)

        # Launch a thread to execute the job
        thread = threading.Thread(target=self.execute_job, args=(job,))
        thread.start()

    def execute_job(self, job: Job):
        # Function to execute the job (similar to the previous implementation)
        # Once the job is completed, move it to the completed_jobs list and remove it from the allocated_jobs list
        # Assuming execute_job function performs the actual execution of the job
        try:
            job.execute(data_transfer=self.data_transfer)
            self.done_queue.add_job(job=job)
            self.ready_queue.remove_job(job_id=job.get_job_id())
            if not len(self.ready_queue.get_jobs()):
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
        self.hrt_msg_srv.register_observers(agent=self)
        self.heartbeat_processor_thread.start()
        self.msg_processor_thread.start()
        self.ctrl_msg_srv.start()
        self.hrt_msg_srv.start()
        self.heartbeat_thread.start()
        self.job_selection_thread.start()
        self.job_scheduling_thread.start()

        if self.heartbeat_thread.is_alive():
            self.heartbeat_thread.join()
        if self.heartbeat_processor_thread.is_alive():
            self.heartbeat_processor_thread.join()
        if self.msg_processor_thread.is_alive():
            self.msg_processor_thread.join()
        if self.job_selection_thread.is_alive():
            self.job_selection_thread.join()
        if self.job_scheduling_thread.is_alive():
            self.job_scheduling_thread.join()

    def stop(self):
        self.shutdown = True
        self.ctrl_msg_srv.stop()
        self.hrt_msg_srv.stop()
        with self.condition:
            self.condition.notify_all()
        self.plot_results()

    def job_scheduling_main(self):
        self.logger.info(f"Starting Job Scheduling Thread: {self}")
        while not self.shutdown:
            try:
                for job in self.selected_queue.get_jobs():
                    ready_queue_load = self.compute_ready_queue_load()
                    if ready_queue_load < self.ready_queue_threshold and self.can_schedule_job(job):
                        self.selected_queue.remove_job(job.get_job_id())
                        self.schedule_job(job)
                    else:
                        time.sleep(0.5)

                time.sleep(5)
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
            jobs = self.job_queue.get_jobs()
        completed_jobs = [j for j in jobs if j.leader_agent_id is not None]
        jobs_per_agent = {}

        for j in completed_jobs:
            if j.leader_agent_id:
                if j.leader_agent_id not in jobs_per_agent:
                    jobs_per_agent[j.leader_agent_id] = 0
                jobs_per_agent[j.leader_agent_id] += 1

        # Save jobs_per_agent to CSV
        with open(f'{self.results_dir}/jobs_per_agent_{self.agent_id}.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['Agent ID', 'Number of Jobs Selected'])
            for agent_id, job_count in jobs_per_agent.items():
                writer.writerow([agent_id, job_count])

        if self.plot_figures:
            # Plotting the jobs per agent as a bar chart
            plt.bar(list(jobs_per_agent.keys()), list(jobs_per_agent.values()), color='blue')
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
            jobs = self.job_queue.get_jobs()
        completed_jobs = [j for j in jobs if j.leader_agent_id is not None]

        wait_times = {}
        selection_times = {}
        scheduling_latency = {}

        # Calculate scheduling latency
        for j in completed_jobs:
            wait_times[j.job_id] = j.selection_started_at - j.created_at
            selection_times[j.job_id] = j.selected_by_agent_at - j.selection_started_at
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

    def _can_shutdown(self, heart_beat: HeartBeat):
        if not heart_beat:
            return False
        if heart_beat.agent.load != 0.0:
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
        with self.neighbor_map_lock:
            self.neighbor_map[peer.agent_id] = peer

    def __remove_peer(self, agent_id: str):
        with self.neighbor_map_lock:
            self.neighbor_map.pop(agent_id)