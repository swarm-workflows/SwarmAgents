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
from logging.handlers import RotatingFileHandler
from typing import List

import psutil as psutil
import yaml

from swarm.comm.messages.heart_beat import HeartBeat
from swarm.comm.messages.message import MessageType
from swarm.comm.message_service import MessageService, Observer
from swarm.models.capacities import Capacities
from swarm.models.agent_info import AgentInfo
from swarm.models.profile import ProfileType, PROFILE_MAP
from swarm.models.task import Task, TaskQueue


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
        self.task_queue = TaskQueue()
        self.allocated_tasks = TaskQueue()
        self.completed_tasks = TaskQueue()
        self.last_updated = time.time()
        self.neighbor_map = {}  # Store neighbor information
        self.profile = None
        self.data_transfer = True
        self.kafka_config = {}
        self.max_pending_elections = 3
        self.logger = None
        self.load_config(config_file)
        self.capacities = self.get_system_info()
        self.message_service = MessageService(config=self.kafka_config, logger=self.logger)
        self.load_per_agent = {}
        self.projected_load_per_agent = {}
        self.cycles = cycles
        self.message_queue = queue.Queue()
        self.condition = threading.Condition()
        self.pending_messages = 0
        self.shutdown = False
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_main,
                                                 daemon=True, name="HeartBeatThread")
        self.msg_processor_thread = threading.Thread(target=self._message_processor_main,
                                                     daemon=True, name="MsgProcessorThread")
        self.main_thread = threading.Thread(target=self.run,
                                            daemon=True, name="AgentLoop")

    def __enqueue(self, incoming: str):
        try:
            message = json.loads(incoming)
            source_agent_id = message.get("agent").get("agent_id")
            if source_agent_id == self.agent_id:
                return

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
        self.neighbor_map[incoming.agent.agent_id] = incoming.agent
        self.last_msg_received_timestamp = time.time()
        self._save_load_metric(incoming.agent.agent_id, incoming.agent.load, incoming.agent.projected_load)
        temp = ""
        for p in self.neighbor_map.values():
            temp += f"[{p}],"
        self.logger.info(f"Received Heartbeat from Agent: MAP:: {temp}")
        self.last_msg_received_timestamp = time.time()

    def _save_load_metric(self, agent_id: str, load: float, projected_load: float = None):
        if agent_id not in self.load_per_agent:
            self.load_per_agent[agent_id] = []
        self.load_per_agent[agent_id].append(load)

        if projected_load:
            if agent_id not in self.projected_load_per_agent:
                self.projected_load_per_agent[agent_id] = []
            self.projected_load_per_agent[agent_id].append(projected_load)

    def _build_heart_beat(self):
        my_load = self.compute_overall_load()

        self._save_load_metric(self.agent_id, my_load)
        agent = AgentInfo(agent_id=self.agent_id,
                          capacities=self.capacities,
                          capacity_allocations=self.allocated_tasks.capacities(),
                          load=my_load)
        return HeartBeat(agent=agent)

    def _heartbeat_main(self):
        while not self.shutdown:
            try:
                self.message_service.produce_message(self._build_heart_beat().to_dict())
                time.sleep(5)
            except Exception as e:
                self.logger.error(f"Error occurred while sending heartbeat e: {e}")
                self.logger.error(traceback.format_exc())

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
                self.pending_messages = len(messages)
                self._process_messages(messages=messages)
                self.pending_messages = 0
            except Exception as e:
                self.logger.error(f"Error occurred while processing message e: {e}")
                self.logger.error(traceback.format_exc())
        self.logger.info("Message Processor Stopped")

    def _process_messages(self, *, messages: List[dict]):
        for message in messages:
            try:
                begin = time.time()
                message_type = message.get('message_type')
                if message_type == str(MessageType.HeartBeat):
                    self._receive_heartbeat(incoming=message)
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
            kafka_config = config.get("kafka", {})
            self.kafka_config = {
                'kafka_bootstrap_servers': kafka_config.get("bootstrap_servers", "localhost:19092"),
                'kafka_topic': kafka_config.get("topic", "agent_load"),
                'consumer_group_id': f'{kafka_config.get("consumer_group_id", "swarm_agent")}-{self.agent_id}'
            }
            runtime_config = config.get("runtime", {})
            profile_name = runtime_config.get("profile", str(ProfileType.BalancedProfile))
            self.profile = PROFILE_MAP.get(profile_name)
            self.data_transfer = runtime_config.get("data_transfer", True)
            self.max_pending_elections = runtime_config.get("max_pending_elections", True)

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

    def can_accommodate_task(self, task: Task, proposed_caps: Capacities = None):
        allocated_caps = self.allocated_tasks.capacities()
        if proposed_caps:
            allocated_caps += proposed_caps
        available = self.capacities - allocated_caps
        #self.logger.debug(f"Agent Total Capacities: {self.capacities}")
        #self.logger.debug(f"Agent Allocated Capacities: {allocated_caps}")
        #self.logger.debug(f"Agent Available Capacities: {available}")
        #self.logger.debug(f"Task: {task.get_task_id()} Requested capacities: {task.get_capacities()}")

        # Check if the agent can accommodate the given task based on its capacities
        # Compare the requested against available
        available = available - task.get_capacities()
        negative_fields = available.negative_fields()
        if len(negative_fields) > 0:
            return False

        if self.data_transfer:
            for data_node in task.get_data_in():
                if not self.is_reachable(hostname=data_node.get_remote_ip()):
                    return False

            for data_node in task.get_data_out():
                if not self.is_reachable(hostname=data_node.get_remote_ip()):
                    return False

        return True

    def compute_overall_load(self, proposed_caps: Capacities = None):
        allocated_caps = self.allocated_tasks.capacities()
        if proposed_caps:
            allocated_caps += proposed_caps

        core_load = (allocated_caps.core / self.capacities.core) * 100
        ram_load = (allocated_caps.ram / self.capacities.ram) * 100
        disk_load = (allocated_caps.disk / self.capacities.disk) * 100

        overall_load = (core_load * self.profile.core_weight +
                        ram_load * self.profile.ram_weight +
                        disk_load * self.profile.disk_weight) / (self.profile.core_weight +
                                                                 self.profile.ram_weight +
                                                                 self.profile.disk_weight)
        return overall_load

    def allocate_task(self, task: Task):
        print(f"Executing: {task.get_task_id()} on agent: {self.agent_id}")
        self.logger.info(f"Executing: {task.get_task_id()} on agent: {self.agent_id}")
        # Add the task to the list of allocated tasks
        self.allocated_tasks.add_task(task=task)

        # Launch a thread to execute the task
        thread = threading.Thread(target=self.execute_task, args=(task,))
        thread.start()

    def execute_task(self, task: Task):
        # Function to execute the task (similar to the previous implementation)
        # Once the task is completed, move it to the completed_tasks list and remove it from the allocated_tasks list
        # Assuming execute_task function performs the actual execution of the task
        try:
            task.execute(data_transfer=self.data_transfer)
            self.completed_tasks.add_task(task=task)
            self.allocated_tasks.remove_task(task_id=task.get_task_id())
        except Exception as e:
            self.logger.error(f"Execution error: {e}")
            self.logger.error(traceback.format_exc())

    def __str__(self):
        return f"agent_id: {self.agent_id} capacities: {self.capacities} load: {self.compute_overall_load()}"

    def run(self):
        self.logger.info(f"Starting agent: {self}")
        self.message_service.register_observers(agent=self)

        cycle = 0
        while cycle <= self.cycles:
            try:
                cycle += 1
                # Filter pending tasks from the task queue
                for task_id, task in self.task_queue.tasks.items():
                    self.logger.info(f"Checking task: {task_id} {task}")
                    if not task.is_pending():
                        self.logger.info(f"Task {task.task_id} in {task.state}; skipping it!")
                        continue
                    if self.can_accommodate_task(task):
                        self.allocate_task(task=task)
                        self.logger.info(f"Allocated {task}; new agent load: {self.compute_overall_load()}")
                    else:
                        self.logger.info(f"Task {task} cannot be accommodated")
            except Exception as e:
                self.logger.error(f"Error occurred while executing cycle: {cycle} e: {e}")
                self.logger.error(traceback.format_exc())
            time.sleep(5)  # Adjust the sleep duration as needed

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

    def send_message(self, message_type: MessageType, task_id: str = None):
        message = {
            "message_type": str(message_type),
            "agent_id": self.agent_id,
            "load": self.compute_overall_load()
        }
        if task_id:
            message["task_id"] = task_id

        # Produce the message to the Kafka topic
        self.message_service.produce_message(message)

    @staticmethod
    def generate_id() -> str:
        return hashlib.sha256(''.join(random.choices(string.ascii_lowercase, k=8)).encode()).hexdigest()[:8]

    def start(self):
        self.message_service.start()

    def stop(self):
        self.message_service.stop()
