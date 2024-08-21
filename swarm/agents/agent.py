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
from logging.handlers import RotatingFileHandler
from typing import List

import psutil as psutil
import yaml
from matplotlib import pyplot as plt

from swarm.comm.messages.heart_beat import HeartBeat
from swarm.comm.messages.message import MessageType
from swarm.comm.message_service import MessageService, Observer
from swarm.models.capacities import Capacities
from swarm.models.agent_info import AgentInfo
from swarm.models.profile import ProfileType, PROFILE_MAP
from swarm.models.job import Job, JobQueue


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
        self.job_queue = JobQueue()
        self.selected_queue = JobQueue()
        self.ready_queue = JobQueue()
        self.done_queue = JobQueue()
        self.last_updated = time.time()
        self.neighbor_map = {}  # Store neighbor information
        self.profile = None
        self.data_transfer = True
        self.kafka_config = {}
        self.logger = None
        self.load_config(config_file)
        self.capacities = self.get_system_info()
        self.message_service = MessageService(config=self.kafka_config, logger=self.logger)
        self.load_per_agent = {}
        self.cycles = cycles
        self.message_queue = queue.Queue()
        self.condition = threading.Condition()
        self.shutdown = False
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_main,
                                                 daemon=True, name="HeartBeatThread")
        self.msg_processor_thread = threading.Thread(target=self._message_processor_main,
                                                     daemon=True, name="MsgProcessorThread")
        self.job_selection_thread = threading.Thread(target=self.job_selection_main,
                                                     daemon=True, name="JobSelectionThread")

        self.job_scheduling_thread = threading.Thread(target=self.job_scheduling_main,
                                                      daemon=True, name="JobSchedulingThread")
        self.projected_queue_threshold = 300.0
        self.ready_queue_threshold = 100.0

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
        self._save_load_metric(incoming.agent.agent_id, incoming.agent.load)
        temp = ""
        for p in self.neighbor_map.values():
            temp += f"[{p}],"
        self.logger.info(f"Received Heartbeat from Agent: MAP:: {temp}")

    def _save_load_metric(self, agent_id: str, load: float):
        if agent_id not in self.load_per_agent:
            self.load_per_agent[agent_id] = []
        self.load_per_agent[agent_id].append(load)

    def _build_heart_beat(self):
        my_load = self.compute_overall_load()
        agent = AgentInfo(agent_id=self.agent_id,
                          capacities=self.capacities,
                          capacity_allocations=self.ready_queue.capacities(),
                          load=my_load)
        self._save_load_metric(self.agent_id, my_load)
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
                self._process_messages(messages=messages)
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

    def can_accommodate_job(self, job: Job):
        allocated_caps = self.ready_queue.capacities()
        allocated_caps += self.selected_queue.capacities()
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
        allocated_caps = self.ready_queue.capacities()
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

    def compute_overall_load(self):
        allocated_caps = self.ready_queue.capacities()
        allocated_caps += self.selected_queue.capacities()

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
        allocated_caps = self.ready_queue.capacities()

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
        except Exception as e:
            self.logger.error(f"Execution error: {e}")
            self.logger.error(traceback.format_exc())

    def __str__(self):
        return f"agent_id: {self.agent_id} capacities: {self.capacities} load: {self.compute_overall_load()}"

    def job_selection_main(self):
        self.logger.info(f"Starting agent: {self}")
        self.message_service.register_observers(agent=self)

        cycle = 0
        while cycle <= self.cycles:
            try:
                cycle += 1
                # Filter pending jobs from the job queue
                for job_id, job in self.job_queue.jobs.items():
                    self.logger.info(f"Checking job: {job_id} {job}")
                    if not job.is_pending():
                        self.logger.info(f"Job {job.job_id} in {job.state}; skipping it!")
                        continue
                    if self.can_accommodate_job(job):
                        self.schedule_job(job=job)
                        self.logger.info(f"Allocated {job}; new agent load: {self.compute_overall_load()}")
                    else:
                        self.logger.info(f"Job {job} cannot be accommodated")
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

    def send_message(self, message_type: MessageType, job_id: str = None):
        message = {
            "message_type": str(message_type),
            "agent_id": self.agent_id,
            "load": self.compute_overall_load()
        }
        if job_id:
            message["job_id"] = job_id

        # Produce the message to the Kafka topic
        self.message_service.produce_message(message)

    @staticmethod
    def generate_id() -> str:
        return hashlib.sha256(''.join(random.choices(string.ascii_lowercase, k=8)).encode()).hexdigest()[:8]

    def start(self):
        self.message_service.register_observers(agent=self)
        self.msg_processor_thread.start()
        self.message_service.start()
        self.heartbeat_thread.start()
        self.job_selection_thread.start()
        self.job_scheduling_thread.start()

    def stop(self):
        self.shutdown = True
        self.message_service.stop()
        with self.condition:
            self.condition.notify_all()
        self.heartbeat_thread.join()
        self.msg_processor_thread.join()
        self.job_selection_thread.join()
        self.job_scheduling_thread.join()

    def job_scheduling_main(self):
        self.logger.info(f"Starting Job Scheduling Thread: {self}")
        while not self.shutdown:
            try:
                job_ids = list(self.selected_queue.jobs.keys())
                for job_id in job_ids:
                    job = self.selected_queue.jobs.get(job_id)
                    if not job:
                        continue

                    ready_queue_load = self.compute_ready_queue_load()
                    if self.can_schedule_job(job) and ready_queue_load < self.ready_queue_threshold:
                        self.selected_queue.remove_job(job_id)
                        self.schedule_job(job)
                    else:
                        time.sleep(0.5)

                time.sleep(5)
            except Exception as e:
                self.logger.error(f"Error occurred while executing e: {e}")
                self.logger.error(traceback.format_exc())
        self.logger.info(f"Stopped Job Scheduling Thread!")

    def plot_jobs_per_agent(self):
        jobs = self.job_queue.jobs.values()
        completed_jobs = [j for j in jobs if j.leader_agent_id is not None]
        jobs_per_agent = {}

        for j in completed_jobs:
            if j.leader_agent_id:
                if j.leader_agent_id not in jobs_per_agent:
                    jobs_per_agent[j.leader_agent_id] = 0
                jobs_per_agent[j.leader_agent_id] += 1

        # Save jobs_per_agent to CSV
        with open('jobs_per_agent.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['Agent ID', 'Number of Jobs Selected'])
            for agent_id, job_count in jobs_per_agent.items():
                writer.writerow([agent_id, job_count])

        # Plotting the jobs per agent as a bar chart
        plt.bar(list(jobs_per_agent.keys()), list(jobs_per_agent.values()), color='blue')
        plt.xlabel('Agent ID')
        plt.ylabel('Number of Jobs Selected')

        # Title with SWARM and number of agents
        num_agents = len(jobs_per_agent)
        plt.title(f'SWARM: Number of Jobs Selected by Each Agent (Total Agents: {num_agents})')

        plt.grid(axis='y', linestyle='--', linewidth=0.5)

        # Save the plot
        plot_path = os.path.join("", 'jobs_per_agent.png')
        plt.savefig(plot_path)
        plt.close()

    def plot_scheduling_latency(self):
        jobs = self.job_queue.jobs.values()
        completed_jobs = [j for j in jobs if j.leader_agent_id is not None]

        wait_time_dynamic_jq = {}
        selection_time_data = {}
        wait_time_select_q = {}
        scheduling_latency_data = {}

        # Calculate scheduling latency
        for j in completed_jobs:
            if j.created_at and j.selection_started_at:
                wait_time_dynamic_jq[j.job_id] = j.created_at - j.selection_started_at
            if j.selected_by_agent_at and j.selection_started_at:
                selection_time_data[j.job_id] = j.selected_by_agent_at - j.selection_started_at
            if j.scheduled_at and j.selected_by_agent_at:
                wait_time_select_q[j.job_id] = j.scheduled_at - j.selected_by_agent_at

            if j.job_id in wait_time_dynamic_jq and selection_time_data:
                scheduling_latency_data[j.job_id] = wait_time_dynamic_jq[j.job_id] + selection_time_data[j.job_id]

        with open('wait_time.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['job_id', 'wait_time_dynamic_jq'])
            for key, value in wait_time_dynamic_jq.items():
                writer.writerow([key, value])

        with open('selection_time.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['job_id', 'selection_time'])
            for key, value in selection_time_data.items():
                writer.writerow([key, value])

        with open('scheduling_latency.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['job_id', 'scheduling_latency'])
            for key, value in scheduling_latency_data.items():
                writer.writerow([key, value])

        # Plotting scheduling latency in red
        plt.plot(list(scheduling_latency_data.keys()), list(scheduling_latency_data.values()),
                 'ro-', label='Scheduling Latency (Wait Time + Selection Time)')

        # Plotting wait time in blue
        if wait_time_dynamic_jq:
            plt.plot(list(wait_time_dynamic_jq.keys()), list(wait_time_dynamic_jq.values()),
                     'bo-', label='Waiting Time')

        # Plotting leader election time in green
        if selection_time_data:
            plt.plot(list(selection_time_data.keys()), list(selection_time_data.values()),
                     'go-', label='Job Selection Time')

        # Title with SWARM and number of agents
        num_agents = len(set([t.leader_agent_id for t in completed_jobs]))
        plt.title(f'SWARM: Scheduling Latency (Total Agents: {num_agents})')

        plt.xlabel('Task Index')
        plt.ylabel('Time Units (seconds)')
        plt.grid(True)

        # Adjusting legend position to avoid overlapping the graph
        plt.legend(loc='upper left', bbox_to_anchor=(1, 1))  # This places the legend outside the plot area

        # Save the plot
        plot_path = os.path.join("", 'job_latency.png')
        plt.savefig(plot_path, bbox_inches='tight')  # bbox_inches='tight' ensures that the entire plot is saved
        plt.close()

    @staticmethod
    def plot_load_per_agent(load_dict: dict, csv_filename='agent_loads.csv',
                            plot_filename='agent_loads_plot.png'):
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

        # Plot the data
        plt.figure(figsize=(10, 6))

        for agent_id, loads in load_dict.items():
            plt.plot(loads, label=f'Agent {agent_id}')

        plt.xlabel('Time Interval')
        plt.ylabel('Load')
        plt.title('Agent Load Over Time')
        plt.legend()
        plt.grid(True)

        # Save the plot to a file
        plt.savefig(plot_filename)

    def plot_results(self):
        self.logger.info("Plotting Results")
        if self.agent_id != "0":
            return
        self.plot_jobs_per_agent()
        self.plot_scheduling_latency()
        self.plot_load_per_agent(self.load_per_agent)
        self.logger.info("Plot completed")
