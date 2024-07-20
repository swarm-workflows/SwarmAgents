import json
import queue
import threading
import time
import traceback
from typing import Dict

import redis
from pyraft.raft import RaftNode

from swarm.agents.agent import Agent
from swarm.agents.pbft_agent import IterableQueue
from swarm.comm.message_service import MessageType
from swarm.models.capacities import Capacities
from swarm.models.task import TaskState, Task, TaskRepository


class ExtendedRaftNode(RaftNode):
    def __init__(self, node_id, address, peers):
        super().__init__(node_id, address, peers)
        self._is_leader = False

    def on_leader(self):
        super().on_leader()
        self._is_leader = True

    def on_follower(self):
        super().on_follower()
        self._is_leader = False

    def is_leader(self):
        return self._is_leader


class RaftAgent(Agent):
    def __init__(self, agent_id: str, config_file: str, cycles: int, address: str = "127.0.0.1", port: int = 5010,
                 peers: Dict[str, str] = {}, redis_host: str = "127.0.0.1", redis_port: int = 6379):
        super(RaftAgent, self).__init__(agent_id=agent_id, config_file=config_file, cycles=cycles)
        self.agent_id = agent_id
        self.peers = peers
        self.raft = ExtendedRaftNode(self.agent_id, f"{address}:{port}", peers)
        self.redis_client = redis.StrictRedis(host=redis_host, port=redis_port, decode_responses=True)
        self.shutdown_flag = threading.Event()
        self.task_list = 'tasks'
        self.message_queue = queue.Queue()
        self.condition = threading.Condition()
        self.pending_messages = 0
        self.heartbeat_thread = threading.Thread(target=self.__heartbeat_main,
                                                 daemon=True, name="HeartBeatThread")
        self.msg_processor_thread = threading.Thread(target=self.__message_processor_main,
                                                     daemon=True, name="MsgProcessorThread")
        self.main_thread = threading.Thread(target=self.run,
                                            daemon=True, name="AgentLoop")
        self.task_repo = TaskRepository(redis_client=self.redis_client)

    def start(self, clean: bool = False):
        self.raft.start()

        if not self.agent_id and clean:
            self.task_repo.delete_all()
        self.message_service.register_observers(agent=self)
        self.msg_processor_thread.start()
        self.message_service.start()
        self.heartbeat_thread.start()
        self.main_thread.start()

    def stop(self):
        super(RaftAgent, self).stop()
        self.shutdown_flag.set()
        self.raft.shutdown()

        if self.msg_processor_thread.is_alive():
            with self.condition:
                self.condition.notify_all()
            self.msg_processor_thread.join()
        if self.heartbeat_thread.is_alive():
            self.heartbeat_thread.join()
        if self.main_thread.is_alive():
            self.main_thread.join()

    def is_leader(self):
        return self.raft.is_leader()

    def can_peer_accommodate_task(self, task: Task, peer_agent: Dict):
        allocated_caps = peer_agent.get("capacity_allocations")
        if not allocated_caps:
            allocated_caps = Capacities()
        capacities = peer_agent.get("capacities")

        if capacities and allocated_caps:
            available = capacities - allocated_caps
            self.logger.debug(f"Agent Total Capacities: {self.capacities}")
            self.logger.debug(f"Agent Allocated Capacities: {allocated_caps}")
            self.logger.debug(f"Agent Available Capacities: {available}")
            self.logger.debug(f"Task: {task.get_task_id()} Requested capacities: {task.get_capacities()}")

            # Check if the agent can accommodate the given task based on its capacities
            # Compare the requested against available
            available = available - task.get_capacities()
            negative_fields = available.negative_fields()
            if len(negative_fields) > 0:
                return False

        return True

    def __find_neighbor_with_lowest_load(self):
        # Initialize variables to track the neighbor with the lowest load
        lowest_load = float('inf')  # Initialize with positive infinity to ensure any load is lower
        lowest_load_neighbor = None

        # Iterate through each neighbor in neighbor_map
        for peer_agent_id, neighbor_info in self.neighbor_map.items():
            # Check if the current load is lower than the lowest load found so far
            if neighbor_info['load'] < lowest_load:
                lowest_load_neighbor = neighbor_info.copy()

        return lowest_load_neighbor

    def run_as_leader(self):
        self.logger.info("Running as leader")
        try:
            processed = 0
            tasks = self.task_repo.get_all_tasks()
            for task in tasks:
                if self.shutdown_flag.is_set():
                    break
                if task.is_pending():
                    processed += 1
                    my_load = self.compute_overall_load()
                    peer = self.__find_neighbor_with_lowest_load()

                    if peer and peer.get('load') < 70.00 and self.can_peer_accommodate_task(peer_agent=peer,
                                                                                            task=task):
                        self.send_message(msg_type=MessageType.Commit, task_id=task.task_id)

                    elif my_load < 70.00 and self.can_accommodate_task(task=task):
                        self.allocate_task(task=task)

                if processed >= 10:
                    time.sleep(5)
                    processed = 0
        except Exception as e:
            self.logger.info(f"RUN Leader -- error: {e}")
            self.logger.info("Running as leader -- stop")
            traceback.print_exc()

    def __receive_heartbeat(self, incoming: dict):
        peer_agent_id = incoming.get("agent_id")
        peer_load = incoming.get("load")
        capacities = incoming.get("capacities")
        capacity_allocations = incoming.get("capacity_allocations")

        if peer_load is None:
            return

        # Update neighbor map
        self.neighbor_map[peer_agent_id] = {
            "agent_id": peer_agent_id,
            "load": peer_load,
        }
        if capacities:
            self.neighbor_map[peer_agent_id]["capacities"] = Capacities.from_dict(capacities)

        if capacity_allocations:
            self.neighbor_map[peer_agent_id]["capacity_allocations"] = Capacities.from_dict(capacity_allocations)

        self.logger.debug(f"Received Heartbeat from Agent: {peer_agent_id}: Neighbors: {len(self.neighbor_map)}")

    def __receive_commit(self, incoming: dict):
        peer_agent_id = incoming.get("agent_id")
        task_id = incoming.get("task_id")

        self.logger.info(f"Received commit from Agent: {peer_agent_id} for Task: {task_id}")
        task = self.task_repo.get_task(task_id=task_id)
        if task:
            if self.can_accommodate_task(task=task):
                self.allocate_task(task=task)
            else:
                self.logger.info(f"Agent: {self.agent_id} cannot execute Task: {task_id}")
                self.fail_task(task=task)
        else:
            self.logger.error(f"Unable to fetch task from queu: {task_id}")

    def send_message(self, msg_type: MessageType, task_id: str = None, payload: dict = None):
        message = {
            "msg_type": msg_type.value,
            "agent_id": self.agent_id,
            "load": self.compute_overall_load()
        }
        if task_id:
            message["task_id"] = task_id

        if payload:
            for key, value in payload.items():
                message[key] = value

        # Produce the message to the Kafka topic
        self.message_service.produce_message(message)

    def __process_messages(self, *, messages: list):
        for message in messages:
            try:
                begin = time.time()
                # Parse the message as JSON
                incoming = json.loads(message)
                peer_agent_id = incoming.get("agent_id")
                if peer_agent_id in str(self.agent_id):
                    return
                self.logger.debug(f"Consumer received message: {incoming}")

                msg_type = MessageType(incoming.get('msg_type'))
                if msg_type == MessageType.HeartBeat:
                    self.__receive_heartbeat(incoming=incoming)
                elif msg_type == MessageType.Commit:
                    self.__receive_commit(incoming=incoming)

                diff = int(time.time() - begin)
                if diff > 0:
                    self.logger.info(f"Event {message.get('msg_type')} TIME: {diff}")
            except Exception as e:
                self.logger.error(f"Error while processing message {type(message)}, {e}")
                self.logger.error(traceback.format_exc())

    def __heartbeat_main(self):
        print("Heartbeat thread Started")
        while not self.shutdown_flag.is_set():
            try:
                # Send heartbeats every 30 seconds
                payload = {"capacities": self.capacities.to_dict()}
                if self.allocated_tasks.capacities():
                    payload["capacity_allocations"] = self.allocated_tasks.capacities().to_dict()
                self.send_message(msg_type=MessageType.HeartBeat, payload=payload)
                time.sleep(10)
            except Exception as e:
                print(f"Error occurred while sending heartbeat e: {e}")
                self.logger.error(traceback.format_exc())
        print("Heartbeat thread Stopped")

    def __message_processor_main(self):
        self.logger.info("Message Processor Started")
        while not self.shutdown_flag.is_set():
            try:
                with self.condition:
                    while not self.shutdown_flag.is_set() and self.message_queue.empty():
                        self.condition.wait()

                if self.shutdown_flag.is_set():
                    break

                messages = self.__dequeue(self.message_queue)
                self.pending_messages = len(messages)
                self.__process_messages(messages=messages)
                self.pending_messages = 0
            except Exception as e:
                self.logger.error(f"Error occurred while processing message e: {e}")
                self.logger.error(traceback.format_exc())
        self.logger.info("Message Processor Stopped")

    def __enqueue(self, incoming):
        try:
            self.message_queue.put_nowait(incoming)
            with self.condition:
                self.condition.notify_all()
            self.logger.debug("Added message to queue {}".format(incoming.__class__.__name__))
        except Exception as e:
            self.logger.error(f"Failed to queue message: {incoming.__class__.__name__} e: {e}")

    def __dequeue(self, queue_obj: queue.Queue):
        events = []
        if not queue_obj.empty():
            try:
                for event in IterableQueue(source_queue=queue_obj):
                    events.append(event)
            except Exception as e:
                self.logger.error(f"Error while adding message to queue! e: {e}")
                self.logger.error(traceback.format_exc())
        return events

    def process_message(self, message):
        self.__enqueue(incoming=message)

    def run_as_follower(self):
        pass

    def run(self):
        self.logger.info(f"Starting agent: {self}")
        self.message_service.register_observers(agent=self)

        while not self.shutdown_flag.is_set():
            try:
                if self.is_leader():
                    # Job Allocation
                    self.run_as_leader()
                else:
                    # Wait for job execution
                    self.run_as_follower()

            except Exception as e:
                self.logger.error(f"Error occurred e: {e}")
                self.logger.error(traceback.format_exc())
            time.sleep(5)  # Adjust the sleep duration as needed

        self.logger.info(f"Stopped agent: {self}")

    def allocate_task(self, task: Task):
        task.set_leader(leader_agent_id=self.agent_id)
        task.set_time_on_queue()
        task.change_state(new_state=TaskState.RUNNING)
        super(RaftAgent, self).allocate_task(task=task)
        self.task_repo.delete_task(task_id=task.get_task_id())
        self.task_repo.save_task(task=task, key_prefix="allocated")

    def fail_task(self, task: Task):
        task.set_leader(leader_agent_id=self.agent_id)
        task.set_time_on_queue()
        task.change_state(new_state=TaskState.FAILED)
        self.task_repo.save_task(task=task, key_prefix="allocated")
