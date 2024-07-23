import json
import os
import queue
import random
import threading
import time
import traceback
from queue import Queue, Empty

from matplotlib import pyplot as plt

from swarm.agents.agent import Agent
from swarm.comm.message_service import MessageType
from swarm.models.capacities import Capacities
from swarm.models.proposal import ProposalContainer, Proposal
from swarm.models.task import Task, TaskState


class IterableQueue:
    def __init__(self, *, source_queue: Queue):
        self.source_queue = source_queue

    def __iter__(self):
        while True:
            try:
                yield self.source_queue.get_nowait()
            except Empty:
                return


class PBFTAgent(Agent):
    def __init__(self, agent_id: str, config_file: str, cycles: int):
        super().__init__(agent_id, config_file, cycles)
        self.outgoing_proposals = ProposalContainer()
        self.incoming_proposals = ProposalContainer()
        self.shutdown = False
        self.shutdown_heartbeat = False
        self.message_queue = queue.Queue()
        self.thread_lock = threading.Lock()
        self.condition = threading.Condition()
        self.heartbeat_thread = threading.Thread(target=self.__heartbeat_main,
                                                 daemon=True, name="HeartBeatThread")
        self.msg_processor_thread = threading.Thread(target=self.__message_processor_main,
                                                     daemon=True, name="MsgProcessorThread")
        self.main_thread = threading.Thread(target=self.run,
                                            daemon=True, name="AgentLoop")
        self.pending_messages = 0
        self.last_msg_received_timestamp = 0

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

    def __process_messages(self, *, messages: list):
        for message in messages:
            try:
                begin = time.time()
                self.__consensus(message=message)
                diff = int(time.time() - begin)
                if diff > 0:
                    self.logger.info(f"Event {message.get('msg_type')} TIME: {diff}")
            except Exception as e:
                self.logger.error(f"Error while processing message {type(message)}, {e}")
                self.logger.error(traceback.format_exc())

    def __message_processor_main(self):
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
                self.__process_messages(messages=messages)
                self.pending_messages = 0
            except Exception as e:
                self.logger.error(f"Error occurred while processing message e: {e}")
                self.logger.error(traceback.format_exc())
        self.logger.info("Message Processor Stopped")

    def __heartbeat_main(self):
        self.send_message(msg_type=MessageType.HeartBeat)
        while not self.shutdown_heartbeat:
            try:
                # Send heartbeats
                #diff = int(time.time() - self.last_msg_received_timestamp)
                #if diff > 30 or len(self.neighbor_map) == 0:
                    #self.send_message(msg_type=MessageType.HeartBeat)
                self.send_message(msg_type=MessageType.HeartBeat)
                time.sleep(10)
            except Exception as e:
                self.logger.error(f"Error occurred while sending heartbeat e: {e}")
                self.logger.error(traceback.format_exc())

    def run(self):
        self.logger.info(f"Starting agent: {self}")
        time.sleep(10)
        completed_tasks = 0
        while not self.shutdown:
            try:
                processed = 0
                # Make a copy of the dictionary keys
                task_ids = list(self.task_queue.tasks.keys())
                for task_id in task_ids:
                    task = self.task_queue.tasks.get(task_id)
                    if not task:
                        continue

                    if task.is_complete():
                        completed_tasks += 1
                        continue

                    diff = int(time.time() - task.time_last_state_change)
                    if diff > 120 and task.get_state() in [TaskState.PREPARE, TaskState.PRE_PREPARE]:
                        task.change_state(new_state=TaskState.PENDING)

                    if not task.is_pending():
                        self.logger.debug(f"Task: {task.task_id} State: {task.state}; skipping it!")
                        continue

                    processed += 1

                    # Trigger leader election for a task after random sleep
                    election_timeout = random.uniform(150, 300) / 1000
                    time.sleep(election_timeout)

                    if self.__can_become_leader(task=task):
                        task.set_time_on_queue()

                        # Send proposal to all neighbors
                        proposal = Proposal(p_id=self.generate_id(), task_id=task.get_task_id(),
                                            agent_id=self.agent_id)

                        self.send_message(msg_type=MessageType.Proposal, task_id=task.get_task_id(),
                                          proposal_id=proposal.p_id, seed=proposal.seed)

                        self.outgoing_proposals.add_proposal(proposal=proposal)

                        # Begin election for Job leader for this task
                        task.change_state(new_state=TaskState.PRE_PREPARE)
                        self.logger.debug(
                            f"Agent: {self.agent_id} sent Proposal: {proposal} for Task: {task.task_id}!")
                    else:
                        self.logger.debug(
                            f"Task: {task.task_id} State: {task.state} cannot be accommodated at this time:")

                    if processed >= 40:
                        time.sleep(1)
                        processed = 0

                time.sleep(1)  # Adjust the sleep duration as needed

            except Exception as e:
                self.logger.error(f"Error occurred while executing e: {e}")
                self.logger.error(traceback.format_exc())

    def __find_neighbor_with_lowest_load(self):
        # Initialize variables to track the neighbor with the lowest load
        lowest_load = float('inf')  # Initialize with positive infinity to ensure any load is lower
        lowest_load_neighbor = None

        # Iterate through each neighbor in neighbor_map
        for peer_agent_id, neighbor_info in self.neighbor_map.items():
            # Check if the current load is lower than the lowest load found so far
            if neighbor_info['load'] < lowest_load:
                # Update lowest load and lowest load neighbor
                lowest_load = neighbor_info['load']
                lowest_load_neighbor = {
                    "agent_id": peer_agent_id,
                    "load": lowest_load
                }

        return lowest_load_neighbor

    def __can_become_leader(self, task: Task) -> bool:
        """
        Check if agent has enough resources to become a leader
            - Agent has resources to executed the task
            - Agent hasn't received a proposal from other agents for this task
            - Agent's load is less than 70%
        :param task:
        :return: True or False
        """
        proposed_capacities = Capacities()
        for task_id in self.outgoing_proposals.tasks():
            if task_id != task.get_task_id():
                proposed_task = self.task_queue.get_task(task_id=task_id)
                proposed_capacities += proposed_task.get_capacities()

        my_load = self.compute_overall_load(allocated=proposed_capacities)
        least_loaded_neighbor = self.__find_neighbor_with_lowest_load()

        if least_loaded_neighbor and least_loaded_neighbor.get('load') < my_load:
            self.logger.debug("Can't become leader as my load is more than all of the neighbors")
            return False

        can_accommodate = self.can_accommodate_task(task=task, allocated=proposed_capacities)
        incoming = self.incoming_proposals.contains(task_id=task.get_task_id())
        if not incoming and \
                can_accommodate and \
                my_load < 70.00:
            return can_accommodate
        self.logger.debug(
            f"__can_become_leader: can_accommodate: {can_accommodate} incoming:{incoming} my_load: {my_load}")
        return False

    def __receive_proposal(self, incoming: dict):
        peer_agent_id = incoming.get("agent_id")
        neighbor_load = incoming.get("load")
        task_id = incoming.get("task_id")
        proposal_id = incoming.get("proposal_id")
        rcvd_seed = incoming.get("seed", 0)

        self.logger.debug(f"Received Proposal from Agent: {peer_agent_id} for Task: {task_id} "
                          f"Proposal: {proposal_id} Seed: {rcvd_seed}")

        task = self.task_queue.get_task(task_id=task_id)
        if not task or task.is_ready() or task.is_complete() or task.is_running():
            return

        #can_accept_task = self.can_accommodate_task(task=task)
        #my_current_load = self.compute_overall_load()

        # Reject proposal in following cases:
        # - I have initiated a proposal and either received accepts from at least 1 peer or
        #   my proposal's seed is smaller than the incoming proposal
        # - Received and accepted proposal from another agent
        # - can't accommodate this task
        # - can accommodate task and neighbor's load is more than mine
        my_proposal = self.outgoing_proposals.get_proposal(task_id=task_id)
        peer_proposal = self.incoming_proposals.get_proposal(task_id=task_id)

        # if (my_proposal and (my_proposal.prepares or my_proposal.seed < rcvd_seed)) or \
        #        (peer_proposal and peer_proposal.seed < rcvd_seed) or \
        #        not can_accept_task or \
        #        (can_accept_task and my_current_load < neighbor_load):
        if (my_proposal and (my_proposal.prepares or my_proposal.seed < rcvd_seed)) or \
                (peer_proposal and peer_proposal.seed < rcvd_seed):
            self.logger.debug(f"Agent {self.agent_id} rejected Proposal for Task: {task_id} from agent"
                              f" {peer_agent_id} - accepted another proposal")
        else:
            self.logger.debug(
                f"Agent {self.agent_id} accepted Proposal for Task: {task_id} from agent"
                f" {peer_agent_id} and is now the leader")

            proposal = Proposal(p_id=proposal_id, task_id=task_id, seed=rcvd_seed,
                                agent_id=peer_agent_id)
            if my_proposal:
                self.outgoing_proposals.remove_proposal(p_id=my_proposal.p_id, task_id=task_id)
            if peer_proposal:
                self.incoming_proposals.remove_proposal(p_id=peer_proposal.p_id, task_id=task_id)
            task.set_time_on_queue()

            self.incoming_proposals.add_proposal(proposal=proposal)
            self.send_message(msg_type=MessageType.Prepare, task_id=task_id, proposal_id=proposal_id)
            task.change_state(TaskState.PREPARE)

    def __receive_prepare(self, incoming: dict):
        peer_agent_id = incoming.get("agent_id")
        task_id = incoming.get("task_id")
        proposal_id = incoming.get("proposal_id")
        rcvd_seed = incoming.get("seed", 0)

        # Prepare for the proposal
        self.logger.debug(f"Received prepare from Agent: {peer_agent_id} for Task: {task_id}: {proposal_id}")

        task = self.task_queue.get_task(task_id=task_id)
        self.logger.debug(f"Task: {task}")
        if not task or task.is_ready() or task.is_complete() or task.is_running():
            return

        # Update the prepare votes
        if self.outgoing_proposals.contains(task_id=task_id, p_id=proposal_id):
            proposal = self.outgoing_proposals.get_proposal(p_id=proposal_id)
        elif self.incoming_proposals.contains(task_id=task_id, p_id=proposal_id):
            proposal = self.incoming_proposals.get_proposal(p_id=proposal_id)
        else:
            proposal = Proposal(task_id=task_id, p_id=proposal_id, seed=rcvd_seed,
                                agent_id=peer_agent_id)
            self.incoming_proposals.add_proposal(proposal=proposal)

        proposal.prepares += 1

        quorum_count = (len(self.neighbor_map) + 1) / 2
        task.change_state(TaskState.PREPARE)

        # Check if vote count is more than quorum
        # if yes, send commit
        if proposal.prepares >= quorum_count:
            self.logger.info(f"Agent: {self.agent_id} Task: {task_id} received quorum "
                             f"prepares: {proposal.prepares}, starting commit!")
            self.send_message(msg_type=MessageType.Commit, task_id=task_id, proposal_id=proposal_id)
            task.change_state(TaskState.COMMIT)

    def __receive_commit(self, incoming: dict):
        peer_agent_id = incoming.get("agent_id")
        task_id = incoming.get("task_id")
        proposal_id = incoming.get("proposal_id")
        rcvd_seed = incoming.get("seed", 0)

        self.logger.debug(f"Received commit from Agent: {peer_agent_id} for Task: {task_id} Proposal: {proposal_id}")
        task = self.task_queue.get_task(task_id=task_id)
        self.logger.debug(f"Task: {task}")

        if not task or task.is_complete() or task.is_ready() or task.is_running() or task.leader_agent_id:
            return

        # Update the commit votes;
        if self.outgoing_proposals.contains(task_id=task_id, p_id=proposal_id):
            proposal = self.outgoing_proposals.get_proposal(p_id=proposal_id)
        elif self.incoming_proposals.contains(task_id=task_id, p_id=proposal_id):
            proposal = self.incoming_proposals.get_proposal(p_id=proposal_id)
        else:
            proposal = Proposal(task_id=task_id, p_id=proposal_id, seed=rcvd_seed,
                                agent_id=peer_agent_id)
            self.incoming_proposals.add_proposal(proposal=proposal)

        proposal.commits += 1
        quorum_count = (len(self.neighbor_map) + 1) / 2

        if proposal.commits >= quorum_count:
            self.logger.info(
                f"Agent: {self.agent_id} received quorum commits for Task: {task_id} Proposal: {proposal}: Task: {task}")
            task.set_time_to_elect_leader()
            task.set_leader(leader_agent_id=proposal.agent_id)
            if self.outgoing_proposals.contains(task_id=task_id, p_id=proposal_id):
                self.logger.info(f"LEADER CONSENSUS achieved for Task: {task_id} Leader: {self.agent_id}")
                task.change_state(new_state=TaskState.READY)
                self.allocate_task(task)
            else:
                self.logger.info(f"PARTICIPANT CONSENSUS achieved for Task: {task_id} Leader: {peer_agent_id}")
                task.change_state(new_state=TaskState.COMMIT)

    def __receive_heartbeat(self, incoming: dict):
        peer_agent_id = incoming.get("agent_id")
        neighbor_load = incoming.get("load")

        if neighbor_load is None:
            return
        self.last_msg_received_timestamp = time.time()

        # Update neighbor map
        self.neighbor_map[peer_agent_id] = {
            "agent_id": peer_agent_id,
            "load": neighbor_load,
        }
        self.logger.info(f"Received Heartbeat from Agent: {peer_agent_id}: MAP: {self.neighbor_map.values()}")

    def __receive_task_status(self, incoming: dict):
        peer_agent_id = incoming.get("agent_id")
        task_id = incoming.get("task_id")
        proposal_id = incoming.get("proposal_id")

        self.logger.debug(f"Received Status from {peer_agent_id} for Task: {task_id} Proposal: {proposal_id}")

        task = self.task_queue.get_task(task_id=task_id)
        task.set_leader(leader_agent_id=peer_agent_id)
        task.set_time_to_completion()
        self.logger.debug(f"Task: {task}")
        if not task or task.is_complete() or task.is_ready():
            return

        # Update the task status based on broadcast message
        task.change_state(new_state=TaskState.COMPLETE)
        self.incoming_proposals.remove_task(task_id=task_id)
        self.outgoing_proposals.remove_task(task_id=task_id)

    def __consensus(self, message):
        """
        Consensus Loop
        :param message:
        :return:
        """
        try:
            # Parse the message as JSON
            incoming = json.loads(message)
            peer_agent_id = incoming.get("agent_id")
            if peer_agent_id in str(self.agent_id):
                return
            self.logger.debug(f"Consumer received message: {incoming}")

            msg_type = MessageType(incoming.get('msg_type'))

            if msg_type == MessageType.HeartBeat:
                self.__receive_heartbeat(incoming=incoming)

            elif msg_type == MessageType.Proposal:
                self.__receive_proposal(incoming=incoming)

            elif msg_type == MessageType.Prepare:
                self.__receive_prepare(incoming=incoming)

            elif msg_type == MessageType.Commit:
                self.__receive_commit(incoming=incoming)

            elif msg_type == MessageType.TaskStatus:
                self.__receive_task_status(incoming=incoming)
            else:
                if msg_type != MessageType.HeartBeat:
                    self.logger.info(f"Ignoring unsupported message: {message}")
        except Exception as e:
            self.logger.error(f"Error while processing incoming message: {message}: {e}")
            self.logger.error(traceback.format_exc())

    def execute_task(self, task: Task):
        super().execute_task(task=task)
        self.send_message(msg_type=MessageType.TaskStatus, task_id=task.get_task_id(),
                          status=task.state)

    def send_message(self, msg_type: MessageType, task_id: str = None, proposal_id: str = None,
                     status: TaskState = None, seed: float = None):
        message = {
            "msg_type": msg_type.value,
            "agent_id": self.agent_id,
            "load": self.compute_overall_load()
        }
        if task_id:
            message["task_id"] = task_id
        if proposal_id:
            message["proposal_id"] = proposal_id
        if status:
            message["status"] = status.value
        if seed:
            message["seed"] = seed

        # Produce the message to the Kafka topic
        self.message_service.produce_message(message)

    def process_message(self, message):
        self.__enqueue(incoming=message)

    def start(self):
        self.message_service.register_observers(agent=self)
        self.msg_processor_thread.start()
        self.message_service.start()
        self.heartbeat_thread.start()
        self.main_thread.start()

    def stop(self):
        self.shutdown_heartbeat = True
        self.shutdown = True
        self.message_service.stop()
        with self.condition:
            self.condition.notify_all()
        self.heartbeat_thread.join()
        self.msg_processor_thread.join()
        self.main_thread.join()

    def plot_results(self):
        if self.agent_id != "0":
            return

        tasks = self.task_queue.tasks.values()
        completed_tasks = [t for t in tasks if t.leader_agent_id is not None]
        waiting_times = [t.time_on_queue for t in tasks if t.time_on_queue is not None]
        leader_election_times = [t.time_to_elect_leader for t in tasks if t.time_to_elect_leader is not None]
        total_tasks = len(tasks)
        tasks_per_agent = {}
        for t in completed_tasks:
            if t.leader_agent_id and t.leader_agent_id not in tasks_per_agent:
                tasks_per_agent[t.leader_agent_id] = 0
            tasks_per_agent[t.leader_agent_id] += 1

        self.logger.info(f"Total Tasks = {total_tasks}, Completed Tasks = {len(completed_tasks)}")

        plt.legend()
        plt.title(f'Scheduling Latency')
        plt.xlabel('Task Index')
        plt.ylabel('Time Units (seconds)')
        plt.plot(waiting_times, 'ro-', label='Waiting Time - time on queue before election')
        plt.plot(leader_election_times, 'bo-', label='Consensus Time - time for leader election ')
        plt.grid(True)
        plot_path = os.path.join("", 'pbft-by-time.png')
        plt.savefig(plot_path)
        plt.close()

        plt.bar(list(tasks_per_agent.keys()), list(tasks_per_agent.values()), color='blue')
        plt.xlabel('Agent ID')
        plt.ylabel('Number of Tasks Executed')
        plt.title('Number of Tasks Executed by Each Agent')
        plt.grid(axis='y', linestyle='--', linewidth=0.5)
        plot_path = os.path.join("", 'pbft-by-agent.png')
        plt.savefig(plot_path)
        plt.close()
