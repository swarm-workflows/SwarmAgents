import csv
import json
import os
import queue
import random
import threading
import time
import traceback
from queue import Queue, Empty

import matplotlib.pyplot as plt

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
            message = json.loads(incoming)
            if message.get("agent_id") == self.agent_id:
                return
            self.message_queue.put_nowait(message)
            with self.condition:
                self.condition.notify_all()
            self.logger.debug(f"Added incoming message to queue: {incoming}")
        except Exception as e:
            self.logger.debug(f"Failed to add incoming message to queue: {incoming}: e: {e}")

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
                self.__consensus(incoming=message)
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

                    # DISABLE THIS
                    #diff = int(time.time() - task.time_last_state_change)
                    #if diff > 120 and task.get_state() in [TaskState.PREPARE, TaskState.PRE_PREPARE]:
                    #    task.change_state(new_state=TaskState.PENDING)

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
                        self.logger.info(f"Added proposal: {proposal}")
                        #self.logger.info(f"Outgoing Proposals: {self.outgoing_proposals.size()}")
                        #self.logger.info(f"Incoming Proposals: {self.incoming_proposals.size()}")

                        # Begin election for Job leader for this task
                        task.change_state(new_state=TaskState.PRE_PREPARE)
                        #self.logger.debug(
                        #    f"Agent: {self.agent_id} sent Proposal: {proposal} for Task: {task.task_id}!")
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
        proposed_capacities = self.__get_proposed_capacities()
        #proposed_capacities += task.get_capacities()
        my_load = self.compute_overall_load(proposed_caps=proposed_capacities)
        #self.logger.info(f"Overall Load: {my_load}")
        least_loaded_neighbor = self.__find_neighbor_with_lowest_load()

        if least_loaded_neighbor and least_loaded_neighbor.get('load') < my_load:
            self.logger.debug(f"Can't become leader as my load: {my_load} is more than all of "
                              f"the neighbors: {least_loaded_neighbor}")
            return False

        can_accommodate = self.can_accommodate_task(task=task, proposed_caps=proposed_capacities)
        incoming = self.incoming_proposals.contains(task_id=task.get_task_id())
        if not incoming and \
                can_accommodate and \
                my_load < 70.00:
            return can_accommodate
        self.logger.info(
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
            self.logger.info(f"Ignoring Proposal: {task}")
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
        #self.logger.debug(f"Task: {task}")
        if not task or task.is_ready() or task.is_complete() or task.is_running():
            self.logger.info(f"Ignoring Prepare: {task}")
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

        quorum_count = (len(self.neighbor_map)) / 2
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
        #self.logger.debug(f"Task: {task}")

        if not task or task.is_complete() or task.is_ready() or task.is_running() or task.leader_agent_id:
            self.logger.info(f"Ignoring Commit: {task}")
            self.incoming_proposals.remove_task(task_id=task_id)
            self.outgoing_proposals.remove_task(task_id=task_id)
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
        quorum_count = (len(self.neighbor_map)) / 2

        if proposal.commits >= quorum_count:
            self.logger.info(
                f"Agent: {self.agent_id} received quorum commits for Task: {task_id} Proposal: {proposal}: Task: {task}")
            task.set_time_to_elect_leader()
            task.set_leader(leader_agent_id=proposal.agent_id)
            if self.outgoing_proposals.contains(task_id=task_id, p_id=proposal_id):
                self.logger.info(f"LEADER CONSENSUS achieved for Task: {task_id} Leader: {self.agent_id}")
                task.change_state(new_state=TaskState.READY)
                self.allocate_task(task)
                self.outgoing_proposals.remove_task(task_id=task_id)
            else:
                self.logger.info(f"PARTICIPANT CONSENSUS achieved for Task: {task_id} Leader: {peer_agent_id}")
                task.change_state(new_state=TaskState.COMMIT)
                self.incoming_proposals.remove_task(task_id=task_id)

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
        #self.logger.debug(f"Task: {task}")
        if not task or task.is_complete() or task.is_ready():
            self.logger.info(f"Ignoring Task Status: {task}")
            return

        # Update the task status based on broadcast message
        task.change_state(new_state=TaskState.COMPLETE)
        self.incoming_proposals.remove_task(task_id=task_id)
        self.outgoing_proposals.remove_task(task_id=task_id)

    def __consensus(self, incoming):
        """
        Consensus Loop
        :param message:
        :return:
        """
        try:
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
                self.logger.info(f"Ignoring unsupported message: {incoming}")
        except Exception as e:
            self.logger.error(f"Error while processing incoming message: {incoming}: {e}")
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
            "load": self.compute_overall_load(proposed_caps=self.__get_proposed_capacities())
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

    def plot_tasks_per_agent(self):
        tasks = self.task_queue.tasks.values()
        completed_tasks = [t for t in tasks if t.leader_agent_id is not None]
        tasks_per_agent = {}

        for t in completed_tasks:
            if t.leader_agent_id:
                if t.leader_agent_id not in tasks_per_agent:
                    tasks_per_agent[t.leader_agent_id] = 0
                tasks_per_agent[t.leader_agent_id] += 1

        # Save tasks_per_agent to CSV
        with open('tasks_per_agent.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['Agent ID', 'Number of Tasks Executed'])
            for agent_id, task_count in tasks_per_agent.items():
                writer.writerow([agent_id, task_count])

        # Plotting the tasks per agent as a bar chart
        plt.bar(list(tasks_per_agent.keys()), list(tasks_per_agent.values()), color='blue')
        plt.xlabel('Agent ID')
        plt.ylabel('Number of Tasks Executed')

        # Title with PBFT and number of agents
        num_agents = len(tasks_per_agent)
        plt.title(f'PBFT: Number of Tasks Executed by Each Agent (Total Agents: {num_agents})')

        plt.grid(axis='y', linestyle='--', linewidth=0.5)

        # Save the plot
        plot_path = os.path.join("", 'tasks-per-agent.png')
        plt.savefig(plot_path)
        plt.close()

    def plot_scheduling_latency(self):
        tasks = self.task_queue.tasks.values()
        completed_tasks = [t for t in tasks if t.leader_agent_id is not None]

        # Calculate scheduling latency
        scheduling_latency = [
            t.time_on_queue + t.time_to_elect_leader
            for t in completed_tasks
            if t.time_on_queue is not None and t.time_to_elect_leader is not None
        ]

        # Save scheduling latency, time_on_queue, and time_to_elect_leader as CSV
        wait_time_data = [(t.time_on_queue,) for t in completed_tasks if t.time_on_queue is not None]
        election_time_data = [(t.time_to_elect_leader,) for t in tasks if t.time_to_elect_leader is not None]
        latency_data = [(latency,) for latency in scheduling_latency]

        with open('wait_time.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['time_on_queue'])
            writer.writerows(wait_time_data)

        with open('election_time.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['time_to_elect_leader'])
            writer.writerows(election_time_data)

        with open('scheduling_latency.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['scheduling_latency'])
            writer.writerows(latency_data)

        # Plotting scheduling latency in red
        plt.plot(scheduling_latency, 'ro-', label='Scheduling Latency (Waiting Time + Leader Election Time)')

        # Plotting wait time in blue
        if wait_time_data:
            wait_times = [data[0] for data in wait_time_data]
            plt.plot(wait_times, 'bo-', label='Waiting Time')

        # Plotting leader election time in green
        if election_time_data:
            election_times = [data[0] for data in election_time_data]
            plt.plot(election_times, 'go-', label='Leader Election Time')

        # Title with PBFT and number of agents
        num_agents = len(set([t.leader_agent_id for t in completed_tasks]))
        plt.title(f'PBFT: Scheduling Latency (Total Agents: {num_agents})')

        plt.xlabel('Task Index')
        plt.ylabel('Time Units (seconds)')
        plt.grid(True)

        # Adjusting legend position to avoid overlapping the graph
        plt.legend(loc='upper left', bbox_to_anchor=(1, 1))  # This places the legend outside the plot area

        # Save the plot
        plot_path = os.path.join("", 'scheduling-latency.png')
        plt.savefig(plot_path, bbox_inches='tight')  # bbox_inches='tight' ensures that the entire plot is saved
        plt.close()

    def plot_results(self):
        if self.agent_id != "0":
            return
        self.plot_tasks_per_agent()
        self.plot_scheduling_latency()

    def __get_proposed_capacities(self):
        proposed_capacities = Capacities()
        tasks = self.outgoing_proposals.tasks()
        for task_id in tasks:
            proposed_task = self.task_queue.get_task(task_id=task_id)
            proposed_capacities += proposed_task.get_capacities()
        self.logger.debug(f"Number of outgoing proposals: {len(tasks)}; Tasks: {tasks}")
        return proposed_capacities
