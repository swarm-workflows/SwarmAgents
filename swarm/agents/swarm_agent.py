import csv
import os
import random
import time
import traceback
from queue import Queue, Empty
from typing import List

import matplotlib.pyplot as plt

from swarm.agents.agent import Agent
from swarm.comm.messages.commit import Commit
from swarm.comm.messages.heart_beat import HeartBeat
from swarm.comm.messages.message_builder import MessageBuilder
from swarm.comm.messages.prepare import Prepare
from swarm.comm.messages.proposal import Proposal
from swarm.comm.messages.task_info import TaskInfo
from swarm.comm.messages.task_status import TaskStatus
from swarm.models.capacities import Capacities
from swarm.models.agent_info import AgentInfo
from swarm.models.profile import Profile
from swarm.models.proposal_info import ProposalContainer, ProposalInfo
from swarm.models.task import Task, TaskState
import numpy as np


class IterableQueue:
    def __init__(self, *, source_queue: Queue):
        self.source_queue = source_queue

    def __iter__(self):
        while True:
            try:
                yield self.source_queue.get_nowait()
            except Empty:
                return


class SwarmAgent(Agent):
    def __init__(self, agent_id: str, config_file: str, cycles: int):
        super().__init__(agent_id, config_file, cycles)
        self.outgoing_proposals = ProposalContainer()
        self.incoming_proposals = ProposalContainer()
        self.shutdown = False
        self.shutdown_heartbeat = False
        self.last_msg_received_timestamp = 0

    def _build_heart_beat(self):
        load = self.compute_overall_load(proposed_caps=self.__get_proposed_capacities())
        agent = AgentInfo(agent_id=self.agent_id,
                          capacities=self.capacities,
                          capacity_allocations=self.allocated_tasks.capacities(),
                          load=load)
        self._save_load_metric(self.agent_id, load)
        return HeartBeat(agent=agent)

    def _process_messages(self, *, messages: List[dict]):
        for message in messages:
            try:
                begin = time.time()
                incoming = MessageBuilder.from_dict(message)

                if isinstance(incoming, HeartBeat):
                    self._receive_heartbeat(incoming=incoming)

                elif isinstance(incoming, Prepare):
                    self.__receive_prepare(incoming=incoming)

                elif isinstance(incoming, Commit):
                    self.__receive_commit(incoming=incoming)

                elif isinstance(incoming, Proposal):
                    self.__receive_proposal(incoming=incoming)

                elif isinstance(incoming, TaskStatus):
                    self.__receive_task_status(incoming=incoming)
                else:
                    self.logger.info(f"Ignoring unsupported message: {message}")
                diff = int(time.time() - begin)
                if diff > 0:
                    self.logger.info(f"Event {message.get('message_type')} TIME: {diff}")
            except Exception as e:
                self.logger.error(f"Error while processing message {type(message)}, {e}")
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
                        proposal = ProposalInfo(p_id=self.generate_id(), task_id=task.get_task_id(),
                                                agent_id=self.agent_id)
                        msg = Proposal(agent=AgentInfo(agent_id=self.agent_id), proposals=[proposal])

                        self.message_service.produce_message(msg.to_dict())
                        self.outgoing_proposals.add_proposal(proposal=proposal)
                        self.logger.info(f"Added proposal: {proposal}")

                        # Begin election for Job leader for this task
                        task.change_state(new_state=TaskState.PRE_PREPARE)
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

    def __compute_cost_matrix(self, tasks: list) -> np.ndarray:
        """
        Compute the cost matrix where rows represent agents and columns represent tasks.
        :param tasks: List of tasks to compute costs for.
        :return: A 2D numpy array where each entry [i, j] is the cost of agent i for task j.
        """
        agent_ids = [self.agent_id] + [peer.agent_id for peer in self.neighbor_map.values()]
        num_agents = len(agent_ids)
        num_tasks = len(tasks)

        # Initialize a cost matrix of shape (num_agents, num_tasks)
        cost_matrix = np.zeros((num_agents, num_tasks))

        # Compute costs for the current agent
        proposed_caps = self.__get_proposed_capacities()
        my_load = self.compute_overall_load(proposed_caps=proposed_caps)

        for j, task in enumerate(tasks):
            cost_of_job = self.compute_task_cost(task=task, total=self.capacities, profile=self.profile)
            feasibility = self.is_task_feasible(total=self.capacities, task=task)
            cost_matrix[0, j] = float('inf')
            if feasibility:
                cost_matrix[0, j] = my_load + feasibility * cost_of_job

        # Compute costs for neighboring agents
        for i, peer in enumerate(self.neighbor_map.values(), start=1):
            for j, task in enumerate(tasks):
                cost_of_job = self.compute_task_cost(task=task, total=peer.capacities, profile=self.profile)
                feasibility = self.is_task_feasible(total=peer.capacities, task=task)
                cost_matrix[i, j] = float('inf')
                if feasibility:
                    cost_matrix[i, j] = peer.load + feasibility * cost_of_job

        return cost_matrix

    def __find_min_cost_agents(self, cost_matrix: np.ndarray) -> list:
        """
        Find the agents with the minimum cost for each task.
        :param cost_matrix: A 2D numpy array where each entry [i, j] is the cost of agent i for task j.
        :return: A list of agent IDs corresponding to the minimum cost for each task.
        """
        # Find the index of the minimum cost for each task (column)
        min_cost_indices = np.argmin(cost_matrix, axis=0)

        # Map the indices back to agent IDs
        agent_ids = [self.agent_id] + [peer.agent_id for peer in self.neighbor_map.values()]
        min_cost_agents = [agent_ids[i] for i in min_cost_indices]

        return min_cost_agents

    def __can_become_leader(self, task: Task) -> bool:
        """
        Check if agent has enough resources to become a leader
            - Agent has resources to executed the task
            - Agent hasn't received a proposal from other agents for this task
            - Agent's load is less than 70%
        :param task:
        :return: True or False
        """
        cost_matrix = self.__compute_cost_matrix([task])
        min_cost_agents = self.__find_min_cost_agents(cost_matrix)
        if min_cost_agents[0] == self.agent_id:
            return True
        return False

    def __receive_proposal(self, incoming: Proposal):
        self.logger.debug(f"Received Proposal from: {incoming.agent.agent_id}")

        for p in incoming.proposals:
            task = self.task_queue.get_task(task_id=p.task_id)
            if not task or task.is_ready() or task.is_complete() or task.is_running():
                self.logger.info(f"Ignoring Proposal: {task}")
                return

            # Reject proposal in following cases:
            # - I have initiated a proposal and either received accepts from at least 1 peer or
            #   my proposal's seed is smaller than the incoming proposal
            # - Received and accepted proposal from another agent
            # - can't accommodate this task
            # - can accommodate task and neighbor's load is more than mine
            my_proposal = self.outgoing_proposals.get_proposal(task_id=p.task_id)
            peer_proposal = self.incoming_proposals.get_proposal(task_id=p.task_id)

            # if (my_proposal and (my_proposal.prepares or my_proposal.seed < rcvd_seed)) or \
            #        (peer_proposal and peer_proposal.seed < rcvd_seed) or \
            #        not can_accept_task or \
            #        (can_accept_task and my_current_load < neighbor_load):
            if (my_proposal and (my_proposal.prepares or my_proposal.seed < p.seed)) or \
                    (peer_proposal and peer_proposal.seed < p.seed):
                self.logger.debug(f"Agent {self.agent_id} rejected Proposal for Task: {p.task_id} from agent"
                                  f" {p.agent_id} - accepted another proposal")
            else:
                self.logger.debug(
                    f"Agent {self.agent_id} accepted Proposal for Task: {p.task_id} from agent"
                    f" {p.agent_id} and is now the leader")

                proposal = ProposalInfo(p_id=p.p_id, task_id=p.task_id, seed=p.seed,
                                        agent_id=p.agent_id)
                if my_proposal:
                    self.outgoing_proposals.remove_proposal(p_id=my_proposal.p_id, task_id=p.task_id)
                if peer_proposal:
                    self.incoming_proposals.remove_proposal(p_id=peer_proposal.p_id, task_id=p.task_id)
                task.set_time_on_queue()

                self.incoming_proposals.add_proposal(proposal=proposal)
                msg = Prepare(agent=AgentInfo(agent_id=self.agent_id), proposals=[proposal])
                self.message_service.produce_message(msg.to_dict())
                task.change_state(TaskState.PREPARE)

    def __receive_prepare(self, incoming: Prepare):

        # Prepare for the proposal
        self.logger.debug(f"Received prepare from: {incoming.agent.agent_id}")

        for p in incoming.proposals:
            task = self.task_queue.get_task(task_id=p.task_id)
            if not task or task.is_ready() or task.is_complete() or task.is_running():
                self.logger.info(f"Ignoring Prepare: {task}")
                return

            # Update the prepare votes
            if self.outgoing_proposals.contains(task_id=p.task_id, p_id=p.p_id):
                proposal = self.outgoing_proposals.get_proposal(p_id=p.p_id)
            elif self.incoming_proposals.contains(task_id=p.task_id, p_id=p.p_id):
                proposal = self.incoming_proposals.get_proposal(p_id=p.p_id)
            else:
                proposal = ProposalInfo(task_id=p.task_id, p_id=p.p_id, seed=p.seed,
                                        agent_id=p.agent_id)
                self.incoming_proposals.add_proposal(proposal=proposal)

            proposal.prepares += 1

            quorum_count = (len(self.neighbor_map)) / 2
            task.change_state(TaskState.PREPARE)

            # Check if vote count is more than quorum
            # if yes, send commit
            if proposal.prepares >= quorum_count:
                self.logger.info(f"Agent: {self.agent_id} Task: {p.task_id} received quorum "
                                 f"prepares: {proposal.prepares}, starting commit!")

                msg = Commit(agent=AgentInfo(agent_id=self.agent_id), proposals=[proposal])
                self.message_service.produce_message(msg.to_dict())

                task.change_state(TaskState.COMMIT)

    def __receive_commit(self, incoming: Commit):
        self.logger.debug(f"Received commit from: {incoming.agent.agent_id}")
        for p in incoming.proposals:
            task = self.task_queue.get_task(task_id=p.task_id)

            if not task or task.is_complete() or task.is_ready() or task.is_running() or task.leader_agent_id:
                self.logger.info(f"Ignoring Commit: {task}")
                self.incoming_proposals.remove_task(task_id=p.task_id)
                self.outgoing_proposals.remove_task(task_id=p.task_id)
                return

            # Update the commit votes;
            if self.outgoing_proposals.contains(task_id=p.task_id, p_id=p.p_id):
                proposal = self.outgoing_proposals.get_proposal(p_id=p.p_id)
            elif self.incoming_proposals.contains(task_id=p.task_id, p_id=p.p_id):
                proposal = self.incoming_proposals.get_proposal(p_id=p.p_id)
            else:
                proposal = ProposalInfo(task_id=p.task_id, p_id=p.p_id, seed=p.seed,
                                        agent_id=p.agent_id)
                self.incoming_proposals.add_proposal(proposal=proposal)

            proposal.commits += 1
            quorum_count = (len(self.neighbor_map)) / 2

            if proposal.commits >= quorum_count:
                self.logger.info(
                    f"Agent: {self.agent_id} received quorum commits for Task: {p.task_id} Proposal: {proposal}: Task: {task}")
                task.set_time_to_elect_leader()
                task.set_leader(leader_agent_id=proposal.agent_id)
                if self.outgoing_proposals.contains(task_id=p.task_id, p_id=p.p_id):
                    self.logger.info(f"LEADER CONSENSUS achieved for Task: {p.task_id} Leader: {self.agent_id}")
                    task.change_state(new_state=TaskState.READY)
                    self.allocate_task(task)
                    self.outgoing_proposals.remove_task(task_id=p.task_id)
                else:
                    self.logger.info(f"PARTICIPANT CONSENSUS achieved for Task: {p.task_id} Leader: {p.agent_id}")
                    task.change_state(new_state=TaskState.COMMIT)
                    self.incoming_proposals.remove_task(task_id=p.task_id)

    def __receive_task_status(self, incoming: TaskStatus):
        self.logger.debug(f"Received Status from: {incoming.agent.agent_id}")

        for t in incoming.tasks:
            task = self.task_queue.get_task(task_id=t.task_id)
            task.set_leader(leader_agent_id=incoming.agent.agent_id)
            task.set_time_to_completion()
            if not task or task.is_complete() or task.is_ready():
                self.logger.info(f"Ignoring Task Status: {task}")
                return

            # Update the task status based on broadcast message
            task.change_state(new_state=TaskState.COMPLETE)
            self.incoming_proposals.remove_task(task_id=t.task_id)
            self.outgoing_proposals.remove_task(task_id=t.task_id)

    def execute_task(self, task: Task):
        super().execute_task(task=task)
        msg = TaskStatus(agent=AgentInfo(agent_id=self.agent_id), tasks=[TaskInfo(task_id=task.get_task_id(),
                                                                                  state=task.state)])
        self.message_service.produce_message(msg.to_dict())

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

        # Title with SWARM and number of agents
        num_agents = len(tasks_per_agent)
        plt.title(f'SWARM: Number of Tasks Executed by Each Agent (Total Agents: {num_agents})')

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

        # Title with SWARM and number of agents
        num_agents = len(set([t.leader_agent_id for t in completed_tasks]))
        plt.title(f'SWARM: Scheduling Latency (Total Agents: {num_agents})')

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
        self.save_load_to_csv_and_plot(self.load_per_agent)
        self.logger.info("Plot completed")

    def __get_proposed_capacities(self):
        proposed_capacities = Capacities()
        tasks = self.outgoing_proposals.tasks()
        for task_id in tasks:
            proposed_task = self.task_queue.get_task(task_id=task_id)
            proposed_capacities += proposed_task.get_capacities()
        self.logger.debug(f"Number of outgoing proposals: {len(tasks)}; Tasks: {tasks}")
        return proposed_capacities

    @staticmethod
    def is_task_feasible(task: Task, total: Capacities, proposed_caps: Capacities = Capacities(),
                         allocated_caps: Capacities = Capacities()):
        allocated_caps += proposed_caps
        available = total - allocated_caps

        # Check if the agent can accommodate the given task based on its capacities
        # Compare the requested against available
        available = available - task.get_capacities()
        negative_fields = available.negative_fields()
        if len(negative_fields) > 0:
            return 0

        return 1

    @staticmethod
    def compute_task_cost(task: Task, total: Capacities, profile: Profile):
        core_load = (task.capacities.core / total.core) * 100
        ram_load = (task.capacities.ram / total.ram) * 100
        disk_load = (task.capacities.disk / total.disk) * 100

        cost = (core_load * profile.core_weight +
                ram_load * profile.ram_weight +
                disk_load * profile.disk_weight) / (profile.core_weight +
                                                    profile.ram_weight +
                                                    profile.disk_weight)
        return cost

    @staticmethod
    def save_load_to_csv_and_plot(load_dict: dict, csv_filename='agent_loads.csv',
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
        plt.title('Agent Loads Over Time')
        plt.legend()
        plt.grid(True)

        # Save the plot to a file
        plt.savefig(plot_filename)
