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

import random
import time
import traceback
from typing import List


from swarm.agents.agent import Agent
from swarm.comm.messages.commit import Commit
from swarm.comm.messages.heart_beat import HeartBeat
from swarm.comm.messages.message_builder import MessageBuilder
from swarm.comm.messages.prepare import Prepare
from swarm.comm.messages.proposal import Proposal
from swarm.comm.messages.job_info import JobInfo
from swarm.comm.messages.job_status import JobStatus
from swarm.models.capacities import Capacities
from swarm.models.agent_info import AgentInfo
from swarm.models.profile import Profile
from swarm.models.proposal_info import ProposalContainer, ProposalInfo
from swarm.models.job import Job, JobState
import numpy as np


class SwarmAgent(Agent):
    def __init__(self, agent_id: int, config_file: str):
        super().__init__(agent_id, config_file)
        self.outgoing_proposals = ProposalContainer()
        self.incoming_proposals = ProposalContainer()

    def _build_heart_beat(self, only_self: bool = False) -> dict:
        agents = {}
        my_load = self.compute_overall_load(proposed_jobs=self.outgoing_proposals.jobs())
        agent = AgentInfo(agent_id=self.agent_id,
                          capacities=self.capacities,
                          capacity_allocations=self.queues.ready_queue.capacities(jobs=self.queues.ready_queue.get_jobs()),
                          load=my_load,
                          last_updated=time.time())
        self._save_load_metric(self.agent_id, my_load)
        if not only_self and isinstance(self.messaging.topology_peer_agent_list, list) and len(self.neighbor_map.values()):
            for peer_agent_id, peer in self.neighbor_map.items():
                if peer_agent_id is None:
                    continue
                agents[peer_agent_id] = peer

        agents[self.agent_id] = agent

        return agents

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

                elif isinstance(incoming, JobStatus):
                    self.__receive_job_status(incoming=incoming)
                else:
                    self.logger.info(f"Ignoring unsupported message: {message}")
                diff = int(time.time() - begin)
                if diff > 0:
                    self.logger.info(f"Event {message.get('message_type')} TIME: {diff}")
            except Exception as e:
                self.logger.error(f"Error while processing message {type(message)}, {e}")
                self.logger.error(traceback.format_exc())

    def job_selection_main(self):
        self.logger.info(f"Starting agent: {self}")
        time.sleep(10)
        completed_jobs = 0
        while not self.shutdown:
            try:
                processed = 0
                for job in self.queues.job_queue.get_jobs():
                    if job.is_complete():
                        completed_jobs += 1
                        continue

                    diff = int(time.time() - job.time_last_state_change)
                    if diff > self.restart_job_selection and job.get_state() in [JobState.PREPARE,
                                                                                 JobState.PRE_PREPARE]:
                        self.logger.info(f"RESTART: Job: {job} reset to Pending")
                        job.change_state(new_state=JobState.PENDING)
                        self.outgoing_proposals.remove_job(job_id=job.get_job_id())
                        self.incoming_proposals.remove_job(job_id=job.get_job_id())
                        self.restart_job_selection_cnt += 1

                    if not job.is_pending():
                        if job.get_leader_agent_id() is None:
                            proposal1 = self.outgoing_proposals.get_proposal(job_id=job.get_job_id())
                            proposal2 = self.incoming_proposals.get_proposal(job_id=job.get_job_id())
                            self.logger.debug(
                                f"Job: {job.job_id} State: {job.state}; out: {proposal1} in: {proposal2} skipping it!")
                        continue

                    processed += 1

                    # Trigger leader election for a job after random sleep
                    election_timeout = random.uniform(150, 300) / 1000
                    time.sleep(election_timeout)

                    if self.__can_select_job(job=job):
                        # Send proposal to all neighbors
                        proposal = ProposalInfo(p_id=self.generate_id(), job_id=job.get_job_id(),
                                                agent_id=self.agent_id)
                        msg = Proposal(agents=[AgentInfo(agent_id=self.agent_id)], proposals=[proposal])

                        self._send_message(json_message=msg.to_dict())
                        self.outgoing_proposals.add_proposal(proposal=proposal)
                        self.logger.info(f"Added proposal: {proposal}")

                        # Begin election for Job leader for this job
                        job.change_state(new_state=JobState.PRE_PREPARE)

                    if processed >= 40:
                        time.sleep(1)
                        processed = 0

                time.sleep(1)  # Adjust the sleep duration as needed

            except Exception as e:
                self.logger.error(f"Error occurred while executing e: {e}")
                self.logger.error(traceback.format_exc())
        self.logger.info(f"Agent: {self} stopped with restarts: {self.restart_job_selection_cnt}!")

    def __compute_cost_matrix(self, jobs: list) -> np.ndarray:
        """
        Compute the cost matrix where rows represent agents and columns represent jobs.
        :param jobs: List of jobs to compute costs for.
        :return: A 2D numpy array where each entry [i, j] is the cost of agent i for job j.
        """
        agent_ids = [self.agent_id] + [peer.agent_id for peer in self.neighbor_map.values()]
        num_agents = len(agent_ids)
        num_jobs = len(jobs)

        # Initialize a cost matrix of shape (num_agents, num_jobs)
        cost_matrix = np.zeros((num_agents, num_jobs))

        # Compute costs for the current agent
        my_load = self.compute_overall_load(proposed_jobs=self.outgoing_proposals.jobs())

        for j, job in enumerate(jobs):
            cost_of_job = self.compute_job_cost(job=job, total=self.capacities, profile=self.profile)
            feasibility = self.is_job_feasible(total=self.capacities, job=job,
                                               projected_load=my_load + cost_of_job)
            cost_matrix[0, j] = float('inf')
            if feasibility:
                cost_matrix[0, j] = my_load + feasibility * cost_of_job

        # Compute costs for neighboring agents
        for i, peer in enumerate(self.neighbor_map.values(), start=1):

            for j, job in enumerate(jobs):
                cost_of_job = self.compute_job_cost(job=job, total=peer.capacities, profile=self.profile)

                feasibility = self.is_job_feasible(total=peer.capacities, job=job,
                                                   projected_load=peer.load + cost_of_job)
                cost_matrix[i, j] = float('inf')
                if feasibility:
                    cost_matrix[i, j] = peer.load + feasibility * cost_of_job

        return cost_matrix

    def __find_min_cost_agents(self, cost_matrix: np.ndarray) -> list:
        """
        Find the agents with the minimum cost for each job, excluding those with infinite costs.
        :param cost_matrix: A 2D numpy array where each entry [i, j] is the cost of agent i for job j.
        :return: A list of agent IDs corresponding to the minimum cost for each job.
        """
        min_cost_agents = []
        agent_ids = [self.agent_id] + [peer.agent_id for peer in self.neighbor_map.values()]

        for j in range(cost_matrix.shape[1]):  # Iterate over each job (column)
            valid_costs = cost_matrix[:, j]  # Get the costs for job j
            finite_costs = valid_costs[valid_costs != float('inf')]  # Filter out infinite costs

            if len(finite_costs) > 0:  # If there are any finite costs
                min_index = np.argmin(finite_costs)  # Find the index of the minimum cost
                original_index = np.where(valid_costs == finite_costs[min_index])[0][0]  # Get the original index
                min_cost_agents.append(agent_ids[original_index])

        return min_cost_agents

    def __can_select_job(self, job: Job) -> bool:
        """
        Check if agent has enough resources to become a leader
            - Agent has resources to executed the job
            - Agent hasn't received a proposal from other agents for this job
            - Agent's load is less than 70%
        :param job:
        :return: True or False
        """
        cost_matrix = self.__compute_cost_matrix([job])
        min_cost_agents = self.__find_min_cost_agents(cost_matrix)
        if min_cost_agents[0] == self.agent_id:
            return True
        self.logger.info(f"Job: {job} not selected for consensus!")
        return False

    def __receive_proposal(self, incoming: Proposal):
        self.logger.debug(f"Received Proposal from: {incoming.agents[0].agent_id}")

        for p in incoming.proposals:
            job = self.queues.job_queue.get_job(job_id=p.job_id)
            if not job or job.is_ready() or job.is_complete() or job.is_running():
                self.logger.info(f"Job: {job} Ignoring Proposal: {p}")
                return

            my_proposal = self.outgoing_proposals.get_proposal(job_id=p.job_id)
            peer_proposal = self.incoming_proposals.get_proposal(job_id=p.job_id)

            if my_proposal and (len(my_proposal.prepares) or my_proposal.seed < p.seed):
                self.logger.debug(f"Job:{p.job_id} Agent:{self.agent_id} rejected Proposal: {p} from agent"
                                  f" {p.agent_id} - my proposal {my_proposal} has prepares or smaller seed")
                self.conflicts += 1
            elif peer_proposal and peer_proposal.seed < p.seed:
                self.logger.debug(f"Job:{p.job_id} Agent:{self.agent_id} rejected Proposal: {p} from agent"
                                  f" {p.agent_id} - already accepted proposal {peer_proposal} with a smaller seed")
                self.conflicts += 1
            else:
                self.logger.debug(
                    f"Job:{p.job_id} Agent:{self.agent_id} accepted Proposal: {p} from agent"
                    f" {p.agent_id} and is now the leader")

                p.prepares = []
                if my_proposal:
                    self.logger.info(f"Removed my Proposal: {my_proposal} in favor of incoming proposal")
                    self.outgoing_proposals.remove_proposal(p_id=my_proposal.p_id, job_id=p.job_id)
                if peer_proposal:
                    self.logger.info(f"Removed peer Proposal: {peer_proposal} in favor of incoming proposal")
                    self.incoming_proposals.remove_proposal(p_id=peer_proposal.p_id, job_id=p.job_id)

                msg = Prepare(agents=[AgentInfo(agent_id=self.agent_id)], proposals=[p])
                self._send_message(json_message=msg.to_dict())

                # Increment the number of prepares to count the prepare being sent
                # Needed to handle 3 agent case
                if incoming.agents[0].agent_id not in p.prepares:
                    p.prepares.append(incoming.agents[0].agent_id)
                self.incoming_proposals.add_proposal(proposal=p)
                job.change_state(JobState.PREPARE)

    def __receive_prepare(self, incoming: Prepare):

        # Prepare for the proposal
        self.logger.debug(f"Received prepare from: {incoming.agents[0].agent_id}")

        for p in incoming.proposals:
            job = self.queues.job_queue.get_job(job_id=p.job_id)
            if not job or job.is_ready() or job.is_complete() or job.is_running():
                self.logger.info(f"Job: {job} Ignoring Prepare: {p}")
                return

            # Update the prepare votes
            if self.outgoing_proposals.contains(job_id=p.job_id, p_id=p.p_id):
                proposal = self.outgoing_proposals.get_proposal(p_id=p.p_id)
            elif self.incoming_proposals.contains(job_id=p.job_id, p_id=p.p_id):
                proposal = self.incoming_proposals.get_proposal(p_id=p.p_id)
            else:
                proposal = p
                p.prepares = []
                self.incoming_proposals.add_proposal(proposal=p)

            if incoming.agents[0].agent_id not in proposal.prepares:
                p.prepares.append(incoming.agents[0].agent_id)
            quorum_count = (len(self.neighbor_map) // 2) + 1  # Ensure a true majority
            job.change_state(JobState.PREPARE)  # Consider the necessity of this state change

            if len(proposal.prepares) >= quorum_count:
                self.logger.info(f"Job: {p.job_id} Agent: {self.agent_id} received quorum "
                                 f"prepares: {proposal.prepares}, starting commit!")

                msg = Commit(agents=[AgentInfo(agent_id=self.agent_id)], proposals=[proposal])
                self._send_message(json_message=msg.to_dict())

                job.change_state(JobState.COMMIT)

    def __receive_commit(self, incoming: Commit):
        self.logger.debug(f"Received commit from: {incoming.agents[0].agent_id}")
        for p in incoming.proposals:
            job = self.queues.job_queue.get_job(job_id=p.job_id)

            if not job or job.is_complete() or job.is_ready() or job.is_running() or \
                    job.leader_agent_id is not None:
                self.logger.info(f"Job: {job} Ignoring Commit: {p}")
                self.incoming_proposals.remove_job(job_id=p.job_id)
                self.outgoing_proposals.remove_job(job_id=p.job_id)
                return

            if self.outgoing_proposals.contains(job_id=p.job_id, p_id=p.p_id):
                proposal = self.outgoing_proposals.get_proposal(p_id=p.p_id)
            elif self.incoming_proposals.contains(job_id=p.job_id, p_id=p.p_id):
                proposal = self.incoming_proposals.get_proposal(p_id=p.p_id)
            else:
                self.logger.info(f"TBD: Job: {p.job_id} Agent: {self.agent_id} received commit without any Prepares")
                proposal = p
                p.prepares = []
                p.commits = []
                self.incoming_proposals.add_proposal(proposal=proposal)

            if incoming.agents[0].agent_id not in proposal.commits:
                proposal.commits.append(incoming.agents[0].agent_id)
            quorum_count = (len(self.neighbor_map) // 2) + 1  # Ensure a true majority

            if len(proposal.commits) >= quorum_count:
                self.logger.info(
                    f"Job: {p.job_id} Agent: {self.agent_id} received quorum commits Proposal: {proposal}: Job: {job}")
                job.set_leader(leader_agent_id=proposal.agent_id)
                if self.outgoing_proposals.contains(job_id=p.job_id, p_id=p.p_id):
                    self.logger.info(f"LEADER CONSENSUS achieved for Job: {p.job_id} Leader: {self.agent_id}")
                    job.change_state(new_state=JobState.READY)
                    self.select_job(job)
                    self.outgoing_proposals.remove_job(job_id=p.job_id)
                else:
                    self.logger.info(f"PARTICIPANT CONSENSUS achieved for Job: {p.job_id} Leader: {p.agent_id}")
                    job.change_state(new_state=JobState.COMMIT)
                    self.incoming_proposals.remove_job(job_id=p.job_id)

    def __receive_job_status(self, incoming: JobStatus):
        self.logger.debug(f"Received Status from: {incoming.agents[0].agent_id}")

        for t in incoming.jobs:
            job = self.queues.job_queue.get_job(job_id=t.job_id)
            if not job:
                self.logger.info(f"Received Job Status for non-existent job: {t.job_id}")
                continue

            if job.is_complete() or job.is_ready():
                self.logger.info(f"Job: {job} Ignoring Job Status (State: {job.state})")
                continue

            # Update the job status based on broadcast message
            self.logger.debug(f"Updating Job: {job.job_id} state to COMPLETE")
            job.set_leader(leader_agent_id=incoming.agents[0].agent_id)
            job.change_state(new_state=JobState.COMPLETE)
            self.incoming_proposals.remove_job(job_id=t.job_id)
            self.outgoing_proposals.remove_job(job_id=t.job_id)

    def execute_job(self, job: Job):
        super().execute_job(job=job)
        msg = JobStatus(agents=[AgentInfo(agent_id=self.agent_id)], jobs=[JobInfo(job_id=job.get_job_id(),
                                                                                  state=job.state)])
        self._send_message(json_message=msg.to_dict())

    def __get_proposed_capacities(self):
        proposed_capacities = Capacities()
        jobs = self.outgoing_proposals.jobs()
        for job_id in jobs:
            proposed_job = self.queues.job_queue.get_job(job_id=job_id)
            proposed_capacities += proposed_job.get_capacities()
        self.logger.debug(f"Number of outgoing proposals: {len(jobs)}; Jobs: {jobs}")
        return proposed_capacities

    @staticmethod
    def compute_job_cost(job: Job, total: Capacities, profile: Profile):
        core_load = (job.capacities.core / total.core) * 100
        ram_load = (job.capacities.ram / total.ram) * 100
        disk_load = (job.capacities.disk / total.disk) * 100

        cost = (core_load * profile.core_weight +
                ram_load * profile.ram_weight +
                disk_load * profile.disk_weight) / (profile.core_weight +
                                                    profile.ram_weight +
                                                    profile.disk_weight)
        return cost
