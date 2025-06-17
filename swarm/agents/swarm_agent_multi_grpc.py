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


from swarm.agents.agent_grpc import Agent
from swarm.comm.messages.commit import Commit
from swarm.comm.messages.message_builder import MessageBuilder
from swarm.comm.messages.prepare import Prepare
from swarm.comm.messages.proposal import Proposal
from swarm.comm.messages.job_status import JobStatus
from swarm.database.repository import Repository
from swarm.models.capacities import Capacities
from swarm.models.agent_info import AgentInfo
from swarm.models.proposal_info import ProposalContainer, ProposalInfo
from swarm.models.job import Job, JobState
import numpy as np


class SwarmAgent(Agent):
    def __init__(self, agent_id: int, config_file: str):
        super().__init__(agent_id, config_file)
        self.outgoing_proposals = ProposalContainer()
        self.incoming_proposals = ProposalContainer()

    def _process(self, messages: list[dict]):
        for message in messages:
            try:
                begin = time.time()
                incoming = MessageBuilder.from_dict(message)

                if isinstance(incoming, Prepare):
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
        while self.live_agent_count != self.configured_agent_count:
            time.sleep(5)
            self.logger.info("[SEL_WAIT] Waiting for Peer map to be populated!")

        while not self.shutdown:
            try:
                proposals = []  # List to accumulate proposals for multiple jobs
                caps_jobs_selected = Capacities()

                '''
                # Compute current consensus load
                in_progress_jobs = [
                    job for job in self.queues.job_queue.get_jobs()
                    if job.get_state() in [JobState.PRE_PREPARE, JobState.PREPARE, JobState.COMMIT] and
                       not self.is_job_completed(job.get_job_id())
                ]
                total_in_progress = len(in_progress_jobs)
                allowed_new_proposals = max(0, self.max_pending_elections - total_in_progress)

                
                if allowed_new_proposals == 0:
                    self.logger.info(f"[CONSENSUS LIMIT] Max active consensus jobs in progress - "
                                     f"{self.max_pending_elections} - {total_in_progress} {allowed_new_proposals}. "
                                     f"Waiting...")
                    time.sleep(1)
                    continue
                '''

                for job in self.queues.job_queue.get_jobs():
                    if self.is_job_completed(job_id=job.get_job_id()):
                        continue

                    '''
                    diff = int(time.time() - job.time_last_state_change)
                    if diff > self.restart_job_selection and job.get_state() in [JobState.PREPARE,
                                                                                 JobState.PRE_PREPARE]:
                        self.logger.info(f"RESTART: Job: {job} reset to Pending")
                        job.change_state(new_state=JobState.PENDING)
                        self.outgoing_proposals.remove_job(job_id=job.get_job_id())
                        self.incoming_proposals.remove_job(job_id=job.get_job_id())
                        self.restart_job_selection_cnt += 1
                    '''

                    if not job.is_pending():
                        if job.get_leader_agent_id() is None:
                            proposal1 = self.outgoing_proposals.get_proposal(job_id=job.get_job_id())
                            proposal2 = self.incoming_proposals.get_proposal(job_id=job.get_job_id())
                            self.logger.debug(
                                f"[SEL_SKIP] Job: {job.job_id} State: {job.state}; out: {proposal1} in: {proposal2} skipping it!")
                        continue

                    # Trigger leader election for a job after random sleep
                    election_timeout = random.uniform(150, 300) / 1000
                    time.sleep(election_timeout)

                    status, cost = self.__can_select_job(job=job, caps_jobs_selected=caps_jobs_selected)
                    if status:
                        # Send proposal to all neighbors
                        proposal = ProposalInfo(p_id=self.generate_id(), job_id=job.get_job_id(),
                                                agent_id=self.agent_id, seed=cost + self.agent_id)
                        proposals.append(proposal)
                        caps_jobs_selected += job.get_capacities()
                        #job.change_state(new_state=JobState.PRE_PREPARE)

                    # ONLY PROPOSE UP TO ALLOWED LIMIT:
                    #if len(proposals) >= min(self.proposal_job_batch_size, allowed_new_proposals):
                    if len(proposals) >= self.proposal_job_batch_size:
                        msg = Proposal(source=self.agent_id,
                                       agents=[AgentInfo(agent_id=self.agent_id)],
                                       proposals=proposals)
                        self._send_message(json_message=msg.to_dict())
                        for p in proposals:
                            self.outgoing_proposals.add_proposal(p)  # Add all proposals
                            # Begin election for Job leader for this job
                            job = self.queues.job_queue.get_job(job_id=p.job_id)
                            job.change_state(new_state=JobState.PRE_PREPARE)
                        proposals.clear()
                        caps_jobs_selected = Capacities()

                # Send remaining proposals if any exist
                if proposals:
                    msg = Proposal(source=self.agent_id, agents=[AgentInfo(agent_id=self.agent_id)],
                                   proposals=proposals)
                    self._send_message(json_message=msg.to_dict())
                    for p in proposals:
                        self.outgoing_proposals.add_proposal(p)
                    proposals.clear()

                time.sleep(0.005)

            except Exception as e:
                self.logger.error(f"Error occurred while executing e: {e}")
                self.logger.error(traceback.format_exc())
        self.logger.info(f"Agent: {self} stopped with restarts: {self.metrics.restart_job_selection_cnt}!")

    def __compute_cost_matrix(self, jobs: List[Job], caps_jobs_selected: Capacities) -> np.ndarray:
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
        projected_load = self.compute_projected_load(overall_load_actual=my_load,
                                                     proposed_caps=caps_jobs_selected)

        for j, job in enumerate(jobs):
            cost_of_job = self.compute_job_cost(job=job, total=self.capacities)
            feasibility = self.is_job_feasible(total=self.capacities, job=job,
                                               projected_load=projected_load + cost_of_job)
            cost_matrix[0, j] = float('inf')
            if feasibility:
                cost_matrix[0, j] = projected_load + feasibility * cost_of_job

        # Compute costs for neighboring agents
        for i, peer in enumerate(self.neighbor_map.values(), start=1):
            projected_load = self.compute_projected_load(overall_load_actual=peer.load,
                                                         proposed_caps=caps_jobs_selected)

            for j, job in enumerate(jobs):
                cost_of_job = self.compute_job_cost(job=job, total=peer.capacities)

                feasibility = self.is_job_feasible(total=peer.capacities, job=job,
                                                   projected_load=projected_load + cost_of_job)
                cost_matrix[i, j] = float('inf')
                if feasibility:
                    cost_matrix[i, j] = projected_load + feasibility * cost_of_job

        return cost_matrix

    def __find_min_cost_agents(self, cost_matrix: np.ndarray) -> list:
        """
        Find the agents with the minimum cost for each job, ensuring:

        :param cost_matrix: A 2D numpy array where each entry [i, j] is the cost of agent i for job j.
        :return: A list of agent IDs corresponding to the minimum cost for each job.
        """
        min_cost_agents = []
        agent_ids = [self.agent_id] + [peer.agent_id for peer in self.neighbor_map.values()]

        for j in range(cost_matrix.shape[1]):  # Iterate over each job (column)
            valid_costs = cost_matrix[:, j]  # Get the costs for job j
            finite_indices = np.where(valid_costs != float('inf'))[0]  # Indices with finite costs

            if len(finite_indices) > 0:
                min_cost = np.min(valid_costs[finite_indices])
                candidate_indices = [i for i in finite_indices if valid_costs[i] == min_cost]
                selected_index = candidate_indices[0]
                min_cost_agents.append((agent_ids[selected_index], min_cost))
        return min_cost_agents

    def __can_select_job(self, job: Job, caps_jobs_selected: Capacities) -> tuple[bool, float]:
        """
        Check if agent has enough resources to become a leader
            - Agent has resources to executed the job
            - Agent hasn't received a proposal from other agents for this job
            - Agent's load is less than 70%
        :param job:
        :param caps_jobs_selected: Capacities of the jobs selected in this cycle
        :return: True or False
        """
        cost_matrix = self.__compute_cost_matrix([job], caps_jobs_selected)
        min_cost_agents = self.__find_min_cost_agents(cost_matrix)
        if len(min_cost_agents):
            agent_id, cost = min_cost_agents[0]
            if agent_id == self.agent_id:
                return True, float(cost)
        self.logger.debug(f"[SEL]: Not picked Job: {job.get_job_id()} - TIME: {job.no_op} "
                          f"MIN Cost Agents: {min_cost_agents}")
        return False, 0.0

    def __receive_proposal(self, incoming: Proposal):
        proposals = []
        proposals_to_forward = []
        for p in incoming.proposals:
            job = self.queues.job_queue.get_job(job_id=p.job_id)
            if not job:
                self.logger.error(f"ERROR ---- Skipping no job found for {p.job_id}")
                continue
            if self.is_job_completed(job_id=job.get_job_id()):
                self.logger.debug(f"Ignoring Proposal: {p} for job: {job.get_job_id()}")
                continue

            my_proposal = self.outgoing_proposals.has_better_proposal(proposal=p)
            peer_proposal = self.incoming_proposals.has_better_proposal(proposal=p)

            if my_proposal:
                self.logger.debug(f"Job:{p.job_id} Agent:{self.agent_id} rejected Proposal: {p} from agent"
                                  f" {p.agent_id} - my proposal {my_proposal} has prepares or smaller seed")
                self.metrics.conflicts += 1
            elif peer_proposal:
                self.logger.debug(f"Job:{p.job_id} Agent:{self.agent_id} rejected Proposal: {p} from agent"
                                  f" {p.agent_id} - already accepted proposal {peer_proposal} with a smaller seed")
                self.metrics.conflicts += 1
            else:
                self.logger.debug(
                    f"Job:{p.job_id} Agent:{self.agent_id} accepted Proposal: {p} from agent"
                    f" {p.agent_id} and is now the leader")

                p.prepares = []
                if my_proposal:
                    self.logger.debug(f"Removed my Proposal: {my_proposal} in favor of incoming proposal")
                    self.outgoing_proposals.remove_proposal(p_id=my_proposal.p_id, job_id=p.job_id)
                if peer_proposal:
                    self.logger.debug(f"Removed peer Proposal: {peer_proposal} in favor of incoming proposal")
                    self.incoming_proposals.remove_proposal(p_id=peer_proposal.p_id, job_id=p.job_id)

                # Increment the number of prepares to count the prepare being sent
                # Needed to handle 3 agent case
                proposals.append(p)
                if incoming.agents[0].agent_id not in p.prepares:
                    p.prepares.append(incoming.agents[0].agent_id)
                self.incoming_proposals.add_proposal(proposal=p)
                job.change_state(JobState.PREPARE)  # Ensure this is where you want the state to change

                # New proposal, forward to my peers
                proposals_to_forward.append(p)

        if len(proposals_to_forward) and self.topology_type == Agent.TOPOLOGY_RING:
            msg = Proposal(source=incoming.agents[0].agent_id,
                           agents=[AgentInfo(agent_id=incoming.agents[0].agent_id)], proposals=proposals_to_forward,
                           forwarded_by=self.agent_id)
            self._send_message(json_message=msg.to_dict(),
                               excluded_peers=[incoming.forwarded_by, incoming.agents[0].agent_id],
                               src=incoming.agents[0].agent_id, fwd=self.agent_id)

        if len(proposals):
            msg = Prepare(source=self.agent_id, agents=[AgentInfo(agent_id=self.agent_id)], proposals=proposals)
            self._send_message(json_message=msg.to_dict())

    def __receive_prepare(self, incoming: Prepare):
        proposals = []
        proposals_to_forward = []
        for p in incoming.proposals:
            job = self.queues.job_queue.get_job(job_id=p.job_id)
            if not job:
                self.logger.error(f"ERROR ---- Skipping no job found for {p.job_id}")
                continue
            if self.is_job_completed(job_id=job.get_job_id()):
                self.logger.debug(f"Job: {job.get_job_id()} Ignoring Prepare: {p}")
                continue

            # I have sent this proposal
            if self.outgoing_proposals.contains(job_id=p.job_id, p_id=p.p_id):
                proposal = self.outgoing_proposals.get_proposal(p_id=p.p_id)
            # Received this proposal
            elif self.incoming_proposals.contains(job_id=p.job_id, p_id=p.p_id):
                proposal = self.incoming_proposals.get_proposal(p_id=p.p_id)
            # New proposal
            else:
                proposal = p
                self.incoming_proposals.add_proposal(proposal=p)

            if incoming.agents[0].agent_id not in proposal.prepares:
                proposal.prepares.append(incoming.agents[0].agent_id)
                # Forward Prepare for peer proposals
                if proposal.agent_id != self.agent_id:
                    proposals_to_forward.append(p)

            # Commit has already been triggered
            if job.is_commit():
                continue

            quorum_count = (len(self.neighbor_map) // 2) + 1  # Ensure a true majority
            job.change_state(JobState.PREPARE)  # Consider the necessity of this state change

            if len(proposal.prepares) >= quorum_count:
                self.logger.debug(f"Job: {p.job_id} Agent: {self.agent_id} received quorum "
                                  f"prepares: {proposal.prepares}, starting commit!")

                # Increment the number of commits to count the commit being sent
                # Needed to handle 3 agent case
                #proposal.commits += 1
                proposals.append(proposal)
                job.change_state(JobState.COMMIT)  # Update job state to COMMIT

        if len(proposals):
            msg = Commit(source=self.agent_id, agents=[AgentInfo(agent_id=self.agent_id)], proposals=proposals)
            self._send_message(json_message=msg.to_dict())

        if len(proposals_to_forward) and self.topology_type == Agent.TOPOLOGY_RING:
            # Use the originators agent agent_id when forwarding the Prepare
            msg = Prepare(source=incoming.agents[0].agent_id, agents=[AgentInfo(agent_id=incoming.agents[0].agent_id)],
                          proposals=proposals_to_forward,
                          forwarded_by=self.agent_id)
            self._send_message(json_message=msg.to_dict(),
                               excluded_peers=[incoming.forwarded_by, incoming.agents[0].agent_id],
                               src=incoming.agents[0].agent_id, fwd=self.agent_id)

    def __receive_commit(self, incoming: Commit):
        proposals_to_forward = []

        for p in incoming.proposals:
            job = self.queues.job_queue.get_job(job_id=p.job_id)
            if not job:
                self.logger.error(f"ERROR ---- Skipping no job found for {p.job_id}")
                continue
            if self.is_job_completed(job_id=job.get_job_id()):
                self.logger.debug(f"Job: {job.get_job_id()} Ignoring Commit: {p}")
                self.incoming_proposals.remove_job(job_id=p.job_id)
                self.outgoing_proposals.remove_job(job_id=p.job_id)
                continue  # Continue instead of return to process other proposals

            if self.outgoing_proposals.contains(job_id=p.job_id, p_id=p.p_id):
                proposal = self.outgoing_proposals.get_proposal(p_id=p.p_id)
            elif self.incoming_proposals.contains(job_id=p.job_id, p_id=p.p_id):
                proposal = self.incoming_proposals.get_proposal(p_id=p.p_id)
            else:
                self.logger.debug(f"TBD: Job: {p.job_id} Agent: {self.agent_id} received commit without any Prepares")
                proposal = p
                self.incoming_proposals.add_proposal(proposal=proposal)

            if incoming.agents[0].agent_id not in proposal.commits:
                proposal.commits.append(incoming.agents[0].agent_id)
                if proposal.agent_id != self.agent_id:
                    proposals_to_forward.append(proposal)

            quorum_count = (len(self.neighbor_map) // 2) + 1  # Ensure a true majority

            if len(proposal.commits) >= quorum_count:
                self.logger.debug(
                    f"Job: {p.job_id} Agent: {self.agent_id} received quorum commits Proposal: {proposal}: "
                    f"Job: {job.get_job_id()}")
                if proposal.agent_id == self.agent_id:
                    job.set_leader(leader_agent_id=proposal.agent_id)
                if self.outgoing_proposals.contains(job_id=p.job_id, p_id=p.p_id):
                    self.logger.info(f"[CON_LEADER] achieved for Job: {p.job_id} Leader: {self.agent_id}")
                    job.change_state(new_state=JobState.READY)
                    self.select_job(job)
                    self.outgoing_proposals.remove_job(job_id=p.job_id)
                else:
                    self.logger.info(f"[CON_PART] achieved for Job: {p.job_id} Leader: {p.agent_id}")
                    job.change_state(new_state=JobState.COMPLETE)
                    self.incoming_proposals.remove_job(job_id=p.job_id)

        if len(proposals_to_forward) and self.topology_type == Agent.TOPOLOGY_RING:
            msg = Commit(source=incoming.agents[0].agent_id, agents=[AgentInfo(agent_id=incoming.agents[0].agent_id)],
                         proposals=proposals_to_forward,
                         forwarded_by=self.agent_id)
            self._send_message(json_message=msg.to_dict(),
                               excluded_peers=[incoming.forwarded_by, incoming.agents[0].agent_id],
                               src=incoming.agents[0].agent_id, fwd=self.agent_id)

    def __receive_job_status(self, incoming: JobStatus):
        jobs_to_fwd = []
        for t in incoming.jobs:
            job = self.queues.job_queue.get_job(job_id=t.job_id)
            if not job:
                self.logger.error(f"ERROR ---- Skipping no job found for {t.job_id}")
                continue

            if self.is_job_completed(job_id=job.get_job_id()):
                self.logger.debug(f"Job: {job.get_job_id()} Ignoring Job Status (State: {job.state})")
                continue

            if incoming.agents[0].agent_id == self.agent_id:
                continue

            # Update the job status based on broadcast message
            self.logger.debug(f"Updating Job: {job.job_id} state to COMPLETE")
            #job.set_leader(leader_agent_id=incoming.agents[0].agent_id)
            job.change_state(new_state=JobState.COMPLETE)
            self.incoming_proposals.remove_job(job_id=t.job_id)
            self.outgoing_proposals.remove_job(job_id=t.job_id)

            jobs_to_fwd.append(job)

        # Forward Job Status
        '''
        if len(jobs_to_fwd) and self.topology_type == Agent.TOPOLOGY_RING:
            # Use the originators agent agent_id when forwarding the Prepare
            msg = JobStatus(agents=[AgentInfo(agent_id=incoming.agents[0].agent_id)], jobs=jobs_to_fwd,
                                    forwarded_by=self.agent_id)
            self._send_message(json_message=msg.to_dict(),
                               excluded_peers=[incoming.forwarded_by, incoming.agents[0].agent_id],
                               src=incoming.agents[0].agent_id, fwd=self.agent_id)
        '''

    def execute_job(self, job: Job):
        self.update_completed_jobs(jobs=[job.get_job_id()])
        self.repo.save(obj=job.to_dict())
        super().execute_job(job=job)
        '''
        msg = JobStatus(agents=[AgentInfo(agent_id=self.agent_id)], jobs=[JobInfo(job_id=job.get_job_id(),
                                                                                  state=job.state)])
        self._send_message(json_message=msg.to_dict())
        '''

    def __get_proposed_capacities(self):
        proposed_capacities = Capacities()
        jobs = self.outgoing_proposals.jobs()
        for job_id in jobs:
            proposed_job = self.queues.job_queue.get_job(job_id=job_id)
            proposed_capacities += proposed_job.get_capacities()
        #self.logger.debug(f"Number of outgoing proposals: {len(jobs)}; Jobs: {jobs}")
        return proposed_capacities

    @staticmethod
    def compute_job_cost(job: Job, total: Capacities) -> float:
        """
        Computes job cost as the average load across core, RAM, and disk
        relative to total available resources (equal weight, no profile).
        """
        core_load = (job.capacities.core / total.core) * 100
        ram_load = (job.capacities.ram / total.ram) * 100
        disk_load = (job.capacities.disk / total.disk) * 100

        cost = (core_load + ram_load + disk_load) / 3
        return round(cost, 2)

    def compute_projected_load(self, overall_load_actual: float, proposed_caps: Capacities) -> float:
        """
        Compute projected overall load using equal weighting for core, RAM, and disk.
        """
        if not proposed_caps:
            return round(overall_load_actual, 2)

        core_inc = (proposed_caps.core / self.capacities.core) * 100
        ram_inc = (proposed_caps.ram / self.capacities.ram) * 100
        disk_inc = (proposed_caps.disk / self.capacities.disk) * 100

        additional_load = (core_inc + ram_inc + disk_inc) / 3
        projected_load = overall_load_actual + additional_load

        return round(projected_load, 2)

    def update_completed_jobs(self, jobs: list[str]):
        super().update_completed_jobs(jobs=jobs)
        for j in jobs:
            self.outgoing_proposals.remove_job(job_id=j)

    def _do_periodic(self):
        while not self.shutdown:
            try:
                agent_info = self.generate_agent_info()
                self.repo.save(agent_info.to_dict(), key_prefix="agent")

                current_time = int(time.time())
                peers = self.repo.get_all_objects(key_prefix="agent")
                active_peer_ids = set()

                for p in peers:
                    peer = AgentInfo.from_dict(p)
                    self._add_peer(peer)
                    active_peer_ids.add(peer.agent_id)

                stale_peers = [
                    peer.agent_id for peer in self.neighbor_map.values()
                    if (current_time - peer.last_updated) >= self.peer_expiry_seconds and
                       peer.agent_id not in active_peer_ids
                ]

                for agent_id in stale_peers:
                    self._remove_peer(agent_id)

                # Batch update job sets
                for prefix, update_fn in [
                    (Repository.KEY_JOB, self.update_completed_jobs)
                ]:
                    jobs = self.repo.get_all_ids(key_prefix=prefix)
                    update_fn(jobs=jobs)

                time.sleep(0.005)

                self.check_queue()
                if self.should_shutdown():
                    print("[SHUTDOWN] Queue has been empty for too long. Triggering shutdown.")
                    break
            except Exception as e:
                self.logger.error(f"Periodic update error: {e}\n{traceback.format_exc()}")

        self.stop()
