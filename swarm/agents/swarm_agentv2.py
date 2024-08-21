import random
import time
import traceback
from queue import Queue, Empty
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
        self.number_of_jobs_per_proposal = 3

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
                proposals = []  # List to accumulate proposals for multiple jobs
                caps_jobs_selected = Capacities()

                # Make a copy of the dictionary keys
                job_ids = list(self.job_queue.jobs.keys())
                for job_id in job_ids:
                    job = self.job_queue.jobs.get(job_id)
                    if not job:
                        continue

                    if job.is_complete():
                        completed_jobs += 1
                        continue

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

                    if self.__can_select_job(job=job, caps_jobs_selected=caps_jobs_selected):
                        # Send proposal to all neighbors
                        proposal = ProposalInfo(p_id=self.generate_id(), job_id=job.get_job_id(),
                                                agent_id=self.agent_id)
                        proposals.append(proposal)
                        caps_jobs_selected += job.get_capacities()

                        # Begin election for Job leader for this job
                        job.change_state(new_state=JobState.PRE_PREPARE)

                    if len(proposals) >= self.number_of_jobs_per_proposal:
                        msg = Proposal(
                            agent=AgentInfo(agent_id=self.agent_id),
                            proposals=proposals
                        )
                        self.message_service.produce_message(msg.to_dict())
                        for p in proposals:
                            self.outgoing_proposals.add_proposal(p)  # Add all proposals
                        proposals.clear()
                        caps_jobs_selected = Capacities()

                        self.logger.info(f"Added proposals: {proposals}")

                    if processed >= 40:
                        time.sleep(1)
                        processed = 0

                # Send remaining proposals if any exist
                if proposals:
                    msg = Proposal(
                        agent=AgentInfo(agent_id=self.agent_id),
                        proposals=proposals
                    )
                    self.message_service.produce_message(msg.to_dict())
                    for p in proposals:
                        self.outgoing_proposals.add_proposal(p)
                    self.logger.info(f"Added remaining proposals: {proposals}")
                    proposals.clear()

                time.sleep(1)  # Adjust the sleep duration as needed

            except Exception as e:
                self.logger.error(f"Error occurred while executing e: {e}")
                self.logger.error(traceback.format_exc())
        self.logger.info(f"Agent: {self} stopped!")

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
        my_load = self.compute_overall_load()
        load = self.compute_projected_load(overall_load_actual=my_load,
                                           proposed_caps=caps_jobs_selected)

        for j, job in enumerate(jobs):
            cost_of_job = self.compute_job_cost(job=job, total=self.capacities, profile=self.profile)
            feasibility = self.is_job_feasible(total=self.capacities, job=job, projected_load=load+cost_of_job)
            cost_matrix[0, j] = float('inf')
            if feasibility:
                cost_matrix[0, j] = load + feasibility * cost_of_job

        # Compute costs for neighboring agents
        for i, peer in enumerate(self.neighbor_map.values(), start=1):
            for j, job in enumerate(jobs):
                cost_of_job = self.compute_job_cost(job=job, total=peer.capacities, profile=self.profile)
                load = self.compute_projected_load(overall_load_actual=peer.load,
                                                   proposed_caps=caps_jobs_selected)

                feasibility = self.is_job_feasible(total=peer.capacities, job=job, projected_load=load+cost_of_job)
                cost_matrix[i, j] = float('inf')

                if feasibility:
                    cost_matrix[i, j] = load + feasibility * cost_of_job

        return cost_matrix

    def __find_min_cost_agents(self, cost_matrix: np.ndarray) -> list:
        """
        Find the agents with the minimum cost for each job.
        :param cost_matrix: A 2D numpy array where each entry [i, j] is the cost of agent i for job j.
        :return: A list of agent IDs corresponding to the minimum cost for each job.
        """
        # Find the index of the minimum cost for each job (column)
        min_cost_indices = np.argmin(cost_matrix, axis=0)

        # Map the indices back to agent IDs
        agent_ids = [self.agent_id] + [peer.agent_id for peer in self.neighbor_map.values()]
        min_cost_agents = [agent_ids[i] for i in min_cost_indices]

        return min_cost_agents

    def __can_select_job(self, job: Job, caps_jobs_selected: Capacities) -> bool:
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
        if min_cost_agents[0] == self.agent_id:
            return True
        self.logger.info(f"Job: {job} not selected for consensus!")
        return False

    def __receive_proposal(self, incoming: Proposal):
        self.logger.debug(f"Received Proposal from: {incoming.agent.agent_id}")

        proposals = []
        for p in incoming.proposals:
            job = self.job_queue.get_job(job_id=p.job_id)
            if not job or job.is_ready() or job.is_complete() or job.is_running():
                self.logger.info(f"Ignoring Proposal: {job}")
                return

            # Reject proposal in following cases:
            # - I have initiated a proposal and either received accepts from at least 1 peer or
            #   my proposal's seed is smaller than the incoming proposal
            # - Received and accepted proposal from another agent
            # - can't accommodate this job
            # - can accommodate job and neighbor's load is more than mine
            my_proposal = self.outgoing_proposals.get_proposal(job_id=p.job_id)
            peer_proposal = self.incoming_proposals.get_proposal(job_id=p.job_id)

            # if (my_proposal and (my_proposal.prepares or my_proposal.seed < rcvd_seed)) or \
            #        (peer_proposal and peer_proposal.seed < rcvd_seed) or \
            #        not can_accept_job or \
            #        (can_accept_job and my_current_load < neighbor_load):
            if (my_proposal and (my_proposal.prepares or my_proposal.seed < p.seed)) or \
                    (peer_proposal and peer_proposal.seed < p.seed):
                self.logger.debug(f"Agent {self.agent_id} rejected Proposal for Job: {p.job_id} from agent"
                                  f" {p.agent_id} - accepted another proposal")
            else:
                self.logger.debug(
                    f"Agent {self.agent_id} accepted Proposal for Job: {p.job_id} from agent"
                    f" {p.agent_id} and is now the leader")

                proposal = ProposalInfo(p_id=p.p_id, job_id=p.job_id, seed=p.seed,
                                        agent_id=p.agent_id)
                if my_proposal:
                    self.outgoing_proposals.remove_proposal(p_id=my_proposal.p_id, job_id=p.job_id)
                if peer_proposal:
                    self.incoming_proposals.remove_proposal(p_id=peer_proposal.p_id, job_id=p.job_id)

                self.incoming_proposals.add_proposal(proposal=proposal)
                proposals.append(proposal)
                #msg = Prepare(agent=AgentInfo(agent_id=self.agent_id), proposals=[proposal])
                #self.message_service.produce_message(msg.to_dict())
                job.change_state(JobState.PREPARE)
        if len(proposals):
            msg = Prepare(agent=AgentInfo(agent_id=self.agent_id), proposals=proposals)
            self.message_service.produce_message(msg.to_dict())

    def __receive_prepare(self, incoming: Prepare):
        proposals = []
        # Prepare for the proposal
        self.logger.debug(f"Received prepare from: {incoming.agent.agent_id}")

        for p in incoming.proposals:
            job = self.job_queue.get_job(job_id=p.job_id)
            if not job or job.is_ready() or job.is_complete() or job.is_running():
                self.logger.info(f"Ignoring Prepare: {job}")
                return

            # Update the prepare votes
            if self.outgoing_proposals.contains(job_id=p.job_id, p_id=p.p_id):
                proposal = self.outgoing_proposals.get_proposal(p_id=p.p_id)
            elif self.incoming_proposals.contains(job_id=p.job_id, p_id=p.p_id):
                proposal = self.incoming_proposals.get_proposal(p_id=p.p_id)
            else:
                proposal = ProposalInfo(job_id=p.job_id, p_id=p.p_id, seed=p.seed,
                                        agent_id=p.agent_id)
                self.incoming_proposals.add_proposal(proposal=proposal)

            proposal.prepares += 1

            quorum_count = (len(self.neighbor_map)) / 2
            job.change_state(JobState.PREPARE)

            # Check if vote count is more than quorum
            # if yes, send commit
            if proposal.prepares >= quorum_count:
                self.logger.info(f"Agent: {self.agent_id} Job: {p.job_id} received quorum "
                                 f"prepares: {proposal.prepares}, starting commit!")

                proposals.append(proposal)

                #msg = Commit(agent=AgentInfo(agent_id=self.agent_id), proposals=[proposal])
                #self.message_service.produce_message(msg.to_dict())

                job.change_state(JobState.COMMIT)
        if len(proposals):
            msg = Commit(agent=AgentInfo(agent_id=self.agent_id), proposals=proposals)
            self.message_service.produce_message(msg.to_dict())

    def __receive_commit(self, incoming: Commit):
        self.logger.debug(f"Received commit from: {incoming.agent.agent_id}")
        for p in incoming.proposals:
            job = self.job_queue.get_job(job_id=p.job_id)

            if not job or job.is_complete() or job.is_ready() or job.is_running() or job.leader_agent_id:
                self.logger.info(f"Ignoring Commit: {job}")
                self.incoming_proposals.remove_job(job_id=p.job_id)
                self.outgoing_proposals.remove_job(job_id=p.job_id)
                return

            # Update the commit votes;
            if self.outgoing_proposals.contains(job_id=p.job_id, p_id=p.p_id):
                proposal = self.outgoing_proposals.get_proposal(p_id=p.p_id)
            elif self.incoming_proposals.contains(job_id=p.job_id, p_id=p.p_id):
                proposal = self.incoming_proposals.get_proposal(p_id=p.p_id)
            else:
                proposal = ProposalInfo(job_id=p.job_id, p_id=p.p_id, seed=p.seed,
                                        agent_id=p.agent_id)
                self.incoming_proposals.add_proposal(proposal=proposal)

            proposal.commits += 1
            quorum_count = (len(self.neighbor_map)) / 2

            if proposal.commits >= quorum_count:
                self.logger.info(
                    f"Agent: {self.agent_id} received quorum commits for Job: {p.job_id} Proposal: {proposal}: Job: {job}")
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
        self.logger.debug(f"Received Status from: {incoming.agent.agent_id}")

        for t in incoming.jobs:
            job = self.job_queue.get_job(job_id=t.job_id)
            job.set_leader(leader_agent_id=incoming.agent.agent_id)
            if not job or job.is_complete() or job.is_ready():
                self.logger.info(f"Ignoring Job Status: {job}")
                return

            # Update the job status based on broadcast message
            job.change_state(new_state=JobState.COMPLETE)
            self.incoming_proposals.remove_job(job_id=t.job_id)
            self.outgoing_proposals.remove_job(job_id=t.job_id)

    def execute_job(self, job: Job):
        super().execute_job(job=job)
        msg = JobStatus(agent=AgentInfo(agent_id=self.agent_id), jobs=[JobInfo(job_id=job.get_job_id(),
                                                                               state=job.state)])
        self.message_service.produce_message(msg.to_dict())

    def __get_proposed_capacities(self):
        proposed_capacities = Capacities()
        jobs = self.outgoing_proposals.jobs()
        for job_id in jobs:
            proposed_job = self.job_queue.get_job(job_id=job_id)
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

    def compute_projected_load(self, overall_load_actual: float, proposed_caps: Capacities):
        if not proposed_caps:
            return overall_load_actual  # No proposed caps, so the load remains the same

        # Calculate the load contribution from the proposed capacities
        core_load_increase = (proposed_caps.core / self.capacities.core) * 100
        ram_load_increase = (proposed_caps.ram / self.capacities.ram) * 100
        disk_load_increase = (proposed_caps.disk / self.capacities.disk) * 100

        # Adjust the overall load based on the increases
        overall_load_projected = overall_load_actual + (
                (core_load_increase * self.profile.core_weight +
                 ram_load_increase * self.profile.ram_weight +
                 disk_load_increase * self.profile.disk_weight) /
                (self.profile.core_weight + self.profile.ram_weight + self.profile.disk_weight)
        )

        return overall_load_projected
