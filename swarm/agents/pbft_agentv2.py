import random
import time
import traceback
from typing import List


from swarm.agents.agent import Agent
from swarm.comm.messages.heart_beat import HeartBeat
from swarm.comm.messages.message import MessageType
from swarm.models.capacities import Capacities
from swarm.models.agent_info import AgentInfo
from swarm.models.proposal_info import ProposalContainer, ProposalInfo
from swarm.models.job import Job, JobState


class PBFTAgent(Agent):
    def __init__(self, agent_id: str, config_file: str, cycles: int):
        super().__init__(agent_id, config_file, cycles)
        self.outgoing_proposals = ProposalContainer()
        self.incoming_proposals = ProposalContainer()
        self.shutdown_heartbeat = False
        self.last_msg_received_timestamp = 0

    def _process_messages(self, *, messages: List[dict]):
        for message in messages:
            try:
                begin = time.time()
                message_type = MessageType(message.get('message_type'))

                if message_type == MessageType.HeartBeat:
                    incoming = HeartBeat.from_dict(message)
                    self._receive_heartbeat(incoming=incoming)

                elif message_type == MessageType.Proposal:
                    self.__receive_proposal(incoming=message)

                elif message_type == MessageType.Prepare:
                    self.__receive_prepare(incoming=message)

                elif message_type == MessageType.Commit:
                    self.__receive_commit(incoming=message)

                elif message_type == MessageType.JobStatus:
                    self.__receive_job_status(incoming=message)
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
                # Make a copy of the dictionary keys
                job_ids = list(self.job_queue.jobs.keys())
                for job_id in job_ids:
                    job = self.job_queue.jobs.get(job_id)
                    if not job:
                        continue

                    if job.is_complete():
                        completed_jobs += 1
                        continue

                    # DISABLE THIS
                    #diff = int(time.time() - job.time_last_state_change)
                    #if diff > 120 and job.get_state() in [JobState.PREPARE, JobState.PRE_PREPARE]:
                    #    job.change_state(new_state=JobState.PENDING)

                    if not job.is_pending():
                        self.logger.debug(f"Job: {job.job_id} State: {job.state}; skipping it!")
                        continue

                    processed += 1

                    # Trigger leader election for a job after random sleep
                    election_timeout = random.uniform(150, 300) / 1000
                    time.sleep(election_timeout)

                    if self.__can_select_job(job=job):

                        # Send proposal to all neighbors
                        proposal = ProposalInfo(p_id=self.generate_id(), job_id=job.get_job_id(),
                                                agent_id=self.agent_id)

                        self.send_message(message_type=MessageType.Proposal, job_id=job.get_job_id(),
                                          proposal_id=proposal.p_id, seed=proposal.seed)

                        self.outgoing_proposals.add_proposal(proposal=proposal)
                        self.logger.info(f"Added proposal: {proposal}")

                        # Begin election for Job leader for this job
                        job.change_state(new_state=JobState.PRE_PREPARE)
                    else:
                        self.logger.debug(
                            f"Job: {job.job_id} State: {job.state} cannot be accommodated at this time:")

                    if processed >= 40:
                        time.sleep(1)
                        processed = 0

                time.sleep(1)  # Adjust the sleep duration as needed

            except Exception as e:
                self.logger.error(f"Error occurred while executing e: {e}")
                self.logger.error(traceback.format_exc())

    def __find_neighbor_with_lowest_load(self) -> AgentInfo:
        # Initialize variables to track the neighbor with the lowest load
        lowest_load = float('inf')  # Initialize with positive infinity to ensure any load is lower
        lowest_load_neighbor = None

        # Iterate through each neighbor in neighbor_map
        for peer_agent_id, neighbor_info in self.neighbor_map.items():
            # Check if the current load is lower than the lowest load found so far
            if neighbor_info.load < lowest_load:
                # Update lowest load and lowest load neighbor
                lowest_load = neighbor_info.load
                lowest_load_neighbor = neighbor_info

        return lowest_load_neighbor

    def __can_select_job(self, job: Job) -> bool:
        """
        Check if agent has enough resources to become a leader
            - Agent has resources to executed the job
            - Agent hasn't received a proposal from other agents for this job
            - Agent's load is less than 70%
        :param job:
        :return: True or False
        """
        my_load = self.compute_overall_load()
        least_loaded_neighbor = self.__find_neighbor_with_lowest_load()

        if least_loaded_neighbor and least_loaded_neighbor.load < my_load:
            self.logger.debug(f"Can't become leader as my load: {my_load} is more than all of "
                              f"the neighbors: {least_loaded_neighbor}")
            return False

        feasibility = self.is_job_feasible(job=job, total=self.capacities, projected_load=my_load)
        incoming = self.incoming_proposals.contains(job_id=job.get_job_id())
        if not incoming and feasibility:
            return feasibility
        self.logger.info(
            f"__can_select_job: feasibility: {feasibility} incoming:{incoming} my_load: {my_load}")
        return False

    def __receive_proposal(self, incoming: dict):
        peer_agent_id = incoming.get("agent_id")
        job_id = incoming.get("job_id")
        proposal_id = incoming.get("proposal_id")
        rcvd_seed = incoming.get("seed", 0)

        self.logger.debug(f"Received Proposal from Agent: {peer_agent_id} for Job: {job_id} "
                          f"Proposal: {proposal_id} Seed: {rcvd_seed}")

        job = self.job_queue.get_job(job_id=job_id)
        if not job or job.is_ready() or job.is_complete() or job.is_running():
            self.logger.info(f"Ignoring Proposal: {job}")
            return

        #can_accept_job = self.can_accommodate_job(job=job)
        #my_current_load = self.compute_overall_load()

        # Reject proposal in following cases:
        # - I have initiated a proposal and either received accepts from at least 1 peer or
        #   my proposal's seed is smaller than the incoming proposal
        # - Received and accepted proposal from another agent
        # - can't accommodate this job
        # - can accommodate job and neighbor's load is more than mine
        my_proposal = self.outgoing_proposals.get_proposal(job_id=job_id)
        peer_proposal = self.incoming_proposals.get_proposal(job_id=job_id)

        # if (my_proposal and (my_proposal.prepares or my_proposal.seed < rcvd_seed)) or \
        #        (peer_proposal and peer_proposal.seed < rcvd_seed) or \
        #        not can_accept_job or \
        #        (can_accept_job and my_current_load < neighbor_load):
        if (my_proposal and (my_proposal.prepares or my_proposal.seed < rcvd_seed)) or \
                (peer_proposal and peer_proposal.seed < rcvd_seed):
            self.logger.debug(f"Agent {self.agent_id} rejected Proposal for Job: {job_id} from agent"
                              f" {peer_agent_id} - accepted another proposal")
        else:
            self.logger.debug(
                f"Agent {self.agent_id} accepted Proposal for Job: {job_id} from agent"
                f" {peer_agent_id} and is now the leader")

            proposal = ProposalInfo(p_id=proposal_id, job_id=job_id, seed=rcvd_seed,
                                    agent_id=peer_agent_id)
            if my_proposal:
                self.outgoing_proposals.remove_proposal(p_id=my_proposal.p_id, job_id=job_id)
            if peer_proposal:
                self.incoming_proposals.remove_proposal(p_id=peer_proposal.p_id, job_id=job_id)

            self.incoming_proposals.add_proposal(proposal=proposal)
            self.send_message(message_type=MessageType.Prepare, job_id=job_id, proposal_id=proposal_id)
            job.change_state(JobState.PREPARE)

    def __receive_prepare(self, incoming: dict):
        peer_agent_id = incoming.get("agent_id")
        job_id = incoming.get("job_id")
        proposal_id = incoming.get("proposal_id")
        rcvd_seed = incoming.get("seed", 0)

        # Prepare for the proposal
        self.logger.debug(f"Received prepare from Agent: {peer_agent_id} for Job: {job_id}: {proposal_id}")

        job = self.job_queue.get_job(job_id=job_id)
        if not job or job.is_ready() or job.is_complete() or job.is_running():
            self.logger.info(f"Ignoring Prepare: {job}")
            return

        # Update the prepare votes
        if self.outgoing_proposals.contains(job_id=job_id, p_id=proposal_id):
            proposal = self.outgoing_proposals.get_proposal(p_id=proposal_id)
        elif self.incoming_proposals.contains(job_id=job_id, p_id=proposal_id):
            proposal = self.incoming_proposals.get_proposal(p_id=proposal_id)
        else:
            proposal = ProposalInfo(job_id=job_id, p_id=proposal_id, seed=rcvd_seed,
                                    agent_id=peer_agent_id)
            self.incoming_proposals.add_proposal(proposal=proposal)

        proposal.prepares += 1

        quorum_count = (len(self.neighbor_map)) / 2
        job.change_state(JobState.PREPARE)

        # Check if vote count is more than quorum
        # if yes, send commit
        if proposal.prepares >= quorum_count:
            self.logger.info(f"Agent: {self.agent_id} Job: {job_id} received quorum "
                             f"prepares: {proposal.prepares}, starting commit!")
            self.send_message(message_type=MessageType.Commit, job_id=job_id, proposal_id=proposal_id)
            job.change_state(JobState.COMMIT)

    def __receive_commit(self, incoming: dict):
        peer_agent_id = incoming.get("agent_id")
        job_id = incoming.get("job_id")
        proposal_id = incoming.get("proposal_id")
        rcvd_seed = incoming.get("seed", 0)

        self.logger.debug(f"Received commit from Agent: {peer_agent_id} for Job: {job_id} Proposal: {proposal_id}")
        job = self.job_queue.get_job(job_id=job_id)

        if not job or job.is_complete() or job.is_ready() or job.is_running() or job.leader_agent_id:
            self.logger.info(f"Ignoring Commit: {job}")
            self.incoming_proposals.remove_job(job_id=job_id)
            self.outgoing_proposals.remove_job(job_id=job_id)
            return

        # Update the commit votes;
        if self.outgoing_proposals.contains(job_id=job_id, p_id=proposal_id):
            proposal = self.outgoing_proposals.get_proposal(p_id=proposal_id)
        elif self.incoming_proposals.contains(job_id=job_id, p_id=proposal_id):
            proposal = self.incoming_proposals.get_proposal(p_id=proposal_id)
        else:
            proposal = ProposalInfo(job_id=job_id, p_id=proposal_id, seed=rcvd_seed,
                                    agent_id=peer_agent_id)
            self.incoming_proposals.add_proposal(proposal=proposal)

        proposal.commits += 1
        quorum_count = (len(self.neighbor_map)) / 2

        if proposal.commits >= quorum_count:
            self.logger.info(
                f"Agent: {self.agent_id} received quorum commits for Job: {job_id} Proposal: {proposal}: Job: {job}")
            job.set_leader(leader_agent_id=proposal.agent_id)
            if self.outgoing_proposals.contains(job_id=job_id, p_id=proposal_id):
                self.logger.info(f"LEADER CONSENSUS achieved for Job: {job_id} Leader: {self.agent_id}")
                job.change_state(new_state=JobState.READY)
                self.select_job(job)
                self.outgoing_proposals.remove_job(job_id=job_id)
            else:
                self.logger.info(f"PARTICIPANT CONSENSUS achieved for Job: {job_id} Leader: {peer_agent_id}")
                job.change_state(new_state=JobState.COMMIT)
                self.incoming_proposals.remove_job(job_id=job_id)
                job.set_scheduled_time()

    def __receive_job_status(self, incoming: dict):
        peer_agent_id = incoming.get("agent_id")
        job_id = incoming.get("job_id")
        proposal_id = incoming.get("proposal_id")

        self.logger.debug(f"Received Status from {peer_agent_id} for Job: {job_id} Proposal: {proposal_id}")

        job = self.job_queue.get_job(job_id=job_id)
        job.set_leader(leader_agent_id=peer_agent_id)
        if not job or job.is_complete() or job.is_ready():
            self.logger.info(f"Ignoring Job Status: {job}")
            return

        # Update the job status based on broadcast message
        job.change_state(new_state=JobState.COMPLETE)
        self.incoming_proposals.remove_job(job_id=job_id)
        self.outgoing_proposals.remove_job(job_id=job_id)

    def execute_job(self, job: Job):
        super().execute_job(job=job)
        self.send_message(message_type=MessageType.JobStatus, job_id=job.get_job_id(),
                          status=job.state)

    def send_message(self, message_type: MessageType, job_id: str = None, proposal_id: str = None,
                     status: JobState = None, seed: float = None):
        message = {
            "message_type": message_type.value,
            "agent_id": self.agent_id,
            "load": self.compute_overall_load()
        }

        if job_id:
            message["job_id"] = job_id
        if proposal_id:
            message["proposal_id"] = proposal_id
        if status:
            message["status"] = status.value
        if seed:
            message["seed"] = seed

        # Produce the message to the Kafka topic
        self.message_service.produce_message(message)

    def __get_proposed_capacities(self):
        proposed_capacities = Capacities()
        jobs = self.outgoing_proposals.jobs()
        for job_id in jobs:
            proposed_job = self.job_queue.get_job(job_id=job_id)
            proposed_capacities += proposed_job.get_capacities()
        self.logger.debug(f"Number of outgoing proposals: {len(jobs)}; Jobs: {jobs}")
        return proposed_capacities
