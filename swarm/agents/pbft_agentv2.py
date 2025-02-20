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
from swarm.comm.messages.heart_beat import HeartBeat
from swarm.comm.messages.message import MessageType
from swarm.models.agent_info import AgentInfo
from swarm.models.proposal_info import ProposalContainer, ProposalInfo
from swarm.models.job import Job, JobState


class PBFTAgent(Agent):
    def __init__(self, agent_id: int, config_file: str, cycles: int, total_agents: int):
        super().__init__(agent_id, config_file, cycles, total_agents=total_agents)
        self.outgoing_proposals = ProposalContainer()
        self.incoming_proposals = ProposalContainer()

    def _build_heart_beat(self, dest_agent_id: str = None) -> dict:
        my_load = self.compute_overall_load(proposed_jobs=self.outgoing_proposals.jobs())
        agents = {}

        agent = AgentInfo(agent_id=self.agent_id,
                          capacities=self.capacities,
                          capacity_allocations=self.ready_queue.capacities(jobs=self.ready_queue.get_jobs()),
                          load=my_load,
                          last_updated=time.time())
        self._save_load_metric(self.agent_id, my_load)
        if isinstance(self.topology_peer_agent_list, list) and len(self.neighbor_map.values()):
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
                for job in self.job_queue.get_jobs():
                    if job.is_complete():
                        completed_jobs += 1
                        continue

                    diff = int(time.time() - job.time_last_state_change)
                    if diff > self.restart_job_selection and job.get_state() in [JobState.PREPARE,
                                                                                 JobState.PRE_PREPARE]:
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

                        self.send_message(message_type=MessageType.Proposal, job_id=job.get_job_id(),
                                          proposal_id=proposal.p_id, seed=proposal.seed)

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
        my_load = self.compute_overall_load(proposed_jobs=self.outgoing_proposals.jobs())
        least_loaded_neighbor = self.__find_neighbor_with_lowest_load()

        if least_loaded_neighbor and least_loaded_neighbor.load < my_load:
            self.logger.info(
                f"__can_select_job: my_load: {my_load} more than neighbor:{least_loaded_neighbor}")
            return False

        feasibility = self.is_job_feasible(job=job, total=self.capacities, projected_load=my_load)
        incoming = self.incoming_proposals.contains(job_id=job.get_job_id())
        if incoming or not feasibility:
            self.logger.info(
                f"__can_select_job: Either infeasible job or already accepted proposal from another agent "
                f"feasibility: {feasibility} incoming:{incoming} my_load: {my_load}")
            return False

        self.logger.info(
            f"__can_select_job: feasibility: {feasibility} incoming:{incoming} my_load: {my_load}")
        return True

    def __receive_proposal(self, incoming: dict):
        peer_agent_id = incoming.get("agent_id")
        job_id = incoming.get("job_id")
        proposal_id = incoming.get("proposal_id")
        rcvd_seed = incoming.get("seed", 0)

        self.logger.debug(f"Received Proposal from Agent: {peer_agent_id} for Job: {job_id} "
                          f"Proposal: {proposal_id} Seed: {rcvd_seed}")

        job = self.job_queue.get_job(job_id=job_id)
        if not job or job.is_ready() or job.is_complete() or job.is_running():
            self.logger.info(f"Job: {job_id} Ignoring Prepare: {proposal_id}")
            return

        my_proposal = self.outgoing_proposals.get_proposal(job_id=job_id)
        peer_proposal = self.incoming_proposals.get_proposal(job_id=job_id)

        if my_proposal and (len(my_proposal.prepares) or my_proposal.seed < rcvd_seed):
            self.logger.debug(f"Job:{job_id} Agent:{self.agent_id} rejected Proposal: {proposal_id} from agent"
                              f" {peer_agent_id} - my proposal {my_proposal} has prepares or smaller seed")
            self.conflicts += 1
        elif peer_proposal and peer_proposal.seed < rcvd_seed:
            self.logger.debug(f"Job:{job_id} Agent:{self.agent_id} rejected Proposal: {proposal_id} from agent"
                              f" {peer_agent_id} - already accepted proposal {peer_proposal} with a smaller seed")
            self.conflicts += 1
        else:
            self.logger.debug(
                f"Agent {self.agent_id} accepted Proposal for Job: {job_id} from agent"
                f" {peer_agent_id} and is now the leader")

            proposal = ProposalInfo(p_id=proposal_id, job_id=job_id, seed=rcvd_seed,
                                    agent_id=peer_agent_id)
            if my_proposal:
                self.logger.info(f"Removed my Proposal: {my_proposal} in favor of incoming proposal")
                self.outgoing_proposals.remove_proposal(p_id=my_proposal.p_id, job_id=job_id)
            if peer_proposal:
                self.logger.info(f"Removed peer Proposal: {peer_proposal} in favor of incoming proposal")
                self.incoming_proposals.remove_proposal(p_id=peer_proposal.p_id, job_id=job_id)

            # Increment the number of prepares to count the prepare being sent
            # Needed to handle 3 agent case
            if peer_agent_id not in proposal.prepares:
                proposal.prepares.append(peer_agent_id)
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
            self.logger.info(f"Job: {job} Ignoring Prepare")
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

        if peer_agent_id not in proposal.prepares:
            proposal.prepares.append(peer_agent_id)
        quorum_count = (len(self.neighbor_map) // 2) + 1  # Ensure a true majority
        job.change_state(JobState.PREPARE)

        # Check if vote count is more than quorum
        # if yes, send commit
        if len(proposal.prepares) >= quorum_count:
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

        if not job or job.is_complete() or job.is_ready() or job.is_running() or \
                job.leader_agent_id is not None:
            self.logger.info(f"Job: {job} Ignoring Commit: {proposal_id}")
            self.incoming_proposals.remove_job(job_id=job_id)
            self.outgoing_proposals.remove_job(job_id=job_id)
            return

        # Update the commit votes;
        if self.outgoing_proposals.contains(job_id=job_id, p_id=proposal_id):
            proposal = self.outgoing_proposals.get_proposal(p_id=proposal_id)
        elif self.incoming_proposals.contains(job_id=job_id, p_id=proposal_id):
            proposal = self.incoming_proposals.get_proposal(p_id=proposal_id)
        else:
            self.logger.info(f"TBD: Job: {job_id} Agent: {self.agent_id} received commit without any Prepares")
            proposal = ProposalInfo(job_id=job_id, p_id=proposal_id, seed=rcvd_seed,
                                    agent_id=peer_agent_id)
            self.incoming_proposals.add_proposal(proposal=proposal)

        if peer_agent_id not in proposal.commits:
            proposal.commits.append(peer_agent_id)
        quorum_count = (len(self.neighbor_map) // 2) + 1  # Ensure a true majority

        if len(proposal.commits) >= quorum_count:
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
            "load": self.compute_overall_load(proposed_jobs=self.outgoing_proposals.jobs())
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
        self._send_message(json_message=message)
