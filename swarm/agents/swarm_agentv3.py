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


from swarm.agents.agentv2 import Agent
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

    def generate_agent_info(self):
        my_load = self.compute_overall_load(proposed_jobs=self.outgoing_proposals.jobs())
        self.metrics.save_load_metric(self.agent_id, my_load)
        return AgentInfo(agent_id=self.agent_id,
                         capacities=self.capacities,
                         capacity_allocations=self.queues.ready_queue.capacities(jobs=self.queues.ready_queue.get_jobs()),
                         load=my_load,
                         last_updated=time.time())

    def _process(self, *, messages: list[dict]):
        for message in messages:
            try:
                begin = time.time()

                diff = int(time.time() - begin)
                if diff > 0:
                    self.logger.info(f"Event {message.get('message_type')} TIME: {diff}")
            except Exception as e:
                self.logger.error(f"Error while processing message {type(message)}, {e}")
                self.logger.error(traceback.format_exc())

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
                return True, cost
        self.logger.debug(f"[SEL]: Not picked Job: {job.get_job_id()} - TIME: {job.no_op} "
                          f"MIN Cost Agents: {min_cost_agents}")
        return False, 0.0

    def execute_job(self, job: Job):
        self.update_completed_jobs(jobs=[job])
        self.redis_repo.save(obj=job.to_dict())
        super().execute_job(job=job)

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

    def job_selection_main(self):
        self.logger.info(f"Starting agent: {self}")
        while self.live_agent_count != self.configured_agent_count:
            time.sleep(1)
            self.logger.info("[SEL_WAIT] Waiting for Peer map to be populated!")

        batch_size = self.proposal_job_batch_size
        job_queue = self.queues.job_queue

        while not self.shutdown:
            try:
                proposals, skipped_jobs = [], []
                caps_jobs_selected = Capacities()

                candidate_jobs = [
                    job for job in job_queue.get_jobs()
                    if job.is_pending() and not self.is_job_completed(job.get_job_id())
                ]

                for job in candidate_jobs:
                    job_id = job.get_job_id()
                    if self.is_job_completed(job_id):
                        continue

                    status, cost = self.__can_select_job(job, caps_jobs_selected)
                    if status:
                        proposal = ProposalInfo(
                            p_id=self.generate_id(), job_id=job_id,
                            agent_id=self.agent_id, seed=cost + self.agent_id)
                        proposals.append(proposal)
                        caps_jobs_selected += job.get_capacities()
                        job.change_state(JobState.PRE_PREPARE)
                    else:
                        skipped_jobs.append(job_id)

                    if len(proposals) >= batch_size:
                        break

                # Push proposals to Redis
                for p in proposals:
                    self.outgoing_proposals.add_proposal(p)
                    self.redis_repo.push_pre_prepare(p.job_id, p.seed, self.agent_id)

                # Handle skipped jobs for ongoing consensus
                for j in skipped_jobs:
                    min_cost_agent, _ = self.redis_repo.get_min_cost_agent_for_job(j)
                    if self.is_job_commit(j):
                        self.redis_repo.push_commit_vote(j, min_cost_agent, self.agent_id)
                        self.queues.job_queue.get_job(j).change_state(JobState.COMMIT)
                    elif self.is_job_prepare(j):
                        self.redis_repo.push_prepare_vote(j, min_cost_agent, self.agent_id)
                        self.queues.job_queue.get_job(j).change_state(JobState.PREPARE)
                    elif self.is_job_pre_prepare(j):
                        self.redis_repo.push_pre_prepare(j, float('inf'), self.agent_id)
                        self.queues.job_queue.get_job(j).change_state(JobState.PRE_PREPARE)

                # PRE_PREPARE → PREPARE
                pre_prepare_jobs = self.redis_repo.get_all_ids(key_prefix=Repository.KEY_PRE_PREPARE)
                for job_id in pre_prepare_jobs:
                    pre_prepare_map = self.redis_repo.get_pre_prepare(job_id)

                    # Only proceed if we've observed at least quorum number of pre_prepare votes
                    if len(pre_prepare_map) < self.calculate_quorum():
                        self.logger.debug(f"[PREPARE-DELAY] Waiting for full pre_prepare quorum on job {job_id}")
                        continue

                    # Proceed with normal leader calculation
                    min_agent, _ = self.redis_repo.get_min_cost_agent_for_job(job_id)
                    job_obj = self.queues.job_queue.get_job(job_id)
                    current_state = job_obj.get_state()

                    # Now cast prepare vote deterministically knowing quorum-sized cost set is stable
                    if min_agent == self.agent_id:
                        if not self.is_job_prepare(job_id):
                            self.logger.debug(f"[PREPARE] LEADER Quorum reached for job {job_id}, casting prepare vote.")
                            self.redis_repo.push_prepare_vote(job_id, self.agent_id, self.agent_id)
                            job_obj.change_state(JobState.PREPARE)
                    else:
                        if current_state not in [JobState.PREPARE, JobState.COMMIT]:
                            self.logger.debug(
                                f"[PREPARE] PARTICIPANT Quorum reached for job {job_id}, casting prepare vote.")
                            self.redis_repo.push_prepare_vote(job_id, min_agent, self.agent_id)
                            job_obj.change_state(JobState.PREPARE)

                # PREPARE → COMMIT (pick most voted leader)
                prepare_jobs = self.redis_repo.get_all_ids(key_prefix=Repository.KEY_PREPARE)
                for job_id in prepare_jobs:
                    prepare_map = self.redis_repo.get_prepare(job_id)

                    leader_vote_count = {}
                    for voter, voted_leader in prepare_map.items():
                        voted_leader = int(voted_leader)
                        leader_vote_count[voted_leader] = leader_vote_count.get(voted_leader, 0) + 1

                    if leader_vote_count:
                        most_voted_leader = max(leader_vote_count, key=leader_vote_count.get)
                        vote_count = leader_vote_count[most_voted_leader]

                        if vote_count < self.calculate_quorum():
                            continue

                        job_obj = self.queues.job_queue.get_job(job_id)
                        if most_voted_leader == self.agent_id:
                            if not self.is_job_commit(job_id):
                                self.logger.debug(
                                    f"[COMMIT] LEADER Quorum reached for job {job_id} on leader {most_voted_leader}, casting commit vote.")
                                self.redis_repo.push_commit_vote(job_id, most_voted_leader, self.agent_id)
                                job_obj.change_state(JobState.COMMIT)
                        else:
                            if not job_obj.is_commit():
                                self.logger.debug(
                                    f"[COMMIT] PARTICIPANT Quorum reached for job {job_id} on leader {most_voted_leader}, casting commit vote.")
                                self.redis_repo.push_commit_vote(job_id, most_voted_leader, self.agent_id)
                                job_obj.change_state(JobState.COMMIT)

                # COMMIT → COMPLETED (pick most voted leader in commit phase)
                commit_jobs = self.redis_repo.get_all_ids(key_prefix=Repository.KEY_COMMIT)
                for job_id in commit_jobs:
                    commit_map = self.redis_repo.get_commit(job_id)

                    leader_vote_count = {}
                    for voter, committed_leader in commit_map.items():
                        committed_leader = int(committed_leader)
                        leader_vote_count[committed_leader] = leader_vote_count.get(committed_leader, 0) + 1

                    if leader_vote_count:
                        most_voted_leader = max(leader_vote_count, key=leader_vote_count.get)
                        vote_count = leader_vote_count[most_voted_leader]

                        if vote_count < self.calculate_quorum():
                            continue

                        job_obj = self.queues.job_queue.get_job(job_id)
                        if not self.is_job_completed(job_id) and not job_obj.is_complete() and not job_obj.is_ready():
                            if most_voted_leader == self.agent_id:
                                self.logger.info(
                                    f"[COMPLETE] Quorum reached for job {job_id} on leader {most_voted_leader}, marking as ready.")
                                job_obj.set_leader(leader_agent_id=most_voted_leader)
                                job_obj.change_state(JobState.READY)
                                self.select_job(job_obj)
                                self.outgoing_proposals.remove_job(job_id=job_obj.job_id)

                time.sleep(0.005)
            except Exception as e:
                self.logger.error(f"Error occurred while executing: {e}\n{traceback.format_exc()}")

        self.logger.info(f"Agent {self} stopped with restarts: {self.metrics.restart_job_selection_cnt}!")

    def _do_periodic(self):
        interval = 5
        while not self.shutdown:
            try:
                agent_info = self.generate_agent_info()
                self.redis_repo.save(agent_info.to_dict(), key_prefix="agent")

                current_time = int(time.time())
                peers = self.redis_repo.get_all_objects(key_prefix="agent")
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
                    (Repository.KEY_JOB, self.update_completed_jobs),
                    (Repository.KEY_PRE_PREPARE, self.update_pre_prepare_jobs),
                    (Repository.KEY_PREPARE, self.update_prepare_jobs),
                    (Repository.KEY_COMMIT, self.update_commit_jobs),
                ]:
                    jobs = self.redis_repo.get_all_ids(key_prefix=prefix)
                    update_fn(jobs=jobs)

                for _ in range(int(interval * 10)):
                    if self.shutdown:
                        break
                    time.sleep(0.1)

                self.check_queue()
                if self.should_shutdown():
                    print("[SHUTDOWN] Queue has been empty for too long. Triggering shutdown.")
                    break
            except Exception as e:
                self.logger.error(f"Periodic update error: {e}\n{traceback.format_exc()}")

        self.stop()