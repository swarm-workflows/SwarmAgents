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
import time
import traceback
from collections import defaultdict

from swarm.models.data_node import DataNode
from swarm.utils.thread_safe_dict import ThreadSafeDict
from swarm.utils.topology import TopologyType
from swarm.agents.agent_grpc import Agent
from swarm.comm.messages.commit import Commit
from swarm.comm.messages.message_builder import MessageBuilder
from swarm.comm.messages.prepare import Prepare
from swarm.comm.messages.proposal import Proposal
from swarm.database.repository import Repository
from swarm.models.capacities import Capacities
from swarm.models.agent_info import AgentInfo
from swarm.models.proposal_info import ProposalContainer, ProposalInfo
from swarm.models.job import Job, JobState
import numpy as np

from swarm.utils.utils import generate_id


class SwarmAgent(Agent):
    def __init__(self, agent_id: int, config_file: str):
        super().__init__(agent_id, config_file)
        self.outgoing_proposals = ProposalContainer()
        self.incoming_proposals = ProposalContainer()

        # Read job cost parameters from config
        job_cfg = self.config.get("job_selection", {})
        weights_cfg = job_cfg.get("cost_weights", {})

        # Relative importance of CPU utilization in job cost (0–1, sum of all weights should ≈ 1.0)
        self.cpu_weight = weights_cfg.get("cpu", 0.4)
        # Relative importance of RAM utilization in job cost (0–1)
        self.ram_weight = weights_cfg.get("ram", 0.3)
        # Relative importance of Disk utilization in job cost (0–1)
        self.disk_weight = weights_cfg.get("disk", 0.2)
        # Relative importance of GPU utilization in job cost (0–1)
        self.gpu_weight = weights_cfg.get("gpu", 0.1)
        # Execution time (in seconds) beyond which jobs incur extra penalty
        self.long_job_threshold = job_cfg.get("long_job_threshold", 20.0)
        # Multiplier for DTN connectivity penalty (0=no effect, >1 increases penalty severity)
        self.connectivity_penalty_factor = job_cfg.get("connectivity_penalty_factor", 1.0)
        # % above min cost allowed in candidate selection (lower = stricter, higher = more agents considered)
        self.selection_threshold_pct = job_cfg.get("selection_threshold_pct", 10.0)

        self.q_table = defaultdict(lambda: 0.0)  # {(agent_id, job_type): score}
        self.learning_rate = 0.1
        self.discount_factor = 0.9
        self.exploration_rate = 0.1  # epsilon-greedy

        # ---- perf caches ----
        self._feas_cache: dict[tuple, bool] = {}
        self._base_cost_cache: dict[tuple, float] = {}

        # optional: cap cache sizes if you have very long runs
        self._feas_cache_max = 100_000
        self._cost_cache_max = 100_000

        # track proposed allocations incrementally to avoid recomputing each time
        self._proposed_allocations = Capacities()

    def __receive_proposal(self, incoming: Proposal):
        proposals = []
        proposals_to_forward = []
        for p in incoming.proposals:
            job = self.queues.job_queue.get_job(job_id=p.job_id)
            if not job:
                self.logger.debug(f"ERROR ---- Skipping no job found for {p.job_id}")
                self.outgoing_proposals.remove_job(job_id=p.job_id)
                self.incoming_proposals.remove_job(job_id=p.job_id)
                proposals_to_forward.append(p)
                continue
            if self.is_job_completed(job_id=job.get_job_id()):
                self.logger.debug(f"Ignoring Proposal: {p} for job: {job.get_job_id()}")
                continue

            my_proposal = self.outgoing_proposals.has_better_proposal(proposal=p)
            peer_proposal = self.incoming_proposals.has_better_proposal(proposal=p)

            if my_proposal:
                self.logger.debug(f"Job:{p.job_id} Agent:{self.agent_id} rejected Proposal: {p} from agent"
                                  f" {p.agent_id} - my proposal {my_proposal} has prepares or smaller seed")
                job_id = job.get_job_id()
                self.metrics.conflicts[job_id] = self.metrics.conflicts.get(job_id, 0) + 1
            elif peer_proposal:
                self.logger.debug(f"Job:{p.job_id} Agent:{self.agent_id} rejected Proposal: {p} from agent"
                                  f" {p.agent_id} - already accepted proposal {peer_proposal} with a smaller seed")
                job_id = job.get_job_id()
                self.metrics.conflicts[job_id] = self.metrics.conflicts.get(job_id, 0) + 1
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

        if len(proposals_to_forward) and self.topology.type in [TopologyType.Star, TopologyType.Ring]:
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
                self.logger.debug(f"ERROR ---- Skipping no job found for {p.job_id}")
                self.outgoing_proposals.remove_job(job_id=p.job_id)
                self.incoming_proposals.remove_job(job_id=p.job_id)
                proposals_to_forward.append(p)
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

            quorum_count = self.calculate_quorum()
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

        if len(proposals_to_forward) and self.topology.type in [TopologyType.Star, TopologyType.Ring]:
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
                self.logger.debug(f"ERROR ---- Skipping no job found for {p.job_id}")
                self.outgoing_proposals.remove_job(job_id=p.job_id)
                self.incoming_proposals.remove_job(job_id=p.job_id)
                proposals_to_forward.append(p)
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

            quorum_count = self.calculate_quorum()

            if len(proposal.commits) >= quorum_count:
                self.logger.debug(
                    f"Job: {p.job_id} Agent: {self.agent_id} received quorum commits Proposal: {proposal}: "
                    f"Job: {job.get_job_id()}")
                if proposal.agent_id == self.agent_id:
                    job.set_leader(leader_agent_id=proposal.agent_id)
                if self.outgoing_proposals.contains(job_id=p.job_id, p_id=p.p_id):
                    self.logger.info(f"[CON_LEADER] achieved for Job: {p.job_id} Leader: {self.agent_id}")
                    print(f"SELECTED: {job.get_job_id()} on agent: {self.agent_id} proposal: {proposal.p_id} "
                          f"commits: {proposal.commits} quorum: {quorum_count}")
                    self.select_job(job)
                    self.outgoing_proposals.remove_job(job_id=p.job_id)
                else:
                    self.logger.info(f"[CON_PART] achieved for Job: {p.job_id} Leader: {p.agent_id}")
                    job.change_state(new_state=JobState.COMPLETE)
                    self.incoming_proposals.remove_job(job_id=p.job_id)

        if len(proposals_to_forward) and self.topology.type in [TopologyType.Star, TopologyType.Ring]:
            msg = Commit(source=incoming.agents[0].agent_id, agents=[AgentInfo(agent_id=incoming.agents[0].agent_id)],
                         proposals=proposals_to_forward,
                         forwarded_by=self.agent_id)
            self._send_message(json_message=msg.to_dict(),
                               excluded_peers=[incoming.forwarded_by, incoming.agents[0].agent_id],
                               src=incoming.agents[0].agent_id, fwd=self.agent_id)

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

                else:
                    self.logger.info(f"Ignoring unsupported message: {message}")
                diff = int(time.time() - begin)
                if diff > 0:
                    self.logger.info(f"Event {message.get('message_type')} TIME: {diff}")
            except Exception as e:
                self.logger.error(f"Error while processing message {type(message)}, {e}")
                self.logger.error(traceback.format_exc())

    def _update_pending_jobs(self, jobs: list[str]):
        for job_id in jobs:
            #job_id = key.split(":")[-1]
            if job_id not in self.queues.job_queue:
                job = self.repo.get(obj_id=job_id, key_prefix=Repository.KEY_JOB, level=self.topology.level,
                                    group=self.topology.group)
                job_obj = Job()
                job_obj.from_dict(job)
                self.queues.job_queue.add_job(job_obj)

    def _update_ready_jobs(self, jobs: list[str]):
        for j in jobs:
            self.incoming_proposals.remove_job(job_id=j)
            self.outgoing_proposals.remove_job(job_id=j)
            self.queues.job_queue.remove_job(job_id=j)

    def _update_completed_jobs(self, jobs: list[str]):
        super()._update_completed_jobs(jobs=jobs)
        for j in jobs:
            self.incoming_proposals.remove_job(job_id=j)
            self.outgoing_proposals.remove_job(job_id=j)
            self.queues.job_queue.remove_job(job_id=j)

    def _restart_selection(self):
        jobs = self.queues.job_queue.get_jobs(states=[JobState.PREPARE, JobState.PRE_PREPARE,
                                                      JobState.COMMIT])
        for job in jobs:
            diff = int(time.time() - job.time_last_state_change)
            if diff > self.restart_job_selection:
                self.logger.info(f"RESTART: Job: {job} reset to Pending")
                job.change_state(new_state=JobState.PENDING)
                self.outgoing_proposals.remove_job(job_id=job.get_job_id())
                self.incoming_proposals.remove_job(job_id=job.get_job_id())
                job_id = job.get_job_id()
                self.metrics.restarts[job_id] = self.metrics.restarts.get(job_id, 0) + 1

    def _refresh_neighbors(self, current_time: float):
        """
        Refresh the neighbor agent map by retrieving peer agents at the same level from Redis
        and removing any that are stale or no longer present.

        :param current_time: The current timestamp used to check for staleness.
        :type current_time: float
        """
        self._refresh_agent_map(
            current_time=current_time,
            target_dict=self.neighbor_map,
            level=self.topology.level,
            groups=[self.topology.group]
        )

    def _refresh_children(self, current_time: float):
        """
        Refresh the children agent map by retrieving agents from the level below (level - 1)
        and removing any that are stale or no longer present.

        :param current_time: The current timestamp used to check for staleness.
        :type current_time: float
        """
        if not self.topology.children or len(self.topology.children) == 0:
            return
        self._refresh_agent_map(
            current_time=current_time,
            target_dict=self.children,
            level=self.topology.level - 1,
            groups=self.topology.children  # assumed to be a List[int]
        )

    def _refresh_agent_map(
            self,
            current_time: float,
            target_dict: ThreadSafeDict[int, AgentInfo],
            level: int,
            groups: list[int]
    ):
        """
        Generic helper to refresh a target agent map (e.g., neighbors or children) by reading from Redis.

        Updates the target map with agents that are fresh and present in Redis,
        and removes agents that are stale or no longer in the store.

        :param current_time: The current timestamp for evaluating staleness.
        :type current_time: float
        :param target_dict: A thread-safe dictionary mapping agent IDs to AgentInfo objects.
        :type target_dict: ThreadSafeDict[int, AgentInfo]
        :param level: The level of the agents to query in the hierarchy.
        :type level: int
        :param groups: group ID within that level to refresh.
        :type groups: list[int]
        """
        active_ids = set()

        for group in groups:
            agent_dicts = self.repo.get_all_objects(
                key_prefix=Repository.KEY_AGENT,
                level=level,
                group=group
            )
            for agent_data in agent_dicts:
                agent = AgentInfo.from_dict(agent_data)
                #if agent.agent_id and agent.agent_id != self.agent_id:
                existing = target_dict.get(agent.agent_id)
                if existing is None or agent.last_updated > existing.last_updated:
                    target_dict.set(agent.agent_id, agent)
                active_ids.add(agent.agent_id)

        for agent_id, agent in list(target_dict.items()):
            if agent_id not in active_ids and (current_time - agent.last_updated) >= self.peer_expiry_seconds:
                target_dict.remove(agent_id)

    def _generate_agent_info(self) -> AgentInfo:
        """
        Generate and return the current AgentInfo object representing this agent.

        For leaf agents (with no children), the info includes:
          - self capacities
          - capacity allocations based on ready queue jobs
          - computed load

        For non-leaf agents (with children), the info aggregates:
          - cumulative capacities and allocations from children
          - cumulative load computed from aggregated values

        :return: An AgentInfo object with the current agent's state
        :rtype: AgentInfo
        """
        current_time = time.time()

        # Leaf agent: report its own resource info
        if not self.topology.children:
            self._load = self.compute_overall_load()
            self.metrics.save_load_metric(load=self._load)
            proposed_load = self.compute_proposed_load()
            agent_info = AgentInfo(
                agent_id=self.agent_id,
                capacities=self.capacities,
                capacity_allocations=self.capacity_allocations,
                load=self._load,
                proposed_load=proposed_load,
                last_updated=current_time,
                dtns=self.config.get("dtns")
            )
        # Non-leaf agent: aggregate info from all children
        else:
            total_capacities = Capacities()
            total_allocations = Capacities()
            # Aggregate DTNs from children, keyed by DTN name to deduplicate
            dtn_map = {}

            for child in self.children.values():
                total_capacities += child.capacities
                total_allocations += child.capacity_allocations
                if child.dtns:
                    dtn_map.update(child.dtns)

            dtns = []
            for dtn in dtn_map.values():
                dtns.append(dtn.to_dict())

            self._capacities = total_capacities
            self._load = self.resource_usage_score(total_allocations, total_capacities)
            proposed_load = self.compute_proposed_load()
            agent_info = AgentInfo(
                agent_id=self.agent_id,
                capacities=total_capacities,
                capacity_allocations=total_allocations,
                load=self._load,
                last_updated=current_time,
                dtns=dtns,
                proposed_load=proposed_load
            )

        return agent_info

    def _do_periodic(self):
        while not self.shutdown:
            try:
                self._restart_selection()
                current_time = int(time.time())
                self._refresh_children(int(current_time))
                agent_info = self._generate_agent_info()
                self.repo.save(agent_info.to_dict(), key_prefix=Repository.KEY_AGENT, level=self.topology.level,
                               group=self.topology.group)

                self._refresh_neighbors(current_time=current_time)

                # Batch update job sets
                for prefix, update_fn, state in [
                    (Repository.KEY_JOB, self._update_pending_jobs, JobState.PENDING.value),
                    (Repository.KEY_JOB, self._update_ready_jobs, JobState.READY.value),
                    (Repository.KEY_JOB, self._update_completed_jobs, JobState.COMPLETE.value),
                ]:
                    jobs = self.repo.get_all_ids(key_prefix=prefix, level=self.topology.level,
                                                 group=self.topology.group, state=state)
                    update_fn(jobs=jobs)

                time.sleep(0.5)

                self.check_queue()
                if self.should_shutdown():
                    print("[SHUTDOWN] Queue has been empty for too long. Triggering shutdown.")
                    break
            except Exception as e:
                self.logger.error(f"Periodic update error: {e}\n{traceback.format_exc()}")

        self.stop()

    def is_job_feasible(self, job: Job, agent: AgentInfo) -> bool:
        """
        Check if a job is feasible for a given agent based on:
          - Current load threshold
          - Resource availability
          - Connectivity to required DTNs for data_in and data_out

        :param job: The job to check.
        :type job: Job
        :param agent: The agent to check feasibility for.
        :type agent: AgentInfo
        :return: True if job is feasible for the agent, False otherwise.
        :rtype: bool
        """
        # Capacity check first (cheapest)
        available = agent.capacities - agent.capacity_allocations
        if not self._has_sufficient_capacity(job, available):
            return False

        # DTN connectivity
        if not hasattr(job, "_required_dtns_cache"):
            rin = {e.name for e in (job.data_in or [])}
            rout = {e.name for e in (job.data_out or [])}
            job._required_dtns_cache = frozenset(rin | rout)

        # normalize agent.dtns to a set of names once per agent signature (already in _agent_sig)
        if isinstance(agent.dtns, dict):
            agent_dtn_names = agent.dtns.keys()
        else:
            agent_dtn_names = [getattr(x, "name", None) or (x.get("name") if isinstance(x, dict) else None)
                               for x in (agent.dtns or [])]

        for d in job._required_dtns_cache:
            if d not in agent_dtn_names:
                return False

        return True

    def compute_overall_load(self):
        allocations = Capacities()
        allocations += self.capacity_allocations

        return self.resource_usage_score(allocated=allocations, total=self.capacities)

    def compute_proposed_load(self):
        allocations = Capacities()
        for j in self.outgoing_proposals.jobs():
            if j not in self.queues.ready_queue and j not in self.queues.selected_queue:
                job = self.queues.job_queue.get_job(job_id=j)
                if job:
                    allocations += job.capacities

        return self.resource_usage_score(allocated=allocations, total=self.capacities)

    def _job_sig(self, job: Job) -> tuple:
        # Required DTNs can be expensive to rebuild; compute once and reuse
        if not hasattr(job, "_required_dtns_cache"):
            rin = {e.name for e in (job.data_in or [])}
            rout = {e.name for e in (job.data_out or [])}
            job._required_dtns_cache = frozenset(rin | rout)
        caps = job.capacities
        return (
            job.get_job_id(),
            round(caps.core, 3), round(caps.ram, 3), round(caps.disk, 3), round(getattr(caps, "gpu", 0.0), 3),
            round(job.execution_time or 0.0, 3),
            job.job_type or "",
            job._required_dtns_cache,
            job.get_state().value,  # flips when PENDING→READY/COMPLETE, invalidates cache automatically
        )

    def _agent_sig(self, agent: AgentInfo) -> tuple:
        caps = agent.capacities or Capacities()
        # Represent DTNs by (name,score) so signature only changes when the set/scores change
        if isinstance(agent.dtns, dict):
            dtn_pairs = tuple(sorted((k, round(getattr(v, "connectivity_score", 1.0), 3)) for k, v in agent.dtns.items()))
        else:
            # if already list of dicts or DataNode, normalize
            def _pair(x):
                name = getattr(x, "name", None) or (x.get("name") if isinstance(x, dict) else None)
                score = getattr(x, "connectivity_score", None) or (x.get("connectivity_score") if isinstance(x, dict) else 1.0)
                return (name, round(float(score), 3))
            dtn_pairs = tuple(sorted(_pair(x) for x in (agent.dtns or [])))

        return (
            agent.agent_id,
            round(caps.core, 3), round(caps.ram, 3), round(caps.disk, 3), round(getattr(caps, "gpu", 0.0), 3),
            dtn_pairs,
        )

    def _feas_key(self, agent: AgentInfo, job: Job) -> tuple:
        return ("F", self._agent_sig(agent), self._job_sig(job))

    def _cost_key(self, agent: AgentInfo, job: Job) -> tuple:
        # base cost doesn’t depend on load; keep load out so cache survives load changes
        return ("C", self._agent_sig(agent), self._job_sig(job))

    def _maybe_trim_cache(self):
        # very simple LRU-ish trim (cheap + good enough)
        if len(self._feas_cache) > self._feas_cache_max:
            self._feas_cache.clear()
        if len(self._base_cost_cache) > self._cost_cache_max:
            self._base_cost_cache.clear()

    def compute_job_cost(
            self,  # now uses self so it can read config defaults
            job: Job,
            total: Capacities,
            dtns: dict[str, DataNode]
    ) -> float:
        """
        Compute the cost of executing a job on an agent based on weighted resource usage,
        bottleneck effects, execution time penalties, and DTN connectivity.

        The cost is calculated as a weighted sum of resource usage ratios, penalized for:
          - High single-resource utilization (bottleneck penalty)
          - Long execution times beyond a configurable threshold
          - Poor connectivity to DTNs required by the job

        :param job: The job whose cost is to be computed.
        :type job: Job
        :param total: Total available resources.
        :type total: Capacities
        :param dtns: DTN info for the agent
        :type dtns: dict[str, DataNode]
        :return: Calculated job cost (higher is more expensive).
        :rtype: float
        """

        # --- Start with defaults from config ---
        cpu_weight = self.cpu_weight
        ram_weight = self.ram_weight
        disk_weight = self.disk_weight
        gpu_weight = self.gpu_weight
        long_job_threshold = self.long_job_threshold
        connectivity_penalty_factor = self.connectivity_penalty_factor

        if not total or total.core <= 0 or total.ram <= 0 or total.disk <= 0:
            return float('inf')

        # --- Dynamic tuning based on job_type ---
        if hasattr(job, "job_type") and job.job_type:
            jt = job.job_type

            # Resource emphasis (scale relative to config)
            if "cpu_bound" in jt:
                cpu_weight *= 1.5
                ram_weight *= 0.7
                disk_weight *= 0.7
                gpu_weight *= 0.7
            elif "ram_bound" in jt:
                ram_weight *= 1.5
                cpu_weight *= 0.7
                disk_weight *= 0.7
                gpu_weight *= 0.7
            elif "disk_bound" in jt:
                disk_weight *= 1.5
                cpu_weight *= 0.7
                ram_weight *= 0.7
                gpu_weight *= 0.7
            elif "gpu_bound" in jt:
                gpu_weight *= 1.5
                cpu_weight *= 0.7
                ram_weight *= 0.7
                disk_weight *= 0.7

            # Normalize weights to sum to ~1
            total_w = cpu_weight + ram_weight + disk_weight + gpu_weight
            cpu_weight /= total_w
            ram_weight /= total_w
            disk_weight /= total_w
            gpu_weight /= total_w

            # Execution time sensitivity
            if "long" in jt:
                long_job_threshold = max(5.0, long_job_threshold * 0.75)
            elif "short" in jt:
                long_job_threshold = long_job_threshold * 1.5

            # Connectivity importance
            if "dtn_heavy" in jt:
                connectivity_penalty_factor *= 1.5
            elif "dtn_light" in jt:
                connectivity_penalty_factor *= 0.5

        # Prevent division by zero for GPUs
        total_gpu = getattr(total, "gpu", 0) or 1
        job_gpu = getattr(job.capacities, "gpu", 0)

        # Resource usage ratios
        core_ratio = job.capacities.core / total.core
        ram_ratio = job.capacities.ram / total.ram
        disk_ratio = job.capacities.disk / total.disk
        gpu_ratio = job_gpu / total_gpu

        # Weighted base score
        base_score = (
                cpu_weight * core_ratio +
                ram_weight * ram_ratio +
                disk_weight * disk_ratio +
                gpu_weight * gpu_ratio
        )

        # Bottleneck penalty
        bottleneck_penalty = max(core_ratio, ram_ratio, disk_ratio, gpu_ratio) ** 2

        # Execution time penalty
        if job.execution_time > long_job_threshold:
            time_penalty = 1.5 + (job.execution_time - long_job_threshold) / long_job_threshold
        else:
            time_penalty = 1 + (job.execution_time / long_job_threshold) ** 2

        # DTN connectivity penalty
        avg_conn = 1.0
        if hasattr(job, "data_in") or hasattr(job, "data_out"):
            required_dtns = {entry.name for entry in (job.data_in or [])} | \
                            {entry.name for entry in (job.data_out or [])}
            agent_dtn_scores = {dtn.name: getattr(dtn, "connectivity_score", 1.0)
                                for dtn in dtns.values()}
            scores = [agent_dtn_scores.get(dtn, 0.0) for dtn in required_dtns]
            if scores:
                avg_conn = sum(scores) / len(scores)

        connectivity_penalty = 1 + connectivity_penalty_factor * (1 - avg_conn)

        # Final cost
        cost = (base_score + bottleneck_penalty) * time_penalty * connectivity_penalty * 100
        return round(cost, 2)

    def compute_cost_matrix(self, jobs: list[Job]) -> np.ndarray:
        """
        Compute a cost matrix for assigning jobs to agents, with feasibility checks,
        bottleneck penalties, and load-aware scaling.

        Rows represent agents, columns represent jobs. Each entry [i, j] represents
        the cost of agent `i` executing job `j`, or infinity if the job is not feasible
        for that agent.

        :param jobs: List of jobs to compute costs for.
        :type jobs: list[Job]
        :return: 2D numpy array of shape (num_agents, num_jobs) containing execution costs.
        :rtype: np.ndarray
        """
        agents_map = self.neighbor_map

        # Snapshot neighbors once (iteration order fixed)
        agent_ids = list(agents_map.keys())
        agents = [agents_map.get(aid) for aid in agent_ids]

        num_agents = len(agent_ids)
        num_jobs = len(jobs)
        cost_matrix = np.full((num_agents, num_jobs), float('inf'))

        # Prebuild job signatures once (also primes _required_dtns_cache)
        job_sigs = [self._job_sig(j) for j in jobs]

        for ai, agent in enumerate(agents):
            if agent is None:
                continue
            agent_sig = self._agent_sig(agent)
            projected_load = agent.load + agent.proposed_load
            load_penalty = 1 + (projected_load / 100) ** 1.5

            for ji, job in enumerate(jobs):
                # ---- cached feasibility ----
                fkey = ("F", agent_sig, job_sigs[ji])
                feas = self._feas_cache.get(fkey)
                if feas is None:
                    feas = self.is_job_feasible(job, agent)
                    self._feas_cache[fkey] = feas
                if not feas:
                    continue

                # ---- cached base cost (no load) ----
                ckey = ("C", agent_sig, job_sigs[ji])
                base_cost = self._base_cost_cache.get(ckey)
                if base_cost is None:
                    base_cost = self.compute_job_cost(job, total=agent.capacities, dtns=agent.dtns)
                    self._base_cost_cache[ckey] = base_cost

                cost_matrix[ai, ji] = round(base_cost * load_penalty, 2)

        self._maybe_trim_cache()
        return cost_matrix

    def find_min_cost_agents(self, cost_matrix: np.ndarray, threshold_pct: float = 10.0) -> list[tuple[int, float]]:
        """
        Determine the agent with the minimum cost for each job, allowing near-minimum costs
        within a given percentage threshold. Logs the top 3 candidate agents for debugging.

        :param cost_matrix: 2D numpy array of job execution costs (rows=agents, cols=jobs).
        :type cost_matrix: np.ndarray
        :param threshold_pct: Percentage above minimum cost allowed for candidate selection.
        :type threshold_pct: float
        :return: List of (agent_id, cost) tuples for each job.
        :rtype: list[tuple[int, float]]
        """
        agents = self.neighbor_map
        agent_ids = list(agents.keys())
        min_cost_agents = []

        for j in range(cost_matrix.shape[1]):
            valid_costs = cost_matrix[:, j]
            finite_mask = np.isfinite(valid_costs)
            if not np.any(finite_mask):
                continue

            min_cost = np.min(valid_costs[finite_mask])
            threshold = min_cost * (1 + threshold_pct / 100.0)

            candidates = [(agent_ids[i], valid_costs[i]) for i in np.where(valid_costs <= threshold)[0]]
            selected_agent = min(candidates, key=lambda x: (x[1], x[0]))
            min_cost_agents.append(selected_agent)

        return min_cost_agents

    def job_selection_main(self):
        self.logger.info(f"Starting agent: {self}")
        while self.live_agent_count != self.configured_agent_count:
            time.sleep(0.5)
            self.logger.info(f"[SEL_WAIT] Waiting for Peer map to be populated: "
                             f"{self.live_agent_count}/{self.configured_agent_count}!")

        while not self.shutdown:
            try:
                pending_jobs = self.queues.job_queue.get_jobs(states=[JobState.PENDING],
                                                              count=self.proposal_job_batch_size)
                if not pending_jobs:
                    time.sleep(0.5)
                    continue
                proposals = []

                # Step 1: Compute cost matrix ONCE for all agents and jobs
                cost_matrix = self.compute_cost_matrix(pending_jobs)

                # Step 2: Use existing helper to get the best agent per job
                assignments = self.find_min_cost_agents(cost_matrix=cost_matrix,
                                                        threshold_pct=self.selection_threshold_pct)

                # Step 3: If this agent is assigned, start proposal
                for job_idx, (selected_agent_id, cost) in enumerate(assignments):
                    if selected_agent_id == self.agent_id:
                        # Send proposal to all neighbors
                        job = pending_jobs[job_idx]
                        proposal = ProposalInfo(p_id=generate_id(), job_id=job.get_job_id(),
                                                agent_id=self.agent_id, seed=round((cost + self.agent_id), 2))
                        proposals.append(proposal)
                        # Begin election for Job leader for this job
                        job.change_state(new_state=JobState.PRE_PREPARE)

                if len(proposals):
                    msg = Proposal(source=self.agent_id,
                                   agents=[AgentInfo(agent_id=self.agent_id)],
                                   proposals=proposals)
                    self._send_message(json_message=msg.to_dict())
                    for p in proposals:
                        self.outgoing_proposals.add_proposal(p)  # Add all proposals
                    proposals.clear()

                # Trigger leader election for a job after random sleep
                #election_timeout = random.uniform(150, 300) / 1000
                #time.sleep(election_timeout)

                time.sleep(0.5)
            except Exception as e:
                self.logger.error(f"Error occurred while executing e: {e}")
                self.logger.error(traceback.format_exc())
        self.logger.info(f"Agent: {self} stopped with restarts: {self.metrics.restarts}!")
