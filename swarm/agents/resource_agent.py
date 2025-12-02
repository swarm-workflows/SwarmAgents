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
import os
import random
import threading
import time
import traceback
from concurrent.futures.thread import ThreadPoolExecutor

from google.genai.types import JobState

from swarm.consensus.engine import ConsensusEngine
from swarm.consensus.interfaces import ConsensusHost, ConsensusTransport, TopologyRouter
from swarm.consensus.messages.message import MessageType
from swarm.models.data_node import DataNode
from swarm.models.object import Object
from swarm.selection.engine import SelectionEngine
from swarm.selection.penalties import apply_multiplicative_penalty
from swarm.topology.topology import TopologyType
from swarm.utils.metrics import Metrics
from swarm.utils.resource_queues import ResourceAgentQueues
from swarm.utils.thread_safe_dict import ThreadSafeDict
from swarm.agents.agent_grpc import Agent
from swarm.consensus.messages.commit import Commit
from swarm.consensus.messages.message_builder import MessageBuilder
from swarm.consensus.messages.prepare import Prepare
from swarm.consensus.messages.proposal import Proposal
from swarm.database.repository import Repository
from swarm.models.capacities import Capacities
from swarm.models.agent_info import AgentInfo
from swarm.consensus.messages.proposal_info import ProposalInfo
from swarm.models.job import Job, ObjectState

from swarm.utils.utils import generate_id, job_capacities


class _HostAdapter(ConsensusHost):
    def __init__(self, agent: "ResourceAgent"):
        self.agent = agent

    def get_object(self, object_id: str):
        # Try local queue first
        job = self.agent.queues.pending_queue.get(object_id)
        if job:
            return job

        # Not in local queue - fetch from Redis on-demand
        # This allows agents to vote on proposals even if they haven't pulled the job locally yet
        try:
            job_data = self.agent.repository.get(
                obj_id=object_id,
                key_prefix=Repository.KEY_JOB,
                level=self.agent.topology.level,
                group=self.agent.topology.group
            )
            if job_data:
                self.agent.logger.debug(f"Fetched job {object_id} from Redis for consensus voting")
                job = Job()
                job.from_dict(job_data)
                if job.state == ObjectState.PENDING:
                    self.agent.queues.pending_queue.add(job)
                return job
        except Exception as e:
            self.agent.logger.warning(f"Failed to fetch job {object_id} from Redis: {e}")
            return None

    def is_agreement_achieved(self, object_id: str): return self.agent.is_job_completed(object_id)
    def calculate_quorum(self): return self.agent.calculate_quorum()
    def on_leader_elected(self, obj: Object, proposal_id: str): self.agent.select_job(obj)
    def on_participant_commit(self, obj: Object, leader_id: int, proposal_id: str):
        obj.state = ObjectState.COMMIT
        # If proposal was committed and leader is a peer (not self)
        self.agent.job_assignments.set(obj.object_id, int(leader_id))
        self.agent.logger.debug(
            f"Tracked peer assignment: Job {obj.object_id} -> Agent {leader_id}"
        )
    def now(self): import time; return time.time()
    def log_debug(self, m): self.agent.logger.debug(m)
    def log_info(self, m): self.agent.logger.info(m)
    def log_warn(self, m): self.agent.logger.warning(m)


class _TransportAdapter(ConsensusTransport):
    def __init__(self, agent: "ResourceAgent"):
        self.agent = agent
    def send(self, dest: int, payload: object) -> None:
        self.agent.send(dest, payload)
    def broadcast(self, payload: object) -> None:
        self.agent.broadcast(payload)


class _RouteAdapter(TopologyRouter):
    def __init__(self, agent: "ResourceAgent"):
        self.agent = agent

    def should_forward(self) -> bool:
        if self.agent.topology.type in [TopologyType.Star, TopologyType.Ring]:
            return True
        return False


class ResourceAgent(Agent):
    def __init__(self, agent_id: int, config_file: str, debug: bool = False):
        super().__init__(agent_id, config_file, debug)
        self.engine = ConsensusEngine(agent_id, _HostAdapter(self), _TransportAdapter(self),
                                      router=_RouteAdapter(self))

        self._capacities = Capacities().from_dict(self.config.get("capacities", {}))
        self._load = 0
        self.metrics = Metrics()
        self.completed_lock = threading.RLock()
        self.completed_jobs_set = set()
        self.queues = ResourceAgentQueues()

        self.threads.update({
            "selection": threading.Thread(target=self.selection_main, daemon=True),
            "scheduling": threading.Thread(target=self.scheduling_main, daemon=True)
        })
        self.executor = ThreadPoolExecutor(max_workers=self.runtime_config.get("executor_workers", 3))

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

        self.selector = SelectionEngine(
            feasible=lambda job, agent: self.is_job_feasible(job, agent),
            cost=self._cost_job_on_agent,
            candidate_key=self._job_sig,
            assignee_key=self._agent_sig,
            candidate_version=lambda job: int(getattr(job, "version", 0) or 0),
            assignee_version=lambda ag: int(getattr(ag, "version", 0) or getattr(ag, "updated_at", 0) or 0),
            cache_enabled=True,
            feas_cache_size=131072,
            cost_cache_size=131072,
            cache_ttl_s=60.0,  # optional, if you want time-based safety
        )

        # Track failed agents and job assignments
        self.failed_agents = ThreadSafeDict[int, float]()  # agent_id -> failure_timestamp
        self.job_assignments = ThreadSafeDict[str, int]()  # job_id -> assigned_agent_id

        # Track jobs delegated to children for hierarchical monitoring
        # Maps: job_id -> {'delegated_at': timestamp, 'groups': [list of child groups]}
        self.delegated_jobs = ThreadSafeDict[str, dict]()  # job_id -> delegation_info

    @property
    def peer_expiry_seconds(self) -> int:
        return self.runtime_config.get("peer_expiry_seconds", 20)

    @property
    def reselection_timeout_s(self) -> float:
        return self.runtime_config.get("reselection_timeout_s", 60.00)

    @property
    def delegation_timeout_s(self) -> float:
        """
        Time threshold for monitoring delegated jobs at child level.
        If a job is not selected by children within this time, it will be
        re-added to the parent's queue.

        Default is 2x reselection_timeout_s to give children enough time
        to complete their selection process.
        """
        return self.runtime_config.get("delegation_timeout_s", self.reselection_timeout_s * 2)

    @property
    def capacities(self) -> Capacities:
        return self._capacities

    @property
    def capacity_allocations(self) -> Capacities:
        return job_capacities(self.queues.ready_queue.gets())

    def start_idle(self):
        if self.metrics.idle_start_time is None:
            self.metrics.idle_start_time = time.time()

    def end_idle(self):
        if self.metrics.idle_start_time is not None:
            idle_duration = time.time() - self.metrics.idle_start_time
            self.metrics.idle_time.append(idle_duration)
            self.metrics.total_idle_time += idle_duration
            self.metrics.idle_start_time = None

    def get_total_idle_time(self):
        self.end_idle()
        return self.metrics.total_idle_time

    @property
    def empty_timeout_seconds(self):
        return self.runtime_config.get("empty_timeout_seconds", 3)

    @property
    def proposal_job_batch_size(self):
        return self.runtime_config.get("jobs_per_proposal", 10)

    @staticmethod
    def _has_sufficient_capacity(job: Job, available: Capacities) -> bool:
        """
        Returns True if the job can be accommodated by the available capacity.
        Also checks data transfer feasibility if enabled.
        """
        # Check if job fits into available capacities
        residual = available - job.capacities
        negative_fields = residual.negative_fields()
        if len(negative_fields) > 0:
            # Debug: log capacity mismatch details
            import logging
            logger = logging.getLogger('agent')
            logger.debug(
                f"[CAPACITY] Job {job.job_id} INSUFFICIENT: "
                f"needs(cores={job.capacities.core}, ram={job.capacities.ram}, "
                f"disk={job.capacities.disk}, gpu={getattr(job.capacities, 'gpu', 0)}) "
                f"vs available(cores={available.core}, ram={available.ram}, "
                f"disk={available.disk}, gpu={getattr(available, 'gpu', 0)}) "
                f"negative_fields={negative_fields}"
            )
            return False
        return True

    @staticmethod
    def resource_usage_score(allocated: Capacities, total: Capacities):
        if allocated == total:
            return 0
        core = (allocated.core / total.core) * 100
        ram = (allocated.ram / total.ram) * 100
        disk = (allocated.disk / total.disk) * 100

        return round((core + ram + disk) / 3, 2)

    @property
    def failure_threshold_seconds(self) -> int:
        """
        Time threshold before declaring an agent as failed.

        Balance considerations:
        - Too low (< 15s): False positives from temporary network issues
        - Too high (> 60s): Slow failure detection, jobs stuck longer
        - Recommended: 30s for local, 45-60s for remote deployments
        """
        return self.runtime_config.get("failure_threshold_seconds", 30)

    @property
    def max_failed_agents(self) -> int:
        """
        Maximum number of failed agents before triggering critical warnings.

        Returns 0 for unlimited failures allowed.
        Default is 1/3 of configured agents (33% failure tolerance).
        """
        max_config = self.runtime_config.get("max_failed_agents", 0)
        if max_config == 0 and self.configured_agent_count > 0:
            return self.configured_agent_count // 3
        return max_config

    @property
    def job_reassignment_enabled(self) -> bool:
        """Enable automatic job reassignment from failed agents."""
        return self.runtime_config.get("job_reassignment_enabled", True)

    @property
    def aggressive_failure_detection(self) -> bool:
        """
        Enable aggressive failure detection mode.

        When True:
        - Failures detected immediately via gRPC callbacks
        - Shorter timeout windows
        - Immediate job reassignment

        When False:
        - Rely primarily on periodic checks
        - Longer grace periods
        - More tolerant of transient issues
        """
        return self.runtime_config.get("aggressive_failure_detection", False)

    def __on_proposal(self, incoming: Proposal):
        self.engine.on_proposal(incoming)

    def __on_prepare(self, incoming: Prepare):
        self.engine.on_prepare(incoming)

    def __on_commit(self, incoming: Commit):
        self.engine.on_commit(incoming)

    def _process(self, messages: list[dict]):
        for message in messages:
            message_type = MessageType(message.get('message_type'))
            try:
                begin = time.time()
                incoming = MessageBuilder.from_dict(message)
                if not self.should_process(msg=incoming):
                    continue
                if isinstance(incoming, Prepare):
                    self.__on_prepare(incoming=incoming)

                elif isinstance(incoming, Commit):
                    self.__on_commit(incoming=incoming)

                elif isinstance(incoming, Proposal):
                    self.__on_proposal(incoming=incoming)

                else:
                    self.logger.info(f"Ignoring unsupported message: {message}")
                diff = int(time.time() - begin)
                if diff > 0:
                    self.logger.info(f"Event {message_type} TIME: {diff}")
            except Exception as e:
                self.logger.error(f"Error while processing message {type(message)}, {e}")
                self.logger.error(traceback.format_exc())

    def _update_pending_jobs(self, jobs: list[str]):
        for job_id in jobs:
            #job_id = key.split(":")[-1]
            if job_id not in self.queues.pending_queue:
                #group = self.topology.level if self.topology.type == TopologyType.Ring else self.topology.group
                group = self.topology.group
                #self.logger.info(f"Adding job {job_id} for: {group}")
                job = self.repository.get(obj_id=job_id, key_prefix=Repository.KEY_JOB,
                                          level=self.topology.level,
                                          group=group)
                job_obj = Job()
                job_obj.from_dict(job)
                self.queues.pending_queue.add(job_obj)

    def _update_ready_jobs(self, jobs: list[str]):
        for j in jobs:
            self.engine.incoming.remove_object(object_id=j)
            self.engine.outgoing.remove_object(object_id=j)
            self.queues.pending_queue.remove(j)

    def _update_completed_jobs(self, jobs: list[str]):
        self.update_jobs(jobs, self.completed_jobs_set, self.completed_lock)
        for j in jobs:
            self.engine.incoming.remove_object(object_id=j)
            self.engine.outgoing.remove_object(object_id=j)
            self.queues.pending_queue.remove(j)

    def _restart_selection(self):
        jobs = self.queues.pending_queue.gets(states=[ObjectState.PREPARE, ObjectState.PRE_PREPARE,
                                                      ObjectState.COMMIT])
        for job in jobs:
            diff = int(time.time() - job.last_transition_at)
            if diff > self.reselection_timeout_s:
                self.logger.info(f"RESTART: Job: {job} reset to Pending")
                print(f"RESTART: Job: {job} reset to Pending {self.reselection_timeout_s} seconds")
                job.state = ObjectState.PENDING
                self.engine.outgoing.remove_object(object_id=job.job_id)
                self.engine.incoming.remove_object(object_id=job.job_id)
                if job.job_id in self.completed_jobs_set:
                    self.completed_jobs_set.remove(job.job_id)
                job_id = job.job_id
                self.metrics.restarts[job_id] = self.metrics.restarts.get(job_id, 0) + 1

                # TODO restart selection for jobs which were assigned to neighbor which just went down

    def _monitor_delegated_jobs(self):
        """
        Monitor jobs delegated to children and re-add them to parent queue if not selected in time.

        For hierarchical topologies, parent agents delegate jobs to their children.
        This method checks if those jobs are still pending at the child level after
        delegation_timeout_s seconds. If so, they are re-added to the parent's queue
        as they may be infeasible for the children to handle.

        The parent will not propose for these jobs (they remain in a monitored state)
        to avoid consensus conflicts.
        """
        # Only applicable for agents with children
        if not self.topology.children:
            return

        current_time = time.time()
        jobs_to_reassign = []
        jobs_processed = []

        # Check all delegated jobs
        for job_id, delegation_info in list(self.delegated_jobs.items()):
            delegated_at = delegation_info.get('delegated_at', 0)
            child_groups = delegation_info.get('groups', [])

            time_since_delegation = current_time - delegated_at

            # Check if timeout has been exceeded
            if time_since_delegation <= self.delegation_timeout_s:
                continue

            # Check job state in Redis at child level(s)
            job_still_pending = False
            job_found = False

            for child_group in child_groups:
                try:
                    job_data = self.repository.get(
                        obj_id=job_id,
                        key_prefix=Repository.KEY_JOB,
                        level=self.topology.level - 1,
                        group=child_group
                    )

                    if job_data:
                        job_found = True
                        job_state = job_data.get('state', ObjectState.PENDING.value)

                        # If job is still PENDING at child level, it wasn't selected
                        if job_state == ObjectState.PENDING.value:
                            job_still_pending = True
                            self.logger.warning(
                                f"Delegated job {job_id} still PENDING at level {self.topology.level - 1}, "
                                f"group {child_group} after {time_since_delegation:.1f}s "
                                f"(timeout: {self.delegation_timeout_s:.1f}s)"
                            )
                            break
                        else:
                            # Job was processed by children
                            self.logger.debug(
                                f"Delegated job {job_id} processed by children (state: {job_state})"
                            )
                            jobs_processed.append(job_id)

                except Exception as e:
                    self.logger.error(
                        f"Error checking delegated job {job_id} at level {self.topology.level - 1}, "
                        f"group {child_group}: {e}"
                    )

            # Re-add to parent queue if still pending or not found (possibly infeasible for children)
            if not job_found or job_still_pending:
                jobs_to_reassign.append((job_id, delegation_info, time_since_delegation))

        for job_id in jobs_processed:
            self.delegated_jobs.remove(job_id)

        # Process reassignments
        for job_id, delegation_info, time_since_delegation in jobs_to_reassign:
            self._reassign_delegated_job(job_id, delegation_info, time_since_delegation)

    def _reassign_delegated_job(self, job_id: str, delegation_info: dict, time_since_delegation: float):
        """
        Re-add a delegated job back to the parent's pending queue.

        This is called when a job delegated to children hasn't been selected within
        the timeout period, indicating it may be infeasible at the child level.

        :param job_id: Job ID to reassign
        :param delegation_info: Delegation metadata (timestamp, groups)
        :param time_since_delegation: Time elapsed since delegation
        """
        child_groups = delegation_info.get('groups', [])

        try:
            # Try to get the job from child level(s)
            job_obj = None
            found_in_group = None

            for child_group in child_groups:
                job_data = self.repository.get(
                    obj_id=job_id,
                    key_prefix=Repository.KEY_JOB,
                    level=self.topology.level - 1,
                    group=child_group
                )

                if job_data:
                    job_obj = Job()
                    job_obj.level = self.topology.level
                    job_obj.from_dict(job_data)
                    found_in_group = child_group
                    break

            # If not found in Redis, check if we have it locally
            if not job_obj:
                job_obj = self.queues.pending_queue.get(job_id)

            if not job_obj:
                self.logger.warning(
                    f"Cannot reassign delegated job {job_id}: not found in child groups or local queue"
                )
                self.delegated_jobs.remove(job_id)
                return

            # Mark job with a flag to prevent proposing for it
            # This prevents the parent from competing with children
            if not hasattr(job_obj, 'delegation_failed'):
                job_obj.delegation_failed = True
                job_obj.delegation_failed_count = 1
                job_obj.add_delegation_failed_agents(self.agent_id)
            else:
                job_obj.delegation_failed = True
                job_obj.delegation_failed_count = getattr(job_obj, 'delegation_failed_count', 0) + 1
                job_obj.add_delegation_failed_agents(self.agent_id)

            # Reset job state to PENDING for parent level
            job_obj.state = ObjectState.PENDING

            # Add back to parent's pending queue if not already there
            if job_id not in self.queues.pending_queue:
                self.queues.pending_queue.add(job_obj)
                self.logger.info(
                    f"Re-added delegated job {job_id} to parent queue "
                    f"(was stuck at child level for {time_since_delegation:.1f}s)"
                )
            else:
                # Update the existing job in the queue
                existing_job = self.queues.pending_queue.get(job_id)
                if existing_job:
                    existing_job.delegation_failed = job_obj.delegation_failed
                    existing_job.delegation_failed_count = job_obj.delegation_failed_count
                    existing_job.add_delegation_failed_agents(self.agent_id)

            # Remove from child level(s) in Redis to avoid duplicate processing
            for child_group in child_groups:
                try:
                    # Delete from child level
                    self.repository.delete(
                        obj_id=job_id,
                        key_prefix=Repository.KEY_JOB,
                        level=self.topology.level - 1,
                        group=child_group
                    )
                    self.logger.debug(
                        f"Removed job {job_id} from child level {self.topology.level - 1}, group {child_group}"
                    )
                except Exception as e:
                    self.logger.error(
                        f"Error removing job {job_id} from child group {child_group}: {e}"
                    )
            # Add to redis for parent's level to be picked for consensus again
            try:
                self.repository.save(obj=job_obj.to_dict(), key_prefix=Repository.KEY_JOB,
                                     level=self.topology.level, group=self.topology.group)
            except Exception as e:
                self.logger.error(
                    f"Error adding job {job_id} to parent's group {self.topology.group} level: {self.topology.level}: {e}"
                )
                traceback.format_exc()

            # Remove from delegated tracking
            self.delegated_jobs.remove(job_id)

            self.metrics.delegation_reassignments[job_id] = {
                'reassigned_at': time.time(),
                'time_at_child_level': time_since_delegation,
                'child_groups': child_groups,
                'delegation_failed_count': job_obj.delegation_failed_count,
                'reason': 'child_selection_timeout'
            }

            self.logger.warning(
                f"DELEGATION_REASSIGN: Job {job_id} re-added to parent level {self.topology.level} "
                f"after {time_since_delegation:.1f}s at child level (attempt #{job_obj.delegation_failed_count})"
            )

        except Exception as e:
            self.logger.error(f"Error reassigning delegated job {job_id}: {e}")
            self.logger.error(traceback.format_exc())
            # Still remove from tracking to avoid repeated errors
            self.delegated_jobs.remove(job_id)

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
            agent_dicts = self.repository.get_all_objects(
                key_prefix=Repository.KEY_AGENT,
                level=level,
                group=group
            )
            for agent_data in agent_dicts:
                agent = AgentInfo.from_dict(agent_data)

                # Skip agents that have been marked as failed
                if agent.agent_id in self.failed_agents:
                    self.logger.debug(
                        f"Skipping failed agent {agent.agent_id} during neighbor refresh "
                        f"(failed at {self.failed_agents.get(agent.agent_id)})"
                    )
                    continue

                # Skip agents that are shutting down gracefully
                if getattr(agent, 'shutting_down', False):
                    self.logger.debug(
                        f"Skipping agent {agent.agent_id} during neighbor refresh "
                        f"(shutting down gracefully)"
                    )
                    # Remove from map if present
                    if agent.agent_id in target_dict:
                        target_dict.remove(agent.agent_id)
                        self.logger.info(
                            f"Removed shutting down agent {agent.agent_id} from map"
                        )
                    continue

                # Track this agent as active (present in Redis)
                active_ids.add(agent.agent_id)

                existing = target_dict.get(agent.agent_id)

                # For new agents: add them if they're not too stale
                # For existing agents: only update if Redis has newer data
                if existing is None:
                    # New agent - check if it's fresh enough (not too stale)
                    time_since_update = current_time - agent.last_updated
                    if time_since_update <= self.peer_expiry_seconds:
                        target_dict.set(agent.agent_id, agent)
                        self.logger.debug(
                            f"Added new agent {agent.agent_id} to map "
                            f"(last_updated: {time_since_update:.1f}s ago)"
                        )
                    else:
                        self.logger.debug(
                            f"Skipping stale agent {agent.agent_id} "
                            f"(last_updated: {time_since_update:.1f}s ago, threshold: {self.peer_expiry_seconds}s)"
                        )
                elif agent.last_updated > existing.last_updated:
                    # Existing agent - update with newer data from Redis
                    target_dict.set(agent.agent_id, agent)

        # Remove agents from map that are no longer active in Redis
        # (but keep agents that are still being processed/committed)
        for agent_id in list(target_dict.keys()):
            if agent_id != self.agent_id and agent_id not in active_ids:
                target_dict.remove(agent_id)
                self.logger.info(
                    f"Removed agent {agent_id} from map (no longer in Redis)"
                )

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
          - DTNs with averaged connectivity scores across children

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
                host=self.grpc_host,
                port=self.grpc_port,
                agent_id=self.agent_id,
                capacities=self.capacities,
                capacity_allocations=self.capacity_allocations,
                load=self._load,
                proposed_load=proposed_load,
                last_updated=current_time,
                dtns=self.config.get("dtns"),
                group=self.topology.group,
                level=self.topology.level
            )
        # Non-leaf agent: aggregate info from all children
        else:
            total_capacities = Capacities()
            total_allocations = Capacities()
            max_child_capacity = None  # Track max capacity across all children

            # Aggregate DTNs with proper connectivity score averaging
            # Track: {dtn_name: {'scores': [list of scores], 'info': DataNode}}
            dtn_aggregation = {}

            for child in self.children.values():
                total_capacities += child.capacities
                total_allocations += child.capacity_allocations

                # Compute element-wise maximum capacity across children
                if max_child_capacity is None:
                    max_child_capacity = Capacities(**child.capacities.to_dict())
                else:
                    for field in ['cpu', 'core', 'gpu', 'ram', 'disk', 'bw', 'burst_size', 'unit', 'mtu']:
                        current_max = getattr(max_child_capacity, field)
                        child_value = getattr(child.capacities, field)
                        setattr(max_child_capacity, field, max(current_max, child_value))

                if child.dtns:
                    for dtn_name, dtn_obj in child.dtns.items():
                        if dtn_name not in dtn_aggregation:
                            dtn_aggregation[dtn_name] = {
                                'scores': [],
                                'info': dtn_obj  # Keep one instance for metadata
                            }
                        # Collect connectivity score from this child
                        score = getattr(dtn_obj, 'connectivity_score', 1.0)
                        dtn_aggregation[dtn_name]['scores'].append(score)

            # Compute averaged DTN connectivity scores
            dtns = []
            for dtn_name, agg_data in dtn_aggregation.items():
                scores = agg_data['scores']
                avg_score = sum(scores) / len(scores) if scores else 1.0

                # Create new DTN dict with averaged score
                dtn_dict = agg_data['info'].to_dict() if hasattr(agg_data['info'], 'to_dict') else dict(agg_data['info'])
                dtn_dict['connectivity_score'] = round(avg_score, 2)
                dtns.append(dtn_dict)

            self._capacities = total_capacities
            self._load = self.resource_usage_score(total_allocations, total_capacities)
            proposed_load = self.compute_proposed_load()
            agent_info = AgentInfo(
                host=self.grpc_host,
                port=self.grpc_port,
                agent_id=self.agent_id,
                capacities=total_capacities,
                capacity_allocations=total_allocations,
                max_child_capacity=max_child_capacity,  # Set max capacity for hierarchical feasibility
                load=self._load,
                last_updated=current_time,
                dtns=dtns,
                proposed_load=proposed_load,
                group=self.topology.group,
                level=self.topology.level
            )

        return agent_info

    def on_periodic(self):
        self._restart_selection()
        self._monitor_delegated_jobs()
        current_time = int(time.time())
        self._refresh_children(int(current_time))

        # Detect and handle agent failures
        failed_agents = self._detect_failed_agents(current_time)
        if failed_agents:
            self._remove_failed_agents(failed_agents)
            self._check_failure_threshold()

        agent_info = self._generate_agent_info()
        self.repository.save(agent_info.to_dict(), key_prefix=Repository.KEY_AGENT,
                             level=self.topology.level,
                             group=self.topology.group)

        self._refresh_neighbors(current_time=current_time)

        # Batch update job sets
        for prefix, update_fn, state in [
            (Repository.KEY_JOB, self._update_pending_jobs, ObjectState.PENDING.value),
            (Repository.KEY_JOB, self._update_ready_jobs, ObjectState.READY.value),
            (Repository.KEY_JOB, self._update_completed_jobs, ObjectState.COMPLETE.value),
        ]:
            #group = self.topology.level if self.topology.type == TopologyType.Ring else self.topology.group
            group = self.topology.group
            jobs = self.repository.get_all_ids(key_prefix=prefix, level=self.topology.level,
                                               group=group, state=state)
            #self.logger.info(f"Fetching IDs for jobs in state {state} from group: {group} job count: {len(jobs)}")
            update_fn(jobs=jobs)

        if self.debug:
            self.save_consensus_votes()
        self.save_consensus_votes()
        #self.save_neighbors()

        self.check_queue()

    def is_job_feasible(self, job: Job, agent: AgentInfo) -> bool:
        """
        Check if a job is feasible for a given agent based on:
          - Current load threshold
          - Resource availability
          - Connectivity to required DTNs for data_in and data_out

        For hierarchical agents (level > 0), feasibility is checked against
        max_child_capacity to ensure at least one child can execute the job.

        :param job: The job to check.
        :type job: Job
        :param agent: The agent to check feasibility for.
        :type agent: AgentInfo
        :return: True if job is feasible for the agent, False otherwise.
        :rtype: bool
        """
        # Prevent parent agents from re-proposing jobs that failed delegation to children
        # This avoids parent-child competition when a job was delegated but timed out at child level
        if self.topology.children and agent.agent_id in job.delegation_failed_agents:
            return False

        # Cache required DTNs for efficiency (computed once, reused multiple times)
        # Combines data_in and data_out DTN requirements into a single frozenset
        if not hasattr(job, "_required_dtns_cache"):
            rin = {e.name for e in (job.data_in or [])}
            rout = {e.name for e in (job.data_out or [])}
            job._required_dtns_cache = frozenset(rin | rout)

        # Special handling when checking feasibility for self as a parent agent
        # Parent agents must verify that at least ONE actual child can execute the job
        # (not just the synthetic max_child_capacity aggregate used for peer evaluation)
        if agent.agent_id == self.agent_id and self.topology.children:
            # Iterate through all child agents to find at least one capable child
            for child_info in self.children.values():
                # Check if child has sufficient capacity for this job
                if not self._has_sufficient_capacity(job, child_info.capacities):
                    continue  # Skip this child, try next

                # Check child DTN connectivity only if job requires DTNs
                # Jobs without DTN requirements (empty _required_dtns_cache) skip this check
                if job._required_dtns_cache:
                    # Extract child's available DTN names (handle dict vs list formats)
                    if isinstance(child_info.dtns, dict):
                        child_dtn_names = child_info.dtns.keys()
                    else:
                        child_dtn_names = [getattr(x, "name", None) or (x.get("name") if isinstance(x, dict) else None)
                                          for x in (child_info.dtns or [])]

                    # Check if child has ALL required DTNs
                    if not all(d in child_dtn_names for d in job._required_dtns_cache):
                        continue  # Skip this child, try next

                # Found a feasible child: has sufficient capacity AND all required DTNs (if any)
                return True

            # No child can handle this job - infeasible for this parent agent
            self.logger.debug(f"Agent: {self.agent_id} (parent) - Job {job.job_id} not feasible on any child")
            return False

        # ============================================================================
        # PEER EVALUATION (Hierarchical Limitation)
        # ============================================================================
        # When evaluating peer agents at level 1 or higher, we can only use aggregate
        # metrics (max_child_capacity, aggregated DTN connectivity) as estimates.
        #
        # LIMITATION: This is an optimistic estimate that may not reflect actual
        # child capabilities within the peer's subtree. For example:
        #   - max_child_capacity shows the peer has ONE child with sufficient resources
        #   - Aggregated DTNs show the peer has children with required DTNs
        #   - BUT: No single child may have BOTH the capacity AND the DTNs
        #
        # CONSEQUENCE: Cascading delegation failures at hierarchical levels:
        #   1. Peer agent appears to be best candidate (lower cost based on aggregates)
        #   2. Peer wins consensus and is assigned the job
        #   3. Peer delegates to its children
        #   4. None of peer's children can actually handle the job (lack capacity OR DTNs)
        #   5. Delegation times out (delegation_timeout_s) and job is reassigned
        #   6. Job returns to parent level for re-selection
        #   7. Process repeats until a parent with capable children is found
        #
        # This is a known tradeoff in distributed hierarchical scheduling:
        #   - Accurate feasibility requires querying all children (expensive, not scalable)
        #   - Aggregate metrics enable fast distributed decisions (scalable, but optimistic)
        #   - Delegation monitoring and reassignment recovers from incorrect estimates
        # ============================================================================

        # Standard feasibility check for leaf agents or when evaluating peers
        # For parent agents, use max_child_capacity as best estimate (see limitation above)
        # For leaf agents, use their own capacities
        if agent.max_child_capacity is not None:
            available = agent.max_child_capacity
        else:
            available = agent.capacities

        if not self._has_sufficient_capacity(job, available):
            self.logger.debug(f"Agent: {self.agent_id} does not have capacity for Job {job.job_id}")
            return False

        # DTN connectivity check using aggregated DTN info from peer's children
        # NOTE: This checks if the peer has ANY children with required DTNs,
        # not whether a SINGLE child has all required DTNs (see limitation above)
        if isinstance(agent.dtns, dict):
            agent_dtn_names = agent.dtns.keys()
        else:
            agent_dtn_names = [getattr(x, "name", None) or (x.get("name") if isinstance(x, dict) else None)
                               for x in (agent.dtns or [])]

        for d in job._required_dtns_cache:
            if d not in agent_dtn_names:
                self.logger.debug(
                    f"[DTN] Agent {agent.agent_id} missing DTN '{d}' for Job {job.job_id}. "
                    f"Job requires: {job._required_dtns_cache}, Agent has: {set(agent_dtn_names)}"
                )
                return False

        return True

    def compute_overall_load(self):
        allocations = Capacities()
        allocations += self.capacity_allocations

        return self.resource_usage_score(allocated=allocations, total=self.capacities)

    def compute_proposed_load(self):
        allocations = Capacities()
        for j in self.engine.outgoing.objects():
            if j not in self.queues.ready_queue and j not in self.queues.selected_queue:
                job = self.queues.pending_queue.get(j)
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
            job.job_id,
            round(caps.core, 3),
            round(caps.ram, 3),
            round(caps.disk, 3),
            round(getattr(caps, "gpu", 0.0), 3),
            round(job.wall_time or 0.0, 3),
            job.job_type or "",
            job._required_dtns_cache,
            #job.state.value,  # flips when PENDING→READY/COMPLETE, invalidates cache automatically
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
            agent.agent_id, agent.version,
            caps.core, caps.ram, caps.disk, caps.gpu,
            #round(caps.core, 3), round(caps.ram, 3), round(caps.disk, 3), round(getattr(caps, "gpu", 0.0), 3),
            dtn_pairs,
        )

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
        if job.job_type:
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
        if job.wall_time > long_job_threshold:
            time_penalty = 1.5 + (job.wall_time - long_job_threshold) / long_job_threshold
        else:
            time_penalty = 1 + (job.wall_time / long_job_threshold) ** 2

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


    def selection_main(self):
        self.logger.info(f"Starting agent: {self}")
        while self.live_agent_count != self.configured_agent_count:
            time.sleep(0.5)
            self.logger.info(f"[SEL_WAIT] Waiting for Peer map to be populated: "
                             f"{self.live_agent_count}/{self.configured_agent_count}!")

        while not self.shutdown:
            try:
                infeasible_jobs = []
                pending_jobs = self.queues.pending_queue.gets(states=[ObjectState.PENDING],
                                                              count=self.proposal_job_batch_size)
                if not pending_jobs:
                    self.logger.warning(f"No pending jobs available for agent Qsize: {self.queues.pending_queue.size()}")
                    #for job in self.queues.pending_queue.objects.values():
                    #    self.logger.info(f"Job: {job.job_id} state: {job.state}")
                    time.sleep(0.5)
                    continue
                proposals = []
                jobs = []

                # Step 1: Compute cost matrix ONCE for all agents and jobs
                agents_map = self.neighbor_map
                agent_ids = list(agents_map.keys())
                agents = [agents_map.get(aid) for aid in agent_ids if agents_map.get(aid) is not None]

                # TEMP HACK
                if self.topology.level > 0:
                    agents = [agents_map.get(self.agent_id)]

                # Build once
                cost_matrix = self.selector.compute_cost_matrix(
                    assignees=agents,
                    candidates=pending_jobs,
                )

                cost_matrix_with_penalities = apply_multiplicative_penalty(cost_matrix=cost_matrix,
                                                                           assignees=agents,
                                                                           factor_fn=self._projected_load_factor)

                # Step 2: Use existing helper to get the best agent per job
                assignments = self.selector.pick_agent_per_candidate(
                    assignees=agents,
                    candidates=pending_jobs,
                    cost_matrix=cost_matrix_with_penalities,
                    objective="min",
                    threshold_pct=self.selection_threshold_pct,  # e.g., 10 means within +10% of best
                    tie_break_key=lambda ag, s: getattr(ag, "agent_id", "")
                )

                # Step 3: If this agent is assigned, start proposal
                for job, (selected_agent, cost) in zip(pending_jobs, assignments):
                    # Skip infeasible jobs and defer them
                    if selected_agent is None and cost == float('inf'):
                        # Job is completely infeasible - no agent can run it
                        infeasible_jobs.append(job)

                        # Track retry attempts
                        if not hasattr(job, 'infeasible_retry_count'):
                            job.infeasible_retry_count = 0
                            job.infeasible_first_seen = time.time()
                        job.infeasible_retry_count += 1

                        self.logger.warning(
                            f"Job {job.job_id} is INFEASIBLE (no agent can satisfy requirements: "
                            f"cores={job.capacities.core}, ram={job.capacities.ram}, "
                            f"disk={job.capacities.disk}, gpu={getattr(job.capacities, 'gpu', 0)}). "
                            f"Attempt #{job.infeasible_retry_count}. Moving to back of queue."
                        )

                        # Set to BLOCKED state and move to end of queue
                        # Will be restored to PENDING after delay (see _restore_infeasible_jobs)
                        job.state = ObjectState.BLOCKED
                        self.queues.pending_queue.move_to_end(job)
                        continue

                    if selected_agent.agent_id != self.agent_id:
                        self.logger.debug(
                            f"Job {job.job_id} is BETTER suited for agent {selected_agent.agent_id}: "
                            f"Moving to back of queue."
                        )

                        self.queues.pending_queue.move_to_end(job)
                        continue

                    if selected_agent.agent_id == self.agent_id:
                        proposal = ProposalInfo(
                            p_id=generate_id(),
                            object_id=job.job_id,
                            agent_id=self.agent_id,
                            cost=round((cost + self.agent_id), 2)
                        )
                        proposals.append(proposal)
                        job.state = ObjectState.PRE_PREPARE

                if len(proposals):
                    self.logger.debug(f"Identified jobs to propose: {proposals}")
                    if self.debug:
                        self.logger.info(f"Identified jobs to select: {jobs}")
                    self.engine.propose(proposals=proposals)
                    proposals.clear()

                if len(infeasible_jobs):
                    self.logger.info(
                        f"Deferred {len(infeasible_jobs)} infeasible jobs to BLOCKED state: "
                        f"{[j.job_id for j in infeasible_jobs]}. "
                        f"Will retry after 5s delay."
                    )
                    infeasible_jobs.clear()

                # Periodically restore BLOCKED infeasible jobs back to PENDING
                # This allows them to be retried after other jobs have been processed
                self._restore_infeasible_jobs()

                # Periodically check for orphaned jobs (in consensus states but no active proposals)
                # This handles jobs stuck due to agent failures where consensus was cleared
                #self._reset_orphaned_jobs()

                time.sleep(0.5)
            except Exception as e:
                self.logger.error(f"Error occurred while executing e: {e}")
                self.logger.error(traceback.format_exc())
        self.logger.info(f"Agent: {self} stopped with restarts: {self.metrics.restarts}!")

    def save_results(self):
        self.logger.info("Saving Results")
        agent_metrics = {
            "id": self.agent_id,
            "restarts": self.metrics.restarts,
            "conflicts": self.engine.conflicts,
            "idle_time": self.metrics.idle_time,
            "load_trace": self.metrics.load,
            "agent_failures": self.metrics.agent_failures,
            "reassignments": self.metrics.reassignments,
            "delegation_reassignments": self.metrics.delegation_reassignments,
            "quorum_changes": self.metrics.quorum_changes,
            "failed_agents": self.failed_agents.to_dict(),
            "failed_agents_count": len(self.failed_agents),
            "final_quorum": self.calculate_quorum(),
        }
        self.repository.save(obj=agent_metrics, key=f"{Repository.KEY_METRICS}:{self.agent_id}")
        self.logger.info("Results saved")

    def save_neighbors(self):
        try:
            neighbors = []
            for neighbor in self.neighbor_map.values():
                neighbors.append(neighbor.to_dict())
            entry = {
                "agent_id": self.agent_id,
                "neighbors": neighbors,
                "timestamp": time.time(),
            }
            self.repository.save(obj=entry,
                    key=f"peers:{self.agent_id}",
                    level=self.topology.level,
                    group=self.topology.group,
                )

            self.logger.debug(f"Saved neighbors for {self.agent_id}")

        except Exception as e:
            # Never let debugging persist crash the agent loop
            self.logger.error(f"save_neighbors failed: {e}", exc_info=True)

    def save_consensus_votes(self):
        """
        Persist a snapshot of consensus state for each job this agent knows about.

        For every job id seen in either proposals container (incoming/outgoing),
        we record:
          - job state (if we can resolve the Job object)
          - all outgoing proposals (p_id, agent_id, cost, prepares, commits)
          - all incoming proposals (same fields)
          - counts of prepares/commits for quick scanning

        Data is saved under a job-scoped key so you can query by job easily:
          f"{Repository.KEY_JOB}:{job_id}:consensus:{self.agent_id}"
        """
        try:
            import time

            ts = int(time.time())

            # Collect all job ids we have any proposals for
            job_ids = set(self.engine.outgoing.objects()) | set(self.engine.incoming.objects())
            #pending_jobs = len(self.queues.pending_queue.gets(states=[ObjectState.PENDING]))

            for job_id in job_ids:
                # Try to resolve job from in-memory queues first
                job = (self.queues.pending_queue.get(job_id) or
                       #self.queues.ready_queue.get(job_id) or
                       #self.queues.selected_queue.get(job_id) or
                       None)

                # Gather proposals
                out_props = self.engine.outgoing.get_proposals_by_object_id(job_id) or []
                in_props = self.engine.incoming.get_proposals_by_object_id(job_id) or []

                def _ser(p):
                    # commits might be a list (ids) or an int in some paths—normalize
                    commits_list = p.commits if isinstance(p.commits, (list, tuple, set)) else []
                    commits_count = (len(commits_list) if commits_list
                                     else (p.commits if isinstance(p.commits, (int, float)) else 0))
                    prepares_list = list(p.prepares) if isinstance(p.prepares, (list, tuple, set)) else []
                    return {
                        "p_id": getattr(p, "p_id", None),
                        "agent_id": getattr(p, "agent_id", None),
                        "cost": getattr(p, "cost", None),
                        "prepares": prepares_list,
                        "prepares_count": len(prepares_list),
                        "commits": commits_list,
                        "commits_count": commits_count,
                    }

                entry = {
                    "agent_id": self.agent_id,
                    "timestamp": ts,
                    "job_id": job_id,
                    "job_state": (job.state.name if job and hasattr(job.state, "name") else
                                  (job.state if job else None)),
                    "outgoing_proposals": [_ser(p) for p in out_props],
                    "incoming_proposals": [_ser(p) for p in in_props],
                }

                # Save under the job namespace so this naturally “sticks” with the job in your DB
                # Example key: job:<job_id>:consensus:<agent_id>
                self.repository.save(
                    obj=entry,
                    key=f"consensus:{self.agent_id}:{Repository.KEY_JOB}:{job_id}",
                    level=self.topology.level,
                    group=self.topology.group,
                )

            #self.logger.info(f"Saved consensus votes snapshot for {len(job_ids)} job(s) pending: {pending_jobs}/{self.queues.pending_queue.size()}.")
            self.logger.debug(f"Saved consensus votes snapshot for {len(job_ids)} job(s).")

        except Exception as e:
            # Never let debugging persist crash the agent loop
            self.logger.error(f"save_consensus_votes failed: {e}", exc_info=True)

    def should_shutdown(self):
        """
        Returns True if queue has been empty for empty_timeout_seconds.
        """
        if os.path.exists("./shutdown"):
            return True
        return False
        #return (time.time() - self.last_non_empty_time) >= self.empty_timeout_seconds

    def check_queue(self):
        """Call this periodically to monitor the queue."""
        candidate_jobs = [
            job for job in self.queues.pending_queue.gets()
            if job.is_pending and not self.is_job_completed(job_id=job.job_id)
        ]
        if candidate_jobs:
            self.last_non_empty_time = time.time()

    @staticmethod
    def update_jobs(jobs: list[str], job_set: set, lock: threading.RLock):
        with lock:
            job_set.update(jobs)

    def is_job_completed(self, job_id: str) -> bool:
        return job_id in self.completed_jobs_set

    def _get_child_groups_for_job(self, job: Job) -> list[int]:
        """
        Identify which child groups can handle a job based on DTN requirements.

        For jobs without DTN requirements, returns all child groups.
        For jobs with DTN requirements, returns only groups whose children have all required DTNs.

        :param job: The job to check
        :return: List of child group IDs that can handle the job
        """
        if not self.topology.children:
            return []

        # Extract required DTNs from job
        required_dtns = set()
        if hasattr(job, 'data_in') and job.data_in:
            required_dtns.update(e.name for e in job.data_in)
        if hasattr(job, 'data_out') and job.data_out:
            required_dtns.update(e.name for e in job.data_out)

        # If no DTN requirements, all groups can handle it
        if not required_dtns:
            return list(self.topology.children)

        # Build map of DTNs available in each child group
        # group_id -> set of available DTN names
        group_dtns = {}
        for child in self.children.values():
            # Now we can use the group field from AgentInfo
            child_group = child.group
            if child_group is None:
                self.logger.warning(
                    f"Child agent {child.agent_id} has no group information, "
                    f"cannot determine DTN availability per group"
                )
                # Fallback: treat all children as one group
                child_group = 0

            if child_group not in group_dtns:
                group_dtns[child_group] = set()

            if child.dtns:
                group_dtns[child_group].update(child.dtns.keys())

        # Find groups that have all required DTNs
        capable_groups = []
        for group_id in self.topology.children:
            available_dtns = group_dtns.get(group_id, set())
            if required_dtns.issubset(available_dtns):
                capable_groups.append(group_id)
            else:
                missing = required_dtns - available_dtns
                self.logger.debug(
                    f"Child group {group_id} cannot handle job {job.job_id}: "
                    f"missing DTNs {missing}"
                )

        if not capable_groups:
            self.logger.warning(
                f"No child groups can handle job {job.job_id} with DTN requirements {required_dtns}. "
                f"Available group DTNs: {group_dtns}. "
                f"This should not happen - feasibility check may have passed incorrectly."
            )

        return capable_groups

    def scheduling_main(self):
        """
        Main job scheduling loop. If this agent has children, forward the job to them.
        Otherwise, schedule it locally if load permits.
        """
        self.logger.info(f"Starting Job Scheduling Thread: {self}")

        while not self.shutdown:
            try:
                selected_jobs = self.queues.selected_queue.gets()
                if not selected_jobs:
                    time.sleep(0.5)
                    continue

                for job in selected_jobs:
                    job_id = job.job_id

                    # Multi-level: delegate job to capable children only
                    if self.topology.children:
                        # Identify which child groups can handle this job
                        capable_groups = self._get_child_groups_for_job(job)

                        if not capable_groups:
                            # Fallback: try all groups (backward compatibility)
                            self.logger.warning(
                                f"Could not determine capable groups for job {job_id}, "
                                f"delegating to all groups"
                            )
                            capable_groups = self.topology.children

                        self.queues.selected_queue.remove(job_id)
                        job.level = self.topology.level - 1
                        job.state = ObjectState.PENDING
                        job.mark_submitted()
                        job.mark_assigned()

                        for child_group in capable_groups:
                            self.repository.save(
                                obj=job.to_dict(),
                                key_prefix=Repository.KEY_JOB,
                                level=self.topology.level - 1,
                                group=child_group
                            )
                            self.logger.debug(
                                f"Delegated job {job_id} to level {self.topology.level - 1}, "
                                f"group {child_group}")

                        # Track delegated job for monitoring
                        self.delegated_jobs.set(job_id, {
                            'delegated_at': time.time(),
                            'groups': capable_groups
                        })
                        self.logger.debug(
                            f"Tracking delegated job {job_id} for monitoring "
                            f"(timeout: {self.delegation_timeout_s:.1f}s)"
                        )

                    # Leaf agent: schedule job if load allows
                    else:
                        if self.can_schedule_job(job):
                            self.queues.selected_queue.remove(job_id)
                            self.schedule_job(job)

                time.sleep(0.5)

            except Exception as e:
                self.logger.error(f"Error in job_scheduling_main: {e}")
                self.logger.error(traceback.format_exc())

        self.logger.info(f"Stopped Job Scheduling Thread!")

    def schedule_job(self, job: Job):
        self.end_idle()
        self.logger.info(f"[SCHEDULED]: {job.job_id} on agent: {self.agent_id}")
        # Add the job to the list of allocated jobs
        self.queues.ready_queue.add(job)

        self._update_completed_jobs(jobs=[job.job_id])
        job.state = ObjectState.COMPLETE
        self.repository.save(obj=job.to_dict(), level=self.topology.level, group=self.topology.group)
        self.executor.submit(self.execute_job, job)

    def select_job(self, job: Job):
        print(f"[SELECTED]: {job.job_id} on agent: {self.agent_id}")
        self.logger.info(f"[SELECTED]: {job.job_id} on agent: {self.agent_id} to Select Queue")

        self.job_assignments.set(job.job_id, self.agent_id)

        job.state = ObjectState.READY
        self.repository.save(obj=job.to_dict(), key_prefix=Repository.KEY_JOB,
                             level=self.topology.level, group=self.topology.group)
        # Add the job to the list of allocated jobs
        self.queues.selected_queue.add(job)

    def can_schedule_job(self, job: Job) -> bool:
        ready_caps = job_capacities(self.queues.ready_queue.gets())
        available = self.capacities - ready_caps
        return self._has_sufficient_capacity(job, available)

    def execute_job(self, job: Job):
        try:
            job_id = job.job_id
            self.logger.info(f"[EXECUTE] Starting job {job_id} on agent {self.agent_id}")
            job.execute()
            self.queues.ready_queue.remove(job_id)
            self.logger.info(f"[COMPLETE] Job {job_id} completed on agent {self.agent_id}")
            if not self.queues.ready_queue.gets():
                self.start_idle()
        except Exception as e:
            self.logger.error(f"[ERROR] Job {job} failed on agent {self.agent_id}: {e}")
            self.logger.error(traceback.format_exc())


    def on_shutdown(self):
        # Mark as shutting down to prevent other agents from treating this as a failure
        agent_info = self._generate_agent_info()
        agent_info.shutting_down = True
        self.repository.save(agent_info.to_dict(), key_prefix=Repository.KEY_AGENT,
                             level=self.topology.level,
                             group=self.topology.group)
        self.logger.info(f"Agent {self.agent_id} marked as shutting_down in Redis")

        self.executor.shutdown(wait=True)
        self.save_results()

    def _cost_job_on_agent(self, job: Job, agent: AgentInfo) -> float:
        return self.compute_job_cost(job=job, total=agent.capacities, dtns=agent.dtns)

    @staticmethod
    def _projected_load_factor(agent: AgentInfo) -> float:
        """
        Compute multiplicative penalty based on projected load for job scheduling.
        Formula: 1 + ((load + proposed_load)/100)^1.5
        """
        projected_load = agent.load + agent.proposed_load
        load_penalty = 1 + (projected_load / 100) ** 1.5
        return load_penalty

    def _detect_failed_agents(self, current_time: float) -> list[int]:
        """
        Identify agents that have failed based on missing heartbeats.

        Checks Redis update timestamps against failure_threshold_seconds.
        Adds jitter to prevent thundering herd when multiple agents detect
        the same failure simultaneously.

        :param current_time: Current timestamp
        :return: List of agent IDs detected as failed this round
        """
        if self.shutdown:
            return []

        failed_this_round = []

        # Add ±10% jitter to threshold to prevent synchronized failure detection
        jitter = random.uniform(0.9, 1.1) if len(self.neighbor_map) > 5 else 1.0
        threshold = self.failure_threshold_seconds * jitter

        for agent_id, agent_info in list(self.neighbor_map.items()):
            # Skip self
            if agent_id == self.agent_id:
                continue

            # Skip agents that are shutting down gracefully
            if getattr(agent_info, 'shutting_down', False):
                self.logger.info(
                    f"Agent {agent_id} is shutting down gracefully, not treating as failure"
                )
                continue

            # Check if agent hasn't updated in Redis beyond threshold
            time_since_update = current_time - agent_info.last_updated

            if time_since_update > threshold:
                # Check if already marked as failed
                if agent_id not in self.failed_agents:
                    self.logger.warning(
                        f"Agent {agent_id} detected as FAILED "
                        f"(no update for {time_since_update:.1f}s, threshold: {threshold:.1f}s)"
                    )
                    self.failed_agents.set(agent_id, current_time)
                    failed_this_round.append(agent_id)

                    # Increment agent version to invalidate cost cache entries
                    agent_info.version += 1
                    self.logger.info(
                        f"Incremented version for failed agent {agent_id} to {agent_info.version} "
                        f"(invalidates {agent_info.version - 1} cached cost/feasibility entries)"
                    )

                    # Record failure in metrics
                    self.metrics.agent_failures[agent_id] = {
                        'detected_at': current_time,
                        'last_seen': agent_info.last_updated,
                        'downtime': time_since_update,
                        'version_at_failure': agent_info.version
                    }

        return failed_this_round

    def _remove_failed_agents(self, agent_ids: list[int]) -> None:
        """
        Remove failed agents from neighbor map and trigger job reassignment.

        :param agent_ids: List of agent IDs to remove
        """
        for agent_id in agent_ids:
            # Remove from neighbor map
            removed_agent = self.neighbor_map.get(agent_id)
            if removed_agent:
                self.neighbor_map.remove(agent_id)
                self.logger.info(
                    f"Removed failed agent {agent_id} from neighbor map. "
                    f"Live agents: {self.live_agent_count}/{self.configured_agent_count}"
                )

                # Track quorum change
                new_quorum = self.calculate_quorum()
                self.metrics.quorum_changes.append({
                    'timestamp': time.time(),
                    'live_agents': self.live_agent_count,
                    'quorum': new_quorum,
                    'reason': f'removed_agent_{agent_id}'
                })

                # Clear stuck consensus proposals involving this failed agent
                self._clear_consensus_for_failed_agent(agent_id)

                # Trigger job reassignment if enabled
                if self.job_reassignment_enabled:
                    self._reassign_jobs_from_failed_agent(agent_id)
                else:
                    self.logger.info(f"Job reassignment disabled, not reassigning jobs from agent {agent_id}")

    def _clear_consensus_for_failed_agent(self, failed_agent_id: int) -> None:
        """
        Clear consensus state for proposals involving a failed agent.

        When an agent fails mid-consensus, proposals waiting for its prepare/commit
        votes will be stuck indefinitely. This method identifies and clears such
        proposals, allowing jobs to restart selection with remaining agents.

        :param failed_agent_id: ID of the failed agent
        """
        cleared_proposals = []
        affected_job_ids = set()

        # Check incoming proposals waiting for failed agent's votes
        #for job_id, proposals in list(self.engine.incoming.proposals_by_object_id.items()):
        for job_id in self.engine.incoming.objects():
            proposals = self.engine.incoming.get_proposals_by_object_id(job_id)
            for p_id, prop_info in list(proposals.items()):
                # Check if this proposal involves the failed agent
                if failed_agent_id == prop_info.agent_id:
                    # Proposal was FROM the failed agent - remove it
                    proposals.pop(p_id, None)
                    cleared_proposals.append((job_id, p_id, 'incoming', 'from_failed'))
                    affected_job_ids.add(job_id)
                    self.logger.info(
                        f"Cleared incoming proposal {p_id} for job {job_id} "
                        f"(proposal from failed agent {failed_agent_id})"
                    )
                elif failed_agent_id in prop_info.prepares:
                    # Failed agent already voted prepare - proposal stuck waiting for commits
                    # Remove the proposal since quorum calculation changed
                    proposals.pop(p_id, None)
                    cleared_proposals.append((job_id, p_id, 'incoming', 'has_prepare'))
                    affected_job_ids.add(job_id)
                    self.logger.info(
                        f"Cleared incoming proposal {p_id} for job {job_id} "
                        f"(failed agent {failed_agent_id} in prepare list)"
                    )

            # Clean up empty job entries
            if not proposals:
                self.engine.incoming.remove_object(job_id)

        # Check outgoing proposals this agent sent
        #for job_id, proposals in list(self.engine.outgoing.proposals_by_object_id.items()):
        for job_id in self.engine.outgoing.objects():
            proposals = self.engine.outgoing.get_proposals_by_object_id(job_id)
            for p_id, prop_info in list(proposals.items()):
                # Check if waiting for failed agent's prepare/commit
                if failed_agent_id in prop_info.prepares or prop_info.agent_id == failed_agent_id:
                    proposals.pop(p_id, None)
                    cleared_proposals.append((job_id, p_id, 'outgoing', 'waiting_for_failed'))
                    affected_job_ids.add(job_id)
                    self.logger.info(
                        f"Cleared outgoing proposal {p_id} for job {job_id} "
                        f"(waiting for failed agent {failed_agent_id})"
                    )

            if not proposals:
                self.engine.outgoing.remove_object(job_id)

        # Reset affected jobs to PENDING state so they can be reselected
        if affected_job_ids:
            for job_id in affected_job_ids:
                job = self.queues.pending_queue.get(job_id)
                #if job and job.state == ObjectState.PRE_PREPARE:
                if job and job.state in [ObjectState.PRE_PREPARE, ObjectState.PREPARE]:
                    old_state = job.state
                    job.state = ObjectState.PENDING
                    self.queues.pending_queue.update(job)
                    self.logger.info(
                        f"Reset job {job_id} from {old_state} to PENDING for reselection "
                        f"(consensus cleared for failed agent {failed_agent_id})"
                    )

        if cleared_proposals:
            self.logger.warning(
                f"Cleared {len(cleared_proposals)} stuck consensus proposals involving failed agent {failed_agent_id}. "
                f"Reset {len(affected_job_ids)} jobs to PENDING state."
            )
            # Record in metrics
            self.metrics.agent_failures[failed_agent_id]['cleared_proposals'] = len(cleared_proposals)
            self.metrics.agent_failures[failed_agent_id]['reset_jobs'] = len(affected_job_ids)
        else:
            self.logger.debug(f"No stuck consensus proposals found for failed agent {failed_agent_id}")

    def _restore_infeasible_jobs(self, retry_delay: float = 5.0) -> None:
        """
        Restore BLOCKED infeasible jobs back to PENDING after a delay.

        This allows infeasible jobs to be retried periodically while not blocking
        the queue for other feasible jobs. Jobs are restored after retry_delay seconds.

        :param retry_delay: Delay in seconds before retrying infeasible jobs
        """
        current_time = time.time()
        restored_count = 0

        # Check all BLOCKED jobs in the queue
        blocked_jobs = self.queues.pending_queue.gets(
            states=[ObjectState.BLOCKED],
            count=100  # Check up to 100 blocked jobs per iteration
        )

        self.logger.debug(f"[RESTORE] Found {len(blocked_jobs)} BLOCKED jobs to check for restore")

        for job in blocked_jobs:
            # Only restore jobs that were blocked due to infeasibility
            has_timestamp = job.last_transition_at is not None
            time_blocked = current_time - getattr(job, 'last_transition_at', current_time)
            self.logger.debug(
                f"[RESTORE] Checking job {job.job_id}: has_timestamp={has_timestamp}, "
                f"time_blocked={time_blocked:.1f}s, retry_delay={retry_delay}s"
            )

            if has_timestamp:
                if time_blocked >= retry_delay:
                    # Restore to PENDING state and move to end of queue
                    job.state = ObjectState.PENDING
                    self.queues.pending_queue.move_to_end(job)
                    restored_count += 1

                    self.logger.debug(
                        f"Restored infeasible job {job.job_id} to PENDING (at back of queue) "
                        f"(retry #{getattr(job, 'infeasible_retry_count', 0)}, "
                        f"blocked for {time_blocked:.1f}s)"
                    )

        if restored_count > 0:
            self.logger.info(f"Restored {restored_count} infeasible jobs to PENDING for retry")

    def _reset_orphaned_jobs(self) -> None:
        """
        Reset orphaned jobs back to PENDING state.

        Orphaned jobs are those stuck in consensus states (PRE_PREPARE, PREPARE, COMMIT)
        but have no active proposals in engine.incoming or engine.outgoing.
        This can happen when:
        - An agent fails mid-consensus and proposals are cleared
        - Consensus clearing happens but job states aren't reset
        - Network partitions cause proposal loss

        This method periodically scans for such jobs and resets them to PENDING
        so they can go through selection again.
        """
        orphaned_states = [ObjectState.PRE_PREPARE, ObjectState.PREPARE, ObjectState.COMMIT]
        reset_count = 0

        # Get all jobs in consensus states
        for state in orphaned_states:
            jobs_in_state = self.queues.pending_queue.gets(
                states=[state],
                count=100  # Check up to 100 jobs per state per iteration
            )

            for job in jobs_in_state:
                job_id = job.job_id
                if job_id in self.completed_jobs_set:
                    continue

                # Check if job has active proposals
                has_incoming = self.engine.incoming.contains(object_id=job_id) and len(self.engine.incoming.get_proposals_by_object_id(job_id)) > 0
                has_outgoing = self.engine.outgoing.contains(object_id=job_id) and len(self.engine.outgoing.get_proposals_by_object_id(job_id)) > 0

                if not has_incoming and not has_outgoing:
                    # Job is orphaned - no active proposals
                    old_state = job.state
                    job.state = ObjectState.PENDING
                    self.queues.pending_queue.update(job)
                    reset_count += 1

                    self.logger.warning(
                        f"Reset orphaned job {job_id} from {old_state} to PENDING "
                        f"(no active proposals found in consensus engine)"
                    )

        if reset_count > 0:
            self.logger.info(f"Reset {reset_count} orphaned jobs to PENDING state")

    def invalidate_agent_cache(self, agent_id: int) -> bool:
        """
        Manually invalidate cost/feasibility cache for a specific agent.

        This increments the agent's version number, causing all cached entries
        involving this agent to be treated as stale by the SelectionEngine.

        :param agent_id: ID of agent whose cache entries should be invalidated
        :return: True if agent was found and version incremented, False otherwise
        """
        agent_info = self.neighbor_map.get(agent_id)
        if agent_info:
            old_version = agent_info.version
            agent_info.version += 1
            self.logger.info(
                f"Manually invalidated cache for agent {agent_id}: "
                f"version {old_version} -> {agent_info.version}"
            )
            return True
        else:
            self.logger.warning(f"Cannot invalidate cache for agent {agent_id}: not found in neighbor_map")
            return False

    def _reassign_jobs_from_failed_agent(self, failed_agent_id: int) -> None:
        """
        Reassign jobs that were assigned to a failed agent.

        This implements the TODO at resource_agent.py:277.

        Jobs are reset to PENDING state and cleared from consensus containers,
        allowing them to go through the selection process again with remaining agents.

        :param failed_agent_id: ID of the failed agent
        """
        jobs_to_reassign = []

        # Find jobs assigned to the failed agent
        for job_id, assigned_agent in list(self.job_assignments.items()):
            if assigned_agent == failed_agent_id:
                jobs_to_reassign.append(job_id)

        if not jobs_to_reassign:
            self.logger.debug(f"No jobs to reassign from failed agent {failed_agent_id}")
            return

        self.logger.info(
            f"Reassigning {len(jobs_to_reassign)} jobs from failed agent {failed_agent_id}: {jobs_to_reassign}"
        )

        reassigned_count = 0
        for job_id in jobs_to_reassign:
            # Get job from pending queue
            job = self.queues.pending_queue.get(job_id)
            if not job:
                # Job might have completed or moved to another state
                self.logger.debug(f"Job {job_id} not in pending queue (likely completed)")
                self.job_assignments.remove(job_id)
                continue

            # Only reassign if job is not already completed
            if self.is_job_completed(job_id):
                self.logger.debug(f"Job {job_id} already completed, skipping reassignment")
                self.job_assignments.remove(job_id)
                continue

            # Reset job to PENDING for reselection
            old_state = job.state
            job.state = ObjectState.PENDING

            # Clear from consensus containers
            self.engine.outgoing.remove_object(object_id=job_id)
            self.engine.incoming.remove_object(object_id=job_id)
            if job.job_id in self.completed_jobs_set:
                self.completed_jobs_set.remove(job.job_id)

            # Remove from assignment tracking
            self.job_assignments.remove(job_id)

            # Track reassignment in metrics
            self.metrics.reassignments[job_id] = {
                'failed_agent': failed_agent_id,
                'reassigned_at': time.time(),
                'reason': 'agent_failure',
                'old_state': old_state.value if hasattr(old_state, 'value') else str(old_state)
            }

            reassigned_count += 1
            self.logger.info(
                f"Job {job_id} reset to PENDING for reassignment "
                f"(was {old_state} on failed agent {failed_agent_id})"
            )

        self.logger.info(f"Successfully reassigned {reassigned_count}/{len(jobs_to_reassign)} jobs")

    def _check_failure_threshold(self) -> None:
        """
        Check if too many agents have failed and log critical warnings.

        When more than max_failed_agents have failed, the system may not be
        able to maintain quorum or process jobs efficiently.
        """
        failed_count = len(self.failed_agents)
        if failed_count == 0:
            return

        max_allowed = self.max_failed_agents
        if 0 < max_allowed <= failed_count:
            self.logger.error(
                f"CRITICAL: {failed_count} agents failed (max threshold: {max_allowed}). "
                f"System stability at risk! "
                f"Live agents: {self.live_agent_count}/{self.configured_agent_count}"
            )

            # Calculate failure rate
            failure_rate = failed_count / self.configured_agent_count if self.configured_agent_count > 0 else 0
            self.logger.error(f"Current failure rate: {failure_rate:.1%}")

    def on_peer_status(self, target: str, up: bool, reason: str):
        """
        Enhanced peer status handling with immediate failure detection.

        Called by gRPC transport when a communication channel goes up or down.
        In aggressive mode, immediately marks agents as failed and triggers reassignment.

        :param target: Target endpoint (host:port)
        :param up: True if channel is up, False if down
        :param reason: Reason for status change
        """
        if self.shutdown:
            return
        if up:
            self.logger.debug(f"Peer {target} is UP: {reason}")
            # TODO: Could implement agent recovery here if enable_agent_recovery is True
            return

        self.logger.warning(f"Peer {target} is DOWN: {reason}")

        # Only take immediate action in aggressive mode
        if not self.aggressive_failure_detection:
            return

        # Try to identify which agent this endpoint belongs to
        try:
            host, port_s = target.rsplit(":", 1)
            port = int(port_s)
            if host == "127.0.0.1":
                host = "localhost"

            failed_agent_id = None
            # Find agent by endpoint
            for agent_id, agent_info in list(self.neighbor_map.items()):
                if agent_info.host == host and agent_info.port == port:
                    self.logger.warning(
                        f"Identified DOWN peer as agent {agent_id} "
                        f"(aggressive failure detection enabled)"
                    )
                    failed_agent_id = agent_id
                    break

            # Mark as failed immediately
            if failed_agent_id:
                if failed_agent_id not in self.failed_agents:
                    self.failed_agents.set(failed_agent_id, time.time())
                self._remove_failed_agents([failed_agent_id])
        except Exception as e:
            self.logger.error(f"Failed to identify down peer {target}: {e}")

    def calculate_quorum(self) -> int:
        """
        Calculate quorum with safeguards for failure scenarios.

        In normal PBFT: quorum = (N // 2) + 1
        With failures: quorum adjusts based on remaining live agents

        Logs warnings when failure rate exceeds 33%.

        :return: Required quorum size for consensus
        """
        live_count = self.live_agent_count
        configured_count = self.configured_agent_count

        # Calculate quorum based on live agents
        quorum = (live_count // 2) + 1

        # Safety check: if too many agents failed, log warning
        if configured_count > 0:
            failure_rate = 1 - (live_count / configured_count)
            if failure_rate > 0.33:  # More than 33% agents failed
                self.logger.warning(
                    f"HIGH FAILURE RATE: {failure_rate:.1%} "
                    f"({configured_count - live_count}/{configured_count} agents down). "
                    f"Quorum: {quorum}/{live_count}"
                )

        # Minimum quorum of 1 to prevent deadlock when few agents remain
        return max(1, quorum)