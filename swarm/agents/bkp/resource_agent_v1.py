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
import threading
import time
import traceback
from concurrent.futures.thread import ThreadPoolExecutor

from swarm.consensus.engine import ConsensusEngine
from swarm.consensus.interfaces import ConsensusHost, ConsensusTransport, TopologyRouter
from swarm.consensus.messages.message import MessageType
from swarm.models.data_node import DataNode
from swarm.models.object import Object
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
import numpy as np

from swarm.utils.utils import generate_id, job_capacities


class _HostAdapter(ConsensusHost):
    def __init__(self, agent: "SwarmAgent"):
        self.agent = agent
    def get_object(self, object_id: str): return self.agent.queues.pending_queue.get(object_id)
    def is_agreement_achieved(self, object_id: str): return self.agent.is_job_completed(object_id)
    def calculate_quorum(self): return self.agent.calculate_quorum()
    def on_leader_elected(self, obj: Object, proposal_id: str): self.agent.select_job(obj)
    def on_participant_commit(self, obj: Object, leader_id: int, proposal_id: str): obj.state = ObjectState.COMMIT
    def now(self): import time; return time.time()
    def log_debug(self, m): self.agent.logger.debug(m)
    def log_info(self, m): self.agent.logger.info(m)
    def log_warn(self, m): self.agent.logger.warning(m)


class _TransportAdapter(ConsensusTransport):
    def __init__(self, agent: "SwarmAgent"):
        self.agent = agent
    def send(self, dest: int, payload: object) -> None:
        self.agent.send(dest, payload)
    def broadcast(self, payload: object) -> None:
        self.agent.broadcast(payload)


class _RouteAdapter(TopologyRouter):
    def __init__(self, agent: "SwarmAgent"):
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

        # ---- perf caches ----
        self._feas_cache: dict[tuple, bool] = {}
        self._base_cost_cache: dict[tuple, float] = {}

        # optional: cap cache sizes if you have very long runs
        self._feas_cache_max = 100_000
        self._cost_cache_max = 100_000

    @property
    def peer_expiry_seconds(self) -> int:
        return self.runtime_config.get("peer_expiry_seconds", 300)

    @property
    def restart_job_selection(self) -> float:
        return self.runtime_config.get("restart_job_selection", 60.00)

    @property
    def data_transfer(self) -> bool:
        return self.runtime_config.get("data_transfer", False)

    @property
    def projected_queue_threshold(self) -> float:
        return self.runtime_config.get("projected_queue_threshold", 300.00)

    @property
    def ready_queue_threshold(self) -> float:
        return self.runtime_config.get("ready_queue_threshold", 100.00)

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
        return self.runtime_config.get("max_time_load_zero", 3)

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
        residual = available - job.get_capacities()
        negative_fields = residual.negative_fields()
        if len(negative_fields) > 0:
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
                job = self.repository.get(obj_id=job_id, key_prefix=Repository.KEY_JOB, level=self.topology.level,
                                          group=self.topology.group)
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
            if diff > self.restart_job_selection:
                self.logger.info(f"RESTART: Job: {job} reset to Pending")
                job.state = ObjectState.PENDING
                self.engine.outgoing.remove_object(object_id=job.job_id)
                self.engine.incoming.remove_object(object_id=job.job_id)
                job_id = job.job_id
                self.metrics.restarts[job_id] = self.metrics.restarts.get(job_id, 0) + 1

                # TODO restart selection for jobs which were assigned to neighbor which just went down

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

        '''
        current_endpoints: set[tuple[str, int]] = set()
        for agent_info in self.neighbor_map.values():
            key = (agent_info.host, agent_info.port)
            current_endpoints.add(key)

            # If the endpoint is new or the agent behind it changed, update it
            # (setdefault wouldn't update an existing mismatched mapping)
            peer_id, is_up = self.peer_by_endpoint.get(key, (None, None))
            if peer_id is None:
                self.peer_by_endpoint.set(key, (agent_info.agent_id, True))

        
        # 4) Prune endpoints that no longer exist
        to_delete = [k for k in self.peer_by_endpoint.keys() if k not in current_endpoints]
        for k in to_delete:
            self.peer_by_endpoint.pop(k, None)
        '''

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
                existing = target_dict.get(agent.agent_id)
                if existing is None or agent.last_updated > existing.last_updated:
                    target_dict.set(agent.agent_id, agent)
                active_ids.add(agent.agent_id)

    '''
    def _refresh_agent_map(
            self,
            current_time: float,
            target_dict: "ThreadSafeDict[int, AgentInfo]",
            level: int,
            groups: list[int],
    ):
        """
        Refresh target agent map (neighbors/children) from the repository.

        - Only add/refresh peers that are UP (per peer_by_endpoint).
        - Remove peers that are stale or marked DOWN.
        - Do not re-add DOWN peers even if they appear in the repository.
        """
        active_ids: set[int] = set()

        # --- Add or refresh entries from repository (but only if transport is UP) ---
        for group in groups:
            agent_dicts = self.repository.get_all_objects(
                key_prefix=Repository.KEY_AGENT,
                level=level,
                group=group,
            )
            for agent_data in agent_dicts:
                agent = AgentInfo.from_dict(agent_data)

                # Check transport/health by endpoint
                peer_id, is_up = self._get_peer_state_for_endpoint(agent.host, agent.port)

                # If we have a known entry for this endpoint and it's DOWN, skip adding/updating
                if peer_id is not None and is_up is False:
                    # If it already exists in the map, proactively evict it so it doesn't linger
                    if target_dict.get(agent.agent_id) is not None:
                        target_dict.remove(agent.agent_id)
                    # Do NOT mark as active; we want it removable in the cleanup phase
                    continue

                # OK to insert/update (unknown endpoints or known-UP endpoints)
                existing = target_dict.get(agent.agent_id)
                if existing is None or (agent.last_updated or 0) > (existing.last_updated or 0):
                    target_dict.set(agent.agent_id, agent)

                # Count as active only if we actually accepted it
                active_ids.add(agent.agent_id)

        # --- Evict stale or DOWN entries ---
        for agent_id, agent in list(target_dict.items()):
            # Evaluate staleness
            is_stale = (current_time - (agent.last_updated or 0)) >= self.peer_expiry_seconds

            # Evaluate transport down by endpoint (fixes the original bug: look up by endpoint)
            peer_id, is_up = self._get_peer_state_for_endpoint(agent.host, agent.port)
            is_down = (peer_id is not None and is_up is False)

            # Remove if not seen as active this cycle, or stale, or reported DOWN
            if (agent_id not in active_ids) or is_stale or is_down:
                target_dict.remove(agent_id)
                
    '''

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
                host=self.grpc_host,
                port=self.grpc_port,
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
                total_capacities += child._capacities
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
                host=self.grpc_host,
                port=self.grpc_port,
                agent_id=self.agent_id,
                capacities=total_capacities,
                capacity_allocations=total_allocations,
                load=self._load,
                last_updated=current_time,
                dtns=dtns,
                proposed_load=proposed_load
            )

        return agent_info

    def on_periodic(self):
        self._restart_selection()
        current_time = int(time.time())
        self._refresh_children(int(current_time))
        agent_info = self._generate_agent_info()
        self.repository.save(agent_info.to_dict(), key_prefix=Repository.KEY_AGENT, level=self.topology.level,
                             group=self.topology.group)

        self._refresh_neighbors(current_time=current_time)

        # Batch update job sets
        for prefix, update_fn, state in [
            (Repository.KEY_JOB, self._update_pending_jobs, ObjectState.PENDING.value),
            (Repository.KEY_JOB, self._update_ready_jobs, ObjectState.READY.value),
            (Repository.KEY_JOB, self._update_completed_jobs, ObjectState.COMPLETE.value),
        ]:
            jobs = self.repository.get_all_ids(key_prefix=prefix, level=self.topology.level,
                                               group=self.topology.group, state=state)
            update_fn(jobs=jobs)

        if self.debug:
            self.save_consensus_votes()
        self.save_neighbors()

        self.check_queue()

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
        # Check against allocated resources only if no caching used for feasibility
        #available = agent.capacities - agent.capacity_allocations
        available = agent.capacities
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
        for j in self.engine.outgoing.objects():
            if j not in self.queues.ready_queue and j not in self.queues.selected_queue:
                job = self.queues.pending_queue.get(j)
                if job:
                    allocations += job._capacities

        return self.resource_usage_score(allocated=allocations, total=self.capacities)

    def _job_sig(self, job: Job) -> tuple:
        # Required DTNs can be expensive to rebuild; compute once and reuse
        if not hasattr(job, "_required_dtns_cache"):
            rin = {e.name for e in (job.data_in or [])}
            rout = {e.name for e in (job.data_out or [])}
            job._required_dtns_cache = frozenset(rin | rout)
        caps = job._capacities
        return (
            job.job_id,
            round(caps.core, 3), round(caps.ram, 3), round(caps.disk, 3), round(getattr(caps, "gpu", 0.0), 3),
            round(job._wall_time or 0.0, 3),
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
            agent.agent_id,
            caps.core, caps.ram, caps.disk, caps.gpu,
            #round(caps.core, 3), round(caps.ram, 3), round(caps.disk, 3), round(getattr(caps, "gpu", 0.0), 3),
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
        job_gpu = getattr(job._capacities, "gpu", 0)

        # Resource usage ratios
        core_ratio = job._capacities.core / total.core
        ram_ratio = job._capacities.ram / total.ram
        disk_ratio = job._capacities.disk / total.disk
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
        if job._wall_time > long_job_threshold:
            time_penalty = 1.5 + (job._wall_time - long_job_threshold) / long_job_threshold
        else:
            time_penalty = 1 + (job._wall_time / long_job_threshold) ** 2

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
                    base_cost = self.compute_job_cost(job, total=agent._capacities, dtns=agent.dtns)
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

    def selection_main(self):
        self.logger.info(f"Starting agent: {self}")
        while self.live_agent_count != self.configured_agent_count:
            time.sleep(0.5)
            self.logger.info(f"[SEL_WAIT] Waiting for Peer map to be populated: "
                             f"{self.live_agent_count}/{self.configured_agent_count}!")

        while not self.shutdown:
            try:
                pending_jobs = self.queues.pending_queue.gets(states=[ObjectState.PENDING],
                                                              count=self.proposal_job_batch_size)
                if not pending_jobs:
                    self.logger.debug(f"No pending jobs available for agent: {self.agent_id}")
                    time.sleep(0.5)
                    continue
                proposals = []
                jobs = []

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
                        proposal = ProposalInfo(p_id=generate_id(), object_id=job.job_id,
                                                agent_id=self.agent_id, cost=round((cost + self.agent_id), 2))
                        proposals.append(proposal)
                        # Begin election for Job leader for this job
                        job.state = ObjectState.PRE_PREPARE

                    else:
                        if self.debug:
                            job = pending_jobs[job_idx]
                            self.logger.debug(f"Agent {selected_agent_id} has chosen job {job.job_id} with cost: {cost}")

                    if self.debug:
                        jobs.append(job.job_id)

                if len(proposals):
                    self.logger.debug(f"Identified jobs to propose: {proposals}")
                    if self.debug:
                        self.logger.info(f"Identified jobs to select: {jobs}")
                    self.engine.propose(proposals=proposals)
                    proposals.clear()

                # Trigger leader election for a job after random sleep
                #election_timeout = random.uniform(150, 300) / 1000
                #time.sleep(election_timeout)

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
            "load_trace": self.metrics.load
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
                    key=f"consensus::{self.agent_id}:{Repository.KEY_JOB}:{job_id}",
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
        return (time.time() - self.last_non_empty_time) >= self.empty_timeout_seconds

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

    def is_job_in_state(self, job_id: str, job_set: set, redis_key_prefix: str, update_fn, state):
        return job_id in job_set

    # Unified check methods
    def is_job_completed(self, job_id: str) -> bool:
        return self.is_job_in_state(
            job_id,
            self.completed_jobs_set,
            Repository.KEY_JOB,
            self._update_completed_jobs,
            state=ObjectState.COMPLETE.value
        )

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

                    # Multi-level: delegate job to children
                    if self.topology.children:
                        self.queues.selected_queue.remove(job_id)
                        job.state = ObjectState.PENDING

                        for child_group in self.topology.children:
                            self.repository.save(
                                obj=job.to_dict(),
                                key_prefix=Repository.KEY_JOB,
                                level=self.topology.level - 1,
                                group=child_group
                            )
                            self.logger.info(
                                f"Delegated job {job_id} to level {self.topology.level - 1}, group {child_group}")

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
            job.execute(data_transfer=self.data_transfer)
            self.queues.ready_queue.remove(job_id)
            self.logger.info(f"[COMPLETE] Job {job_id} completed on agent {self.agent_id}")
            if not self.queues.ready_queue.gets():
                self.start_idle()
        except Exception as e:
            self.logger.error(f"[ERROR] Job {job} failed on agent {self.agent_id}: {e}")
            self.logger.error(traceback.format_exc())


    def on_shutdown(self):
        self.executor.shutdown(wait=True)
        self.save_results()