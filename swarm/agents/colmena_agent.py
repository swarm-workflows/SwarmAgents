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
import time
import traceback

from swarm.comm.colmena_consensus import colmena_consensus_pb2_grpc, colmena_consensus_pb2
from swarm.comm.colmena_consensus.colmena_servicer import SelectionServicer
from swarm.consensus.engine import ConsensusEngine
from swarm.consensus.interfaces import ConsensusHost, ConsensusTransport, TopologyRouter
from swarm.consensus.messages.message import MessageType
from swarm.models.data_node import DataNode
from swarm.models.object import Object
from swarm.selection.engine import SelectionEngine
from swarm.topology.topology import TopologyType
from swarm.utils.metrics import Metrics
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
from swarm.models.role import Role, ObjectState

import grpc

from swarm.utils.utils import generate_id

import threading


class _HostAdapter(ConsensusHost):
    def __init__(self, agent: "ColmenaAgent"):
        self.agent = agent

    def get_object(self, object_id: str): return self.agent.role if getattr(self.agent.role, "role_id", None) == object_id else None
    def set_pending_proposal(self, proposal: Proposal): self.agent.pending_proposals.setdefault(proposal.object_id, []).append(proposal)
    def is_agreement_achieved(self, object_id: str): return getattr(self.agent.role, "is_complete", None)
    def calculate_quorum(self): return self.agent.calculate_quorum()
    def on_leader_elected(self, obj: Object, proposal_id: str): self.agent.trigger_decision(obj)
    def on_participant_commit(self, obj: Object, leader_id: int, proposal_id: str): self.agent.role = None
    def now(self): import time; return time.time()
    def log_debug(self, m): self.agent.logger.debug(m)
    def log_info(self, m): self.agent.logger.info(m)
    def log_warn(self, m): self.agent.logger.warning(m)


class _TransportAdapter(ConsensusTransport):
    def __init__(self, agent: "ColmenaAgent"):
        self.agent = agent

    def send(self, dest: int, payload: object) -> None:
        self.agent.send(dest, payload)

    def broadcast(self, payload: object) -> None:
        self.agent.broadcast(payload)


class _RouteAdapter(TopologyRouter):
    def __init__(self, agent: "ColmenaAgent"):
        self.agent = agent

    def should_forward(self) -> bool:
        if self.agent.topology.type in [TopologyType.Star, TopologyType.Ring]:
            return True
        return False


class ColmenaAgent(Agent):
    def __init__(self, agent_id: int, config_file: str, debug: bool = False):
        super().__init__(agent_id, config_file, debug)
        self.engine = ConsensusEngine(agent_id, _HostAdapter(self), _TransportAdapter(self),
                                      router=_RouteAdapter(self))

        self._capacities = Capacities().from_dict(self.config.get("capacities", {}))
        self._load = 0
        self.metrics = Metrics()
        self._role = None
        self._role_lock = threading.RLock()
        self.pending_proposals = {}

        self.selector = SelectionEngine(
            feasible=lambda role, agent: self.is_role_feasible(role, agent),
            cost=self._cost_role_on_agent,
            candidate_key=self._role_sig,
            assignee_key=self._agent_sig,
            candidate_version=lambda role: int(getattr(role, "version", 0) or 0),
            assignee_version=lambda ag: int(getattr(ag, "version", 0) or getattr(ag, "updated_at", 0) or 0),
            cache_enabled=True,
            feas_cache_size=131072,
            cost_cache_size=131072,
            cache_ttl_s=60.0,  # optional, if you want time-based safety
        )

    @property
    def role(self):
        """ Thread-safe role getter"""
        with self._role_lock:
            return self._role

    @role.setter
    def role(self, new_role):
        """ Thread-safe role setter """
        with self._role_lock:
            self._role = new_role

    @property
    def peer_expiry_seconds(self) -> int:
        return self.runtime_config.get("peer_expiry_seconds", 300)

    @property
    def capacities(self) -> Capacities:
        import psutil
        cpu_percent = 100 - psutil.cpu_percent(interval=1)
        ram_percent = 100 - psutil.virtual_memory().percent
        disk_percent = 100 - psutil.disk_usage('/').percent
        capacities = Capacities.from_dict({'core': cpu_percent, 'ram': ram_percent, 'disk': disk_percent})
        return capacities

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

    @staticmethod
    def _has_sufficient_capacity(role: Role, available: Capacities) -> bool:
        """
        Returns True if the role can be accommodated by the available capacity.
        Also checks data transfer feasibility if enabled.
        """
        # Check if role fits into available capacities
        residual = available - role.get_capacities()
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

    @property
    def restart_job_selection(self) -> float:
        return self.runtime_config.get("restart_job_selection", 60.00)

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
                    self.logger.info("Received prepare")
                    self.__on_prepare(incoming=incoming)

                elif isinstance(incoming, Commit):
                    self.logger.info("Received commit")
                    self.__on_commit(incoming=incoming)

                elif isinstance(incoming, Proposal):
                    self.logger.info("Received proposal")
                    self.__on_proposal(incoming=incoming)

                else:
                    self.logger.info(f"Ignoring unsupported message: {message}")
                diff = int(time.time() - begin)
                if diff > 0:
                    self.logger.info(f"Event {message_type} TIME: {diff}")
            except Exception as e:
                self.logger.error(f"Error while processing message {type(message)}, {e}")
                self.logger.error(traceback.format_exc())

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
                existing = target_dict.get(agent.agent_id)
                if existing is None or agent.last_updated > existing.last_updated:
                    target_dict.set(agent.agent_id, agent)
                active_ids.add(agent.agent_id)

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
            agent_info = AgentInfo(
                host=self.grpc_host,
                port=self.grpc_port,
                agent_id=self.agent_id,
                capacities=self.capacities,
                load=self._load,
                last_updated=current_time,
                dtns=self.config.get("dtns")
            )
        # Non-leaf agent: aggregate info from all children
        else:
            total_capacities = self.capacities
            total_allocations = self.capacities

            for child in self.children.values():
                total_capacities += child.capacities

            self._load = self.resource_usage_score(total_allocations, total_capacities)
            agent_info = AgentInfo(
                host=self.grpc_host,
                port=self.grpc_port,
                agent_id=self.agent_id,
                capacities=total_capacities,
                load=self._load,
                last_updated=current_time
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

    def _restart_selection(self):
        if self.role is not None:
            diff = int(time.time() - self.role.time_last_state_change)
            if diff > self.restart_job_selection:
                self.logger.info(f"RESTART: Role: {self.role} reset to Pending")
                self.role.state = ObjectState.PENDING
                self.engine.outgoing.remove_object(object_id=self.role.role_id)
                self.engine.incoming.remove_object(object_id=self.role.role_id)
                role_id = self.role.role_id
                self.metrics.restarts[role_id] = self.metrics.restarts.get(role_id, 0) + 1
                self.start_consensus(self.role)

    def is_role_feasible(self, role: Role, agent: AgentInfo) -> bool:
        """
        Check if a role is feasible for a given agent based on:
          - Current load threshold
          - Resource availability
          - Connectivity to required DTNs for data_in and data_out

        :param role: The role to check.
        :type role: Role
        :param agent: The agent to check feasibility for.
        :type agent: AgentInfo
        :return: True if role is feasible for the agent, False otherwise.
        :rtype: bool
        """
        # Capacity check first (cheapest)
        # Check against allocated resources only if no caching used for feasibility
        # available = agent.capacities - agent.capacity_allocations
        return self._has_sufficient_capacity(role, agent.capacities)

    def compute_overall_load(self):
        return self.resource_usage_score(allocated=Capacities(), total=self.capacities)

    def _role_sig(self, role: Role) -> tuple:
        # Required DTNs can be expensive to rebuild; compute once and reuse
        caps = role.capacities
        return (
            role.role_id,
            round(caps.core, 3), round(caps.ram, 3), round(caps.disk, 3), round(getattr(caps, "gpu", 0.0), 3)
        )

    def _agent_sig(self, agent: AgentInfo) -> tuple:
        caps = agent.capacities or Capacities()

        return (
            agent.agent_id,
            caps.core, caps.ram, caps.disk, caps.gpu
        )

    def compute_role_cost(
            self,  # now uses self so it can read config defaults
            role: Role,
            total: Capacities,
            dtns: dict[str, DataNode]
    ) -> float:
        """
        Compute the cost of executing a role on an agent based on weighted resource usage,
        bottleneck effects, execution time penalties, and DTN connectivity.

        :param role: The role whose cost is to be computed.
        :type role: Role
        :param total: Total available resources.
        :type total: Capacities
        :param dtns: DTN info for the agent
        :type dtns: dict[str, DataNode]
        :return: Calculated role cost (higher is more expensive).
        :rtype: float
        """

        if not total or total.core <= 0 or total.ram <= 0 or total.disk <= 0:
            return float('inf')

        # Prevent division by zero for GPUs
        total_gpu = getattr(total, "gpu", 0) or 1
        role_gpu = getattr(role.capacities, "gpu", 0)

        # Resource usage ratios
        core_ratio = role.capacities.core / total.core
        ram_ratio = role.capacities.ram / total.ram
        disk_ratio = role.capacities.disk / total.disk
        gpu_ratio = role_gpu / total_gpu

        # Weighted base score
        base_score = (
                core_ratio +
                ram_ratio +
                disk_ratio +
                gpu_ratio
        )

        # Bottleneck penalty
        bottleneck_penalty = max(core_ratio, ram_ratio, disk_ratio, gpu_ratio) ** 2

        # Final cost
        cost = (base_score + bottleneck_penalty) * 100
        return round(cost, 2)

    def start(self):
        self.logger.info(f"Starting colmena agent")
        colmena_servicer = SelectionServicer(trigger_consensus_fn=self.trigger_consensus)
        self.transport.server.add_service(
            colmena_consensus_pb2_grpc.add_SelectionServiceServicer_to_server,
            colmena_servicer
        )
        host = "172.17.0.1"
        port = 50055  # Hardcoded...
        channel = grpc.insecure_channel(f"{host}:{port}")
        self.transport._colmena_client = colmena_consensus_pb2_grpc.SchedulingServiceStub(channel)

        super().start()

    def trigger_consensus(self, role: Role):
        # Step 1: Compute cost matrix ONCE for all agents and roles (for now one role at a time)
        if self.role is not None:
            self.logger.info("Consensus in process, try again later.")
            return

        self.role = role
        self.role.role_id = f"{role.role_id}"
        self.start_consensus(role)

    def start_consensus(self, role: Role):
        if self.debug:
            self.logger.info("Start consensus called!")
            self.logger.info(f"Role capacities: {role.capacities}")

        agents_map = self.neighbor_map
        agent_ids = list(agents_map.keys())
        agents = [agents_map.get(aid) for aid in agent_ids if agents_map.get(aid) is not None]


        for agent_id, agent in agents_map.items():
            if agent is not None:
                self.logger.info(f"Agent: {agent_id}, capacities: {agent.capacities}")

        # Build once
        cost_matrix = self.selector.compute_cost_matrix(
            assignees=agents,
            candidates=[role],
        )
        self.logger.info(f"cost matrix: {cost_matrix}")

        # Step 2: Use existing helper to get the best agent per role
        assignments = self.selector.pick_agent_per_candidate(
            assignees=agents,
            candidates=[role],
            cost_matrix=cost_matrix,
            objective="min",
            threshold_pct=10.0,  # e.g., 10 means within +10% of best, TODO: Take from config file
            tie_break_key=lambda ag, s: getattr(ag, "agent_id", "")
        )

        # Step 3: If this agent is assigned, start proposal
        proposals = []
        self.logger.info(f"Assignments: {assignments}")
        for selected_agent, cost in assignments:
            self.logger.info(f"Selected agent: {selected_agent}, cost: {cost}")
            if selected_agent and selected_agent.agent_id == self.agent_id:
                self.logger.info(f"Selected agent is this - sending proposal")
                proposal = ProposalInfo(
                    p_id=generate_id(),
                    object_id=role.role_id,
                    agent_id=self.agent_id,
                    seed=round((cost + self.agent_id), 2)
                )
                proposals.append(proposal)
                role.state = ObjectState.PRE_PREPARE

        if len(proposals):
            self.logger.debug(f"Identified roles to propose: {proposals}")
            if self.debug:
                self.logger.info(f"Identified role to select: {role}")
            self.engine.propose(proposals=proposals)
            proposals.clear()

        role_id = role.role_id
        # Check if there are any pending proposals for this role_id
        pending_list = self.pending_proposals.get(role_id, [])

        if pending_list:
            # Pop the list to remove it from the dict and iterate over each proposal
            for proposal in self.pending_proposals.pop(role_id, []):
                self.engine.on_proposal(proposal)

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

    def trigger_decision(self, role: Role):
        print(f"[SELECTED]: {role.role_id} on agent: {self.agent_id}")

        role.leader_id = self.agent_id
        self.role.state = ObjectState.READY
        # Build TriggerRoleRequest
        req = colmena_consensus_pb2.TriggerRoleRequest(
            roleId=role.role_id,  # or some role mapping
            serviceId=role.service_id,  # if role has service_id
            startOrStop=role.startOrStop,  # start the role
        )

        try:
            # Make the gRPC call
            self.transport._colmena_client.TriggerRole(req)
            self.logger.info(f"Triggered role execution via gRPC for role {role.role_id}")
        except grpc.RpcError as e:
            self.logger.error(f"Failed to trigger role: {e}")

        self.repository.save(obj=role.to_dict(), key_prefix=Repository.KEY_ROLE,
                             level=self.topology.level, group=self.topology.group)
        self.role = None

    def should_shutdown(self):
        """
        Returns True if queue has been empty for empty_timeout_seconds.
        """
        if os.path.exists("./shutdown"):
            return True
        #return (time.time() - self.last_non_empty_time) >= self.empty_timeout_seconds
        return False

    def on_shutdown(self):
        self.save_results()

    def _cost_role_on_agent(self, role: Role, agent: AgentInfo) -> float:
        return self.compute_role_cost(role=role, total=agent.capacities, dtns=agent.dtns)
