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
import logging
import time
import traceback
from typing import Any, List, Optional, Dict

from swarm.models.object import Object, ObjectState
from swarm.models.capacities import Capacities
from swarm.models.data_node import DataNode


class Job(Object):
    """
    A schedulable/dispatchable unit of work with lifecycle timestamps aligned with common WMS terms:
      - submitted_at: when the job was created/submitted to the system
      - planning_started_at: when scheduling/planning (a.k.a. 'selection') began
      - assigned_at: when an agent was chosen for this job
      - started_at: when execution actually began on the worker
      - completed_at: when execution finished (success or failure)

    Backward-compatibility:
      from_dict() accepts old keys: created_at, selection_started_at, selected_by_agent_at, scheduled_at.
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        super().__init__()
        # Resource intents and allocations
        self._capacities: Optional[Capacities] = None
        self._capacity_allocations: Optional[Capacities] = None

        # Classification + status
        self._job_type: Optional[str] = None
        self._exit_status: int = 0

        # Data deps
        self.data_in: List[DataNode] = []
        self.data_out: List[DataNode] = []
        self.transfer_in_time: Optional[float] = None
        self.transfer_out_time: Optional[float] = None
        self.level = 0

        # Timing (epoch seconds) - dicts to support multi-level latency tracking
        # Key: level (int), Value: timestamp (float)
        self._submitted_at: Dict[int, float] = {}
        self._selection_started_at: Dict[int, float] = {}
        self._assigned_at: Dict[int, float] = {}
        self._started_at: Dict[int, float] = {}
        self._completed_at: Dict[int, float] = {}
        self._wall_time: Optional[float] = None
        self._reasoning_time: Optional[float] = None
        self._delegation_failed = False
        self._delegation_failed_count: int = 0
        self._delegation_failed_agents: List[int] = []

        # Meta
        self.logger = logger if logger else logging.getLogger(self.__class__.__name__)

    @property
    def delegation_failed(self) -> bool:
        return self._delegation_failed

    @delegation_failed.setter
    def delegation_failed(self, value: bool):
        self._delegation_failed = value

    @property
    def delegation_failed_count(self) -> int:
        return self._delegation_failed_count

    @delegation_failed_count.setter
    def delegation_failed_count(self, value: int):
        self._delegation_failed_count = value

    @property
    def delegation_failed_agents(self) -> List[int]:
        return self._delegation_failed_agents

    @delegation_failed_agents.setter
    def delegation_failed_agents(self, value: List[int]):
        self._delegation_failed_agents = value

    def add_delegation_failed_agents(self, value: int):
        self._delegation_failed_agents.append(value)

    # ---------- Convenience/derived ----------
    @property
    def exit_status(self) -> int:
        return self._exit_status

    @exit_status.setter
    def exit_status(self, value: int) -> None:
        self._exit_status = value

    @property
    def job_type(self) -> str:
        return self._job_type

    @job_type.setter
    def job_type(self, value: str) -> None:
        self._job_type = value

    @property
    def age(self) -> float:
        """Seconds since submission."""
        first_submit = self._submitted_at.get(0, time.time()) if self._submitted_at else time.time()
        return max(0.0, time.time() - first_submit)

    def duration_run(self) -> Optional[float]:
        """Actual runtime (started -> completed). Uses last timestamps."""
        with self.lock:
            if not self._started_at or not self._completed_at:
                return None
            last_started = max(self._started_at.values())
            last_completed = max(self._completed_at.values())
            return max(0.0, last_completed - last_started)

    def duration_wait(self) -> Optional[float]:
        """Queueing delay (submitted -> started). Uses first submission and last start."""
        with self.lock:
            if not self._started_at:
                return None
            first_submit = self._submitted_at.get(0, time.time())
            last_started = max(self._started_at.values())
            return max(0.0, last_started - first_submit)

    def duration_consensus(self) -> Optional[float]:
        """Planning latency (selection_started -> assigned). Uses last timestamps."""
        with self.lock:
            if not self._selection_started_at or not self._assigned_at:
                return None
            last_selection = max(self._selection_started_at.values())
            last_assigned = max(self._assigned_at.values())
            return max(0.0, last_assigned - last_selection)

    # ---------- IDs ----------
    @property
    def job_id(self) -> str:
        with self.lock:
            return self.object_id

    @job_id.setter
    def job_id(self, value: str):
        with self.lock:
            self.object_id = value

    # ---------- Capacities ----------
    @property
    def capacities(self) -> Optional[Capacities]:
        with self.lock:
            return self._capacities

    @capacities.setter
    def capacities(self, cap: Optional[Capacities]) -> None:
        with self.lock:
            assert cap is None or isinstance(cap, Capacities)
            self._capacities = cap

    @property
    def capacity_allocations(self) -> Optional[Capacities]:
        with self.lock:
            return self._capacity_allocations

    @capacity_allocations.setter
    def capacity_allocations(self, cap: Optional[Capacities]) -> None:
        with self.lock:
            assert cap is None or isinstance(cap, Capacities)
            self._capacity_allocations = cap

    # ---------- Times ----------
    @property
    def wall_time(self) -> Optional[float]:
        with self.lock:
            return self._wall_time

    @wall_time.setter
    def wall_time(self, wall_time: Optional[float]):
        with self.lock:
            self._wall_time = wall_time if wall_time is None else float(wall_time)

    @property
    def submitted_at(self) -> float:
        """Return the first submission time (backward compatible)."""
        with self.lock:
            return self._submitted_at.get(0, time.time()) if self._submitted_at else time.time()

    @property
    def submitted_at_dict(self) -> Dict[int, float]:
        """Return all submission timestamps for multi-level tracking."""
        with self.lock:
            return dict(self._submitted_at)

    @submitted_at.setter
    def submitted_at(self, ts: float):
        """Set submission time (replaces dict with single value at level 0)."""
        with self.lock:
            self._submitted_at = {0: float(ts)}

    def mark_submitted(self, ts: Optional[float] = None) -> None:
        """Add a submission timestamp at the specified level (for multi-level tracking)."""
        with self.lock:
            self._submitted_at[self.level] = float(ts or time.time())

    @property
    def selection_started_at(self) -> Optional[float]:
        """Return the last selection start time (backward compatible)."""
        with self.lock:
            return max(self._selection_started_at.values()) if self._selection_started_at else None

    @property
    def selection_started_at_dict(self) -> Dict[int, float]:
        """Return all selection start timestamps for multi-level tracking."""
        with self.lock:
            return dict(self._selection_started_at)

    def mark_selection_started(self, ts: Optional[float] = None) -> None:
        """Add a selection start timestamp at the specified level (for multi-level tracking)."""
        with self.lock:
            self._selection_started_at[self.level] = float(ts or time.time())

    @property
    def assigned_at(self) -> Optional[float]:
        """Return the last assignment time (backward compatible)."""
        with self.lock:
            return max(self._assigned_at.values()) if self._assigned_at else None

    @property
    def assigned_at_dict(self) -> Dict[int, float]:
        """Return all assignment timestamps for multi-level tracking."""
        with self.lock:
            return dict(self._assigned_at)

    def mark_assigned(self, ts: Optional[float] = None) -> None:
        """Add an assignment timestamp at the specified level (for multi-level tracking)."""
        with self.lock:
            self._assigned_at[self.level] = float(ts or time.time())

    @property
    def started_at(self) -> Optional[float]:
        """Return the last start time (backward compatible)."""
        with self.lock:
            return max(self._started_at.values()) if self._started_at else None

    @property
    def started_at_dict(self) -> Dict[int, float]:
        """Return all start timestamps for multi-level tracking."""
        with self.lock:
            return dict(self._started_at)

    def mark_started(self, ts: Optional[float] = None) -> None:
        """Add a start timestamp at the specified level (for multi-level tracking)."""
        with self.lock:
            self._started_at[self.level] = float(ts or time.time())

    @property
    def completed_at(self) -> Optional[float]:
        """Return the last completion time (backward compatible)."""
        with self.lock:
            return max(self._completed_at.values()) if self._completed_at else None

    @property
    def completed_at_dict(self) -> Dict[int, float]:
        """Return all completion timestamps for multi-level tracking."""
        with self.lock:
            return dict(self._completed_at)

    def mark_completed(self, ts: Optional[float] = None) -> None:
        """Add a completion timestamp at the specified level (for multi-level tracking)."""
        with self.lock:
            self._completed_at[self.level] = float(ts or time.time())

    @property
    def reasoning_time(self) -> Optional[float]:
        with self.lock:
            return self._reasoning_time

    @reasoning_time.setter
    def reasoning_time(self, val: Optional[float]):
        with self.lock:
            self._reasoning_time = None if val is None else float(val)

    # ---------- Leader ----------
    @property
    def leader_id(self) -> Optional[int]:
        with self.lock:
            return self._leader_id

    @leader_id.setter
    def leader_id(self, lid: Optional[int]):
        with self.lock:
            self._leader_id = None if lid is None else int(lid)

    # ---------- Data deps ----------
    def add_incoming_data_dep(self, data_node: DataNode):
        with self.lock:
            self.data_in.append(data_node)

    def add_outgoing_data_dep(self, data_node: DataNode):
        with self.lock:
            self.data_out.append(data_node)

    def get_data_in(self) -> List[DataNode]:
        with self.lock:
            return list(self.data_in)

    def get_data_out(self) -> List[DataNode]:
        with self.lock:
            return list(self.data_out)

    # ---------- State ----------
    def on_state_changed(self, old_state: ObjectState, new_state: ObjectState):
        self.logger.debug("Transitioning job %s from %s to %s", self.job_id, old_state, new_state)
        if old_state == new_state:
            return
        #if new_state in (ObjectState.PRE_PREPARE, ObjectState.PREPARE):
        if old_state == ObjectState.PENDING and new_state == ObjectState.PRE_PREPARE:
            self.mark_selection_started()
        elif old_state == ObjectState.PENDING and new_state == ObjectState.PREPARE:
            self.mark_selection_started()
        elif new_state is ObjectState.READY:
            self.mark_assigned()
        elif new_state is ObjectState.RUNNING:
            self.mark_started()
        elif new_state is ObjectState.COMPLETE:
            self.mark_completed()

    # ---------- Exec simulation ----------
    def execute(self):
        try:
            self.logger.info("Starting execution for job: %s", self.job_id)
            self.state = ObjectState.RUNNING
            self.mark_started()

            # TODO: staged-in transfers using self.data_in if data_transfer

            # Simulate execution
            wt = self.wall_time or 0.0
            self.logger.info("Sleeping for %s seconds to simulate job execution", wt)
            if wt > 0:
                time.sleep(wt)

            # TODO: staged-out transfers using self.data_out if data_transfer

            self.state = ObjectState.COMPLETE
            self.mark_completed()
            self.logger.info("Completed execution for job: %s", self.job_id)
        except Exception as e:
            self.logger.error("Error executing job %s: %s", self.job_id, e)
            self.logger.error(traceback.format_exc())

    # ---------- Introspection helpers ----------
    def __repr__(self):
        fields: Dict[str, Any] = {
            "job_id": self.job_id or "NONE",
            "state": getattr(self.state, "name", str(self.state)),
            "submitted_at": self.submitted_at,
            "selection_started_at": self.selection_started_at,
            "assigned_at": self.assigned_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "wall_time": self.wall_time,
            "exit_status": self.exit_status,
            "leader_id": self.leader_id,
            "job_type": self.job_type,
        }
        return f"Job({fields})"

    __str__ = __repr__

    # ---------- (De)serialization ----------
    def to_dict(self) -> dict:
        with self.lock:
            return {
                "id": self.job_id,
                "capacities": self.capacities.to_dict() if self.capacities else None,
                "capacity_allocations": (
                    self.capacity_allocations.to_dict() if self.capacity_allocations else None
                ),
                "wall_time": self.wall_time,
                "data_in": [dn.to_dict() for dn in self.data_in],
                "data_out": [dn.to_dict() for dn in self.data_out],
                "state": self.state.value,
                "exit_status": self.exit_status,
                "transfer_in_time": self.transfer_in_time,
                "transfer_out_time": self.transfer_out_time,
                # canonical keys (dicts for multi-level latency tracking):
                "submitted_at": self.submitted_at_dict,
                "selection_started_at": self.selection_started_at_dict,
                "assigned_at": self.assigned_at_dict,
                "started_at": self.started_at_dict,
                "completed_at": self.completed_at_dict,
                "leader_id": self.leader_id,
                "last_transition_at": self.last_transition_at,
                "job_type": self.job_type,
                "reasoning_time": self.reasoning_time,
                "delegation_failed_agents": self.delegation_failed_agents,
                "delegation_failed": self.delegation_failed,
                "delegation_failed_count": self.delegation_failed_count,
                "level": self.level,
            }

    def from_dict(self, job_data: dict):
        with self.lock:
            self.job_id = job_data["id"]

            self.capacities = (
                Capacities.from_dict(job_data["capacities"]) if job_data.get("capacities") else None
            )
            self.capacity_allocations = (
                Capacities.from_dict(job_data["capacity_allocations"])
                if job_data.get("capacity_allocations")
                else None
            )
            self.wall_time = job_data.get("wall_time")

            self.data_in = [DataNode.from_dict(d) for d in job_data.get("data_in", [])]
            self.data_out = [DataNode.from_dict(d) for d in job_data.get("data_out", [])]

            self.state = ObjectState(job_data["state"]) if job_data.get("state") else ObjectState.PENDING
            self.exit_status = int(job_data.get("exit_status", 0))
            self.transfer_in_time = job_data.get("transfer_in_time")
            self.transfer_out_time = job_data.get("transfer_out_time")

            self.level = job_data.get("level")

            # Back-compat + canonical mapping (supports single values, lists, and dicts)
            submitted = job_data.get("submitted_at", time.time())
            if isinstance(submitted, dict):
                # Convert string keys to int if needed
                self._submitted_at = {int(k): float(v) for k, v in submitted.items()}
            elif isinstance(submitted, list):
                # Convert list to dict with index as level
                self._submitted_at = {i: float(v) for i, v in enumerate(submitted)}
            else:
                self._submitted_at = {self.level: float(submitted)}

            selection_started = job_data.get("selection_started_at")
            if isinstance(selection_started, dict):
                self._selection_started_at = {int(k): float(v) for k, v in selection_started.items()}
            elif isinstance(selection_started, list):
                self._selection_started_at = {i: float(v) for i, v in enumerate(selection_started)}
            elif selection_started is not None:
                self._selection_started_at = {self.level: float(selection_started)}
            else:
                self._selection_started_at = {}

            assigned = job_data.get("assigned_at")
            if isinstance(assigned, dict):
                self._assigned_at = {int(k): float(v) for k, v in assigned.items()}
            elif isinstance(assigned, list):
                self._assigned_at = {i: float(v) for i, v in enumerate(assigned)}
            elif assigned is not None:
                self._assigned_at = {self.level: float(assigned)}
            else:
                self._assigned_at = {}

            started = job_data.get("started_at")
            if isinstance(started, dict):
                self._started_at = {int(k): float(v) for k, v in started.items()}
            elif isinstance(started, list):
                self._started_at = {i: float(v) for i, v in enumerate(started)}
            elif started is not None:
                self._started_at = {self.level: float(started)}
            else:
                self._started_at = {}

            completed = job_data.get("completed_at")
            if isinstance(completed, dict):
                self._completed_at = {int(k): float(v) for k, v in completed.items()}
            elif isinstance(completed, list):
                self._completed_at = {i: float(v) for i, v in enumerate(completed)}
            elif completed is not None:
                self._completed_at = {self.level: float(completed)}
            else:
                self._completed_at = {}

            self.leader_id = job_data.get("leader_id")
            self.last_transition_at = job_data.get("last_transition_at")

            self.reasoning_time = job_data.get("reasoning_time")
            self.classify_job_type()

            self.delegation_failed_agents = job_data.get("delegation_failed_agents")
            self.delegation_failed = job_data.get("delegation_failed")
            self.delegation_failed_count = job_data.get("delegation_failed_count")

    # ---------- Classification ----------
    def classify_job_type(self) -> Optional[str]:
        """
        Classify job_type by dominant resource, nominal wall-time bucket,
        and data I/O intensity.
        """
        with self.lock:
            cap = self._capacities
            core = getattr(cap, "core", 0) if cap else 0
            ram = getattr(cap, "ram", 0) if cap else 0
            disk = getattr(cap, "disk", 0) if cap else 0
            gpu = getattr(cap, "gpu", 0) if cap else 0

            resource_ratios = {
                "cpu_bound": core,
                "ram_bound": ram,
                "disk_bound": disk,
                "gpu_bound": gpu,
            }
            resource_class = max(resource_ratios, key=resource_ratios.get)

            wt = self._wall_time or 0
            if wt <= 5:
                time_class = "short"
            elif wt <= 20:
                time_class = "medium"
            else:
                time_class = "long"

            required_dtns = (self.data_in or []) + (self.data_out or [])
            io_class = "dtn_heavy" if len(required_dtns) > 1 else "dtn_light"

            self.job_type = f"{resource_class}_{time_class}_{io_class}"
            return self.job_type

