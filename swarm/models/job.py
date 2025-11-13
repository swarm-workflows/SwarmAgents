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
from typing import Tuple, Any, List, Optional, Dict

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

        # Timing (epoch seconds)
        self._submitted_at: float = time.time()
        self._selection_started_at: Optional[float] = None
        self._assigned_at: Optional[float] = None
        self._started_at: Optional[float] = None
        self._completed_at: Optional[float] = None
        self._wall_time: Optional[float] = None
        self._reasoning_time: Optional[float] = None

        # Meta
        self.logger = logger if logger else logging.getLogger(self.__class__.__name__)

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
        return max(0.0, time.time() - (self._submitted_at or time.time()))

    def duration_run(self) -> Optional[float]:
        """Actual runtime (started -> completed)."""
        with self.lock:
            if self._started_at is None or self._completed_at is None:
                return None
            return max(0.0, self._completed_at - self._started_at)

    def duration_wait(self) -> Optional[float]:
        """Queueing delay (submitted -> started)."""
        with self.lock:
            if self._started_at is None:
                return None
            return max(0.0, self._started_at - self._submitted_at)

    def duration_consensus(self) -> Optional[float]:
        """Planning latency (selection_started -> assigned)."""
        with self.lock:
            if self._selection_started_at is None or self._assigned_at is None:
                return None
            return max(0.0, self._assigned_at - self._selection_started_at)

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
        with self.lock:
            return self._submitted_at

    @submitted_at.setter
    def submitted_at(self, ts: float):
        with self.lock:
            self._submitted_at = float(ts)

    @property
    def selection_started_at(self) -> Optional[float]:
        with self.lock:
            return self._selection_started_at

    def mark_selection_started(self, ts: Optional[float] = None) -> None:
        with self.lock:
            self._selection_started_at = float(ts or time.time())

    @property
    def assigned_at(self) -> Optional[float]:
        with self.lock:
            return self._assigned_at

    def mark_assigned(self, ts: Optional[float] = None) -> None:
        with self.lock:
            self._assigned_at = float(ts or time.time())

    @property
    def started_at(self) -> Optional[float]:
        with self.lock:
            return self._started_at

    def mark_started(self, ts: Optional[float] = None) -> None:
        with self.lock:
            self._started_at = float(ts or time.time())

    @property
    def completed_at(self) -> Optional[float]:
        with self.lock:
            return self._completed_at

    def mark_completed(self, ts: Optional[float] = None) -> None:
        with self.lock:
            self._completed_at = float(ts or time.time())

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
        if new_state in (ObjectState.PRE_PREPARE, ObjectState.PREPARE):
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
            "submitted_at": self._submitted_at,
            "selection_started_at": self._selection_started_at,
            "assigned_at": self._assigned_at,
            "started_at": self._started_at,
            "completed_at": self._completed_at,
            "wall_time": self._wall_time,
            "exit_status": self.exit_status,
            "leader_id": self._leader_id,
            "job_type": self.job_type,
        }
        return f"Job({fields})"

    __str__ = __repr__

    # ---------- (De)serialization ----------
    def to_dict(self) -> dict:
        with self.lock:
            return {
                "id": self.job_id,
                "capacities": self._capacities.to_dict() if self._capacities else None,
                "capacity_allocations": (
                    self._capacity_allocations.to_dict() if self._capacity_allocations else None
                ),
                "wall_time": self._wall_time,
                "data_in": [dn.to_dict() for dn in self.data_in],
                "data_out": [dn.to_dict() for dn in self.data_out],
                "state": self.state.value,
                "exit_status": self.exit_status,
                "transfer_in_time": self.transfer_in_time,
                "transfer_out_time": self.transfer_out_time,
                # canonical keys:
                "submitted_at": self._submitted_at,
                "selection_started_at": self._selection_started_at,
                "assigned_at": self._assigned_at,
                "started_at": self._started_at,
                "completed_at": self._completed_at,
                "leader_id": self._leader_id,
                "last_transition_at": getattr(self, "last_transition_at", None),
                "job_type": self.job_type,
                "reasoning_time": self._reasoning_time,
            }

    def from_dict(self, job_data: dict):
        with self.lock:
            self.job_id = job_data["id"]

            self._capacities = (
                Capacities.from_dict(job_data["capacities"]) if job_data.get("capacities") else None
            )
            self._capacity_allocations = (
                Capacities.from_dict(job_data["capacity_allocations"])
                if job_data.get("capacity_allocations")
                else None
            )
            self._wall_time = job_data.get("wall_time")

            self.data_in = [DataNode.from_dict(d) for d in job_data.get("data_in", [])]
            self.data_out = [DataNode.from_dict(d) for d in job_data.get("data_out", [])]

            self.state = ObjectState(job_data["state"]) if job_data.get("state") else ObjectState.PENDING
            self.exit_status = int(job_data.get("exit_status", 0))
            self.transfer_in_time = job_data.get("transfer_in_time")
            self.transfer_out_time = job_data.get("transfer_out_time")

            # Back-compat + canonical mapping
            self._submitted_at = float(
                job_data.get("submitted_at", job_data.get("submitted_at", time.time()))
            )
            self._selection_started_at = self._coalesce_time(
                job_data, "selection_started_at", "selection_started_at"
            )
            self._assigned_at = self._coalesce_time(job_data, "assigned_at", "assigned_at")

            # Previously called "scheduled_at" but used at RUNNING; now "started_at"
            self._started_at = self._coalesce_time(job_data, "started_at", "started_at")
            self._completed_at = job_data.get("completed_at")

            self._leader_id = job_data.get("leader_id")
            self.last_transition_at = job_data.get("last_transition_at")

            self._reasoning_time = job_data.get("reasoning_time")
            self.classify_job_type()

    @staticmethod
    def _coalesce_time(d: dict, canonical: str, legacy: str) -> Optional[float]:
        v = d.get(canonical, d.get(legacy))
        return None if v is None else float(v)

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

