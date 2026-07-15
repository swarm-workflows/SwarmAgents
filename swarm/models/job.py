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
from swarm.models.quantum import QuantumSpec


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
        self.service_id = None
        self.startOrStop = None
        # Resource intents and allocations
        self._capacities: Optional[Capacities] = None
        self._capacity_allocations: Optional[Capacities] = None

        # Classification + status
        self._job_type: Optional[str] = None
        self._exit_status: int = 0
        self._should_fail: bool = False  # Pre-determined failure flag from job generator

        # Quantum component (None = purely classical job). When present,
        # capacities/wall_time describe the classical component and the spec
        # describes the quantum component (see swarm/models/quantum.py).
        self._quantum: Optional[QuantumSpec] = None
        self._quantum_time: Optional[float] = None  # simulated quantum execution seconds

        # Split hybrid sub-jobs (Phase 2, docs/QUANTUM_HYBRID_DESIGN.md):
        #   sub_role      : None (whole job) | "quantum" (measurement producer)
        #                   | "classical" (stream consumer)
        #   linked_job_id : the other half of a split pair
        #   experiment_id : measurement stream this job produces/consumes
        #   data_predicate: {"experiment_id", "min_snapshots", "total_snapshots"} —
        #                   job is not selectable until min_snapshots exist
        #   state_data    : stateful-job-pool state (partial results, progress)
        self._sub_role: Optional[str] = None
        self._linked_job_id: Optional[str] = None
        self._experiment_id: Optional[str] = None
        self._data_predicate: Optional[Dict] = None
        self._state_data: Dict = {}

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

    # ---------- Quantum component ----------
    @property
    def quantum(self) -> Optional[QuantumSpec]:
        with self.lock:
            return self._quantum

    @quantum.setter
    def quantum(self, spec) -> None:
        with self.lock:
            if spec is None or isinstance(spec, QuantumSpec):
                self._quantum = spec
            elif isinstance(spec, dict):
                self._quantum = QuantumSpec.from_dict(spec)
            else:
                raise ValueError("Unsupported value type for quantum spec")

    @property
    def job_class(self) -> str:
        """classical (no quantum spec), quantum (one-shot offload), or hybrid
        (continuous classical<->quantum loop)."""
        with self.lock:
            if self._quantum is None:
                return "classical"
            return "hybrid" if self._quantum.hybrid else "quantum"

    @property
    def quantum_time(self) -> Optional[float]:
        with self.lock:
            return self._quantum_time

    # ---------- Split sub-jobs / data-triggered fields ----------
    @property
    def sub_role(self) -> Optional[str]:
        with self.lock:
            return self._sub_role

    @sub_role.setter
    def sub_role(self, value: Optional[str]) -> None:
        with self.lock:
            assert value in (None, "quantum", "classical")
            self._sub_role = value

    @property
    def linked_job_id(self) -> Optional[str]:
        with self.lock:
            return self._linked_job_id

    @linked_job_id.setter
    def linked_job_id(self, value: Optional[str]) -> None:
        with self.lock:
            self._linked_job_id = value

    @property
    def experiment_id(self) -> Optional[str]:
        with self.lock:
            return self._experiment_id

    @experiment_id.setter
    def experiment_id(self, value: Optional[str]) -> None:
        with self.lock:
            self._experiment_id = value

    @property
    def data_predicate(self) -> Optional[Dict]:
        with self.lock:
            return self._data_predicate

    @data_predicate.setter
    def data_predicate(self, value: Optional[Dict]) -> None:
        with self.lock:
            self._data_predicate = value

    @property
    def state_data(self) -> Dict:
        with self.lock:
            return dict(self._state_data)

    def update_state_data(self, **kwargs) -> None:
        with self.lock:
            self._state_data.update(kwargs)

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
            if self._quantum is not None:
                self._execute_quantum()
            else:
                wt = self.wall_time or 0.0
                self.logger.info("Sleeping for %s seconds to simulate job execution", wt)
                if wt > 0:
                    #time.sleep(wt)
                    time.sleep(1)

            # TODO: staged-out transfers using self.data_out if data_transfer

            self.state = ObjectState.COMPLETE

            # Check pre-determined failure flag (from job generator)
            if self._should_fail:
                self._exit_status = 1
                self.logger.info("Job %s marked as FAILED (pre-determined)", self.job_id)
            else:
                self._exit_status = 0

            self.mark_completed()
            self.logger.info("Completed execution for job: %s (exit_status=%d)", self.job_id, self._exit_status)
        except Exception as e:
            self.logger.error("Error executing job %s: %s", self.job_id, e)
            self.logger.error(traceback.format_exc())
            self.state = ObjectState.COMPLETE
            self._exit_status = 1  # Real failure
            self.mark_completed()

    def _execute_quantum(self):
        """
        Simulate a quantum or hybrid job (data-triggered runtime model):
          quantum  : classical prep (compile/state-prep) -> one circuit offload
          hybrid   : `iterations` rounds of circuit execution; each round's
                     measurement data satisfies the shot predicate and triggers
                     the classical update step (e.g. variational parameter update)
        Total simulated sleep stays ~1s to match classical job simulation;
        the phase breakdown is recorded in quantum_time / logs.
        """
        spec = self._quantum
        shots = spec.required_shots()
        iterations = max(1, spec.iterations) if spec.hybrid else 1
        wt = self.wall_time or 0.0

        self.logger.info(
            "Job %s (%s): %d iteration(s) x %d shots on %d qubits (depth=%d)",
            self.job_id, self.job_class, iterations, shots, spec.qubits, spec.circuit_depth)

        # Split the 1s simulation budget across classical and quantum phases
        step_sleep = 1.0 / (iterations * 2 + 1)
        q_time = 0.0

        time.sleep(step_sleep)  # classical prep: compile circuit, prepare state
        for i in range(iterations):
            q_start = time.time()
            time.sleep(step_sleep)  # quantum phase: execute circuit, collect shots
            q_time += time.time() - q_start
            self.logger.debug(
                "Job %s iteration %d/%d: %d shots collected (predicate satisfied), "
                "running classical update", self.job_id, i + 1, iterations, shots)
            time.sleep(step_sleep)  # classical phase: post-process measurements

        with self.lock:
            self._quantum_time = round(q_time, 3)
        self.logger.info(
            "Job %s (%s) simulated: wall_time=%s quantum_time=%.3fs",
            self.job_id, self.job_class, wt, q_time)

    # ---------- Split execution (Phase 2: data-triggered runtime) ----------
    def execute_producer(self, layer, site: Optional[str] = None):
        """
        Execute the quantum half of a split hybrid job: announce the producer
        site, then publish one snapshot batch per iteration to the measurement
        layer. `layer` is duck-typed (see MeasurementLayer): announce_producer,
        publish.
        """
        try:
            spec = self._quantum
            exp = self._experiment_id or f"exp-{self.job_id}"
            iterations = max(1, spec.iterations if spec else 1)
            shots = spec.required_shots() if spec else 0

            self.logger.info("Job %s (producer): %d snapshot batches x %d shots -> %s",
                             self.job_id, iterations, shots, exp)
            self.state = ObjectState.RUNNING
            self.mark_started()

            layer.announce_producer(exp, site)
            step_sleep = 1.0 / (iterations + 1)
            q_start = time.time()
            time.sleep(step_sleep)  # compile + state-prep
            for i in range(1, iterations + 1):
                time.sleep(step_sleep)  # circuit execution for this iteration
                count = layer.publish(exp, {"job_id": self.job_id, "iteration": i,
                                            "shots": shots})
                self.logger.debug("Job %s published snapshot %d/%d (stream=%d)",
                                  self.job_id, i, iterations, count)
            with self.lock:
                self._quantum_time = round(time.time() - q_start, 3)
                self._state_data.update({"snapshots_published": iterations})

            self.state = ObjectState.COMPLETE
            self._exit_status = 1 if self._should_fail else 0
            self.mark_completed()
            self.logger.info("Job %s (producer) completed: %d snapshots (exit_status=%d)",
                             self.job_id, iterations, self._exit_status)
        except Exception as e:
            self.logger.error("Error executing producer job %s: %s", self.job_id, e)
            self.logger.error(traceback.format_exc())
            self.state = ObjectState.COMPLETE
            self._exit_status = 1
            self.mark_completed()

    def execute_consumer(self, layer, timeout_s: float = 60.0, persist_cb=None):
        """
        Execute the classical half of a split hybrid job (or a post-processing
        job): consume snapshot batches from the measurement layer as they
        arrive and run one incremental update per batch, maintaining partial
        state (stateful job pool). Completes when `total_snapshots` batches
        are processed; fails if the stream stalls for `timeout_s` (e.g. the
        producer died — the standard failure machinery then takes over).

        `persist_cb(job)`, when given, is invoked after each update so partial
        state survives agent failure.
        """
        try:
            pred = self._data_predicate or {}
            exp = pred.get("experiment_id") or self._experiment_id or f"exp-{self.job_id}"
            total = int(pred.get("total_snapshots", 1))

            self.logger.info("Job %s (consumer): awaiting %d snapshot batches from %s",
                             self.job_id, total, exp)
            self.state = ObjectState.RUNNING
            self.mark_started()

            processed = int(self._state_data.get("snapshots_processed", 0))
            partial = float(self._state_data.get("partial_result", 0.0))
            last_id = self._state_data.get("last_stream_id", "0-0")
            step_sleep = min(0.2, 1.0 / (total + 1))
            last_data_at = time.time()

            while processed < total:
                entries = layer.read_from(exp, last_id=last_id, block_ms=500)
                if not entries:
                    if time.time() - last_data_at > timeout_s:
                        raise TimeoutError(
                            f"measurement stream {exp} stalled: "
                            f"{processed}/{total} snapshots after {timeout_s}s")
                    continue
                last_data_at = time.time()
                for entry_id, payload in entries:
                    time.sleep(step_sleep)  # incremental classical update
                    processed += 1
                    # Simulated running estimate (e.g. moment/expectation update)
                    partial += float(payload.get("shots", 0))
                    last_id = entry_id
                    with self.lock:
                        self._state_data.update({
                            "snapshots_processed": processed,
                            "partial_result": partial,
                            "last_stream_id": last_id,
                        })
                    if persist_cb is not None:
                        try:
                            persist_cb(self)
                        except Exception as e:
                            self.logger.debug("persist_cb failed for %s: %s", self.job_id, e)
                    if processed >= total:
                        break

            self.state = ObjectState.COMPLETE
            self._exit_status = 1 if self._should_fail else 0
            self.mark_completed()
            self.logger.info("Job %s (consumer) completed: %d snapshots processed (exit_status=%d)",
                             self.job_id, processed, self._exit_status)
        except Exception as e:
            self.logger.error("Error executing consumer job %s: %s", self.job_id, e)
            self.state = ObjectState.COMPLETE
            self._exit_status = 1
            self.mark_completed()

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
            "last_transition_at": self.last_transition_at
        }
        return f"Job({fields})"

    __str__ = __repr__

    # ---------- (De)serialization ----------
    def to_dict(self, compact: bool = False) -> dict:
        # Snapshot all fields under lock (fast — just reference copies)
        with self.lock:
            snap_id = self._object_id
            snap_capacities = self._capacities
            snap_cap_alloc = self._capacity_allocations
            snap_wall_time = self._wall_time
            snap_data_in = list(self.data_in)
            snap_data_out = list(self.data_out)
            snap_state = self._state
            snap_exit_status = self._exit_status
            snap_should_fail = self._should_fail
            snap_transfer_in = self.transfer_in_time
            snap_transfer_out = self.transfer_out_time
            snap_submitted = dict(self._submitted_at)
            snap_selection_started = dict(self._selection_started_at)
            snap_assigned = dict(self._assigned_at)
            snap_started = dict(self._started_at)
            snap_completed = dict(self._completed_at)
            snap_leader_id = self._leader_id
            snap_last_transition = self._last_transition_at
            snap_job_type = self._job_type
            snap_reasoning_time = self._reasoning_time
            snap_deleg_agents = list(self._delegation_failed_agents)
            snap_deleg_failed = self._delegation_failed
            snap_deleg_count = self._delegation_failed_count
            snap_level = self.level
            snap_quantum = self._quantum
            snap_quantum_time = self._quantum_time
            snap_sub_role = self._sub_role
            snap_linked = self._linked_job_id
            snap_experiment = self._experiment_id
            snap_predicate = dict(self._data_predicate) if self._data_predicate else None
            snap_state_data = dict(self._state_data) if self._state_data else None

        # Serialize outside lock (slow — dict building, DataNode conversion)
        result = {
            "id": snap_id,
            "capacities": snap_capacities.to_dict() if snap_capacities else None,
            "capacity_allocations": (
                snap_cap_alloc.to_dict() if snap_cap_alloc else None
            ),
            "wall_time": snap_wall_time,
            "data_in": [dn.to_dict() for dn in snap_data_in],
            "data_out": [dn.to_dict() for dn in snap_data_out],
            "state": snap_state.value,
            "exit_status": snap_exit_status,
            "should_fail": snap_should_fail,
            "transfer_in_time": snap_transfer_in,
            "transfer_out_time": snap_transfer_out,
            "submitted_at": snap_submitted,
            "selection_started_at": snap_selection_started,
            "assigned_at": snap_assigned,
            "started_at": snap_started,
            "completed_at": snap_completed,
            "leader_id": snap_leader_id,
            "last_transition_at": snap_last_transition,
            "job_type": snap_job_type,
            "reasoning_time": snap_reasoning_time,
            "delegation_failed_agents": snap_deleg_agents,
            "delegation_failed": snap_deleg_failed,
            "delegation_failed_count": snap_deleg_count,
            "level": snap_level,
            "quantum": snap_quantum.to_dict() if snap_quantum else None,
            "quantum_time": snap_quantum_time,
            "sub_role": snap_sub_role,
            "linked_job_id": snap_linked,
            "experiment_id": snap_experiment,
            "data_predicate": snap_predicate,
            "state_data": snap_state_data,
        }

        if compact:
            return {
                k: v for k, v in result.items()
                if v is not None and v != [] and v != {}
            }

        return result

    def from_dict(self, job_data: dict):
        # Parse all values outside the lock (slow — Capacities, DataNode construction)
        parsed_id = job_data["id"]
        parsed_capacities = (
            Capacities.from_dict(job_data["capacities"]) if job_data.get("capacities") else None
        )
        parsed_cap_alloc = (
            Capacities.from_dict(job_data["capacity_allocations"])
            if job_data.get("capacity_allocations")
            else None
        )
        parsed_wall_time = job_data.get("wall_time")
        parsed_data_in = [DataNode.from_dict(d) for d in (job_data.get("data_in") or [])]
        parsed_data_out = [DataNode.from_dict(d) for d in (job_data.get("data_out") or [])]
        parsed_state = ObjectState(job_data["state"]) if job_data.get("state") else ObjectState.PENDING
        parsed_exit_status = int(job_data.get("exit_status", 0))
        parsed_should_fail = bool(job_data.get("should_fail", False))
        parsed_transfer_in = job_data.get("transfer_in_time")
        parsed_transfer_out = job_data.get("transfer_out_time")
        parsed_level = job_data.get("level")

        # Back-compat + canonical mapping (supports single values, lists, and dicts)
        submitted = job_data.get("submitted_at", time.time())
        if isinstance(submitted, dict):
            parsed_submitted = {int(k): float(v) for k, v in submitted.items()}
        elif isinstance(submitted, list):
            parsed_submitted = {i: float(v) for i, v in enumerate(submitted)}
        else:
            parsed_submitted = {(parsed_level or 0): float(submitted)}

        selection_started = job_data.get("selection_started_at")
        if isinstance(selection_started, dict):
            parsed_selection_started = {int(k): float(v) for k, v in selection_started.items()}
        elif isinstance(selection_started, list):
            parsed_selection_started = {i: float(v) for i, v in enumerate(selection_started)}
        elif selection_started is not None:
            parsed_selection_started = {(parsed_level or 0): float(selection_started)}
        else:
            parsed_selection_started = {}

        assigned = job_data.get("assigned_at")
        if isinstance(assigned, dict):
            parsed_assigned = {int(k): float(v) for k, v in assigned.items()}
        elif isinstance(assigned, list):
            parsed_assigned = {i: float(v) for i, v in enumerate(assigned)}
        elif assigned is not None:
            parsed_assigned = {(parsed_level or 0): float(assigned)}
        else:
            parsed_assigned = {}

        started = job_data.get("started_at")
        if isinstance(started, dict):
            parsed_started = {int(k): float(v) for k, v in started.items()}
        elif isinstance(started, list):
            parsed_started = {i: float(v) for i, v in enumerate(started)}
        elif started is not None:
            parsed_started = {(parsed_level or 0): float(started)}
        else:
            parsed_started = {}

        completed = job_data.get("completed_at")
        if isinstance(completed, dict):
            parsed_completed = {int(k): float(v) for k, v in completed.items()}
        elif isinstance(completed, list):
            parsed_completed = {i: float(v) for i, v in enumerate(completed)}
        elif completed is not None:
            parsed_completed = {(parsed_level or 0): float(completed)}
        else:
            parsed_completed = {}

        parsed_leader_id = job_data.get("leader_id")
        parsed_last_transition = job_data.get("last_transition_at")
        parsed_reasoning_time = job_data.get("reasoning_time")
        parsed_deleg_agents = job_data.get("delegation_failed_agents")
        parsed_deleg_failed = job_data.get("delegation_failed")
        parsed_deleg_count = job_data.get("delegation_failed_count")
        parsed_quantum = (
            QuantumSpec.from_dict(job_data["quantum"]) if job_data.get("quantum") else None
        )
        parsed_quantum_time = job_data.get("quantum_time")
        parsed_sub_role = job_data.get("sub_role")
        parsed_linked = job_data.get("linked_job_id")
        parsed_experiment = job_data.get("experiment_id")
        parsed_predicate = job_data.get("data_predicate")
        parsed_state_data = job_data.get("state_data") or {}

        # Assign all parsed values under lock (fast — just reference assignments)
        with self.lock:
            self._object_id = parsed_id
            self._capacities = parsed_capacities
            self._capacity_allocations = parsed_cap_alloc
            self._wall_time = float(parsed_wall_time) if parsed_wall_time is not None else None
            self.data_in = parsed_data_in
            self.data_out = parsed_data_out
            self._state = parsed_state
            self._exit_status = parsed_exit_status
            self._should_fail = parsed_should_fail
            self.transfer_in_time = parsed_transfer_in
            self.transfer_out_time = parsed_transfer_out
            self.level = parsed_level
            self._submitted_at = parsed_submitted
            self._selection_started_at = parsed_selection_started
            self._assigned_at = parsed_assigned
            self._started_at = parsed_started
            self._completed_at = parsed_completed
            self._leader_id = int(parsed_leader_id) if parsed_leader_id is not None else None
            self._last_transition_at = parsed_last_transition
            self._reasoning_time = float(parsed_reasoning_time) if parsed_reasoning_time is not None else None
            self._delegation_failed_agents = parsed_deleg_agents if parsed_deleg_agents is not None else []
            self._delegation_failed = parsed_deleg_failed if parsed_deleg_failed is not None else False
            self._delegation_failed_count = parsed_deleg_count if parsed_deleg_count is not None else 0
            self._quantum = parsed_quantum
            self._quantum_time = float(parsed_quantum_time) if parsed_quantum_time is not None else None
            self._sub_role = parsed_sub_role
            self._linked_job_id = parsed_linked
            self._experiment_id = parsed_experiment
            self._data_predicate = parsed_predicate
            self._state_data = parsed_state_data

        self.classify_job_type()

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

            # A quantum component dominates the classification: the QPU is the
            # scarce resource regardless of the classical demand profile
            if self._quantum is not None:
                resource_class = "hybrid" if self._quantum.hybrid else "quantum"

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

