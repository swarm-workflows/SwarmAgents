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
"""
Quantum job and resource models for hybrid quantum-classical scheduling.

Job taxonomy (see docs/QUANTUM_HYBRID_DESIGN.md):
  - classical : Job with no QuantumSpec (existing behavior, unchanged)
  - quantum   : Job with a QuantumSpec, hybrid=False. One-shot circuit offload:
                the classical component (Job.capacities/wall_time) models circuit
                compilation and state preparation, then the circuit runs once on
                the quantum backend.
  - hybrid    : Job with a QuantumSpec, hybrid=True. Variational-style loop with
                continuous classical<->quantum interaction over `iterations`
                rounds (parameter update -> circuit execution -> measurement).
                Both components are co-scheduled on one agent that owns a
                quantum backend (Phase 1).

Resource side: an agent may own a QuantumBackend (real hardware or a noisy
simulator). Additive capacity (qubit count) lives in Capacities.qubits so the
existing feasibility/allocation arithmetic applies; non-additive quality
attributes (CLOPS, fidelity, error rate, calibration downtime, architecture)
live here and are checked/penalized explicitly.
"""
import math
from typing import List, Optional

from swarm.models.json_field import JSONField

# Two-sided normal quantile lookup for deriving shot counts from a requested
# (error, confidence) pair; keys are confidence levels.
_Z_SCORES = {0.90: 1.645, 0.95: 1.960, 0.99: 2.576}


class QuantumSpec(JSONField):
    """
    Quantum component of a job (requirements side).

    Attributes mirror the Q-DISTRI brainstorming doc:
      - qubits       : number of qubits required for the circuit
      - circuit_depth: circuit layer count (used with CLOPS for runtime estimates)
      - shots        : requested measurement shots (0 = derive from error/confidence)
      - clops        : minimum backend CLOPS required (0 = any)
      - arch         : preferred architecture (superconducting, ion-trap,
                       neutral-atom, photonic); empty = any
      - fidelity     : minimum acceptable gate fidelity (0 = any)
      - error        : target statistical error on the estimated observable
      - confidence   : confidence level for `error` (0.90/0.95/0.99)
      - output_type  : "expectation" or "histogram"
      - hybrid       : True for variational-style jobs with a continuous
                       classical<->quantum loop
      - iterations   : number of classical<->quantum rounds (1 for one-shot)
      - gates        : gate set required by the circuit; empty = any
    """

    def __init__(self, **kwargs):
        self.qubits = 0
        self.circuit_depth = 0
        self.shots = 0
        self.clops = 0
        self.arch = ""
        self.fidelity = 0.0
        self.error = 0.0
        self.confidence = 0.0
        self.output_type = ""
        self.hybrid = False
        self.iterations = 1
        self.gates = []
        # One-shot quantum jobs: push a classical post-processing job to the
        # pool on completion (quantum agents feeding the classical pool)
        self.post_process = False
        self._set_fields(**kwargs)

    def _set_fields(self, forgiving=False, **kwargs):
        for k, v in kwargs.items():
            try:
                self.__getattribute__(k)
                self.__setattr__(k, v)
            except AttributeError:
                report = f"Unable to set field {k} of quantum spec, no such field available " \
                         f"{[k for k in self.__dict__.keys()]}"
                if forgiving:
                    print(report)
                else:
                    raise QuantumModelException(report)
        return self

    def required_shots(self) -> int:
        """
        Shots to execute: the explicit request, or the count needed to reach the
        requested (error, confidence) for a binomial estimator
        (n >= z^2 / (4 * error^2), worst case p=0.5).
        """
        if self.shots > 0:
            return int(self.shots)
        if self.error > 0:
            z = _Z_SCORES.get(round(self.confidence, 2), 1.960)
            return int(math.ceil((z * z) / (4.0 * self.error * self.error)))
        return 1024

    def estimated_quantum_time(self, backend_clops: float) -> float:
        """
        Estimated seconds of quantum execution across all iterations using the
        CLOPS runtime model: iterations * shots * depth / CLOPS.
        """
        if backend_clops <= 0:
            return 0.0
        depth = max(1, self.circuit_depth)
        return (max(1, self.iterations) * self.required_shots() * depth) / backend_clops

    def __str__(self):
        kind = "hybrid" if self.hybrid else "quantum"
        return (f"{{ {kind}: qubits: {self.qubits}, depth: {self.circuit_depth}, "
                f"shots: {self.required_shots()}, iterations: {self.iterations} }}")


class QuantumBackend(JSONField):
    """
    Quantum resource owned by an agent (real hardware or noisy simulator).

    Attributes (resource side of the Q-DISTRI brainstorming doc):
      - name                    : backend identifier
      - arch                    : superconducting, ion-trap, neutral-atom, photonic
      - qubits                  : qubit count (mirrored into Capacities.qubits)
      - clops                   : circuit layer operations per second
      - gate_fidelity           : average gate fidelity (0-1)
      - error_rate              : average error rate per layer (0-1)
      - supported_gates         : native gate set; empty = accepts any circuit
      - calibration_downtime_pct: fraction of time unavailable for calibration (0-1)
      - simulator               : True for classical simulation of a quantum backend
    """

    def __init__(self, **kwargs):
        self.name = ""
        self.arch = ""
        self.qubits = 0
        self.clops = 0
        self.gate_fidelity = 1.0
        self.error_rate = 0.0
        self.supported_gates = []
        self.calibration_downtime_pct = 0.0
        self.simulator = False
        self._set_fields(**kwargs)

    def _set_fields(self, forgiving=False, **kwargs):
        for k, v in kwargs.items():
            try:
                self.__getattribute__(k)
                self.__setattr__(k, v)
            except AttributeError:
                report = f"Unable to set field {k} of quantum backend, no such field available " \
                         f"{[k for k in self.__dict__.keys()]}"
                if forgiving:
                    print(report)
                else:
                    raise QuantumModelException(report)
        return self

    def supports(self, spec: Optional[QuantumSpec]) -> bool:
        """
        Feasibility of running `spec` on this backend: qubit count, minimum
        CLOPS, architecture preference, gate fidelity floor, and gate set.
        """
        if spec is None:
            return True
        if spec.qubits > self.qubits:
            return False
        if spec.clops > 0 and self.clops < spec.clops:
            return False
        if spec.arch and self.arch and spec.arch != self.arch:
            return False
        if spec.fidelity > 0 and self.gate_fidelity < spec.fidelity:
            return False
        if spec.gates and self.supported_gates:
            if not set(spec.gates).issubset(set(self.supported_gates)):
                return False
        return True

    def quality_penalty_factor(self) -> float:
        """
        Multiplicative cost penalty in [0, ~1] reflecting backend quality:
        noisier backends and backends that spend more time in calibration are
        more expensive to pick (risk of re-execution / waiting).
        """
        return self.error_rate + self.calibration_downtime_pct

    def __str__(self):
        kind = "sim" if self.simulator else "hw"
        return (f"{{ backend: {self.name} ({self.arch}/{kind}), qubits: {self.qubits}, "
                f"clops: {self.clops:,}, fidelity: {self.gate_fidelity} }}")


class QuantumModelException(Exception):
    def __init__(self, msg: str):
        assert msg is not None
        super().__init__(f"Quantum model exception: {msg}")
