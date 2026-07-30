# MIT License
#
# Copyright (c) 2024 swarm-workflows
#
# Author: Komal Thareja(kthare10@renci.org)
"""Unit tests for hybrid quantum-classical job support."""

import unittest

from swarm.models.agent_info import AgentInfo
from swarm.models.capacities import Capacities
from swarm.models.job import Job
from swarm.models.quantum import QuantumBackend, QuantumSpec


def _backend(**overrides) -> QuantumBackend:
    base = {
        "name": "aer-sim-64", "arch": "superconducting", "qubits": 64,
        "clops": 100000, "gate_fidelity": 0.999, "error_rate": 0.001,
        "calibration_downtime_pct": 0.0, "simulator": True,
    }
    base.update(overrides)
    return QuantumBackend.from_dict(base)


class TestQuantumSpec(unittest.TestCase):
    def test_required_shots_explicit(self):
        spec = QuantumSpec(shots=4096)
        self.assertEqual(spec.required_shots(), 4096)

    def test_required_shots_from_error_confidence(self):
        # n >= z^2 / (4 * error^2); z=1.96, error=0.01 -> 9604
        spec = QuantumSpec(error=0.01, confidence=0.95)
        self.assertEqual(spec.required_shots(), 9604)
        # Tighter error needs more shots; higher confidence needs more shots
        self.assertGreater(QuantumSpec(error=0.005, confidence=0.95).required_shots(),
                           spec.required_shots())
        self.assertGreater(QuantumSpec(error=0.01, confidence=0.99).required_shots(),
                           spec.required_shots())

    def test_required_shots_default(self):
        self.assertEqual(QuantumSpec().required_shots(), 1024)

    def test_estimated_quantum_time(self):
        spec = QuantumSpec(shots=1000, circuit_depth=100, iterations=10, hybrid=True)
        # 10 * 1000 * 100 / 100000 = 10s
        self.assertAlmostEqual(spec.estimated_quantum_time(100000), 10.0)
        self.assertEqual(spec.estimated_quantum_time(0), 0.0)

    def test_serialization_round_trip(self):
        spec = QuantumSpec(qubits=12, circuit_depth=50, shots=2048, arch="ion-trap",
                           fidelity=0.99, hybrid=True, iterations=20,
                           output_type="expectation", gates=["cx", "rz"])
        spec2 = QuantumSpec.from_dict(spec.to_dict())
        self.assertEqual(spec2.qubits, 12)
        self.assertEqual(spec2.arch, "ion-trap")
        self.assertTrue(spec2.hybrid)
        self.assertEqual(spec2.gates, ["cx", "rz"])


class TestQuantumBackend(unittest.TestCase):
    def test_supports_qubits(self):
        self.assertTrue(_backend().supports(QuantumSpec(qubits=64)))
        self.assertFalse(_backend().supports(QuantumSpec(qubits=65)))

    def test_supports_clops(self):
        self.assertTrue(_backend().supports(QuantumSpec(qubits=8, clops=100000)))
        self.assertFalse(_backend(clops=2000).supports(QuantumSpec(qubits=8, clops=100000)))

    def test_supports_arch(self):
        self.assertTrue(_backend().supports(QuantumSpec(qubits=8, arch="superconducting")))
        self.assertFalse(_backend().supports(QuantumSpec(qubits=8, arch="ion-trap")))
        # No preference matches any architecture
        self.assertTrue(_backend(arch="ion-trap").supports(QuantumSpec(qubits=8)))

    def test_supports_fidelity(self):
        self.assertTrue(_backend().supports(QuantumSpec(qubits=8, fidelity=0.99)))
        self.assertFalse(_backend(gate_fidelity=0.98).supports(QuantumSpec(qubits=8, fidelity=0.99)))

    def test_supports_gates(self):
        b = _backend(supported_gates=["cx", "rz", "h"])
        self.assertTrue(b.supports(QuantumSpec(qubits=8, gates=["cx", "h"])))
        self.assertFalse(b.supports(QuantumSpec(qubits=8, gates=["cx", "ccx"])))
        # Backend with no declared gate set accepts any circuit
        self.assertTrue(_backend().supports(QuantumSpec(qubits=8, gates=["ccx"])))

    def test_classical_spec_always_supported(self):
        self.assertTrue(_backend().supports(None))

    def test_quality_penalty_factor(self):
        noisy = _backend(error_rate=0.01, calibration_downtime_pct=0.1)
        clean = _backend()
        self.assertGreater(noisy.quality_penalty_factor(), clean.quality_penalty_factor())


class TestCapacitiesQubits(unittest.TestCase):
    def test_arithmetic(self):
        total = Capacities(core=8, ram=32, disk=500, qubits=64)
        job = Capacities(core=2, ram=8, disk=100, qubits=20)
        residual = total - job
        self.assertEqual(residual.qubits, 44)
        self.assertEqual((residual + job).qubits, 64)

    def test_insufficient_qubits_is_negative(self):
        available = Capacities(core=8, ram=32, disk=500, qubits=16)
        demand = Capacities(core=1, ram=1, disk=1, qubits=32)
        self.assertIn("qubits", (available - demand).negative_fields())

    def test_backward_compat_from_dict(self):
        # Old serialized capacities without qubits default to 0
        caps = Capacities.from_dict({"core": 4, "ram": 16, "disk": 250})
        self.assertEqual(caps.qubits, 0)


class TestJobQuantum(unittest.TestCase):
    def _job_dict(self, hybrid: bool) -> dict:
        return {
            "id": "q1",
            "wall_time": 5.0,
            "capacities": {"core": 2, "ram": 8, "disk": 50, "qubits": 16},
            "quantum": {"qubits": 16, "circuit_depth": 80, "shots": 2048,
                        "hybrid": hybrid, "iterations": 10 if hybrid else 1},
            "state": 1,
        }

    def test_job_class(self):
        classical = Job()
        classical.from_dict({"id": "c1", "wall_time": 1.0,
                             "capacities": {"core": 1, "ram": 1, "disk": 1}, "state": 1})
        self.assertEqual(classical.job_class, "classical")

        quantum = Job()
        quantum.from_dict(self._job_dict(hybrid=False))
        self.assertEqual(quantum.job_class, "quantum")

        hybrid = Job()
        hybrid.from_dict(self._job_dict(hybrid=True))
        self.assertEqual(hybrid.job_class, "hybrid")

    def test_classification_prefers_quantum(self):
        job = Job()
        job.from_dict(self._job_dict(hybrid=True))
        self.assertTrue(job.job_type.startswith("hybrid_"))
        job2 = Job()
        job2.from_dict(self._job_dict(hybrid=False))
        self.assertTrue(job2.job_type.startswith("quantum_"))

    def test_round_trip(self):
        job = Job()
        job.from_dict(self._job_dict(hybrid=True))
        job2 = Job()
        job2.from_dict(job.to_dict())
        self.assertEqual(job2.quantum.qubits, 16)
        self.assertEqual(job2.quantum.iterations, 10)
        self.assertEqual(job2.capacities.qubits, 16)
        self.assertEqual(job2.job_class, "hybrid")

    def test_classical_round_trip_has_no_quantum(self):
        job = Job()
        job.from_dict({"id": "c1", "wall_time": 1.0,
                       "capacities": {"core": 1, "ram": 1, "disk": 1}, "state": 1})
        self.assertIsNone(job.quantum)
        job2 = Job()
        job2.from_dict(job.to_dict())
        self.assertIsNone(job2.quantum)


class TestAgentInfoQuantum(unittest.TestCase):
    def test_round_trip_with_backend(self):
        ai = AgentInfo.from_dict({
            "agent_id": 3,
            "capacities": {"core": 8, "ram": 32, "disk": 500, "qubits": 32},
            "quantum_backend": {"name": "aqt-ion-32", "arch": "ion-trap", "qubits": 32,
                                "clops": 2000, "gate_fidelity": 0.9995, "error_rate": 0.0005},
        })
        ai2 = AgentInfo.from_dict(ai.to_dict())
        self.assertEqual(ai2.quantum_backend.name, "aqt-ion-32")
        self.assertEqual(ai2.capacities.qubits, 32)

    def test_classical_agent_has_no_backend(self):
        ai = AgentInfo.from_dict({"agent_id": 2,
                                  "capacities": {"core": 2, "ram": 8, "disk": 100}})
        self.assertIsNone(ai.quantum_backend)
        ai2 = AgentInfo.from_dict(ai.to_dict())
        self.assertIsNone(ai2.quantum_backend)


class TestJobGeneratorQuantum(unittest.TestCase):
    PROFILES = {
        "1": {"core": 32, "ram": 128, "disk": 1000, "gpu": 4, "qubits": 64,
              "quantum_backend": {"name": "aer-sim-64", "arch": "superconducting",
                                  "qubits": 64, "clops": 100000, "gate_fidelity": 0.999,
                                  "error_rate": 0.001, "simulator": True}},
        "2": {"core": 2, "ram": 8, "disk": 100, "gpu": 0, "qubits": 0,
              "quantum_backend": None},
    }

    def _generator(self, quantum=0.3, hybrid=0.2):
        import job_generator
        g = job_generator.JobGenerator(job_count=0, agent_profile_path=None)
        g.quantum_fraction = quantum
        g.hybrid_fraction = hybrid
        g.agent_profiles = dict(self.PROFILES)
        g.quantum_profiles = {aid: p for aid, p in g.agent_profiles.items()
                              if p.get("quantum_backend")}
        return g

    def test_quantum_jobs_target_backend_agents(self):
        g = self._generator()
        for i in range(50):
            job = g.generate_job(i, enable_dtns=False)
            if job.get("quantum"):
                self.assertEqual(job["target_agent"], "1")
                self.assertLessEqual(job["quantum"]["qubits"], 64)
                self.assertEqual(job["capacities"]["qubits"], job["quantum"]["qubits"])

    def test_feasibility_requires_backend(self):
        g = self._generator()
        job = None
        for i in range(200):
            j = g.generate_job(i, enable_dtns=False)
            if j.get("quantum"):
                job = j
                break
        self.assertIsNotNone(job)
        self.assertTrue(g.is_job_feasible(job, self.PROFILES["1"]))
        self.assertFalse(g.is_job_feasible(job, self.PROFILES["2"]))

    def test_fit_all_stays_classical(self):
        g = self._generator(quantum=0.5, hybrid=0.5)
        for i in range(20):
            self.assertIsNone(g.generate_job(i, enable_dtns=False, fit_all=True).get("quantum"))

    def test_requires_quantum_profile(self):
        import job_generator
        with self.assertRaises(ValueError):
            job_generator.JobGenerator(job_count=0, agent_profile_path=None,
                                       quantum_fraction=0.5)


if __name__ == "__main__":
    unittest.main()
