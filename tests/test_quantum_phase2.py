# MIT License
#
# Copyright (c) 2024 swarm-workflows
#
# Author: Komal Thareja(kthare10@renci.org)
"""Unit tests for Phase 2: split hybrid co-scheduling, the measurement data
layer, data predicates, and streaming (stateful) execution."""

import threading
import time
import unittest

from swarm.models.job import Job
from swarm.quantum.split import (build_post_process_job, experiment_id_for,
                                 split_comm_penalty, split_hybrid_job)


class StubLayer:
    """In-memory stand-in for MeasurementLayer (same duck-typed surface)."""

    def __init__(self):
        self.streams = {}   # exp -> list[dict]
        self.sites = {}     # exp -> site
        self.lock = threading.Lock()

    def announce_producer(self, exp, site):
        with self.lock:
            self.sites[exp] = site or ""

    def publish(self, exp, payload):
        with self.lock:
            self.streams.setdefault(exp, []).append(payload)
            return len(self.streams[exp])

    def snapshot_count(self, exp):
        with self.lock:
            return len(self.streams.get(exp, []))

    def predicate_satisfied(self, exp, min_snapshots):
        return self.snapshot_count(exp) >= max(1, int(min_snapshots))

    def producer_site(self, exp):
        with self.lock:
            return self.sites.get(exp)

    def read_from(self, exp, last_id="0-0", block_ms=1000, count=16):
        start = int(last_id.split("-")[0]) if last_id != "0-0" else 0
        with self.lock:
            entries = self.streams.get(exp, [])[start:start + count]
        if not entries:
            time.sleep(min(0.05, block_ms / 1000.0))
            return []
        return [(f"{start + i + 1}-0", p) for i, p in enumerate(entries)]


HYBRID_JOB = {
    "id": "7",
    "wall_time": 10.0,
    "capacities": {"core": 4, "ram": 16, "disk": 100, "gpu": 0, "qubits": 20},
    "quantum": {"qubits": 20, "circuit_depth": 50, "shots": 1024,
                "hybrid": True, "iterations": 8},
    "should_fail": False,
}


class TestSplitHybridJob(unittest.TestCase):
    def test_split_produces_linked_pair(self):
        q, c = split_hybrid_job(HYBRID_JOB)
        self.assertEqual(q["id"], "7-q")
        self.assertEqual(c["id"], "7-c")
        self.assertEqual(q["linked_job_id"], "7-c")
        self.assertEqual(c["linked_job_id"], "7-q")
        self.assertEqual(q["experiment_id"], c["experiment_id"])
        self.assertEqual(q["experiment_id"], experiment_id_for("7"))

    def test_quantum_sub_keeps_qubits_classical_sub_does_not(self):
        q, c = split_hybrid_job(HYBRID_JOB)
        self.assertEqual(q["capacities"]["qubits"], 20)
        self.assertNotIn("qubits", c["capacities"])
        self.assertEqual(c["capacities"]["core"], 4)
        # Quantum sub only needs a prep-sized classical footprint
        self.assertLessEqual(q["capacities"]["core"], 1)

    def test_classical_sub_predicate_matches_iterations(self):
        _, c = split_hybrid_job(HYBRID_JOB)
        pred = c["data_predicate"]
        self.assertEqual(pred["min_snapshots"], 1)
        self.assertEqual(pred["total_snapshots"], 8)

    def test_non_hybrid_jobs_not_split(self):
        classical = {"id": "1", "wall_time": 1.0, "capacities": {"core": 1}}
        one_shot = {**HYBRID_JOB, "quantum": {**HYBRID_JOB["quantum"], "hybrid": False}}
        already_split = {**HYBRID_JOB, "sub_role": "quantum"}
        self.assertIsNone(split_hybrid_job(classical))
        self.assertIsNone(split_hybrid_job(one_shot))
        self.assertIsNone(split_hybrid_job(already_split))

    def test_sub_jobs_round_trip_through_job_model(self):
        q, c = split_hybrid_job(HYBRID_JOB)
        for sub, role in ((q, "quantum"), (c, "classical")):
            job = Job()
            job.from_dict({**sub, "state": 1})
            job2 = Job()
            job2.from_dict(job.to_dict())
            self.assertEqual(job2.sub_role, role)
            self.assertEqual(job2.experiment_id, experiment_id_for("7"))
        job = Job()
        job.from_dict({**c, "state": 1})
        self.assertEqual(job.data_predicate["total_snapshots"], 8)


class TestPostProcessJob(unittest.TestCase):
    def test_builder(self):
        post = build_post_process_job("42", "exp-42", "histogram")
        self.assertEqual(post["id"], "42-post")
        self.assertEqual(post["sub_role"], "classical")
        self.assertEqual(post["data_predicate"]["experiment_id"], "exp-42")
        self.assertEqual(post["data_predicate"]["total_snapshots"], 1)
        job = Job()
        job.from_dict({**post, "state": 1})
        self.assertEqual(job.job_class, "classical")


class TestCommPenalty(unittest.TestCase):
    def test_same_or_unknown_site_is_free(self):
        self.assertEqual(split_comm_penalty(1.0, 10, "a", "a"), 1.0)
        self.assertEqual(split_comm_penalty(1.0, 10, None, "a"), 1.0)
        self.assertEqual(split_comm_penalty(1.0, 10, "a", None), 1.0)
        self.assertEqual(split_comm_penalty(0.0, 10, "a", "b"), 1.0)

    def test_cross_site_scales_with_volume_and_saturates(self):
        low = split_comm_penalty(1.0, 1, "a", "b")
        high = split_comm_penalty(1.0, 50, "a", "b")
        huge = split_comm_penalty(1.0, 5000, "a", "b")
        self.assertGreater(low, 1.0)
        self.assertGreater(high, low)
        self.assertEqual(high, huge)  # saturates at 1 + factor
        self.assertAlmostEqual(huge, 2.0)


class TestStreamingExecution(unittest.TestCase):
    def _pair(self):
        q, c = split_hybrid_job(HYBRID_JOB)
        qj, cj = Job(), Job()
        qj.from_dict({**q, "state": 1})
        cj.from_dict({**c, "state": 1})
        return qj, cj

    def test_producer_publishes_iterations(self):
        layer = StubLayer()
        qj, _ = self._pair()
        qj.execute_producer(layer, site="site-A")
        exp = experiment_id_for("7")
        self.assertEqual(layer.snapshot_count(exp), 8)
        self.assertEqual(layer.producer_site(exp), "site-A")
        self.assertEqual(qj.exit_status, 0)
        self.assertEqual(qj.state_data["snapshots_published"], 8)

    def test_consumer_processes_stream(self):
        layer = StubLayer()
        qj, cj = self._pair()
        qj.execute_producer(layer, site="site-A")
        cj.execute_consumer(layer, timeout_s=5.0)
        self.assertEqual(cj.exit_status, 0)
        self.assertEqual(cj.state_data["snapshots_processed"], 8)
        self.assertEqual(cj.state_data["partial_result"], 8 * 1024)

    def test_consumer_concurrent_with_producer(self):
        layer = StubLayer()
        qj, cj = self._pair()
        t = threading.Thread(target=qj.execute_producer, args=(layer,), kwargs={"site": "s"})
        t.start()
        cj.execute_consumer(layer, timeout_s=10.0)
        t.join()
        self.assertEqual(cj.exit_status, 0)
        self.assertEqual(cj.state_data["snapshots_processed"], 8)

    def test_consumer_times_out_on_stalled_stream(self):
        layer = StubLayer()
        _, cj = self._pair()
        layer.publish(experiment_id_for("7"), {"shots": 1024})  # 1 of 8, then silence
        start = time.time()
        cj.execute_consumer(layer, timeout_s=0.5)
        self.assertEqual(cj.exit_status, 1)
        self.assertLess(time.time() - start, 5.0)
        # Partial state survives for diagnosis / restart
        self.assertEqual(cj.state_data["snapshots_processed"], 1)

    def test_consumer_persist_cb_called(self):
        layer = StubLayer()
        qj, cj = self._pair()
        qj.execute_producer(layer, site=None)
        saved = []
        cj.execute_consumer(layer, timeout_s=5.0, persist_cb=lambda j: saved.append(
            j.state_data["snapshots_processed"]))
        self.assertEqual(len(saved), 8)
        self.assertEqual(saved[-1], 8)


class TestMeasurementLayerRedis(unittest.TestCase):
    """Exercises the real MeasurementLayer against fakeredis when available."""

    def setUp(self):
        try:
            import fakeredis
        except ImportError:
            self.skipTest("fakeredis not installed")
        from swarm.quantum.measurement_layer import MeasurementLayer
        self.layer = MeasurementLayer(fakeredis.FakeStrictRedis(), ttl_s=60,
                                      predicate_cache_s=0.0)

    def test_publish_count_predicate(self):
        self.assertFalse(self.layer.predicate_satisfied("e1", 1))
        self.assertEqual(self.layer.publish("e1", {"shots": 100}), 1)
        self.assertEqual(self.layer.publish("e1", {"shots": 100}), 2)
        self.assertTrue(self.layer.predicate_satisfied("e1", 2))
        self.assertFalse(self.layer.predicate_satisfied("e1", 3))

    def test_producer_site(self):
        self.assertIsNone(self.layer.producer_site("e2"))
        self.layer.announce_producer("e2", "fabric-renc")
        self.assertEqual(self.layer.producer_site("e2"), "fabric-renc")

    def test_read_from_incremental(self):
        for i in range(3):
            self.layer.publish("e3", {"iteration": i + 1, "shots": 10})
        entries = self.layer.read_from("e3", last_id="0-0", block_ms=10)
        self.assertEqual(len(entries), 3)
        self.assertEqual(entries[0][1]["iteration"], 1)
        last_id = entries[-1][0]
        self.assertEqual(self.layer.read_from("e3", last_id=last_id, block_ms=10), [])
        self.layer.publish("e3", {"iteration": 4, "shots": 10})
        more = self.layer.read_from("e3", last_id=last_id, block_ms=10)
        self.assertEqual(len(more), 1)
        self.assertEqual(more[0][1]["iteration"], 4)


if __name__ == "__main__":
    unittest.main()
