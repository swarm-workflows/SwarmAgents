# MIT License
#
# Copyright (c) 2024 swarm-workflows
#
# Author: Komal Thareja(kthare10@renci.org)
"""
Tests for the Phase-2 data-layer changes (docs/SCALABILITY_REVIEW.md):
registry-SET discovery instead of SCAN, single-MGET batch reads, WATCH-free
single-writer saves with TTL. Runs against fakeredis (no server needed).
"""

import unittest

import fakeredis

from swarm.database.repository import Repository


def _repo():
    return Repository(redis_client=fakeredis.FakeStrictRedis(decode_responses=True))


class SaveFastRegistryTests(unittest.TestCase):
    def test_save_fast_registers_and_ttls(self):
        repo = _repo()
        repo.save_fast({"agent_id": 7, "load": 0.5}, key_prefix=Repository.KEY_AGENT,
                       level=1, group=0, ttl_s=90)
        key = "agent:1:0:7"
        self.assertTrue(repo.redis.exists(key))
        self.assertGreater(repo.redis.ttl(key), 0)
        self.assertIn(key, repo.redis.smembers("members:1:0"))

    def test_registry_discovery_avoids_scan_and_prunes_stale(self):
        repo = _repo()
        for aid in (1, 2, 3):
            repo.save_fast({"agent_id": aid}, key_prefix=Repository.KEY_AGENT,
                           level=0, group=2, ttl_s=90)
        # Simulate agent 2's key expiring (TTL) while the registry still lists it.
        repo.redis.delete("agent:0:2:2")
        objs = repo.get_all_objects(key_prefix=Repository.KEY_AGENT, level=0, group=2)
        self.assertEqual(sorted(o["agent_id"] for o in objs), [1, 3])
        # Stale entry pruned from the registry on read.
        self.assertNotIn("agent:0:2:2", repo.redis.smembers("members:0:2"))

    def test_scan_fallback_when_registry_empty(self):
        repo = _repo()
        # Key written by an old-version agent (no registry entry).
        repo.redis.set("agent:0:0:9", '{"agent_id": 9}')
        objs = repo.get_all_objects(key_prefix=Repository.KEY_AGENT, level=0, group=0)
        self.assertEqual([o["agent_id"] for o in objs], [9])

    def test_watch_save_also_registers_agents(self):
        repo = _repo()
        repo.save({"id": 4, "state": 1}, key_prefix=Repository.KEY_AGENT, level=0, group=1)
        self.assertIn("agent:0:1:4", repo.redis.smembers("members:0:1"))

    def test_delete_unregisters(self):
        repo = _repo()
        repo.save_fast({"agent_id": 5}, key_prefix=Repository.KEY_AGENT, level=0, group=0)
        repo.delete(obj_id="5", key_prefix=Repository.KEY_AGENT, level=0, group=0)
        self.assertNotIn("agent:0:0:5", repo.redis.smembers("members:0:0"))


class BatchReadTests(unittest.TestCase):
    def test_get_many_single_roundtrip_semantics(self):
        repo = _repo()
        repo.save({"id": "j1", "state": 1}, level=0, group=0)
        repo.save({"id": "j3", "state": 1}, level=0, group=0)
        out = repo.get_many(["j1", "j2", "j3"], key_prefix=Repository.KEY_JOB,
                            level=0, group=0)
        self.assertEqual(sorted(out.keys()), ["j1", "j3"])  # missing j2 omitted
        self.assertEqual(out["j1"]["state"], 1)

    def test_get_many_grouped(self):
        repo = _repo()
        repo.save({"id": "j1", "state": 8}, level=0, group=0)
        repo.save({"id": "j2", "state": 1}, level=0, group=3)
        out = repo.get_many_grouped([("j1", 0), ("j2", 3), ("jX", 1)],
                                    key_prefix=Repository.KEY_JOB, level=0)
        self.assertEqual(out[("j1", 0)]["state"], 8)
        self.assertEqual(out[("j2", 3)]["state"], 1)
        self.assertNotIn(("jX", 1), out)

    def test_job_state_index_still_maintained(self):
        repo = _repo()
        repo.save({"id": "j1", "state": 1}, level=0, group=0)
        repo.save({"id": "j1", "state": 8}, level=0, group=0)  # transition 1 -> 8
        self.assertEqual(repo.redis.smembers("state:0:0:8"), {"job:0:0:j1"})
        self.assertEqual(repo.redis.smembers("state:0:0:1"), set())


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
