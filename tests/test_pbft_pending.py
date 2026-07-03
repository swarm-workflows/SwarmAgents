# MIT License
#
# Copyright (c) 2024 swarm-workflows
#
# Author: Komal Thareja(kthare10@renci.org)
"""
Regression tests for out-of-order PBFT message handling.

Before the _set_pending_safe guard, a host without set_pending_* raised
AttributeError inside the on_proposal batch loop, silently dropping every
remaining proposal in the message (lost quorum votes -> re-proposal storms).
"""

import unittest
from typing import Dict, List, Optional

from swarm.consensus.engine import ConsensusEngine
from swarm.consensus.messages.proposal import Proposal
from swarm.consensus.messages.prepare import Prepare
from swarm.consensus.messages.proposal_info import ProposalInfo
from swarm.models.agent_info import AgentInfo
from swarm.models.object import ObjectState


class _FakeJob:
    def __init__(self, object_id: str):
        self.object_id = object_id
        self.leader_id = None
        self.state = None
        self.is_commit = False


class _HostNoPending:
    """Host WITHOUT set_pending_* (the ResourceAgent bug this guards against)."""

    def __init__(self, jobs: Dict[str, _FakeJob]):
        self.jobs = jobs
        self.warns: List[str] = []

    def get_object(self, oid) -> Optional[_FakeJob]:
        return self.jobs.get(oid)

    def is_agreement_achieved(self, oid): return False
    def calculate_quorum(self): return 99  # never reach quorum in these tests
    def on_leader_elected(self, obj, p_id): pass
    def on_participant_commit(self, obj, leader_id, p_id): pass
    def now(self): return 0.0
    def log_debug(self, m): pass
    def log_info(self, m): pass
    def log_warn(self, m): self.warns.append(m)


class _HostWithPending(_HostNoPending):
    """Host WITH set_pending_* (Colmena/fixed-ResourceAgent behavior)."""

    def __init__(self, jobs):
        super().__init__(jobs)
        self.pending: List[tuple] = []

    def set_pending_proposal(self, msg, oid): self.pending.append(("proposal", oid))
    def set_pending_prepare(self, msg, oid): self.pending.append(("prepare", oid))
    def set_pending_commit(self, msg, oid): self.pending.append(("commit", oid))


class _Transport:
    def __init__(self):
        self.broadcasts: List[object] = []

    def send(self, dest, payload): pass
    def broadcast(self, payload): self.broadcasts.append(payload)


class _Router:
    def should_forward(self): return False


def _proposal_msg(object_ids: List[str], source=2):
    infos = [ProposalInfo(p_id=f"p-{oid}", object_id=oid, cost=1.0, agent_id=str(source))
             for oid in object_ids]
    return Proposal(source=source, agents=[AgentInfo(agent_id=source)], proposals=infos)


class MissingJobBatchResilienceTests(unittest.TestCase):
    """A proposal for a missing job must not abort the remaining batch."""

    def test_batch_survives_host_without_pending_hooks(self):
        # job-b exists locally; job-a does NOT (raced ahead of the job write).
        jobs = {"job-b": _FakeJob("job-b")}
        host = _HostNoPending(jobs)
        transport = _Transport()
        eng = ConsensusEngine(agent_id=1, host=host, transport=transport, router=_Router())

        eng.on_proposal(_proposal_msg(["job-a", "job-b"]))

        # job-b was still processed: a PREPARE went out for it, state advanced.
        self.assertEqual(len(transport.broadcasts), 1)
        prepare = transport.broadcasts[0]
        self.assertIsInstance(prepare, Prepare)
        self.assertEqual([p.object_id for p in prepare.proposals], ["job-b"])
        self.assertEqual(jobs["job-b"].state, ObjectState.PREPARE)
        # And the missing-hook condition was surfaced, not raised.
        self.assertTrue(any("set_pending_proposal" in w for w in host.warns))

    def test_pending_hook_receives_missing_job_messages(self):
        jobs = {"job-b": _FakeJob("job-b")}
        host = _HostWithPending(jobs)
        transport = _Transport()
        eng = ConsensusEngine(agent_id=1, host=host, transport=transport, router=_Router())

        eng.on_proposal(_proposal_msg(["job-a", "job-b"]))

        self.assertIn(("proposal", "job-a"), host.pending)
        # job-b still processed normally.
        self.assertEqual(jobs["job-b"].state, ObjectState.PREPARE)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
