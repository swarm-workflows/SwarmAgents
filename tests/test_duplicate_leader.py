"""One job, one leader selection per agent (first slice pilot under Snow, 2026-10-07).

The engine's finalize and the claim sweep (`_adopt_unannounced_claims`) can both elect this
agent for one job. The sweep's "undecided" snapshot was taken before the finalize recorded the
decision, so both called `select_job` and the job ran twice on the same agent — 15 of 600 jobs
in the Hier-30 Snow pilot. `on_leader_elected` now goes through a check-and-set.
"""
import os
import sys
import threading
from unittest.mock import MagicMock

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)
sys.path.insert(0, os.path.join(REPO, "tests"))

from swarm.agents.resource_agent import ResourceAgent, _HostAdapter  # noqa: E402
from swarm.consensus.gossip_engine import GossipConsensusEngine  # noqa: E402
from swarm.models.job import Job, ObjectState  # noqa: E402
from swarm.queue.simple_queue import SimpleQueue  # noqa: E402


def _agent():
    a = ResourceAgent.__new__(ResourceAgent)
    a.logger = MagicMock()
    a.agent_id = 7
    a._init_decision_state()
    a.select_job = MagicMock()
    a.job_assignments = MagicMock()
    return a


def _job(jid="31"):
    j = Job()
    j.job_id = jid
    j.wall_time = 1.0
    j.state = ObjectState.PENDING
    return j


def test_a_second_leader_election_for_one_job_is_refused():
    a = _agent()
    host = _HostAdapter(a)
    host.on_leader_elected(_job(), "p1")
    host.on_leader_elected(_job(), "adopted-claim")
    assert a.select_job.call_count == 1
    assert a.duplicate_leader_refusals == 1


def test_the_claim_sweep_after_the_finalize_does_not_select_again():
    """The pilot's interleaving: finalize elects, then the sweep reads the claim and adopts."""
    a = _agent()
    host = _HostAdapter(a)
    a.engine = MagicMock(spec=GossipConsensusEngine)
    a.engine.host = host
    a.queues = MagicMock()
    a.queues.pending_queue = SimpleQueue()
    a.queues.pending_queue.add(_job())
    a.topology = MagicMock(level=0, group=0)
    a.repository = MagicMock()
    a.repository.get_assignments.return_value = {"31": 7}
    # The sweep's snapshot is taken first, while the job is still undecided...
    real_lock = a.completed_lock

    class _Hook:
        def __enter__(self):
            return real_lock.__enter__()

        def __exit__(self, *exc):
            out = real_lock.__exit__(*exc)
            if not getattr(self, "fired", False):
                self.fired = True
                host.on_leader_elected(_job(), "p1")     # ...then the finalize lands
            return out

    a.completed_lock = _Hook()
    a._adopt_unannounced_claims()
    a.completed_lock = real_lock
    assert a.select_job.call_count == 1, "the job was selected twice"


def test_a_reset_lets_a_genuine_re_election_through():
    a = _agent()
    a.engine = MagicMock()
    host = _HostAdapter(a)
    host.on_leader_elected(_job(), "p1")
    a._forget_decided("31")                      # what every reset path does
    host.on_leader_elected(_job(), "p2")
    assert a.select_job.call_count == 2


def test_concurrent_elections_select_exactly_once():
    a = _agent()
    host = _HostAdapter(a)
    threads = [threading.Thread(target=host.on_leader_elected, args=(_job(), f"p{i}"))
               for i in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert a.select_job.call_count == 1
