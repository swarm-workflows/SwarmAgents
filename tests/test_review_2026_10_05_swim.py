"""Code review 2026-10-05 §11: SWIM membership and recovery, over a real in-process bus.

(a) Members were seeded from the host's peers only while SWIM knew nobody, so a late registrant
    was learned only through third-party piggyback.
(b) A ping from a peer held FAILED — first-hand proof of life — changed nothing, so a false
    verdict was undone only if the victim heard its own rumour inside the piggyback window.
(c) A relay forwarded the target's ack with target_agent=None (on_ack had already popped the
    probe), so a late relayed ack could not clear a suspicion.
"""
import os
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)
sys.path.insert(0, os.path.join(REPO, "tests"))

from swarm.consensus.messages.swim_ack import SwimAck  # noqa: E402
from swarm.consensus.messages.swim_ping import SwimPing  # noqa: E402
from swarm.consensus.messages.swim_ping_req import SwimPingReq  # noqa: E402
from swarm.membership.swim import ALIVE, FAILED, _Member  # noqa: E402
from test_swim import _Bus, _Clock, _make_swim  # noqa: E402


def test_a_late_registrant_is_admitted_on_the_next_pick():
    clock, bus = _Clock(), _Bus()
    a, host_a = _make_swim(1, bus, [1, 2, 3], clock)
    a._pick_probe_target()                         # knows 2, 3
    host_a._peers.append(4)                        # agent 4 registers later
    a._pick_probe_target()
    assert a.status_of(4) == ALIVE


def test_admission_never_overrides_a_verdict():
    clock, bus = _Clock(), _Bus()
    a, _ = _make_swim(1, bus, [1, 2, 3], clock)
    with a._lock:
        a._members[2] = _Member(agent_id=2, status=FAILED, incarnation=3)
    a._pick_probe_target()
    assert a.status_of(2) == FAILED


def test_a_falsely_failed_agent_recovers_by_pinging():
    """Agent 1 holds 2 FAILED. 2 pings 1; 1's ack carries the rumour; 2 refutes with a higher
    incarnation; 2's next ping carries the refutation; 1 accepts it."""
    clock, bus = _Clock(), _Bus()
    one, _ = _make_swim(1, bus, [1, 2], clock)
    two, _ = _make_swim(2, bus, [1, 2], clock)
    with one._lock:
        one._members[2] = _Member(agent_id=2, status=FAILED, incarnation=0)

    two.host.send(1, SwimPing(source=2, probe_id="p1", updates=two._fresh_piggy()))
    assert two._self_incarnation >= 1                      # it heard and refuted
    two.host.send(1, SwimPing(source=2, probe_id="p2", updates=two._fresh_piggy()))
    assert one.status_of(2) == ALIVE


def test_an_unknown_pinger_is_admitted():
    clock, bus = _Clock(), _Bus()
    one, _ = _make_swim(1, bus, [1], clock)
    _make_swim(9, bus, [1, 9], clock)
    bus.deliver(9, 1, SwimPing(source=9, probe_id="p", updates=[]))
    assert one.status_of(9) == ALIVE


def test_a_relayed_ack_names_its_target():
    """1 asks relay 2 to probe 3; 3 acks to 2; 2 forwards to 1 — with target 3, not None."""
    clock, bus = _Clock(), _Bus()
    one, _ = _make_swim(1, bus, [1, 2, 3], clock)
    two, _ = _make_swim(2, bus, [1, 2, 3], clock)
    _make_swim(3, bus, [1, 2, 3], clock)
    forwarded = []
    real = bus.deliver

    def spy(src, dst, payload):
        if isinstance(payload, SwimAck) and src == 2 and dst == 1:
            forwarded.append(payload.target_agent)
        real(src, dst, payload)
    bus.deliver = spy
    two.on_ping_req(SwimPingReq(source=1, probe_id="r1", target_agent=3, updates=[]))
    clock.advance(0.06)
    two._handle_expirations(clock())
    assert forwarded == [3]
