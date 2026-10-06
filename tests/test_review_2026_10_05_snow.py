"""Code review 2026-10-05 §1-§4: the shipped Snow configuration did not do what the design says.

§1  A Snow peer answered a query with its BASE cost while the initiator advertised its cost
    times `_projected_load_factor`. Under any load every peer could undercut the initiator, every
    peer voted for itself, no candidate reached α, and the job abandoned to the reselection
    timeout (300 s as shipped). Snow placement ignored load and feasibility; PBFT's did not.
§2  An agent's OWN AgentInfo held its DTNs as raw config dicts (the setter's list branch did not
    convert), `compute_job_cost` raised on `dtn.name`, and `native_cost_for_job` swallowed that
    into "no opinion" — on every job that needed a DTN.
§3  "No opinion" was sent as a vote for the initiator, so §2 (and every LLM-cache miss) became
    an endorsement of whoever asked first.
§4  `aggressive_failure_detection: true` was shipped (code default False): one 0.7 s health
    probe timeout failed a live peer and re-ran its jobs. And the gRPC UP callback — which fired
    on every probe, not on transitions — was the only thing that ever readmitted a failed peer,
    heartbeat verdicts included, so a zombie whose server still answered was readmitted to
    broadcast while `neighbor_map` (the quorum denominator) still excluded it.
"""
import math
import os
import sys
import threading
from unittest.mock import MagicMock

import numpy as np
import pytest
import yaml

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)
sys.path.insert(0, os.path.join(REPO, "tests"))

from swarm.agents.resource_agent import ResourceAgent  # noqa: E402
from swarm.consensus.messages.proposal_info import ProposalInfo  # noqa: E402
from swarm.consensus.messages.snow_response import SnowResponse  # noqa: E402
from swarm.models.agent_info import AgentInfo  # noqa: E402
from swarm.models.capacities import Capacities  # noqa: E402
from swarm.models.data_node import DataNode  # noqa: E402
from swarm.models.job import Job  # noqa: E402
from swarm.selection.penalties import apply_multiplicative_penalty  # noqa: E402
from swarm.utils.thread_safe_dict import ThreadSafeDict  # noqa: E402

from test_snow import _latest_query_item, _make_engine  # noqa: E402

TOTAL = Capacities(core=8, ram=32, disk=500)

# The shape `generate_configs.py --dtns` writes into an agent's YAML, and what
# `_generate_agent_info` hands to AgentInfo as `dtns=self.config.get("dtns")`.
CONFIG_DTNS = [
    {"name": "dtn1", "ip": "192.168.100.1", "user": "dtn_user1", "connectivity_score": 0.9},
    {"name": "dtn4", "ip": "192.168.100.4", "user": "dtn_user4", "connectivity_score": 0.5},
]


def _job(job_id="j1", dtn_in=(), cores=2, ram=8, disk=50):
    j = Job()
    j.job_id = job_id
    j.capacities = Capacities(core=cores, ram=ram, disk=disk)
    j.wall_time = 1.0
    for n in dtn_in:
        j.add_incoming_data_dep(DataNode(name=n))
    return j


def _agent(info: AgentInfo, pending: dict, feasible=True):
    """A ResourceAgent with real cost, wire and proposal paths; feasibility stubbed."""
    a = ResourceAgent.__new__(ResourceAgent)
    a.logger = MagicMock()
    a.agent_id = info.agent_id
    a.cpu_weight, a.ram_weight, a.disk_weight = 0.4, 0.3, 0.3
    a.gpu_weight, a.qpu_weight = 0.0, 0.0
    a.long_job_threshold = 20.0
    a.connectivity_penalty_factor = 1.0
    a.quantum_penalty_factor = 1.0
    a.split_comm_penalty_factor = 0.0
    a.last_agent_info = info
    a.queues = MagicMock()
    a.queues.pending_queue.get.side_effect = lambda oid: pending.get(oid)
    a.is_job_feasible = (lambda job, agent: feasible) if not callable(feasible) else feasible
    return a


def _info(agent_id=2, load=0.0, proposed=0.0, dtns=None):
    return AgentInfo(agent_id=agent_id, capacities=TOTAL, load=load, proposed_load=proposed,
                     dtns=dtns if dtns is not None else [])


# --------------------------------------------------------------------------- #
# §2 — an agent's own DTNs are DataNodes, so it can price a DTN job
# --------------------------------------------------------------------------- #

class TestOwnDtnsAreDataNodes:
    def test_config_list_becomes_datanodes(self):
        info = _info(dtns=CONFIG_DTNS)
        assert set(info.dtns) == {"dtn1", "dtn4"}
        assert all(isinstance(v, DataNode) for v in info.dtns.values())
        assert info.dtns["dtn1"].connectivity_score == pytest.approx(0.9)

    def test_in_process_and_round_tripped_agree(self):
        """The two copies used to differ in type: dicts in-process, DataNodes after Redis."""
        info = _info(dtns=CONFIG_DTNS)
        back = AgentInfo.from_dict(info.to_dict())
        assert {k: v.to_dict() for k, v in info.dtns.items()} == \
               {k: v.to_dict() for k, v in back.dtns.items()}

    def test_no_dtns_is_empty_not_an_error(self):
        """`self.config.get("dtns")` is None for a fleet generated without --dtns."""
        assert AgentInfo(agent_id=1, dtns=None).dtns == {}

    def test_a_dict_of_datanodes_is_accepted(self):
        dn = DataNode(name="d", connectivity_score=0.5)
        assert AgentInfo(agent_id=1, dtns={"d": dn}).dtns["d"] is dn

    def test_an_agent_prices_a_dtn_job_on_its_own_info(self):
        """The defect end to end: this returned None (an abstention, formerly an endorsement)
        for every job that needed a DTN, because `compute_job_cost` raised on a dict."""
        job = _job(dtn_in=["dtn1"])
        a = _agent(_info(dtns=CONFIG_DTNS), {"j1": job})
        got = a.native_cost_for_job("j1")
        assert got is not None
        assert math.isfinite(got[0]) and got[0] > 0


# --------------------------------------------------------------------------- #
# §1 — a peer answers in the units the initiator proposes in
# --------------------------------------------------------------------------- #

def _proposal_cost_as_selection_main_builds_it(agent: ResourceAgent, job: Job, info: AgentInfo):
    """Mirror of `selection_main`: base matrix → × load factor → proposal_cost."""
    base = np.array([[agent._cost_job_on_agent(job, info)]])
    penalised = apply_multiplicative_penalty(cost_matrix=base, assignees=[info],
                                             factor_fn=agent._projected_load_factor)
    return agent.proposal_cost(job, float(penalised[0, 0]))


class TestWireCostMatchesProposalCost:
    @pytest.mark.parametrize("load,proposed", [(0.0, 0.0), (40.0, 10.0), (90.0, 30.0)])
    def test_wire_cost_is_the_cost_this_agent_would_propose(self, load, proposed):
        job = _job()
        info = _info(load=load, proposed=proposed)
        a = _agent(info, {"j1": job})
        assert a.wire_cost_for_job("j1") == \
            pytest.approx(_proposal_cost_as_selection_main_builds_it(a, job, info))

    def test_wire_cost_carries_the_load_factor(self):
        job = _job()
        idle = _agent(_info(load=0.0), {"j1": job}).wire_cost_for_job("j1")
        busy = _agent(_info(load=80.0, proposed=20.0), {"j1": job}).wire_cost_for_job("j1")
        assert busy == pytest.approx(round(idle * 2.0, 2), abs=0.02)   # 1 + (100/100)^1.5 = 2

    def test_the_old_undercut_no_longer_happens(self):
        """Two identical agents at the same load. The initiator advertises its penalised
        cost; the peer used to answer its base cost, which is strictly lower whenever load > 0,
        so it voted for itself. Now the two numbers are equal and the tie-break decides,
        the same way it decided which of them proposed."""
        job = _job()
        info_init = _info(agent_id=1, load=50.0)
        info_peer = _info(agent_id=2, load=50.0)
        advertised = _proposal_cost_as_selection_main_builds_it(
            _agent(info_init, {"j1": job}), job, info_init)
        peer = _agent(info_peer, {"j1": job})
        assert peer.wire_cost_for_job("j1") == pytest.approx(advertised)
        assert peer.native_cost_for_job("j1")[0] < advertised   # the number it used to send

    def test_an_infeasible_peer_answers_infinity(self):
        a = _agent(_info(), {"j1": _job()}, feasible=False)
        assert a.wire_cost_for_job("j1") == float("inf")

    def test_a_job_not_in_the_pending_queue_is_no_opinion(self):
        assert _agent(_info(), {}).wire_cost_for_job("j1") is None

    def test_feasibility_is_checked_against_this_agents_own_info(self):
        seen = []
        info = _info(agent_id=7)
        a = _agent(info, {"j1": _job()}, feasible=lambda job, ag: seen.append(ag) or True)
        a.wire_cost_for_job("j1")
        assert seen == [info]


# --------------------------------------------------------------------------- #
# §3 — no opinion is an abstention, and α is taken over the voters
# --------------------------------------------------------------------------- #

class TestAbstention:
    def test_no_opinion_names_no_candidate(self):
        eng = _make_engine(agent_id=2, peers=(1, 3, 4), my_cost=None)[0]
        ans = eng._answer_query("q", "j", q_preferred=1, q_cost=25.0)
        assert ans["preferred_agent"] is None and ans["cost"] is None

    def test_an_infeasible_peer_never_prefers_itself_even_on_a_tie(self):
        """An initiator that sent no cost is +inf; an infeasible peer is +inf too. The tie
        used to fall to tiebreak_rank, which can pick the peer that cannot run the job."""
        for agent_id in range(2, 40):
            eng = _make_engine(agent_id=agent_id, peers=(1,), my_cost=float("inf"))[0]
            ans = eng._answer_query("q", f"job-{agent_id}", q_preferred=1, q_cost=None)
            assert ans["preferred_agent"] == 1

    def test_an_infeasible_peer_yields_to_a_finite_initiator(self):
        eng = _make_engine(agent_id=2, peers=(1,), my_cost=float("inf"))[0]
        assert eng._answer_query("q", "j", q_preferred=1, q_cost=30.0)["preferred_agent"] == 1

    @staticmethod
    def _round(eng, transport, job_id, answers):
        eng._tick(now=0.0)
        q = _latest_query_item(transport, job_id)
        for peer, choice in answers:
            eng.on_snow_response(SnowResponse(
                source=peer, query_id=q["query_id"], job_id=job_id,
                preferred_agent=choice, cost=None if choice is None else 1.0,
                already_decided=False))
        eng._tick(now=0.0)

    def test_abstentions_do_not_carry_the_initiator(self):
        """Four peers sampled: one votes for a rival, three abstain. Before the fix the three
        were votes for the initiator and it won 3-1. Now nobody has an opinion but the rival's
        supporter, the rival takes the round, and the initiator does not finalize itself."""
        eng, host, transport, cas = _make_engine(agent_id=1, peers=(2, 3, 4, 5), k=4,
                                                 alpha=0.7, beta=2)
        eng.propose([ProposalInfo(p_id="p", object_id="job-a", cost=10.0, agent_id="1")])
        for _ in range(2):
            self._round(eng, transport, "job-a", [(2, 2), (3, None), (4, None), (5, None)])
        assert cas.get("job-a") != 1
        assert host.leader_events == []

    def test_alpha_is_over_voters_so_abstentions_cannot_stall_a_round(self):
        """Two voters agree, two abstain. α=0.7 over 4 sampled would need 3 votes and stall;
        over the 2 that voted it needs 2 and the round succeeds."""
        eng, host, transport, cas = _make_engine(agent_id=1, peers=(2, 3, 4, 5), k=4,
                                                 alpha=0.7, beta=2)
        eng.propose([ProposalInfo(p_id="p", object_id="job-b", cost=10.0, agent_id="1")])
        for _ in range(2):
            self._round(eng, transport, "job-b", [(2, 1), (3, 1), (4, None), (5, None)])
        assert cas.get("job-b") == 1
        assert host.leader_events == ["job-b"]

    def test_silence_is_not_an_abstention(self):
        """A peer that never answers stays in the denominator: otherwise one fast responder
        would decide every round on its own. Two of four answer (both for the initiator), two
        are silent — 2 < round(0.7 × 4) = 3 — so no round is won."""
        eng, host, transport, cas = _make_engine(agent_id=1, peers=(2, 3, 4, 5), k=4,
                                                 alpha=0.7, beta=2)
        eng.propose([ProposalInfo(p_id="p", object_id="job-c", cost=10.0, agent_id="1")])
        for t in (0.0, 1.0, 2.0):
            eng._tick(now=t)
            q = _latest_query_item(transport, "job-c")
            for peer in (2, 3):
                eng.on_snow_response(SnowResponse(
                    source=peer, query_id=q["query_id"], job_id="job-c",
                    preferred_agent=1, cost=10.0, already_decided=False))
            eng._tick(now=t + 0.6)     # past the round deadline: evaluate with 2 of 4
        assert cas.get("job-c") is None

    def test_an_abstention_survives_the_wire(self):
        from swarm.consensus.messages.snow_batch import SnowResponseBatch
        import json
        b = SnowResponseBatch(source=3, items=[{"query_id": "q", "job_id": "j",
                                                "preferred_agent": None, "cost": None,
                                                "already_decided": False}])
        assert json.loads(b.to_json())["items"][0]["preferred_agent"] is None


# --------------------------------------------------------------------------- #
# §4 — gRPC health is a hint; heartbeat decides both directions
# --------------------------------------------------------------------------- #

class TestShippedDefaults:
    def test_aggressive_detection_is_off_in_the_shipped_config(self):
        with open(os.path.join(REPO, "config_swarm_multi.yml")) as f:
            cfg = yaml.safe_load(f)
        assert cfg["runtime"]["aggressive_failure_detection"] is False

    def test_shipped_value_matches_the_code_default(self):
        a = ResourceAgent.__new__(ResourceAgent)
        a.runtime_config = {}
        assert a.aggressive_failure_detection is False


class TestHealthEventsAreTransitions:
    def _pool(self):
        from swarm.comm.grpc_client import ChannelPool, ChannelEntry
        events = []
        pool = ChannelPool.__new__(ChannelPool)
        pool._lock = threading.Lock()
        pool._on_status = lambda target, up, reason: events.append((target, up))
        entry = ChannelEntry.__new__(ChannelEntry)
        entry.target, entry.up = "h:1", False
        pool._entries = {"h:1": entry}
        return pool, events

    def test_repeated_probes_emit_once(self):
        pool, events = self._pool()
        for _ in range(5):
            pool._set_up("h:1", True, "health=1")
        for _ in range(5):
            pool._set_up("h:1", False, "health_rpc=DEADLINE_EXCEEDED")
        pool._set_up("h:1", True, "health=1")
        assert events == [("h:1", True), ("h:1", False), ("h:1", True)]

    def test_an_unknown_target_emits_nothing(self):
        pool, events = self._pool()
        pool._set_up("other:2", False, "x")
        assert events == []


def _recovery_agent():
    a = ResourceAgent.__new__(ResourceAgent)
    a.logger = MagicMock()
    a.shutdown = False
    a.runtime_config = {}
    a.failed_agents = ThreadSafeDict()
    a._failed_last_seen = {}
    a.metrics = MagicMock()
    a.metrics.agent_recoveries = []
    a.neighbor_map = ThreadSafeDict()
    return a


class TestReadmissionIsByHeartbeat:
    def test_a_grpc_up_no_longer_readmits(self):
        """A process whose periodic thread died still answers health checks."""
        a = _recovery_agent()
        a.failed_agents.set(5, 1000.0)
        a.on_peer_status("h:5", True, "health=1")
        assert 5 in a.failed_agents

    def test_a_fresher_record_readmits(self):
        a = _recovery_agent()
        a.failed_agents.set(5, 1000.0)
        a._note_failed_last_seen(5, AgentInfo(agent_id=5, last_updated=500.0))
        assert a._readmit_if_heartbeat_resumed(AgentInfo(agent_id=5, last_updated=501.0))
        assert 5 not in a.failed_agents
        assert 5 not in a._failed_last_seen
        assert [r["agent_id"] for r in a.metrics.agent_recoveries] == [5]

    def test_the_same_record_does_not_readmit(self):
        """The stale record that caused the verdict is still in Redis until its TTL; reading
        it again must not undo the verdict."""
        a = _recovery_agent()
        a.failed_agents.set(5, 1000.0)
        a._note_failed_last_seen(5, AgentInfo(agent_id=5, last_updated=500.0))
        assert not a._readmit_if_heartbeat_resumed(AgentInfo(agent_id=5, last_updated=500.0))
        assert 5 in a.failed_agents

    def test_comparison_is_on_the_peers_clock_not_ours(self):
        """Our detection time (1000) is on OUR clock; the peer's stamps are on ITS clock, here
        running 600 s behind. A fresh heartbeat at 501 is still newer than 500 and readmits;
        comparing against our detection time would have kept it out forever."""
        a = _recovery_agent()
        a.failed_agents.set(5, 1000.0)
        a._note_failed_last_seen(5, AgentInfo(agent_id=5, last_updated=500.0))
        assert a._readmit_if_heartbeat_resumed(AgentInfo(agent_id=5, last_updated=501.0))

    def test_recovery_disabled_never_readmits(self):
        a = _recovery_agent()
        a.runtime_config = {"enable_agent_recovery": False}
        a.failed_agents.set(5, 1000.0)
        a._note_failed_last_seen(5, AgentInfo(agent_id=5, last_updated=500.0))
        assert not a._readmit_if_heartbeat_resumed(AgentInfo(agent_id=5, last_updated=900.0))

    def test_a_record_with_no_stamp_does_not_readmit(self):
        a = _recovery_agent()
        a.failed_agents.set(5, 1000.0)
        assert not a._readmit_if_heartbeat_resumed(AgentInfo(agent_id=5))   # last_updated 0.0

    def test_both_detectors_record_the_judged_stamp(self):
        src = open(os.path.join(REPO, "swarm/agents/resource_agent.py")).read()
        hb = src[src.index("def _detect_failed_agents"):src.index("def _remove_failed_agents")]
        start = src.index("def on_peer_status")
        grpc = src[start:src.index("def calculate_quorum", start)]
        assert "_note_failed_last_seen(agent_id, agent_info)" in hb
        assert "_note_failed_last_seen(failed_agent_id, failed_agent_info)" in grpc
        assert "failed_agents.remove" not in grpc
