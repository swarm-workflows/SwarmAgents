"""P0-4: the counters the campaign's figures are computed from.

The one metric here that did not exist in any form before is **context age at decision
time** — how old a coordinator's view of its child groups was at the instant it delegated.
The conference paper's only interaction claim is that faster finalization leaves a
coordinator deciding on younger context, and no pre-existing counter can be made to yield
it, so a defect in this file is a figure that cannot be produced rather than one that looks
slightly wrong.

What these tests hold to:

* the age is taken at the **decision**, not when the view was built — under
  `delegation.policy: llm` those are seconds apart and the gap is the whole effect;
* an unknown age is counted as unknown, never substituted with a large one, or the staleness
  tail would sit exactly where the dead groups are;
* a negative age is clock skew, is clamped, and is counted, so a run with an NTP problem is
  visible rather than quietly biased;
* adding the two timestamps to `GroupSnapshot` does not move the bandit's feature layout,
  because a changed `schema_version` discards every persisted LinUCB model;
* one record per delegation even though the LLM path re-enters the bandit on five different
  fallback routes, and a fallback is recorded as the rule that actually decided;
* a drop is not a send, a failed inference is not free, and a `None` quantile never reaches
  the Prometheus file (node_exporter rejects the whole file on one bad line).
"""
import os
import sys
import tempfile
import threading
import time

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from swarm.consensus.engine import ConsensusEngine  # noqa: E402
from swarm.consensus.messages.proposal_info import ProposalInfo  # noqa: E402
from swarm.rl.context import ContextExtractor, GroupSnapshot, snapshots_from_children  # noqa: E402
from swarm.utils.instrumentation import (ContextAge, DecisionLog, DecisionRecord,  # noqa: E402
                                         LlmUsage, MessageCounters, RunningStats,
                                         context_age, extract_usage, flatten_for_prom,
                                         render_prom, write_textfile)


# --------------------------------------------------------------------------------------------
# Where the age comes from: child records, stamped on the child's clock.
# --------------------------------------------------------------------------------------------

class _Caps:
    def __init__(self, core=8, ram=32, gpu=0):
        self.core, self.ram, self.gpu = core, ram, gpu


class _Child:
    def __init__(self, group, last_updated, core=8, used=0):
        self.group = group
        self.last_updated = last_updated
        self.capacities = _Caps(core=core)
        self.capacity_allocations = _Caps(core=used, ram=0, gpu=0)


def test_snapshot_carries_the_freshest_and_stalest_child_stamp():
    """One quiet child in a healthy group: fresh on `observed_at`, stale on the other.

    Both ends are kept because they answer different questions — how recently the coordinator
    heard anything about the group, and how stale the worst contributor to its headroom
    numbers is. A group of nine where one child went silent is genuinely both.
    """
    now = time.time()
    snaps = snapshots_from_children(
        [_Child(1, now - 1.0), _Child(1, now - 90.0), _Child(2, now - 2.0)], [])
    assert snaps[1].observed_at == pytest.approx(now - 1.0)
    assert snaps[1].oldest_observed_at == pytest.approx(now - 90.0)
    assert snaps[2].observed_at == snaps[2].oldest_observed_at == pytest.approx(now - 2.0)


def test_a_group_with_no_timestamps_has_an_unknown_age_not_a_large_one():
    """`last_updated` defaults to 0.0 on AgentInfo, so an untimestamped child must not be
    read as "observed at the epoch" — that is a 56-year age landing in the p99."""
    snaps = snapshots_from_children([_Child(1, 0.0)], [])
    assert snaps[1].observed_at is None
    age = context_age(snaps, selected=[1])
    assert age.unknown == 1
    assert age.mean is None and age.max is None
    assert age.remote_unknown == 1 and age.remote_mean is None


def test_a_wall_clock_step_cannot_reach_the_headline_age():
    """One host is not enough — it has to be one host's MONOTONIC clock.

    The repaired slice steps its wall clock whenever it drifts past 0.1 s (`makestep 0.1
    -1`), so a wall-clock pair spanning a correction is wrong by the size of the step and
    can come out negative. Here the wall clock jumps back 2 s between the stamp and the
    decision; the monotonic pair is unmoved.
    """
    mono = time.monotonic()
    wall = time.time()
    snaps = {1: GroupSnapshot(received_at=mono - 3.0, observed_at=wall - 3.0)}
    # Decision taken 3 s later on the monotonic clock, but the wall clock was stepped back
    # 2 s in the meantime, so wall-clock arithmetic would report 1 s.
    age = context_age(snaps, [1], now=wall - 2.0, now_monotonic=mono)
    assert age.mean == pytest.approx(3.0), "monotonic pair must ignore the step"
    assert age.remote_mean == pytest.approx(1.0), "the wall-clock series absorbs it"


def test_local_stamps_come_from_the_seen_at_map_not_the_child_record():
    """The coordinator's own delivery time is what the headline age is measured from, so it
    has to come from the caller's map. It is deliberately NOT an AgentInfo field: that
    object round-trips through Redis, and a local observation written back would be read by
    peers as if it meant something to them."""
    now = time.time()
    child = _Child(1, now - 30.0)
    child.agent_id = 7
    snaps = snapshots_from_children([child], [], seen_at={7: now - 2.0})
    assert snaps[1].observed_at == pytest.approx(now - 30.0)
    assert snaps[1].received_at == pytest.approx(now - 2.0)


def test_unknown_groups_are_counted_but_never_averaged_in():
    mono = time.monotonic()
    snaps = {1: GroupSnapshot(received_at=mono - 5.0), 2: GroupSnapshot()}
    age = context_age(snaps, selected=[1], now_monotonic=mono)
    assert age.unknown == 1
    assert age.mean == pytest.approx(5.0)
    assert age.max == pytest.approx(5.0)


def test_clock_skew_cannot_reach_the_headline_age():
    """The measurement this whole item exists to produce has to survive the fleet it runs
    on. Measured on the slice 2026-09-14: 66 of 92 hosts had not reached an NTP server in 30
    days and were free-running 0.4-1.1 s apart, which drove 229 of one coordinator's 229
    decisions to a negative remote age and pinned its whole column at zero. The headline age
    takes both terms from the coordinator's own clock, so the offset cancels exactly."""
    now, mono = time.time(), time.monotonic()
    # A child whose clock runs 1.1 s fast, whose record the coordinator took delivery of 4 s
    # ago. Only the remote reading is corrupted.
    snaps = {1: GroupSnapshot(observed_at=now + 1.1, received_at=mono - 4.0)}
    age = context_age(snaps, selected=[1], now=now, now_monotonic=mono)
    assert age.mean == pytest.approx(4.0), "local age must be untouched by the offset"
    assert age.skewed == 1
    assert age.remote_mean == 0.0
    assert age.skew_max_s == pytest.approx(1.1, abs=1e-3)


def test_the_skew_magnitude_is_recorded_not_just_the_count():
    """A count alone cannot tell a 1 ms artefact from a 1.1 s free-running clock, which is
    exactly the judgement a reader has to make about whether the remote series is usable."""
    now, mono = time.time(), time.monotonic()
    snaps = {1: GroupSnapshot(observed_at=now + 0.002, received_at=mono - 1.0),
             2: GroupSnapshot(observed_at=now + 0.9, received_at=mono - 1.0)}
    age = context_age(snaps, selected=[1], now=now, now_monotonic=mono)
    assert age.skewed == 2
    assert age.skew_max_s == pytest.approx(0.9, abs=1e-3)


def test_a_negative_local_age_is_impossible_by_construction():
    """`received_at` is stamped by this agent before the decision it is compared against, so
    the headline series has no skew term at all — not a small one, none."""
    mono = time.monotonic()
    age = context_age({1: GroupSnapshot(received_at=mono - 0.001)},
                      selected=[1], now_monotonic=mono)
    assert age.mean >= 0.0
    assert age.skewed == 0


def test_chosen_age_covers_only_the_groups_delegated_to():
    mono = time.monotonic()
    snaps = {1: GroupSnapshot(received_at=mono - 1.0),
             2: GroupSnapshot(received_at=mono - 11.0)}
    age = context_age(snaps, selected=[2], now_monotonic=mono)
    assert age.chosen == pytest.approx(11.0)
    assert age.mean == pytest.approx(6.0)


def test_age_is_taken_at_the_decision_not_at_the_build():
    """The whole point. A snapshot built 2 s before the decision is 2 s older at the
    decision, and under `delegation.policy: llm` that gap is the inference itself. Freezing
    the age at build time would define the effect the interaction claim tests out of
    existence."""
    built_at = time.monotonic()
    snaps = {1: GroupSnapshot(received_at=built_at - 3.0)}
    at_build = context_age(snaps, [1], now_monotonic=built_at)
    at_decision = context_age(snaps, [1], now_monotonic=built_at + 2.0)
    assert at_build.mean == pytest.approx(3.0)
    assert at_decision.mean == pytest.approx(5.0)


def test_the_new_snapshot_fields_do_not_move_the_bandit_feature_layout():
    """`schema_version` fingerprints the feature layout and persisted LinUCB state is
    DISCARDED when it changes. Adding two timestamps to GroupSnapshot must therefore be
    invisible to the extractor, or every deployed model silently resets to cold start."""
    extractor = ContextExtractor({"job_types": ["cpu_short_low"]})
    assert "observed_at" not in extractor.feature_names
    assert "oldest_observed_at" not in extractor.feature_names
    assert extractor.dim == len(extractor.feature_names)
    fresh = GroupSnapshot(cpu_headroom=0.5, observed_at=time.time())
    stale = GroupSnapshot(cpu_headroom=0.5, observed_at=time.time() - 600.0)
    assert list(extractor.group_features(fresh)) == list(extractor.group_features(stale))


# --------------------------------------------------------------------------------------------
# The decision log.
# --------------------------------------------------------------------------------------------

def _record(policy="bandit", age=None, decide_s=0.1, job_id="j"):
    return DecisionRecord(ts=time.time(), job_id=job_id, job_type="cpu_short_low",
                          policy=policy, n_candidates=3, candidates=[1, 2, 3],
                          selected=[1], decide_s=decide_s,
                          age=age or ContextAge(mean=2.0, min=1.0, max=3.0, chosen=2.0,
                                        remote_mean=2.1))


def test_decision_log_summary_counts_by_policy():
    log = DecisionLog()
    for policy in ("bandit", "bandit", "llm"):
        log.add(_record(policy=policy))
    summary = log.summary()
    assert summary["decisions"] == 3
    assert summary["by_policy"] == {"bandit": 2, "llm": 1}
    assert summary["ctx_age_mean"] == pytest.approx(2.0)


def test_the_ring_drops_the_oldest_rows_and_the_aggregate_stays_exact():
    """When the bound binds it is the cold-start exploration that goes, not the converged
    regime the figure is about. The aggregate covers every decision either way, so a
    truncated log never silently shortens the distribution it reports."""
    log = DecisionLog(max_records=2)
    for i in range(5):
        log.add(_record(job_id=f"j{i}"))
    kept = [r["job_id"] for r in log.records()]
    assert kept == ["j3", "j4"]
    summary = log.summary()
    assert summary["decisions"] == 5
    assert summary["records_kept"] == 2
    assert summary["records_dropped"] == 3
    assert summary["ctx_age_n"] == 5


def test_a_record_with_no_age_does_not_enter_the_age_distribution():
    log = DecisionLog()
    log.add(_record(age=ContextAge(unknown=3)))
    summary = log.summary()
    assert summary["decisions"] == 1
    assert summary["ctx_age_n"] == 0
    assert summary["ctx_unknown_groups"] == 3


# --------------------------------------------------------------------------------------------
# Message accounting.
# --------------------------------------------------------------------------------------------

def test_a_dropped_send_is_not_a_sent_message():
    """A fan-out abandoned because the broadcast pool was saturated never reached the
    network. Folding it into `sent` would understate the message cost per *delivered*
    message of exactly the saturated coordinator the scaling figure is about."""
    counters = MessageCounters()
    counters.record_sent("PREPARE", 120)
    counters.record_dropped("PREPARE")
    snap = counters.snapshot()
    assert snap["sent_msgs"] == 1
    assert snap["sent_bytes"] == 120
    assert snap["dropped_msgs"] == 1
    assert snap["dropped_by_type"] == {"PREPARE": 1}


def test_counters_do_not_lose_increments_under_concurrency():
    """`d[k] = d.get(k, 0) + 1` is not atomic across threads even under the GIL, and the
    broadcast pool bumps these from 16 workers at once. Without the lock the message counts
    undercount in precisely the saturated runs the figure exists to show."""
    counters = MessageCounters()
    per_thread = 2000

    def bump():
        for _ in range(per_thread):
            counters.record_sent("SNOW_QUERY", 10)

    threads = [threading.Thread(target=bump) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    snap = counters.snapshot()
    assert snap["sent_msgs"] == 8 * per_thread
    assert snap["sent_bytes"] == 8 * per_thread * 10


def test_send_and_broadcast_are_both_counted_by_the_transport():
    """`send()` used to build its own request next to `_send_raw`'s, so a counter added to
    one would have missed every Snow query. Both go through one funnel now."""
    from swarm.comm.grpc_transport import GrpcTransport
    from swarm.consensus.messages.prepare import Prepare
    from swarm.models.agent_info import AgentInfo

    sent = []

    class _T(GrpcTransport):
        def __init__(self):
            self.logger = __import__("logging").getLogger("t")
            self.observers = []
            self._init_instrumentation()
            self._bcast_pool = None
            self._bcast_sem = None
            self.bcast_workers = 4
            self.client = type("C", (), {
                "call_unary": lambda *a, **k: sent.append(1)})()

    msg = Prepare(source=1, agents=[AgentInfo(agent_id=1)],
                  proposals=[ProposalInfo(p_id="p", object_id="j", cost=1.0, agent_id="1")])
    t = _T()
    t.send(host="h", port=1, src=1, dest=2, payload=msg)
    snap = t.counters.snapshot()
    assert snap["sent_msgs"] == 1
    assert snap["sent_bytes"] > 0
    assert snap["recv_msgs"] == 0

    t.record_inbound(str(msg.message_type), 512)
    assert t.counters.snapshot()["recv_bytes"] == 512


def test_the_servicer_tolerates_an_observer_without_the_counter_hook():
    """`record_inbound` is optional on the Observer protocol — a test double that only
    implements `on_message` must not start failing because inbound accounting was added."""
    from swarm.comm.grpc_server import ConsensusServiceServicer

    seen = []

    class _BareObserver:
        def on_message(self, msg):
            seen.append(msg)

    class _Req:
        sender_id, receiver_id, message_type, timestamp = "1", "2", "PREPARE", 0
        payload = "{}"

        def ByteSize(self):
            return 7

    ConsensusServiceServicer(_BareObserver()).SendMessage(_Req(), None)
    assert len(seen) == 1


# --------------------------------------------------------------------------------------------
# Consensus finalization.
# --------------------------------------------------------------------------------------------

class _Host:
    def __init__(self, quorum=2):
        self._quorum = quorum
        self.objects = {}

    def calculate_quorum(self):
        return self._quorum

    def get_object(self, object_id):
        return self.objects.get(object_id)

    def is_agreement_achieved(self, object_id):
        return False

    def on_leader_elected(self, obj, p_id):
        pass

    def on_participant_commit(self, obj, leader, p_id):
        pass

    def log_debug(self, *a, **k):
        pass

    log_info = log_warn = log_error = log_debug


class _Transport:
    def __init__(self):
        self.sent = []

    def send(self, dest, payload):
        self.sent.append(payload)

    def broadcast(self, payload):
        self.sent.append(payload)


class _Router:
    def should_forward(self):
        return False


def _pbft(agent_id=1, quorum=2):
    return ConsensusEngine(agent_id=agent_id, host=_Host(quorum=quorum),
                           transport=_Transport(), router=_Router())


def test_pbft_stats_are_empty_before_anything_finalizes():
    stats = _pbft().consensus_stats()
    assert stats["protocol"] == "pbft"
    assert stats["finalized"] == 0
    assert stats["finalize_s_p50"] is None
    assert "rounds_mean" not in stats  # PBFT has no round count; a column of 3s means nothing


def test_pbft_times_a_finalize_from_the_proposal():
    engine = _pbft()
    proposal = ProposalInfo(p_id="p1", object_id="j1", cost=1.0, agent_id=1)
    engine.propose([proposal])
    time.sleep(0.01)
    proposal.commits.extend([1, 2])
    engine._record_finalize(proposal)
    stats = engine.consensus_stats()
    assert stats["finalized"] == 1
    assert stats["finalize_s_p50"] > 0
    assert stats["votes_p50"] == 2


def test_pbft_measures_at_the_proposer_only():
    """A participant's clock would start when it first heard of the object, which is a
    different and shorter quantity — and averaging the two would make PBFT's finalize time
    look better the more peers echoed a proposal."""
    engine = _pbft(agent_id=1)
    peer_proposal = ProposalInfo(p_id="p2", object_id="j2", cost=1.0, agent_id=9)
    peer_proposal.commits.extend([9, 2])
    engine._record_finalize(peer_proposal)
    assert engine.consensus_stats()["finalized"] == 0


def test_a_reproposal_restarts_the_clock_and_is_counted():
    """A job re-proposed after a reselection timeout starts a new attempt. Carrying the
    timed-out attempt's start into it would report the reselection timeout as consensus
    latency — a 60 s-scale number in a sub-second distribution."""
    engine = _pbft()
    first = ProposalInfo(p_id="p1", object_id="j1", cost=1.0, agent_id=1)
    engine.propose([first])
    first_start = engine._proposed_at["j1"]
    time.sleep(0.01)
    second = ProposalInfo(p_id="p2", object_id="j1", cost=1.0, agent_id=1)
    engine.propose([second])
    assert engine._proposed_at["j1"] > first_start
    assert engine.reproposals == 1


# --------------------------------------------------------------------------------------------
# LLM call accounting.
# --------------------------------------------------------------------------------------------

def test_a_failed_inference_is_counted_and_timed():
    """A timeout burned the whole `llm.timeout_seconds` before it raised. Charging only the
    successes would make inference look cheapest in the runs with the most fallbacks."""
    usage = LlmUsage("delegate")
    usage.record(6.0, failed=True)
    usage.record(1.0, usage=type("U", (), {"input_tokens": 10, "output_tokens": 3,
                                           "requests": 1})())
    snap = usage.snapshot()
    assert snap["calls"] == 2
    assert snap["failures"] == 1
    assert snap["input_tokens"] == 10
    assert snap["latency_mean"] == pytest.approx(3.5)


def test_extract_usage_tolerates_a_result_that_has_none():
    assert extract_usage(object()) is None
    assert extract_usage(type("R", (), {"usage": lambda self: None})()) is None


def test_extract_usage_reads_the_older_pydantic_ai_field_names():
    class _Old:
        request_tokens, response_tokens, requests = 11, 4, 1

    result = type("R", (), {"usage": lambda self: _Old()})()
    shim = extract_usage(result)
    assert shim.input_tokens == 11 and shim.output_tokens == 4


# --------------------------------------------------------------------------------------------
# Prometheus export.
# --------------------------------------------------------------------------------------------

def test_render_drops_values_node_exporter_would_reject():
    """node_exporter rejects the ENTIRE file on one malformed line, so an empty
    distribution's `None` p95 must be dropped rather than take every other metric with it."""
    text = render_prom([
        ("swarm_ok", {"agent": "1"}, 3),
        ("swarm_missing", {"agent": "1"}, None),
        ("swarm_nan", {"agent": "1"}, float("nan")),
        ("swarm_inf", {"agent": "1"}, float("inf")),
        ("swarm_text", {"agent": "1"}, "snow"),
    ])
    assert 'swarm_ok{agent="1"}' in text
    for absent in ("swarm_missing", "swarm_nan", "swarm_inf", "swarm_text"):
        assert absent not in text


def test_a_metric_family_is_written_as_one_contiguous_group():
    """The text format requires every sample of a family after that family's single TYPE
    line. Samples arrive interleaved — one message type at a time, each carrying `_msgs` and
    `_bytes` — so a writer that emits in arrival order produces a second TYPE line for a
    family already seen, which is a parse error and costs the whole file."""
    counters = MessageCounters()
    counters.record_sent("PREPARE", 210)
    counters.record_sent("COMMIT", 190)
    text = render_prom(list(flatten_for_prom("swarm", {"messages": counters.snapshot()})))

    lines = [line for line in text.splitlines() if line]
    assert len([line for line in lines if line.startswith("# TYPE")]) == \
        len({line for line in lines if line.startswith("# TYPE")}), "duplicate TYPE line"
    seen, current = set(), None
    for line in lines:
        if line.startswith("#"):
            continue
        name = line.split("{")[0].split(" ")[0]
        if name != current:
            assert name not in seen, f"{name} samples are not contiguous"
            seen.add(name)
            current = name


def test_message_types_become_a_label_not_a_metric_name():
    """One metric name per message type cannot be aggregated: `sum by (type)` needs the type
    to be a label. It is also unbounded cardinality in the name space rather than the label
    space, which is where Prometheus expects it."""
    counters = MessageCounters()
    counters.record_sent("SNOW_QUERY", 64)
    samples = list(flatten_for_prom("swarm", {"messages": counters.snapshot()}))
    by_type = [s for s in samples if "_by_type" in s[0]]
    assert by_type, "the per-type breakdown must still be exported"
    for name, labels, _ in by_type:
        assert "SNOW_QUERY" not in name
        assert labels.get("type") == "SNOW_QUERY"


def test_flatten_skips_strings_and_keeps_nested_numbers():
    samples = list(flatten_for_prom(
        "swarm", {"consensus": {"protocol": "snow", "finalized": 12}}, {"agent": "3"}))
    names = {name for name, _, _ in samples}
    assert "swarm_consensus_finalized" in names
    assert "swarm_consensus_protocol" not in names


def test_write_textfile_is_atomic_within_the_target_directory():
    """node_exporter scans the directory and `os.replace` is only atomic within a
    filesystem. A tmp file in /tmp would be a cross-device rename and a collector that can
    read a half-written file."""
    with tempfile.TemporaryDirectory() as directory:
        path = os.path.join(directory, "swarm_agent_1.prom")
        write_textfile(path, "swarm_x 1\n")
        write_textfile(path, "swarm_x 2\n")
        assert open(path).read() == "swarm_x 2\n"
        assert os.listdir(directory) == ["swarm_agent_1.prom"]  # no tmp left behind


# --------------------------------------------------------------------------------------------
# Sampling.
# --------------------------------------------------------------------------------------------

def test_the_quantile_sample_is_uniform_not_a_head_or_a_tail():
    """A head-truncated sample reports the warm-up as the whole run and a tail-truncated one
    reports only the converged regime. Either reads as a real distribution; neither is one."""
    stats = RunningStats(sample_cap=200, seed=7)
    for i in range(20000):
        stats.add(i)
    summary = stats.summary()
    assert summary["n"] == 20000
    assert summary["sampled"] == 200
    assert summary["min"] == 0 and summary["max"] == 19999
    # A head sample would put p50 near 100; a tail sample near 19900.
    assert 8000 < summary["p50"] < 12000
