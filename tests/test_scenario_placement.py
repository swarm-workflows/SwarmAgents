"""Tests for the chaos-scenario placement parser (`scenarios/helpers.py`).

Fixtures mirror the real orchestrator log, whose producer is
`plotting/single_run.py:plot_scheduling_latency_and_jobs`. That function prints one
`[label] Jobs per agent:` block per invocation, and the flat pipeline invokes it **twice on any
run containing restarts** — `[all]` over every job, then `[no_restarts]` over the filtered set.
Those are two populations of one run, so they must not be merged, summed, or deduplicated
against each other.

These pin the four ways the log has already misled a metric:

1. **An agent that wins no jobs emits no line at all.** Both Jain's fairness and the capture
   ratio sized their denominator from the log, so the worst-hit agents deleted themselves from
   the statistics measuring how badly they were hit. Fairness is inflated by `n_logged/fleet`
   and capture ratios roughly halved — always in the direction that weakens the finding.
2. **A restart run carries two blocks.** Summing every match double-counts placements;
   deduplicating by id silently substitutes `[no_restarts]` for the real population. `[all]` is
   what these experiments mean.
3. **Ambiguity must be refused, not guessed.** A hierarchical run mislabels all three of its
   level blocks `[no_restarts]` (finding 12), and a legacy header-less log with repeated ids
   carries no evidence of which occurrence is authoritative. Every earlier bug here was silent.
4. **The fleet can exceed `AGENTS`** in a dynamic-agent run, so the denominator must follow the
   log upward while never following it *down*.

Bugs 1 and 2 were real, found 2026-08-21; see `CHAOS_JUNGLE_LLM_TEST_PLAN.md` §7.2.
"""
from __future__ import annotations

import json
import os
import sys

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO_ROOT, "scenarios"))

helpers = pytest.importorskip("helpers", reason="scenarios/helpers.py not importable")


@pytest.fixture
def fake_repo(tmp_path, monkeypatch):
    """Point the parser at a throwaway tree and hand back a log-writer."""
    monkeypatch.setattr(helpers, "REPO", str(tmp_path))

    def write(name: str, body: str) -> str:
        (tmp_path / "runs" / name).mkdir(parents=True, exist_ok=True)
        (tmp_path / f"runs_{name}.log").write_text(body)
        return f"runs/{name}"

    return write


def block(label: str, counts: dict[int, int]) -> str:
    """One "[label] Jobs per agent:" block, formatted exactly as the producer prints it."""
    lines = [f"\n[{label}] Jobs per agent:"]
    lines += [f"  Agent {aid}: {jobs} jobs" for aid, jobs in sorted(counts.items())]
    return "\n".join(lines) + "\n"


def flat(n: int, jobs: int = 10, label: str = "all") -> str:
    return block(label, {i: jobs for i in range(1, n + 1)})


# --- 1. agents absent from the log still count in the denominator -----------------------

def test_absent_agents_still_size_the_denominator(fake_repo):
    """4 agents hold everything and 26 are idle: that is maximally unfair, not perfectly fair."""
    run = fake_repo("sparse", flat(4))
    placed, fleet = helpers.placement(run)

    assert len(placed) == 4, "only the agents that placed work appear in the log"
    assert fleet == helpers.AGENTS, "the denominator must be the configured fleet"

    # n=len(placed) would give (40^2)/(4*400) = 1.0 — 'perfectly fair'. The truth is
    # (40^2)/(30*400) = 0.133, and the difference is the whole bug.
    assert helpers.collect(run)["jains_fairness"] == 0.133


def test_absent_agents_do_not_deflate_capture(fake_repo):
    """The idle group's per-agent rate must divide by its real size, not its logged size."""
    # 8 faulted agents take 32 each (256); one healthy agent takes 44, 21 take nothing.
    counts = {i: 32 for i in range(1, 9)}
    counts[9] = 44
    run = fake_repo("capture", block("all", counts))
    split = helpers.load_split(run, 8, reference=True)

    assert split["faulted"]["agents"] == 8
    assert split["healthy"]["agents"] == 22, "21 silent agents belong to the healthy group"
    assert split["healthy"]["jobs_per_agent"] == pytest.approx(2.0, abs=0.01)
    # Keying off the log would divide 44 by 1 and report 0.73x — inverting the finding.
    assert split["capture_ratio"] == pytest.approx(16.0, abs=0.05)


# --- 2. a restart run carries two blocks; [all] is the one we mean ----------------------

def test_restart_run_uses_the_all_block(fake_repo):
    """[all] and [no_restarts] are different populations. Take [all], never the last one."""
    all_jobs = {i: 10 for i in range(1, 31)}
    filtered = {i: 9 for i in range(1, 31)}       # 30 restarted jobs excluded
    run = fake_repo("restarts",
                    block("all", all_jobs) + "RESTART: Job j-7\n" + block("no_restarts", filtered))

    placed, fleet = helpers.placement(run)
    assert placed == all_jobs, "the restart-filtered block must not win"
    assert fleet == 30

    metrics = helpers.collect(run)
    # Summing both blocks gives 570; taking the last gives 270. Correct is 300.
    assert metrics["jobs_completed"] == 300


def test_both_readers_agree_on_a_restart_log(fake_repo):
    """collect() and load_split() must not disagree about the same log — they used to."""
    run = fake_repo("agree",
                    block("all", {i: 10 for i in range(1, 31)})
                    + block("no_restarts", {i: 4 for i in range(1, 31)}))

    total_collect = helpers.collect(run)["jobs_completed"]
    split = helpers.load_split(run, 15, reference=True)
    total_split = split["faulted"]["jobs"] + split["healthy"]["jobs"]

    assert total_collect == total_split == 300
    assert split["capture_ratio"] == 1.0


def test_block_ends_at_unrelated_output(fake_repo):
    """A block stops at the first non-agent line, so later output cannot leak into it."""
    run = fake_repo("bounded",
                    block("all", {1: 100, 2: 200})
                    + "Total failed agents: 0\n  Agent 3: 999 jobs\n")
    placed, _ = helpers.placement(run)

    assert placed == {1: 100, 2: 200}, "Agent 3 sits past the block terminator"


# --- 3. ambiguity is refused, never guessed --------------------------------------------

def test_hierarchical_log_is_refused(fake_repo):
    """Three level blocks, all mislabelled 'no_restarts' (finding 12) — cannot pick one."""
    run = fake_repo("hier",
                    block("no_restarts", {1: 100})
                    + block("no_restarts", {2: 100})
                    + block("no_restarts", {3: 100}))

    with pytest.raises(SystemExit, match="no unambiguous whole-fleet placement"):
        helpers.placement(run)


def test_lone_non_all_block_is_refused(fake_repo):
    """Being the only block does not make a subset the fleet.

    One level-filtered or restart-filtered invocation emits a single [no_restarts] block. A
    'len(blocks) == 1 is unambiguous' shortcut accepted it and reported that subset as the whole
    fleet — the same corruption as the hierarchical case, with one block instead of three.
    """
    run = fake_repo("lone_filtered", block("no_restarts", {i: 10 for i in range(1, 31)}))

    with pytest.raises(SystemExit, match="no unambiguous whole-fleet placement"):
        helpers.placement(run)


def test_duplicate_all_blocks_are_refused(fake_repo):
    """Two blocks both claiming to be the whole fleet is also unresolvable."""
    run = fake_repo("two_all",
                    block("all", {1: 300}) + block("all", {2: 300}))

    with pytest.raises(SystemExit, match="no unambiguous whole-fleet placement"):
        helpers.placement(run)


def test_headerless_log_with_repeated_ids_is_refused(fake_repo):
    """No header and duplicate ids: no evidence for which occurrence is authoritative."""
    run = fake_repo("legacy_dupe",
                    "  Agent 1: 99 jobs\n  Agent 2: 1 jobs\n  Agent 1: 5 jobs\n")

    with pytest.raises(SystemExit, match="agent ids repeat"):
        helpers.placement(run)


def test_headerless_log_without_repeats_is_accepted(fake_repo):
    """A hand-written log stays readable as long as it is unambiguous."""
    run = fake_repo("legacy_ok", "  Agent 1: 200 jobs\n  Agent 2: 100 jobs\n")
    placed, fleet = helpers.placement(run)

    assert placed == {1: 200, 2: 100}
    assert fleet == helpers.AGENTS


# --- 4. the fleet may exceed AGENTS, but never shrink below it --------------------------

def test_fleet_follows_the_log_upward(fake_repo):
    """A dynamic-agent run names ids above AGENTS; the denominator must grow to match."""
    run = fake_repo("dynamic", flat(40))
    placed, fleet = helpers.placement(run)

    assert len(placed) == 40
    assert fleet == 40, "clamping to AGENTS would over-report fairness for the extra agents"
    assert helpers.collect(run)["jains_fairness"] == 1.0


def test_fleet_never_shrinks_below_agents(fake_repo):
    """The failure mode being guarded: a sparse log must not shrink the denominator."""
    run = fake_repo("tiny", block("all", {3: 300}))
    _, fleet = helpers.placement(run)

    assert fleet == helpers.AGENTS
    # One agent holding all 300 jobs is the least fair outcome possible: 1/30.
    assert helpers.collect(run)["jains_fairness"] == pytest.approx(1 / 30, abs=0.001)


# --- edge cases -------------------------------------------------------------------------

def test_missing_log_is_handled(fake_repo):
    assert helpers.placement("runs/nonexistent") == ({}, helpers.AGENTS)
    assert helpers.load_split("runs/nonexistent", 8, reference=True) == {}


def test_log_with_no_placement_lines(fake_repo):
    run = fake_repo("empty", "nothing to see here\n")
    assert helpers.placement(run) == ({}, helpers.AGENTS)
    assert helpers.load_split(run, 8, reference=True) == {}
    assert "jains_fairness" not in helpers.collect(run)


def test_empty_block_is_handled(fake_repo):
    """A header with no agent lines under it must not crash or invent placements."""
    run = fake_repo("hollow", "\n[all] Jobs per agent:\nTotal failed agents: 0\n")
    assert helpers.placement(run) == ({}, helpers.AGENTS)


class TestRestartAndLatencyMetrics:
    """Section 4 calls restarts and conflicts primary metrics; nothing captured them until
    2026-08-22, so every result table was silent about them — and silence is not a zero.
    A 140-560 s scheduling latency invites "jobs are being reselected", and only a measured
    restart count refutes it.
    """

    def _run(self, tmp_path, monkeypatch, *, metrics=None, jobs_csv=None, agent_log=None):
        monkeypatch.setattr(helpers, "REPO", str(tmp_path))
        d = tmp_path / "runs" / "r"
        d.mkdir(parents=True, exist_ok=True)
        (tmp_path / "runs_r.log").write_text(block("all", {1: 300}))
        if metrics is not None:
            (d / "metrics.json").write_text(json.dumps(metrics))
        if jobs_csv is not None:
            (d / "all_jobs.csv").write_text(jobs_csv)
        if agent_log is not None:
            (d / "agent-1.log").write_text(agent_log)
        return "runs/r"

    def test_restarts_and_conflicts_read_from_metrics_json(self, tmp_path, monkeypatch):
        metrics = {
            "1": {"restarts": {"job-1": 2, "job-2": 1}, "conflicts": {"job-9": 3}},
            "2": {"restarts": {}, "conflicts": {"job-7": 1, "job-8": 1}},
        }
        run = self._run(tmp_path, monkeypatch, metrics=metrics)
        m = helpers.collect(run)
        assert m["restarts"] == 3
        assert m["conflicts"] == 5

    def test_zero_is_reported_not_omitted(self, tmp_path, monkeypatch):
        """An explicit 0 is the whole point — it is the control for a long latency."""
        run = self._run(tmp_path, monkeypatch,
                        metrics={"1": {"restarts": {}, "conflicts": {}}})
        m = helpers.collect(run)
        assert m["restarts"] == 0 and m["conflicts"] == 0
        assert "restarts" in m and "conflicts" in m

    def test_per_job_collections_are_counted_by_length(self, tmp_path, monkeypatch):
        """Values may be counts or per-job lists depending on what Metrics recorded."""
        metrics = {"1": {"restarts": {"job-1": ["t1", "t2", "t3"]}, "conflicts": {"j": 2}}}
        run = self._run(tmp_path, monkeypatch, metrics=metrics)
        m = helpers.collect(run)
        assert m["restarts"] == 3 and m["conflicts"] == 2

    def test_log_markers_are_the_real_strings(self, tmp_path, monkeypatch):
        """These must match resource_agent.py and gossip_engine.py verbatim.

        `RESTART: Job:` comes from a print() (not the logger), and only reaches the agent log
        because run_test redirects stdout into it. Guessing at a marker instead of reading the
        source is how an earlier check concluded 'zero restarts' with no evidence.
        """
        log = ("RESTART: Job: job-4 reset to Pending 60.0 seconds\n"
               "some other line\n"
               "elapsed=3.0s — max_rounds exhausted, leaving for reselection\n"
               "RESTART: Job: job-9 reset to Pending 60.0 seconds\n")
        run = self._run(tmp_path, monkeypatch, metrics={"1": {}}, agent_log=log)
        m = helpers.collect(run)
        assert m["restart_log_lines"] == 2
        assert m["reselection_log_lines"] == 1

    def test_logs_are_a_crosscheck_not_a_substitute(self, tmp_path, monkeypatch):
        """metrics.json wins for `restarts`; a disagreement stays visible under its own key."""
        run = self._run(tmp_path, monkeypatch,
                        metrics={"1": {"restarts": {"j": 5}}},
                        agent_log="RESTART: Job: j reset to Pending 60.0 seconds\n")
        m = helpers.collect(run)
        assert m["restarts"] == 5, "metrics.json is authoritative"
        assert m["restart_log_lines"] == 1, "the log count is reported separately"

    def test_crosscheck_keys_are_actually_reported(self):
        """A cross-check that is collected but never printed is not a cross-check.

        Both keys were produced by collect() and omitted from _KEYS, so a report could show
        "job restarts 0" from metrics.json while the logs held nonzero evidence, with nothing on
        screen to reveal it. This pins the claim to the table that has to carry it.
        """
        reported = {key for key, _label, _fmt in helpers._KEYS}
        for key in ("restarts", "conflicts", "restart_log_lines", "reselection_log_lines",
                    "pool_wait_mean_s", "selection_mean_s"):
            assert key in reported, f"{key} is collected but never printed by report()"

    def test_source_disagreement_is_announced(self, tmp_path, monkeypatch, capsys):
        """Printing two rows relies on the reader comparing them; say it out loud too."""
        run = self._run(tmp_path, monkeypatch,
                        metrics={"1": {"restarts": {}}},
                        agent_log="RESTART: Job: j reset to Pending 60.0 seconds\n" * 3)
        helpers.collect(run)
        out = capsys.readouterr().out
        assert "restart sources disagree" in out
        assert "metrics.json=0" in out and "log lines=3" in out

    def test_reselection_logs_announced_when_restarts_are_zero(self, tmp_path, monkeypatch,
                                                              capsys):
        """The Snow path logs reselection without going through the restart counter."""
        run = self._run(tmp_path, monkeypatch,
                        metrics={"1": {"restarts": {}}},
                        agent_log="elapsed=9s — max_rounds exhausted, leaving for reselection\n")
        helpers.collect(run)
        out = capsys.readouterr().out
        assert "leaving for reselection" in out and "does NOT" in out

    def test_reselection_logs_announced_when_restarts_are_nonzero(self, tmp_path, monkeypatch,
                                                                 capsys):
        """The case a chained `elif` hid: sources agree at a NONZERO value, and reselection
        evidence exists anyway.

        `restarts == restart_log_lines == 2` skips the disagreement branch, and gating the
        reselection check on `restarts == 0` then skipped that too — so 5 lines of Snow-path
        evidence vanished precisely when the run was most disturbed. The two checks must be
        independent.
        """
        log = ("RESTART: Job: a reset to Pending 60.0 seconds\n"
               "RESTART: Job: b reset to Pending 60.0 seconds\n"
               + "elapsed=9s — max_rounds exhausted, leaving for reselection\n" * 5)
        run = self._run(tmp_path, monkeypatch,
                        metrics={"1": {"restarts": {"a": 1, "b": 1}}}, agent_log=log)
        m = helpers.collect(run)
        out = capsys.readouterr().out

        assert m["restarts"] == 2 == m["restart_log_lines"], "precondition: sources agree, nonzero"
        assert "disagree" not in out, "they agree, so no disagreement warning"
        assert "5 'leaving for reselection' line(s)" in out, "the Snow evidence must surface"
        assert m["reselection_log_lines"] == 5

    def test_agreement_is_silent(self, tmp_path, monkeypatch, capsys):
        """No warning when the sources agree — otherwise every clean run cries wolf."""
        run = self._run(tmp_path, monkeypatch, metrics={"1": {"restarts": {}}}, agent_log="quiet\n")
        helpers.collect(run)
        assert "disagree" not in capsys.readouterr().out

    def test_falls_back_to_logs_when_metrics_json_absent(self, tmp_path, monkeypatch):
        run = self._run(tmp_path, monkeypatch,
                        agent_log="RESTART: Job: j reset to Pending 60.0 seconds\n" * 4)
        m = helpers.collect(run)
        assert m["restarts"] == 4, "the metric must never be simply absent"

    def test_latency_splits_into_pool_wait_and_selection(self, tmp_path, monkeypatch):
        """The split is what stops a long latency being read as slow consensus or a restart.

        Measured on the real runs: selection ~1.0 s flat, pool wait 78-311 s.
        """
        rows = ["job_id,submitted_at,selection_started_at,assigned_at,scheduling_latency"]
        for i in range(10):
            sub, started, assigned = 100.0, 100.0 + 50 + i, 100.0 + 51 + i
            rows.append(f"j{i},{sub},{started},{assigned},{assigned - sub}")
        run = self._run(tmp_path, monkeypatch, metrics={"1": {}},
                        jobs_csv="\n".join(rows) + "\n")
        m = helpers.collect(run)

        assert m["selection_mean_s"] == 1.0, "selection is the small term"
        assert m["pool_wait_mean_s"] == pytest.approx(54.5, abs=0.1)
        # The total must reconcile with its parts, or the split is worse than not having it.
        assert m["sched_latency_mean_s"] == pytest.approx(
            m["pool_wait_mean_s"] + m["selection_mean_s"], abs=0.1)

    def test_missing_phase_columns_do_not_break_collect(self, tmp_path, monkeypatch):
        run = self._run(tmp_path, monkeypatch, metrics={"1": {}},
                        jobs_csv="job_id,scheduling_latency\nj1,12.0\n")
        m = helpers.collect(run)
        assert m["sched_latency_mean_s"] == 12.0
        assert "pool_wait_mean_s" not in m


def test_underscore_log_name_is_found(fake_repo):
    """Hand-written orchestrator logs use underscores where run dirs use hyphens."""
    run = fake_repo("cj_baseline_x", flat(30))
    # placement() tries runs_<base>.log, then runs_<base with - -> _>.log, so a hyphenated
    # run dir still resolves to the underscored log.
    by_hyphen, _ = helpers.placement("runs/cj-baseline-x")
    by_underscore, _ = helpers.placement(run)
    assert by_hyphen == by_underscore != {}
