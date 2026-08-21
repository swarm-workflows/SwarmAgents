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

Bugs 1 and 2 were real, found 2026-08-21; see `CHAOS_JUNGLE_LLM_TEST_PLAN.md` 4d.2.
"""
from __future__ import annotations

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


def test_underscore_log_name_is_found(fake_repo):
    """Hand-written orchestrator logs use underscores where run dirs use hyphens."""
    run = fake_repo("cj_baseline_x", flat(30))
    # placement() tries runs_<base>.log, then runs_<base with - -> _>.log, so a hyphenated
    # run dir still resolves to the underscored log.
    by_hyphen, _ = helpers.placement("runs/cj-baseline-x")
    by_underscore, _ = helpers.placement(run)
    assert by_hyphen == by_underscore != {}
