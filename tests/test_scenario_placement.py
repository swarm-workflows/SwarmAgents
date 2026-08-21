"""Tests for the chaos-scenario placement parser (`scenarios/helpers.py`).

These pin the three ways the orchestrator log has already misled a published metric:

1. **An agent that wins no jobs emits no line at all.** Both Jain's fairness and the capture
   ratio sized their denominator from the log, so the worst-hit agents deleted themselves from
   the statistics measuring how badly they were hit. Fairness is inflated by `n_logged/fleet`
   and capture ratios roughly halved — always in the direction that weakens the finding.
2. **The per-agent summary can be emitted twice.** A job restart or reassignment re-emits it.
   `collect()` summed a *list* of matches while `load_split()` built a *dict*, so on such a log
   the two disagreed: placements double-counted in one, silently deduplicated in the other.
3. **The fleet can be larger than `AGENTS`** in a dynamic-agent run, so the denominator must
   follow the log upward while never following it *down*.

The first two were real bugs found on 2026-08-21; see `CHAOS_JUNGLE_LLM_TEST_PLAN.md` 4d.2.
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


def _flat(n: int, jobs: int = 10) -> str:
    return "\n".join(f"Agent {i}: {jobs} jobs" for i in range(1, n + 1))


# --- 1. agents absent from the log still count in the denominator -----------------------

def test_absent_agents_still_size_the_denominator(fake_repo):
    """4 agents hold everything and 26 are idle: that is maximally unfair, not perfectly fair."""
    run = fake_repo("sparse", _flat(4))
    placed, fleet = helpers.placement(run)

    assert len(placed) == 4, "only the agents that placed work appear in the log"
    assert fleet == helpers.AGENTS, "the denominator must be the configured fleet"

    # n=len(placed) would give (40^2)/(4*400) = 1.0 — 'perfectly fair'. The truth is
    # (40^2)/(30*400) = 0.133, and the difference is the whole bug.
    assert helpers.collect(run)["jains_fairness"] == 0.133


def test_absent_agents_do_not_deflate_capture(fake_repo):
    """The idle group's per-agent rate must divide by its real size, not its logged size."""
    # 8 faulted agents take 32 each (256); one healthy agent takes 44, 21 take nothing.
    body = _flat(8, 32) + "\nAgent 9: 44 jobs"
    run = fake_repo("capture", body)
    split = helpers.load_split(run, 8, reference=True)

    assert split["faulted"]["agents"] == 8
    assert split["healthy"]["agents"] == 22, "21 silent agents belong to the healthy group"
    assert split["healthy"]["jobs_per_agent"] == pytest.approx(2.0, abs=0.01)
    # Keying off the log would divide 44 by 1 and report 0.73x — inverting the finding.
    assert split["capture_ratio"] == pytest.approx(16.0, abs=0.05)


# --- 2. a re-emitted summary must not double-count -------------------------------------

def test_duplicate_summary_is_not_double_counted(fake_repo):
    """A restart/reassignment re-emits the summary; placements must not be counted twice."""
    block = _flat(30)
    run = fake_repo("dupe", f"{block}\n[reassign] settling after RESTART: Job j-1\n{block}")

    placed, fleet = helpers.placement(run)
    assert len(placed) == 30 and fleet == 30

    metrics = helpers.collect(run)
    assert metrics["jobs_completed"] == 300, "a list-based sum would report 600"
    assert metrics["jains_fairness"] == 1.0


def test_both_readers_agree_on_a_duplicated_log(fake_repo):
    """collect() and load_split() must not disagree about the same log — they used to."""
    block = _flat(30)
    run = fake_repo("agree", f"{block}\nRESTART: Job j-7\n{block}")

    total_collect = helpers.collect(run)["jobs_completed"]
    split = helpers.load_split(run, 15, reference=True)
    total_split = split["faulted"]["jobs"] + split["healthy"]["jobs"]

    assert total_collect == total_split == 300
    assert split["capture_ratio"] == 1.0


def test_last_occurrence_is_authoritative(fake_repo):
    """When a summary is revised, the final block is the true one."""
    run = fake_repo("revised",
                    "Agent 1: 99 jobs\nAgent 2: 1 jobs\nRESTART: Job j-3\nAgent 1: 5 jobs")
    placed, _ = helpers.placement(run)

    assert placed[1] == 5, "the revised count wins"
    assert placed[2] == 1, "an agent absent from the revision keeps its value"
    assert helpers.collect(run)["jobs_completed"] == 6


# --- 3. the fleet may exceed AGENTS, but never shrink below it --------------------------

def test_fleet_follows_the_log_upward(fake_repo):
    """A dynamic-agent run names ids above AGENTS; the denominator must grow to match."""
    run = fake_repo("dynamic", _flat(40))
    placed, fleet = helpers.placement(run)

    assert len(placed) == 40
    assert fleet == 40, "clamping to AGENTS would over-report fairness for the extra agents"
    assert helpers.collect(run)["jains_fairness"] == 1.0


def test_fleet_never_shrinks_below_agents(fake_repo):
    """The failure mode being guarded: a sparse log must not shrink the denominator."""
    run = fake_repo("tiny", "Agent 3: 300 jobs")
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


def test_underscore_log_name_is_found(fake_repo):
    """Hand-written orchestrator logs use underscores where run dirs use hyphens."""
    run = fake_repo("cj_baseline_x", _flat(30))
    # placement() tries runs_<base>.log, then runs_<base with - -> _>.log, so a hyphenated
    # run dir still resolves to the underscored log.
    by_hyphen, _ = helpers.placement("runs/cj-baseline-x")
    by_underscore, _ = helpers.placement(run)
    assert by_hyphen == by_underscore != {}
