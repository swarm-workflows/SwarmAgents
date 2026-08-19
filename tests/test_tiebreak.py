# MIT License
#
# Copyright (c) 2024 swarm-workflows
#
# Author: Komal Thareja(kthare10@renci.org)
"""Tie-breaking must be deterministic across agents and neutral with respect to agent id.

Both halves matter and they pull against each other: "lowest id wins" is perfectly
deterministic and completely biased, while a random tie-break is unbiased and makes agents
disagree about who won. These tests pin both properties at once.
"""
import subprocess
import sys
from collections import Counter

import numpy as np

from swarm.consensus.messages.proposal_info import ProposalContainer, ProposalInfo
from swarm.selection.engine import SelectionEngine
from swarm.utils.tiebreak import dominates, tiebreak_rank


class _Agent:
    def __init__(self, agent_id):
        self.agent_id = agent_id


class _Job:
    def __init__(self, job_id):
        self.job_id = job_id


def _selector():
    return SelectionEngine(
        feasible=lambda c, a: True,
        cost=lambda c, a: 0.0,
        candidate_key=lambda c: c.job_id,
        assignee_key=lambda a: a.agent_id,
        candidate_version=lambda c: 0,
        assignee_version=lambda a: 0,
        cache_enabled=False,
    )


def test_rank_is_stable_across_processes():
    """hash() is salted per process; a tie-break built on it would make agents disagree."""
    code = ("from swarm.utils.tiebreak import tiebreak_rank;"
            "print(tiebreak_rank('job-7', 3))")
    runs = {subprocess.run([sys.executable, "-c", code], capture_output=True, text=True,
                           env={"PYTHONHASHSEED": seed, "PATH": "/usr/bin:/bin"}).stdout.strip()
            for seed in ("0", "1", "12345")}
    assert len(runs) == 1, f"rank varies with PYTHONHASHSEED: {runs}"
    assert runs.pop() == str(tiebreak_rank("job-7", 3))


def test_rank_does_not_favour_low_ids():
    """Over many jobs every agent should win a comparable share of the ties."""
    agents = list(range(1, 31))
    winners = Counter(min(agents, key=lambda a: tiebreak_rank(f"job-{j}", a))
                      for j in range(3000))
    assert len(winners) == len(agents), "some agent never wins a tie"
    # A fair permutation gives 100 each; allow generous slack, but nothing like the 50%
    # of the workload that agents 1-10 took under the id tie-break.
    assert max(winners.values()) < 3 * min(winners.values())
    low = sum(v for a, v in winners.items() if a <= 10)
    assert 0.25 < low / sum(winners.values()) < 0.42, f"low ids took {low}/3000"


def test_dominates_is_a_strict_total_order():
    assert dominates("j", 1.0, 5, 2.0, 1), "cheaper must win regardless of id"
    a_wins = dominates("j", 2.0, 5, 2.0, 1)
    b_wins = dominates("j", 2.0, 1, 2.0, 5)
    assert a_wins != b_wins, "exactly one side of a tie must win"
    assert not dominates("j", 2.0, 5, 2.0, 5), "an entry cannot beat itself"
    assert not dominates("j", None, 5, 2.0, 1), "unpriced loses to a real cost"
    assert dominates("j", 2.0, 5, None, 1), "a real cost beats unpriced"


def test_selection_spreads_tied_columns():
    """The regression itself: identical costs across agents must not all land on agent 1."""
    agents = [_Agent(i) for i in range(1, 31)]
    jobs = [_Job(f"job-{i}") for i in range(300)]
    tied = np.full((len(agents), len(jobs)), 75.0)  # every agent bids the same, as they do

    picked = _selector().pick_agent_per_candidate(
        assignees=agents, candidates=jobs, cost_matrix=tied, objective="min",
        tie_break_key=lambda ag, s, cand: tiebreak_rank(cand.job_id, ag.agent_id),
    )
    winners = Counter(a.agent_id for a, _ in picked)
    assert len(winners) > 20, f"tied jobs concentrated on {len(winners)} agents"
    assert max(winners.values()) < len(jobs) // 4

    # Same inputs, same answer — every agent runs this independently and must agree.
    again = _selector().pick_agent_per_candidate(
        assignees=agents, candidates=jobs, cost_matrix=tied, objective="min",
        tie_break_key=lambda ag, s, cand: tiebreak_rank(cand.job_id, ag.agent_id),
    )
    assert [a.agent_id for a, _ in picked] == [a.agent_id for a, _ in again]


def test_selection_still_prefers_the_cheaper_agent():
    """Tie-breaking must only apply to exact ties — cost still decides everything else."""
    agents = [_Agent(i) for i in range(1, 31)]
    jobs = [_Job("job-x")]
    costs = np.full((len(agents), 1), 75.0)
    costs[17, 0] = 10.0  # agent 18 is genuinely cheapest

    picked = _selector().pick_agent_per_candidate(
        assignees=agents, candidates=jobs, cost_matrix=costs, objective="min",
        tie_break_key=lambda ag, s, cand: tiebreak_rank(cand.job_id, ag.agent_id),
    )
    assert picked[0][0].agent_id == 18


def test_has_better_proposal_breaks_ties_by_rank():
    container = ProposalContainer()
    mine = ProposalInfo(p_id="p-mine", object_id="job-1", agent_id=30, cost=5.0)
    theirs = ProposalInfo(p_id="p-theirs", object_id="job-1", agent_id=1, cost=5.0)
    container.add_proposal(theirs)

    expect_theirs = tiebreak_rank("job-1", 1) < tiebreak_rank("job-1", 30)
    assert (container.has_better_proposal(mine) is theirs) == expect_theirs

    cheaper = ProposalInfo(p_id="p-cheap", object_id="job-1", agent_id=30, cost=1.0)
    assert container.has_better_proposal(cheaper) is None, "cost must outrank the tie-break"
