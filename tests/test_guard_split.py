"""The load guard's share of regret (2026-10-20).

`mab.load_slack` narrows a bandit's choice to groups near the least-loaded one. In E2, where the
failure profile makes one group best for each job type, the guard can remove that group from the
choice, and the oracle used to charge the resulting regret to the policy. Each decision now
records the candidates the guard left (`eligible`), and the oracle splits total regret into the
guard's part (best overall minus best allowed) and the policy's part (best allowed minus chosen).
"""
import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)
sys.path.insert(0, os.path.join(REPO, "tests"))

from evaluation.oracle import score_run  # noqa: E402
from swarm.rl.context import GroupSnapshot  # noqa: E402
from swarm.utils.instrumentation import DecisionRecord  # noqa: E402

from test_bandit_herding import _job, _mgr  # noqa: E402
from test_oracle import _decision, _run  # noqa: E402


class TestManagerReportsTheEligibleSet:
    def test_a_narrowed_choice_is_reported(self):
        snaps = {6: GroupSnapshot(inflight=700), 7: GroupSnapshot(inflight=3),
                 8: GroupSnapshot(inflight=10)}
        m = _mgr(slack=9, snaps=snaps)
        m.select_groups([6, 7, 8], job=_job(1), top_k=1)
        assert m.pop_eligible() == [7, 8]
        assert m.pop_eligible() is None          # popped once

    def test_without_the_guard_every_candidate_is_eligible(self):
        m = _mgr(slack=0)
        m.select_groups([6, 7, 8], job=_job(1), top_k=1)
        assert m.pop_eligible() == [6, 7, 8]

    def test_a_fan_out_covering_every_candidate_is_reported_whole(self):
        m = _mgr(slack=9)
        m.select_groups([6, 7], job=_job(1), top_k=2)
        assert m.pop_eligible() == [6, 7]


class TestTheRecord:
    def test_eligible_rides_on_the_row(self):
        r = DecisionRecord(ts=1.0, job_id="j", job_type=None, policy="bandit", n_candidates=3,
                           candidates=[6, 7, 8], selected=[7], decide_s=0.0, eligible=[7, 8])
        assert r.as_dict()["eligible"] == [7, 8]

    def test_absent_when_no_bandit_decided(self):
        r = DecisionRecord(ts=1.0, job_id="j", job_type=None, policy="random", n_candidates=2,
                           candidates=[6, 7], selected=[7], decide_s=0.0)
        assert "eligible" not in r.as_dict()


class TestTheOracleSplit:
    def test_guard_excluded_the_best_group(self):
        """cpu job: group 1 is best; the guard left only group 0; the policy chose it."""
        d = _decision([0])
        d["eligible"] = [0]
        rows, summary = score_run(_run([d]))
        r = rows[0]
        assert r["narrowed"] is True
        assert r["regret_guard"] == pytest.approx(r["regret"])
        assert r["regret_policy"] == 0.0
        assert summary["decisions_guard_excluded_best"] == 1

    def test_the_policy_chose_badly_inside_an_unnarrowed_set(self):
        d = _decision([0])
        d["eligible"] = [0, 1]
        rows, summary = score_run(_run([d]))
        r = rows[0]
        assert r["narrowed"] is False and r["regret_guard"] == 0.0
        assert r["regret_policy"] == pytest.approx(r["regret"])
        assert summary["decisions_narrowed"] == 0

    def test_the_parts_add_up_to_the_total(self):
        ds = []
        for i, (sel, elig) in enumerate([([0], [0]), ([0], [0, 1]), ([1], [0, 1]), ([1], [1])]):
            d = _decision(sel, job_id=f"j{i}")
            d["eligible"] = elig
            ds.append(d)
        rows, summary = score_run(_run(ds))
        for r in rows:
            assert r["regret_guard"] + r["regret_policy"] == pytest.approx(r["regret"])
        assert summary["regret_guard_total"] + summary["regret_policy_total"] == \
            pytest.approx(summary["regret_total"])

    def test_absent_without_eligible_sets(self):
        rows, summary = score_run(_run([_decision([0])]))
        assert "regret_guard" not in rows[0]
        assert "regret_guard_total" not in summary


class TestTheAgentRecordsIt:
    def test_a_bandit_decision_carries_the_eligible_set(self):
        from test_delegation import _Job, make_agent
        a = make_agent()
        mgr = _mgr(slack=9, snaps={1: GroupSnapshot(inflight=500), 2: GroupSnapshot(inflight=0),
                                   3: GroupSnapshot(inflight=2)})
        a.mab_manager = mgr
        a._build_group_snapshots = lambda: {}
        a._select_child_groups = (lambda job, groups, top_k=None, snapshots=None:
                                  mgr.select_groups(groups, job=job, top_k=1))
        mgr._eligible_tls.eligible = [99]                  # a stale value from an earlier call
        a._delegate_child_groups(_Job(), [1, 2, 3])
        row = a.decision_log.records()[0]
        assert row["eligible"] == [2, 3]

    def test_a_decision_without_a_bandit_has_no_eligible_key(self):
        from test_delegation import _Job, make_agent
        a = make_agent()
        a.mab_manager = None
        a._build_group_snapshots = lambda: {}
        a._select_child_groups = lambda job, groups, top_k=None, snapshots=None: [groups[0]]
        a._delegate_child_groups(_Job(), [1, 2, 3])
        assert "eligible" not in a.decision_log.records()[0]
