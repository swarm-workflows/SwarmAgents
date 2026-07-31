# MIT License
#
# Copyright (c) 2024 swarm-workflows
#
# Author: Komal Thareja(kthare10@renci.org)
"""Unit tests for the failed-agent recovery state machine (ROADMAP item 14)."""

import threading

import pytest

from swarm.utils.recovery import RecoveryTracker
from swarm.utils.thread_safe_dict import ThreadSafeDict


GRACE = 10.0
FAILED_AT = 100.0


@pytest.fixture
def failed():
    return ThreadSafeDict()


@pytest.fixture
def tracker(failed):
    return RecoveryTracker(failed, grace_seconds=GRACE)


def fail_agent(failed, agent_id=7, ts=FAILED_AT):
    failed.set(agent_id, ts)


def obs(tracker, agent_id, last_updated, now, fresh=True):
    return tracker.observe(agent_id, last_updated=last_updated, fresh=fresh, now=now)


class TestPassiveRecovery:
    """Heartbeat path: promotion needs post-failure progress + a grace window."""

    def test_non_failed_agent_never_promotes(self, tracker):
        assert obs(tracker, 1, last_updated=150.0, now=151.0) is False
        assert obs(tracker, 1, last_updated=250.0, now=251.0) is False

    def test_first_post_failure_heartbeat_starts_window_not_promotes(self, tracker, failed):
        fail_agent(failed)
        assert obs(tracker, 7, last_updated=200.0, now=201.0) is False
        assert tracker.recovering_since(7) == 201.0
        assert 7 in failed

    def test_promotes_only_after_grace_elapsed(self, tracker, failed):
        fail_agent(failed)
        assert obs(tracker, 7, last_updated=200.0, now=200.0) is False
        assert obs(tracker, 7, last_updated=209.0, now=200.0 + GRACE - 0.1) is False
        assert obs(tracker, 7, last_updated=210.0, now=200.0 + GRACE) is True
        assert 7 not in failed
        assert tracker.recovering_since(7) is None

    def test_pre_failure_heartbeat_never_promotes(self, tracker, failed):
        """Regression: a dead agent's lingering record is 'fresh' for up to the
        failure threshold after the crash, but its last_updated is frozen
        before the failure timestamp — it must not start a recovery window.
        (Observed live: SIGKILL at t, channel-down failure at t+5, spurious
        heartbeat 'recovery' at t+11 before this check existed.)"""
        fail_agent(failed, ts=FAILED_AT)
        # Record frozen just before the failure was declared, still recent
        stale_write = FAILED_AT - 4.0
        assert obs(tracker, 7, last_updated=stale_write, now=FAILED_AT + 2) is False
        assert tracker.recovering_since(7) is None
        assert obs(tracker, 7, last_updated=stale_write, now=FAILED_AT + GRACE + 5) is False
        assert 7 in failed

    def test_promotion_happens_exactly_once(self, tracker, failed):
        fail_agent(failed)
        obs(tracker, 7, last_updated=200.0, now=200.0)
        assert obs(tracker, 7, last_updated=211.0, now=200.0 + GRACE) is True
        assert obs(tracker, 7, last_updated=300.0, now=301.0) is False

    def test_stale_observation_resets_window(self, tracker, failed):
        fail_agent(failed)
        obs(tracker, 7, last_updated=200.0, now=200.0)
        # Dying gasp: record stops advancing and goes stale
        assert obs(tracker, 7, last_updated=200.0, now=205.0, fresh=False) is False
        assert tracker.recovering_since(7) is None
        # Fresh progress again: window restarts from scratch
        assert obs(tracker, 7, last_updated=210.0, now=210.0) is False
        assert obs(tracker, 7, last_updated=219.0, now=210.0 + GRACE - 0.1) is False
        assert obs(tracker, 7, last_updated=220.0, now=210.0 + GRACE) is True


class TestDirectRecovery:
    """Channel-up / SWIM-ack path: immediate promotion."""

    def test_direct_recovery_promotes_immediately(self, tracker, failed):
        fail_agent(failed)
        assert tracker.recover_direct(7) is True
        assert 7 not in failed

    def test_direct_recovery_for_healthy_agent_is_false(self, tracker):
        assert tracker.recover_direct(3) is False

    def test_direct_recovery_clears_pending_window(self, tracker, failed):
        fail_agent(failed)
        obs(tracker, 7, last_updated=200.0, now=200.0)
        assert tracker.recover_direct(7) is True
        assert tracker.recovering_since(7) is None
        assert tracker.recovering_count() == 0

    def test_direct_recovery_is_idempotent(self, tracker, failed):
        fail_agent(failed)
        assert tracker.recover_direct(7) is True
        assert tracker.recover_direct(7) is False


class TestFlapping:
    """An agent that recovers and fails again goes through the full cycle."""

    def test_refailure_after_recovery_requires_new_window(self, tracker, failed):
        fail_agent(failed, ts=100.0)
        obs(tracker, 7, last_updated=200.0, now=200.0)
        assert obs(tracker, 7, last_updated=210.0, now=200.0 + GRACE) is True

        # Fails again at t=300; heartbeats must now beat the NEW failure ts
        fail_agent(failed, ts=300.0)
        assert obs(tracker, 7, last_updated=250.0, now=305.0) is False  # pre-failure write
        assert obs(tracker, 7, last_updated=306.0, now=306.0) is False  # window restarts
        assert obs(tracker, 7, last_updated=316.0, now=306.0 + GRACE) is True


class TestPlainDictCompat:
    """Tracker works with a plain dict (no .remove method)."""

    def test_plain_dict_backing(self):
        failed = {7: FAILED_AT}
        tracker = RecoveryTracker(failed, grace_seconds=GRACE)
        assert tracker.recover_direct(7) is True
        assert failed == {}


class TestThreadSafety:
    def test_concurrent_observations_promote_once(self, failed):
        tracker = RecoveryTracker(failed, grace_seconds=0.0)  # promote on 2nd obs
        fail_agent(failed)
        obs(tracker, 7, last_updated=200.0, now=200.0)

        promotions = []
        barrier = threading.Barrier(8)

        def worker():
            barrier.wait()
            if obs(tracker, 7, last_updated=210.0, now=210.0):
                promotions.append(1)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(promotions) == 1
        assert 7 not in failed
