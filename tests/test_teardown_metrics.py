"""Regression guards for teardown and metrics attribution.

Both smoke runs of the P0-1 delegation pair (smoke-g2-bandit, smoke-g2-llm) produced a
metrics.json that could not be used: the first held 1 agent out of 30, the second held 15
agents' payloads left behind by the first. Two defects, both silent — every plot rendered and
every number looked plausible:

  * `on_shutdown` saved metrics only after `executor.shutdown(wait=True)`, which since
    runtime.wall_time_* simulates real job durations can hold teardown for minutes, while the
    runner had already read Redis and moved on.
  * an agent that outlived its run wrote its metrics whenever it was finally killed, which can
    be after the NEXT run flushed Redis; the payload is keyed by agent id, so it reads as that
    agent's numbers for the new run.
"""
import argparse
import json
import logging
import os
import sys
import threading
import time
from concurrent.futures.thread import ThreadPoolExecutor

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import run_test  # noqa: E402
from plotting import data as plotting_data  # noqa: E402
from swarm.agents.resource_agent import ResourceAgent  # noqa: E402


@pytest.fixture(autouse=True)
def _reset_expected_run_id():
    yield
    plotting_data.set_expected_run_id(None)


class _DrainStub:
    """Minimal host for the two teardown helpers, which only touch executor and logger."""

    _drain_executor = ResourceAgent._drain_executor
    _save_results_safely = ResourceAgent._save_results_safely

    def __init__(self, workers=2):
        self.logger = logging.getLogger("drain-stub")
        self.executor = ThreadPoolExecutor(max_workers=workers)
        self.saved = []

    def save_results(self):
        self.saved.append(time.time())


class TestExecutorDrain:
    """The drain is bounded: a job still running must not hold up the metrics save."""

    def test_returns_at_the_deadline_when_a_job_will_not_finish(self):
        stub = _DrainStub()
        release = threading.Event()
        stub.executor.submit(release.wait)  # stands in for a long simulated wall time
        try:
            started = time.monotonic()
            stub._drain_executor(0.5)
            elapsed = time.monotonic() - started
            assert 0.4 <= elapsed < 5.0, f"drain took {elapsed:.2f}s, expected to give up at 0.5s"
        finally:
            release.set()

    def test_waits_for_a_job_that_finishes_inside_the_budget(self):
        stub = _DrainStub()
        done = []
        stub.executor.submit(lambda: (time.sleep(0.3), done.append(1)))
        stub._drain_executor(20.0)
        assert done == [1], "a job that fits in the budget must still be waited for"

    def test_queued_jobs_are_cancelled_rather_than_started(self):
        """A queued job would only start a fresh simulated sleep nothing waits for."""
        stub = _DrainStub(workers=1)
        release = threading.Event()
        started = []
        stub.executor.submit(release.wait)
        stub.executor.submit(lambda: started.append(1))
        try:
            stub._drain_executor(0.5)
        finally:
            release.set()
        time.sleep(0.3)
        assert started == [], "queued work should be cancelled at teardown"


class TestSaveResultsNeverAbortsTeardown:
    def test_a_failing_save_is_logged_and_swallowed(self, caplog):
        stub = _DrainStub()

        def boom():
            raise RuntimeError("dictionary changed size during iteration")

        stub.save_results = boom
        with caplog.at_level(logging.ERROR):
            stub._save_results_safely("pre-drain")
        assert "pre-drain save_results failed" in caplog.text


class TestShutdownOrder:
    """Metrics must be saved before the drain, not only after it."""

    def test_on_shutdown_saves_before_and_after_the_drain(self):
        import inspect
        body = inspect.getsource(ResourceAgent.on_shutdown)
        pre = body.index('_save_results_safely("pre-drain")')
        drain = body.index("_drain_executor(")
        post = body.index('_save_results_safely("post-drain")')
        assert pre < drain < post, (
            "the pre-drain save is the whole fix: with it after the drain, a run whose jobs "
            "are still executing reports no metrics at all"
        )
        assert "self.executor.shutdown(wait=True)\n" not in body, (
            "an unbounded drain in on_shutdown reintroduces the original defect"
        )


class TestMetricsPayloadIsStamped:
    def test_resource_and_colmena_agents_both_stamp_run_id(self):
        import inspect
        payload = inspect.getsource(ResourceAgent._save_results_locked)
        assert '"run_id": os.environ.get("SWARM_RUN_ID")' in payload
        assert '"saved_at"' in payload
        colmena = open(os.path.join(os.path.dirname(os.path.dirname(
            os.path.abspath(__file__))), "swarm/agents/colmena_agent.py")).read()
        assert '"run_id": os.environ.get("SWARM_RUN_ID")' in colmena, (
            "an unstamped payload is dropped by the collector, so every agent type must stamp"
        )


class TestRunIdFilter:
    def _entries(self):
        return {
            1: {"id": 1, "run_id": "now", "load_trace": []},
            2: {"id": 2, "run_id": "earlier", "saved_at": 1788970181.0, "load_trace": []},
            3: {"id": 3, "load_trace": []},  # pre-fix agent, or one started by hand
        }

    def test_keeps_only_this_run(self, capsys):
        plotting_data.set_expected_run_id("now")
        kept = plotting_data._filter_by_run_id(self._entries())
        assert set(kept) == {1}
        err = capsys.readouterr().err
        assert "dropped 2 metrics payload(s)" in err
        assert "agent 2" in err and "agent 3" in err

    def test_no_expected_id_keeps_everything(self):
        """The default (a hand-run plot_latency_jobs.py) must not start dropping data."""
        plotting_data.set_expected_run_id(None)
        assert set(plotting_data._filter_by_run_id(self._entries())) == {1, 2, 3}


def _args(tmp_path, **over):
    base = dict(run_dir=str(tmp_path), db_host="localhost", metrics_wait_seconds=0,
                allow_missing_metrics=0, expect_silent_agents="", mode="local",
                agent_hosts_file=None, agent_hosts=None)
    base.update(over)
    return argparse.Namespace(**base)


class TestMetricsCompletenessGate:
    def test_complete_metrics_pass(self, tmp_path, monkeypatch):
        monkeypatch.setattr(run_test, "_metrics_in_redis", lambda a, r: ({1, 2, 3}, {}))
        assert run_test.report_metrics_completeness(
            _args(tmp_path), {1, 2, 3}, "now", True) is True
        assert not (tmp_path / "metrics_shortfall.json").exists()

    def test_a_shortfall_fails_the_run_and_is_recorded(self, tmp_path, monkeypatch):
        monkeypatch.setattr(run_test, "_metrics_in_redis",
                            lambda a, r: ({1}, {2: "earlier", 3: "earlier"}))
        assert run_test.report_metrics_completeness(
            _args(tmp_path), {1, 2, 3}, "now", True) is False
        shortfall = json.loads((tmp_path / "metrics_shortfall.json").read_text())
        assert shortfall["missing_agents"] == [2, 3]
        assert shortfall["foreign_payloads"] == {"2": "earlier", "3": "earlier"}

    def test_sigkilled_agents_can_be_declared(self, tmp_path, monkeypatch):
        """A failure-injection run kills agents outright; that is expected, not a defect."""
        monkeypatch.setattr(run_test, "_metrics_in_redis", lambda a, r: ({1, 2}, {}))
        assert run_test.report_metrics_completeness(
            _args(tmp_path, allow_missing_metrics=1), {1, 2, 3}, "now", True) is True

    def test_the_allowance_is_not_a_blanket_pass(self, tmp_path, monkeypatch):
        monkeypatch.setattr(run_test, "_metrics_in_redis", lambda a, r: ({1}, {}))
        assert run_test.report_metrics_completeness(
            _args(tmp_path, allow_missing_metrics=1), {1, 2, 3}, "now", True) is False


class TestDeclaredSilentAgents:
    """A count cannot tell the intended casualties from the accidental ones."""

    def test_declared_ids_account_for_the_shortfall(self, tmp_path, monkeypatch):
        monkeypatch.setattr(run_test, "_metrics_in_redis", lambda a, r: ({1, 4}, {}))
        assert run_test.report_metrics_completeness(
            _args(tmp_path, expect_silent_agents="2,3"), {1, 2, 3, 4}, "now", True) is True

    def test_a_different_agent_being_silent_still_fails(self, tmp_path, monkeypatch):
        """Same count as declared, wrong agents: the fault measured was not the one intended."""
        monkeypatch.setattr(run_test, "_metrics_in_redis", lambda a, r: ({2, 3}, {}))
        assert run_test.report_metrics_completeness(
            _args(tmp_path, expect_silent_agents="2,3"), {1, 2, 3, 4}, "now", True) is False
        shortfall = json.loads((tmp_path / "metrics_shortfall.json").read_text())
        assert shortfall["unexpectedly_silent"] == [1, 4]
        assert shortfall["expect_silent_agents"] == [2, 3]

    def test_kills_that_did_not_take_are_called_out(self, tmp_path, monkeypatch, capsys):
        monkeypatch.setattr(run_test, "_metrics_in_redis", lambda a, r: ({1, 2, 3}, {}))
        assert run_test.report_metrics_completeness(
            _args(tmp_path, expect_silent_agents="2"), {1, 2, 3}, "now", True) is True
        assert "did not take effect" in capsys.readouterr().out


class TestHostsFileForStop:
    def test_prefers_the_declared_hosts_file(self, tmp_path):
        declared = tmp_path / "given_hosts.txt"
        declared.write_text("agent-1\n")
        got = run_test._hosts_file_for_stop(
            _args(tmp_path, agent_hosts_file=str(declared)), ["agent-9"])
        assert got == str(declared)

    def test_writes_one_when_the_declared_file_is_gone(self, tmp_path):
        """cleanup_between_runs deletes agent_hosts.txt, and a stop with no hosts file is a
        silent no-op that leaves every agent running."""
        got = run_test._hosts_file_for_stop(
            _args(tmp_path, agent_hosts_file=str(tmp_path / "deleted.txt")),
            ["agent-1", "agent-2"])
        assert open(got).read().split() == ["agent-1", "agent-2"]

    def test_no_hosts_at_all_raises_rather_than_pretending(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        with pytest.raises(SystemExit):
            run_test._hosts_file_for_stop(_args(tmp_path), [])


class _StopStub:
    """Minimal host for Agent.stop(), which only touches the events, queues and transport."""

    from swarm.agents.agent_grpc import Agent as _Agent
    stop = _Agent.stop

    def __init__(self):
        self._stop_lock = threading.Lock()
        self._stopped = False
        self._stop_complete = threading.Event()
        self._stop_thread_ident = None
        self.logger = logging.getLogger("stop-stub")
        self.shutdown = False
        self.condition = threading.Condition()
        self.queues = type("Q", (), {
            "message_event": threading.Event(), "pending_event": threading.Event(),
            "selected_event": threading.Event()})()
        self.transport = type("T", (), {"stop": lambda self_: None})()
        self.calls = 0

    def on_shutdown(self):
        self.calls += 1


class TestStopIsOnceOnly:
    """stop() is reached from the SIGTERM handler and from the periodic thread's own
    shutdown condition, and the stop script touches the flag file and signals in one breath,
    so both fire. Running the teardown twice let the pre-drain payload land after the
    post-drain one."""

    def test_second_call_is_a_no_op(self):
        stub = _StopStub()
        stub.stop()
        stub.stop()
        stub.stop()
        assert stub.calls == 1

    def test_a_second_caller_waits_for_the_in_progress_teardown(self):
        """The sequence that loses metrics: the stop script touches the shutdown flag and
        SIGTERMs in one breath, so the periodic thread is already inside save_results when the
        handler runs. The handler os._exit()s the moment stop() returns, so stop() must not
        return while that save is still in flight."""
        from swarm.agents.agent_grpc import Agent

        saved = []
        entered = threading.Event()

        class _Stub(_StopStub):
            def on_shutdown(self):
                entered.set()
                time.sleep(0.4)   # stands in for the Redis write
                saved.append("metrics")

        stub = _Stub()
        threading.Thread(target=stub.stop, name="periodic").start()
        assert entered.wait(timeout=2), "teardown never started"
        # The signal handler's call, on another thread, as the real one is
        stub.stop(wait_timeout=5.0)
        assert saved == ["metrics"], "stop() returned before the in-flight save finished"

    def test_the_wait_is_bounded_when_teardown_hangs(self):
        release = threading.Event()
        entered = threading.Event()

        class _Stub(_StopStub):
            def on_shutdown(self):
                entered.set()
                release.wait()

        stub = _Stub()
        threading.Thread(target=stub.stop, daemon=True).start()
        assert entered.wait(timeout=2)
        try:
            started = time.monotonic()
            stub.stop(wait_timeout=0.5)
            elapsed = time.monotonic() - started
            assert 0.4 <= elapsed < 5.0, f"waited {elapsed:.2f}s, expected to give up at 0.5s"
        finally:
            release.set()

    def test_reentry_on_the_teardown_thread_does_not_deadlock(self):
        """A stop() reached from inside on_shutdown must not wait on an event only it can
        set — and must report False, so a signal handler does not exit the process out from
        under the teardown it just interrupted."""
        results = []

        class _Stub(_StopStub):
            def on_shutdown(self):
                results.append(self.stop(wait_timeout=30.0))  # would hang if it waited
                self.calls += 1

        stub = _Stub()
        done = threading.Event()

        def _run():
            stub.stop()
            done.set()

        threading.Thread(target=_run, daemon=True).start()
        assert done.wait(timeout=5), "re-entrant stop() deadlocked"
        assert stub.calls == 1
        assert results == [False], "same-thread re-entry must report that teardown is unfinished"

    def test_a_failed_teardown_still_releases_waiters(self):
        class _Stub(_StopStub):
            def on_shutdown(self):
                raise RuntimeError("redis is gone")

        stub = _Stub()
        stub.stop()
        started = time.monotonic()
        stub.stop(wait_timeout=5.0)
        assert time.monotonic() - started < 1.0, "a failed teardown left waiters blocked"

    def test_concurrent_callers_only_run_teardown_once(self):
        class _Stub(_StopStub):
            def on_shutdown(self):
                time.sleep(0.05)  # widen the window the old code raced in
                self.calls += 1

        stub = _Stub()
        threads = [threading.Thread(target=stub.stop) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert stub.calls == 1


class TestSigtermReturnStatus:
    def test_a_completed_or_timed_out_stop_reports_true(self):
        """Only same-thread re-entry may report False; a timeout must not stop the handler
        from exiting, because the stop script's SIGKILL is seconds away."""
        stub = _StopStub()
        assert stub.stop() is True

        release, entered = threading.Event(), threading.Event()

        class _Hanging(_StopStub):
            def on_shutdown(self):
                entered.set()
                release.wait()

        hung = _Hanging()
        threading.Thread(target=hung.stop, daemon=True).start()
        assert entered.wait(timeout=2)
        try:
            assert hung.stop(wait_timeout=0.3) is True
        finally:
            release.set()

    def test_the_handler_refuses_to_exit_on_an_unfinished_teardown(self):
        source = open(os.path.join(os.path.dirname(os.path.dirname(
            os.path.abspath(__file__))), "main.py")).read()
        start = source.index("def _sigterm_handler")
        handler = source[start:source.index("signal.signal(signal.SIGTERM", start)]
        assert "if not agent.stop(" in handler
        assert handler.index("return") < handler.index("os._exit(0)"), (
            "the early return has to precede the exit, or it exits regardless"
        )


class TestSigtermDoesNotWaitOnJobThreads:
    def test_handler_exits_the_process_rather_than_joining_workers(self):
        """ThreadPoolExecutor workers are non-daemon, so interpreter shutdown joins them and
        sys.exit() waited out the full simulated wall time — defeating the bounded drain."""
        source = open(os.path.join(os.path.dirname(os.path.dirname(
            os.path.abspath(__file__))), "main.py")).read()
        start = source.index("def _sigterm_handler")
        handler = source[start:source.index("signal.signal(signal.SIGTERM", start)]
        assert "os._exit(0)" in handler
        assert "logging.shutdown()" in handler, "os._exit skips the log flush atexit would do"
        assert "sys.exit(0)" not in handler
