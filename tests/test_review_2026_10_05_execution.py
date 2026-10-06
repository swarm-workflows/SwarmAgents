"""Code review 2026-10-05 §61-§63: real execution, before any real-workflow run.

§61 A refusal (inputs not stageable, no runtime, an unrunnable spec) was persisted as exit 1
    and its reason dropped, so it read exactly like the workflow failing.
§62 A docker job's timeout killed the `docker run` client, not the container, which dockerd
    owns; and jobs run in their own sessions, so none died with the agent after its drain.
§63 "Produced by this run" was tested against the LOCATION hash: a name in the readiness set
    with no location fell through to the inputs root and read a stale same-named file.
"""
import csv
import os
import subprocess
import sys
from unittest.mock import MagicMock

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from swarm.execution import runner, staging  # noqa: E402
from swarm.models.job import Job  # noqa: E402


@pytest.fixture(autouse=True)
def _clean_policy():
    runner.configure()
    staging.configure()
    staging.set_context()
    yield
    runner.configure()
    staging.configure()
    staging.set_context()


class _Node:
    def __init__(self, file):
        self.file = file
        self.name = "local"


# --------------------------------------------------------------------------- §63
class TestProducedWithoutLocation:
    def _setup(self, tmp_path):
        inputs_root = str(tmp_path / "inputs")
        os.makedirs(inputs_root)
        with open(os.path.join(inputs_root, "out.csv"), "wb") as f:
            f.write(b"last week's file")
        work = str(tmp_path / "work")
        os.makedirs(work)
        staging.configure(enabled=True)
        runner.configure(mode="real", roots={"inputs": inputs_root})
        return work

    def test_a_produced_name_with_no_location_is_refused(self, tmp_path):
        work = self._setup(tmp_path)
        staging.set_context(produced=lambda names: {"out.csv"} & set(names))
        staged, refusal = runner.stage_inputs([_Node("out.csv")], work,
                                              locator=lambda names: {}, run_id="r1")
        assert staged == []
        assert "produced by this run but has no recorded location" in refusal
        assert not os.path.exists(os.path.join(work, "out.csv"))

    def test_a_root_input_nobody_produced_still_resolves(self, tmp_path):
        work = self._setup(tmp_path)
        staging.set_context(produced=lambda names: set())
        staged, refusal = runner.stage_inputs([_Node("out.csv")], work,
                                              locator=lambda names: {}, run_id="r1")
        assert refusal == "" and staged == ["out.csv"]

    def test_a_failed_produced_lookup_refuses(self, tmp_path):
        work = self._setup(tmp_path)

        def broken(names):
            raise ConnectionError("redis down")
        staging.set_context(produced=broken)
        _staged, refusal = runner.stage_inputs([_Node("out.csv")], work,
                                               locator=lambda names: {}, run_id="r1")
        assert "produced-names lookup failed" in refusal

    def test_staging_off_is_unchanged(self, tmp_path):
        work = self._setup(tmp_path)
        staging.configure(enabled=False)
        staging.set_context(produced=lambda names: {"out.csv"})
        _staged, refusal = runner.stage_inputs([_Node("out.csv")], work)
        assert refusal == ""

    def test_the_agent_installs_the_query(self):
        src = open(os.path.join(REPO, "swarm/agents/agent_grpc.py")).read()
        assert "produced=self.repository.produced_names" in src


# --------------------------------------------------------------------------- §62
class TestContainersCanBeKilled:
    def test_docker_run_gets_a_name_and_init(self):
        cmd, name = runner._name_container(
            ["docker", "run", "--rm", "-v", "/w:/w", "--entrypoint", "/x", "img"], "job/1", "run-9")
        assert cmd[:5] == ["docker", "run", "--name", name, "--init"]
        assert cmd[5:] == ["--rm", "-v", "/w:/w", "--entrypoint", "/x", "img"]
        assert name.startswith("swarm-run-9-job_1-")

    def test_other_runtimes_are_untouched(self):
        for cmd in (["apptainer", "exec", "x.sif", "/x"], ["/bin/true"]):
            assert runner._name_container(cmd, "j", "r") == (cmd, None)

    def test_a_timeout_kills_the_container_too(self, monkeypatch):
        calls = []
        monkeypatch.setattr(runner.subprocess, "run",
                            lambda cmd, **k: calls.append(cmd) or MagicMock(returncode=0))
        proc = subprocess.Popen(["sleep", "30"], start_new_session=True)
        runner._kill_group(proc, "swarm-r-j-1")
        assert ["docker", "kill", "swarm-r-j-1"] in calls
        assert proc.poll() is not None

    def test_terminate_all_kills_what_is_still_running(self, monkeypatch):
        monkeypatch.setattr(runner.subprocess, "run", lambda cmd, **k: MagicMock(returncode=0))
        proc = subprocess.Popen(["sleep", "30"], start_new_session=True)
        runner._register(proc, None)
        try:
            assert runner.terminate_all() >= 1
            assert proc.poll() is not None
        finally:
            runner._unregister(proc)

    def test_a_finished_job_leaves_the_registry(self, tmp_path):
        from swarm.models.execution import ExecutionSpec
        script = tmp_path / "ok.sh"
        script.write_text("#!/bin/sh\nexit 0\n")
        script.chmod(0o755)
        runner.configure(mode="real", work_dir=str(tmp_path / "w"))
        spec = ExecutionSpec(path=str(script), arguments=[], pfn=str(script))
        res = runner.run(spec, "j1")
        assert res.exit_status == 0, res.reason
        assert runner._LIVE == {}

    def test_the_agent_drain_calls_terminate_all(self):
        src = open(os.path.join(REPO, "swarm/agents/resource_agent.py")).read()
        body = src[src.index("    def _drain_executor"):src.index("    def _save_results_safely")]
        assert "runner.terminate_all()" in body


# --------------------------------------------------------------------------- §61
class TestRefusalsAreRecorded:
    def test_a_refusal_reason_round_trips(self):
        j = Job()
        j.job_id = "j1"
        j._refusal_reason = "input 'x' could not be staged"
        back = Job()
        back.from_dict(j.to_dict())
        assert back.refusal_reason == "input 'x' could not be staged"

    def test_no_refusal_is_none(self):
        back = Job()
        back.from_dict(Job().to_dict() | {"id": "j2"})
        assert back.refusal_reason is None

    def test_a_refused_execution_sets_it(self, monkeypatch):
        j = Job()
        j.job_id = "j1"
        j._execution = MagicMock()
        monkeypatch.setattr(runner, "run", lambda *a, **k: runner.ExecutionResult(
            exit_status=1, refused=True, reason="no container runtime"))
        assert j._execute_real() == 1
        assert j.refusal_reason == "no container runtime"

    def test_the_export_and_collector_carry_it(self, tmp_path):
        from plotting.data import save_jobs
        from evaluation.collect import run_metrics
        jobs = []
        for jid, reason, status in (("a", None, 0), ("b", "refused: no runtime", 1), ("c", None, 2)):
            j = Job()
            j.job_id = jid
            j.leader_id = 1
            j.state = j.state.__class__.COMPLETE
            j.mark_submitted()
            j.mark_completed()
            j.exit_status = status
            j._refusal_reason = reason
            jobs.append(j)
        run_dir = tmp_path / "mesh-3" / "run01"
        run_dir.mkdir(parents=True)
        save_jobs(jobs, str(run_dir))
        with open(run_dir / "all_jobs.csv") as f:
            rows = list(csv.reader(f))
        assert rows[0][-1] == "refused"
        assert all(len(r) == len(rows[0]) for r in rows)
        m = run_metrics(run_dir, expected_jobs=3)
        assert m["exit_failures"] == 2
        assert m["exec_refusals"] == 1
