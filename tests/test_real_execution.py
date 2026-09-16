# MIT License
#
# Copyright (c) 2024 swarm-workflows
#
# Author: Komal Thareja(kthare10@renci.org)
"""Running a workflow job for real instead of sleeping for its wall time.

The happy path here is small; nearly all of this is about the ways real execution could
quietly stop being a comparison. Three in particular, each of which would leave a run that
looks healthy and means nothing:

* a real run whose exit statuses are the *replayed* ones from the original Pegasus run;
* a run that silently mixes executed and simulated jobs because something was misconfigured
  and the code fell back to the sleep;
* a job that ran outside its container because the declared runtime was not installed.

All three are refusals or explicit precedence rules in the code, and all three are pinned
here. There is a genuine subprocess in these tests, not only mocks: the no-container path is
the one that can be exercised without Docker, and a runner that never actually ran anything
would pass a suite made entirely of mocks.
"""
import os
import shutil
import stat
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from swarm.execution import runner  # noqa: E402
from swarm.models.execution import ExecutionSpec  # noqa: E402
from swarm.models.job import Job  # noqa: E402

#: An executable that genuinely exists on this host. Not a hardcoded /bin/true: that path
#: does not exist on macOS, where the runner then (correctly) refuses it, and the test would
#: be asserting against its own portability bug rather than against the runner.
TRUE_BIN = shutil.which("true") or sys.executable


def script(tmp: str, name: str, body: str) -> str:
    """A real, executable shell script on disk."""
    path = os.path.join(tmp, name)
    Path(path).write_text("#!/bin/sh\n" + body)
    os.chmod(path, os.stat(path).st_mode | stat.S_IEXEC | stat.S_IRUSR)
    return path


class PolicyTestCase(unittest.TestCase):
    """Saves and restores the process-wide policy, which is global by design."""

    def setUp(self):
        self._saved = runner.policy()

    def tearDown(self):
        runner._POLICY = self._saved


# --------------------------------------------------------------------------------------
# Actually running something.
# --------------------------------------------------------------------------------------

class TestRunsForReal(PolicyTestCase):
    def test_a_script_runs_and_its_exit_status_is_reported(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = script(tmp, "ok.sh", "echo hello; exit 0")
            runner.configure(mode="real", work_dir=os.path.join(tmp, "work"))
            spec = ExecutionSpec.from_dict({"path": "/unused", "pfn": path, "arguments": []})
            result = runner.run(spec, "job-1")
        self.assertFalse(result.refused, result.reason)
        self.assertEqual(result.exit_status, 0)

    def test_a_failing_script_reports_its_own_status(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = script(tmp, "bad.sh", "exit 3")
            runner.configure(mode="real", work_dir=os.path.join(tmp, "work"))
            spec = ExecutionSpec.from_dict({"path": "/unused", "pfn": path, "arguments": []})
            result = runner.run(spec, "job-2")
        self.assertEqual(result.exit_status, 3)
        self.assertFalse(result.refused)

    def test_the_job_runs_in_the_shared_working_directory(self):
        """Bare logical file names only resolve between jobs of a DAG if they share a cwd."""
        with tempfile.TemporaryDirectory() as tmp:
            work = os.path.join(tmp, "work")
            path = script(tmp, "w.sh", "pwd > where.txt")
            runner.configure(mode="real", work_dir=work)
            spec = ExecutionSpec.from_dict({"path": "/unused", "pfn": path, "arguments": []})
            runner.run(spec, "job-3")
            self.assertEqual(Path(work, "where.txt").read_text().strip(),
                             os.path.realpath(work))

    def test_one_jobs_output_is_visible_to_the_next(self):
        with tempfile.TemporaryDirectory() as tmp:
            work = os.path.join(tmp, "work")
            producer = script(tmp, "p.sh", "echo payload > shared.txt")
            consumer = script(tmp, "c.sh", "test -f shared.txt")
            runner.configure(mode="real", work_dir=work)
            runner.run(ExecutionSpec.from_dict(
                {"path": "/u", "pfn": producer, "arguments": []}), "p")
            result = runner.run(ExecutionSpec.from_dict(
                {"path": "/u", "pfn": consumer, "arguments": []}), "c")
        self.assertEqual(result.exit_status, 0)

    def test_arguments_are_passed(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = script(tmp, "a.sh", 'test "$1" = "--flag"')
            runner.configure(mode="real", work_dir=os.path.join(tmp, "work"))
            spec = ExecutionSpec.from_dict(
                {"path": "/u", "pfn": path, "arguments": ["--flag"]})
            result = runner.run(spec, "job-4")
        self.assertEqual(result.exit_status, 0)

    def test_output_is_captured_to_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            work = os.path.join(tmp, "work")
            path = script(tmp, "o.sh", "echo OUT; echo ERR >&2")
            runner.configure(mode="real", work_dir=work)
            result = runner.run(ExecutionSpec.from_dict(
                {"path": "/u", "pfn": path, "arguments": []}), "job-5")
            self.assertEqual(Path(result.stdout_path).read_text().strip(), "OUT")
            self.assertEqual(Path(result.stderr_path).read_text().strip(), "ERR")

    def test_a_hung_job_times_out_and_is_killed(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = script(tmp, "hang.sh", "sleep 60")
            runner.configure(mode="real", work_dir=os.path.join(tmp, "work"), timeout_s=0.5)
            result = runner.run(ExecutionSpec.from_dict(
                {"path": "/u", "pfn": path, "arguments": []}), "job-6")
        self.assertEqual(result.exit_status, 124)
        self.assertIn("timed out", result.reason)


# --------------------------------------------------------------------------------------
# Refusals. Each of these would otherwise be a run that looks fine and compares nothing.
# --------------------------------------------------------------------------------------

class TestRefusals(PolicyTestCase):
    def test_a_missing_executable_is_refused_not_guessed(self):
        with tempfile.TemporaryDirectory() as tmp:
            runner.configure(mode="real", work_dir=os.path.join(tmp, "work"))
            result = runner.run(ExecutionSpec.from_dict(
                {"path": "/u", "pfn": "/nowhere/missing.py", "arguments": []}), "job-7")
        self.assertTrue(result.refused)
        self.assertIn("not found", result.reason)

    def test_unknown_arguments_are_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = script(tmp, "x.sh", "exit 0")
            runner.configure(mode="real", work_dir=os.path.join(tmp, "work"))
            result = runner.run(ExecutionSpec.from_dict(
                {"path": "/u", "pfn": path, "arguments": None}), "job-8")
        self.assertTrue(result.refused)

    def test_no_working_directory_is_refused(self):
        runner.configure(mode="real", work_dir="")
        result = runner.run(ExecutionSpec.from_dict(
            {"path": "/u", "pfn": TRUE_BIN, "arguments": []}), "job-9")
        self.assertTrue(result.refused)
        self.assertIn("working directory", result.reason)

    def test_a_missing_container_runtime_refuses_rather_than_running_on_the_host(self):
        """The one refusal that would otherwise produce a *passing* job: the code would run,
        just not in the container that defines what was being measured."""
        runner.configure(mode="real", work_dir="/tmp", container_runtime="none")
        spec = ExecutionSpec.from_dict({
            "path": "/srv/x", "arguments": [], "pfn": TRUE_BIN,
            "container": {"name": "c", "kind": "singularity", "image": "file:///i.sif"}})
        cmd, reason = runner.build_command(spec, "/tmp")
        self.assertEqual(cmd, [])
        self.assertIn("refusing to run outside the container", reason)

    def test_a_refusal_is_not_a_silent_fallback_to_simulation(self):
        """The failure this whole design guards against: a run whose jobs are part executed
        and part simulated, with nothing in the results telling them apart."""
        runner.configure(mode="real", work_dir="")
        job = Job()
        job.from_dict({"id": "j", "wall_time": 30.0,
                       "execution": {"path": "/srv/x", "pfn": TRUE_BIN}})
        job.execute()
        self.assertNotEqual(job.exit_status, 0)


# --------------------------------------------------------------------------------------
# Which jobs execute, and whose exit status wins.
# --------------------------------------------------------------------------------------

class TestModeAndPrecedence(PolicyTestCase):
    def test_simulate_is_the_default_even_for_a_job_that_could_run(self):
        runner.configure()
        job = Job()
        job.from_dict({"id": "j", "wall_time": 0.0,
                       "execution": {"path": "/srv/x", "pfn": TRUE_BIN}})
        self.assertFalse(job._real_execution_applies())

    def test_a_job_without_a_spec_simulates_even_in_a_real_run(self):
        """So a workflow and a synthetic background load can share one run."""
        runner.configure(mode="real", work_dir="/tmp")
        job = Job()
        job.from_dict({"id": "j", "wall_time": 0.0})
        self.assertFalse(job._real_execution_applies())

    def test_a_real_exit_status_overrides_the_replayed_one(self):
        """`should_fail` replays what the job did on somebody else's cluster months ago. It
        must never overwrite the status of a process that just ran here."""
        with tempfile.TemporaryDirectory() as tmp:
            path = script(tmp, "ok.sh", "exit 0")
            runner.configure(mode="real", work_dir=os.path.join(tmp, "work"))
            job = Job()
            job.from_dict({"id": "j", "wall_time": 0.0, "should_fail": True,
                           "execution": {"path": "/u", "pfn": path, "arguments": []}})
            job.execute()
        self.assertEqual(job.exit_status, 0)

    def test_a_simulated_job_still_replays_its_failure(self):
        runner.configure()
        job = Job()
        job.from_dict({"id": "j", "wall_time": 0.0, "should_fail": True})
        job.execute()
        self.assertEqual(job.exit_status, 1)


# --------------------------------------------------------------------------------------
# Path rewriting and image resolution.
# --------------------------------------------------------------------------------------

class TestPathsAndImages(PolicyTestCase):
    def test_a_submit_host_path_is_rewritten(self):
        runner.configure(path_rewrites=[{"from": "/home/ubuntu/wf", "to": "/export/wf"}])
        self.assertEqual(runner.rewrite_path("/home/ubuntu/wf/bin/a.py"), "/export/wf/bin/a.py")

    def test_the_longest_prefix_wins_regardless_of_config_order(self):
        """Config order must not decide which path a job runs from."""
        runner.configure(path_rewrites=[{"from": "/home", "to": "/A"},
                                        {"from": "/home/ubuntu/wf", "to": "/B"}])
        self.assertEqual(runner.rewrite_path("/home/ubuntu/wf/x.py"), "/B/x.py")

    def test_an_unmatched_path_is_left_alone(self):
        runner.configure(path_rewrites=[{"from": "/home/ubuntu/wf", "to": "/export/wf"}])
        self.assertEqual(runner.rewrite_path("/opt/tool"), "/opt/tool")

    def test_a_file_uri_image_is_stripped_and_rewritten(self):
        runner.configure(path_rewrites=[{"from": "/home/ubuntu/wf", "to": "/export/wf"}])
        spec = ExecutionSpec.from_dict({"path": "/u", "container": {
            "name": "c", "kind": "singularity", "image": "file:///home/ubuntu/wf/i.sif"}})
        self.assertEqual(runner.resolve_image(spec), "/export/wf/i.sif")

    def test_a_docker_uri_is_stripped(self):
        spec = ExecutionSpec.from_dict({"path": "/u", "container": {
            "name": "c", "kind": "docker", "image": "docker://repo/img:1"}})
        self.assertEqual(runner.resolve_image(spec), "repo/img:1")

    def test_an_override_replaces_the_catalogs_image_by_name(self):
        """The slice has Docker; the catalogs declare .sif. Which image stands in for which is
        a deployment decision, stated in config, never inferred from the host."""
        runner.configure(image_overrides={"soil": "swarm/soil:1.0"})
        spec = ExecutionSpec.from_dict({"path": "/u", "container": {
            "name": "soil", "kind": "singularity", "image": "file:///wf/Soil.sif"}})
        self.assertEqual(runner.resolve_image(spec), "swarm/soil:1.0")


# --------------------------------------------------------------------------------------
# Command construction.
# --------------------------------------------------------------------------------------

class TestBuildCommand(PolicyTestCase):
    def _spec(self, **over):
        base = {"path": "/srv/analyze", "arguments": [], "pfn": TRUE_BIN,
                "pfn_type": "stageable",
                "container": {"name": "c", "kind": "docker", "image": "docker://img:1"}}
        base.update(over)
        return ExecutionSpec.from_dict(base)

    def test_docker_binds_the_work_dir_and_overrides_the_entrypoint(self):
        runner.configure(container_runtime="docker")
        with patch("shutil.which", return_value="/usr/bin/docker"):
            cmd, reason = runner.build_command(self._spec(), "/work")
        self.assertEqual(reason, "")
        self.assertIn("-w", cmd)
        self.assertIn("/work:/work", cmd)
        # Without --entrypoint the image's own entrypoint swallows the command.
        self.assertIn("--entrypoint", cmd)
        self.assertEqual(cmd[cmd.index("--entrypoint") + 1], "/srv/analyze")

    def test_stageable_code_is_bound_onto_the_in_container_path(self):
        """Reproduces what Pegasus does: the code is not in the image, it is staged to the
        path the record names."""
        runner.configure(container_runtime="docker")
        with patch("shutil.which", return_value="/usr/bin/docker"):
            cmd, _ = runner.build_command(self._spec(), "/work")
        self.assertIn(f"{TRUE_BIN}:/srv/analyze:ro", cmd)

    def test_installed_code_is_not_bound_over(self):
        """`installed` code already exists in the image; a bind would shadow it."""
        runner.configure(container_runtime="docker")
        with patch("shutil.which", return_value="/usr/bin/docker"):
            cmd, _ = runner.build_command(self._spec(pfn_type="installed"), "/work")
        self.assertNotIn(f"{TRUE_BIN}:/srv/analyze:ro", cmd)

    def test_apptainer_form(self):
        runner.configure(container_runtime="apptainer")
        spec = self._spec(container={"name": "c", "kind": "singularity",
                                     "image": "/img.sif"})
        with patch("shutil.which", return_value="/usr/bin/apptainer"):
            cmd, reason = runner.build_command(spec, "/work")
        self.assertEqual(reason, "")
        self.assertEqual(cmd[:2], ["apptainer", "exec"])
        self.assertIn("--pwd", cmd)
        self.assertIn("/img.sif", cmd)

    def test_without_a_container_the_pfn_is_executed_directly(self):
        runner.configure(container_runtime="none")
        spec = ExecutionSpec.from_dict(
            {"path": "/srv/x", "arguments": ["-v"], "pfn": TRUE_BIN})
        cmd, reason = runner.build_command(spec, "/work")
        self.assertEqual(cmd, [TRUE_BIN, "-v"])
        self.assertEqual(reason, "")

    def test_a_sif_image_on_a_docker_only_host_is_refused(self):
        """The default situation on this fleet: agent VMs have docker only, every catalog
        declares a .sif. Docker cannot run one, and handing it one produces a confusing
        runtime error instead of an answer, so it is refused up front and the message names
        the key that fixes it."""
        runner.configure(container_runtime="docker")
        spec = self._spec(container={"name": "soil", "kind": "singularity",
                                     "image": "/wf/Soil.sif"})
        with patch("shutil.which", return_value="/usr/bin/docker"):
            cmd, reason = runner.build_command(spec, "/work")
        self.assertEqual(cmd, [])
        self.assertIn("image_overrides", reason)

    def test_an_override_makes_a_sif_container_runnable_under_docker(self):
        """Supplying a docker image for a .sif container is precisely what the override is
        for; second-guessing it would make the escape hatch unusable."""
        runner.configure(container_runtime="docker",
                         image_overrides={"soil": "swarm/soil:1.0"})
        spec = self._spec(container={"name": "soil", "kind": "singularity",
                                     "image": "/wf/Soil.sif"})
        with patch("shutil.which", return_value="/usr/bin/docker"):
            cmd, reason = runner.build_command(spec, "/work")
        self.assertEqual(reason, "")
        self.assertIn("swarm/soil:1.0", cmd)

    def test_an_imageless_container_is_refused(self):
        runner.configure(container_runtime="docker")
        spec = self._spec(container={"name": "c", "kind": "docker", "image": ""})
        cmd, reason = runner.build_command(spec, "/work")
        self.assertEqual(cmd, [])


if __name__ == "__main__":
    unittest.main()
