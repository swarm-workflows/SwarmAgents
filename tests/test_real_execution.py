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

    def test_a_docker_uri_keeps_its_scheme_until_the_runtime_is_known(self):
        """This used to strip the scheme here, which is the wrong layer: docker wants it
        gone, apptainer requires it. `build_command` decides -- see TestImageSchemePerRuntime."""
        spec = ExecutionSpec.from_dict({"path": "/u", "container": {
            "name": "c", "kind": "docker", "image": "docker://repo/img:1"}})
        self.assertEqual(runner.resolve_image(spec), "docker://repo/img:1")

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


# --------------------------------------------------------------------------------------
# Defects found reviewing the first cut. Each only fires in anger.
# --------------------------------------------------------------------------------------

class TestImageSchemePerRuntime(PolicyTestCase):
    """`docker://` belongs to docker and must survive for apptainer.

    Stripping it unconditionally made apptainer look for a local FILE named `repo:tag`,
    which fails with an error naming a path nobody wrote.
    """

    def _spec(self, kind, image):
        return ExecutionSpec.from_dict({
            "path": "/srv/x", "arguments": [], "pfn_type": "installed",
            "container": {"name": "c", "kind": kind, "image": image}})

    def test_docker_gets_the_bare_reference(self):
        runner.configure(container_runtime="docker")
        with patch("shutil.which", return_value="/usr/bin/docker"):
            cmd, reason = runner.build_command(self._spec("docker", "docker://repo/img:1"), "/w")
        self.assertEqual(reason, "")
        self.assertIn("repo/img:1", cmd)
        self.assertNotIn("docker://repo/img:1", cmd)

    def test_apptainer_keeps_the_scheme(self):
        runner.configure(container_runtime="apptainer")
        with patch("shutil.which", return_value="/usr/bin/apptainer"):
            cmd, reason = runner.build_command(self._spec("docker", "docker://repo/img:1"), "/w")
        self.assertEqual(reason, "")
        self.assertIn("docker://repo/img:1", cmd)

    def test_a_docker_uri_is_not_mistaken_for_a_sif(self):
        """The .sif guard must not fire on a docker:// image just because the catalog
        called the container singularity — apptainer can pull from a registry."""
        runner.configure(container_runtime="docker")
        with patch("shutil.which", return_value="/usr/bin/docker"):
            cmd, reason = runner.build_command(
                self._spec("singularity", "docker://repo/img:1"), "/w")
        self.assertEqual(reason, "")
        self.assertIn("repo/img:1", cmd)


class TestRuntimePreference(PolicyTestCase):
    def test_auto_prefers_apptainer_over_the_singularity_alias(self):
        """Current packages ship `singularity` as a symlink to `apptainer`. Preferring that
        name depends on an alias a future package could drop, and logs a command naming a
        program that is not the one that ran."""
        runner.configure(container_runtime="auto")
        spec = ExecutionSpec.from_dict({
            "path": "/srv/x", "arguments": [], "pfn_type": "installed",
            "container": {"name": "c", "kind": "singularity", "image": "/i.sif"}})
        with patch("shutil.which", lambda b: f"/usr/bin/{b}"):   # both present
            cmd, reason = runner.build_command(spec, "/w")
        self.assertEqual(reason, "")
        self.assertEqual(cmd[0], "apptainer")

    def test_the_singularity_alias_is_still_accepted_when_it_is_all_there_is(self):
        runner.configure(container_runtime="auto")
        spec = ExecutionSpec.from_dict({
            "path": "/srv/x", "arguments": [], "pfn_type": "installed",
            "container": {"name": "c", "kind": "singularity", "image": "/i.sif"}})
        with patch("shutil.which", lambda b: "/usr/bin/singularity" if b == "singularity" else None):
            cmd, reason = runner.build_command(spec, "/w")
        self.assertEqual(reason, "")
        self.assertEqual(cmd[0], "singularity")


class TestNoZombies(PolicyTestCase):
    def test_a_timed_out_process_is_reaped(self):
        """Killing without waiting leaves a zombie per timeout, and an agent runs many jobs
        over its life."""
        with tempfile.TemporaryDirectory() as tmp:
            path = script(tmp, "hang.sh", "sleep 60")
            runner.configure(mode="real", work_dir=os.path.join(tmp, "work"), timeout_s=0.3)
            import subprocess as sp
            spawned = []
            real_popen = sp.Popen

            def capture(*a, **kw):
                proc = real_popen(*a, **kw)
                spawned.append(proc)
                return proc

            with patch("subprocess.Popen", side_effect=capture):
                result = runner.run(ExecutionSpec.from_dict(
                    {"path": "/u", "pfn": path, "arguments": []}), "z")
            self.assertTrue(spawned, "expected a process to have been started")
            proc = spawned[0]
        self.assertEqual(result.exit_status, 124)
        # returncode is set only once the child has been waited for.
        self.assertIsNotNone(proc.returncode)


class TestOutputHandleLeak(PolicyTestCase):
    def test_the_first_log_handle_is_closed_when_the_second_fails(self):
        """The fallback rebinds both names to DEVNULL, so without an explicit close the
        `finally` closes DEVNULL and leaks the real file."""
        opened = []
        real_open = open

        def flaky(path, *a, **kw):
            if str(path).endswith(".err"):
                raise OSError("no space left on device")
            handle = real_open(path, *a, **kw)
            opened.append(handle)
            return handle

        with tempfile.TemporaryDirectory() as tmp:
            path = script(tmp, "ok.sh", "exit 0")
            runner.configure(mode="real", work_dir=os.path.join(tmp, "work"))
            with patch("builtins.open", side_effect=flaky):
                result = runner.run(ExecutionSpec.from_dict(
                    {"path": "/u", "pfn": path, "arguments": []}), "leak")
        self.assertEqual(result.exit_status, 0)     # losing logs must not lose the run
        self.assertTrue(opened, "expected the .out handle to have been opened")
        self.assertTrue(all(h.closed for h in opened), "a log handle was leaked")


class TestEnvironmentIsScrubbed(PolicyTestCase):
    def test_agent_secrets_do_not_reach_the_job(self):
        env = {"PATH": "/usr/bin", "HOME": "/root", "OPENAI_API_KEY": "sk-secret",
               "REDIS_PASSWORD": "hunter2", "MY_TOKEN": "t", "SWARM_RUN_ID": "run-1"}
        out = runner.job_environment(env)
        for leaked in ("OPENAI_API_KEY", "REDIS_PASSWORD", "MY_TOKEN", "SWARM_RUN_ID"):
            self.assertNotIn(leaked, out)

    def test_ordinary_environment_survives(self):
        """A denylist, not an allowlist: real workflow code needs ambient environment, and
        an allowlist would break workflows in ways that look like workflow bugs."""
        env = {"PATH": "/usr/bin", "HOME": "/root", "LANG": "C.UTF-8",
               "HTTPS_PROXY": "http://p:3128"}
        self.assertEqual(runner.job_environment(env), env)

    def test_the_running_job_really_cannot_see_them(self):
        with tempfile.TemporaryDirectory() as tmp:
            work = os.path.join(tmp, "work")
            path = script(tmp, "e.sh", 'echo "${OPENAI_API_KEY:-ABSENT}"')
            runner.configure(mode="real", work_dir=work)
            with patch.dict(os.environ, {"OPENAI_API_KEY": "sk-secret"}):
                result = runner.run(ExecutionSpec.from_dict(
                    {"path": "/u", "pfn": path, "arguments": []}), "env")
            self.assertEqual(Path(result.stdout_path).read_text().strip(), "ABSENT")


# --------------------------------------------------------------------------------------
# Roots: naming code, inputs and images without absolute paths.
# --------------------------------------------------------------------------------------

class _Node:
    """Just enough DataNode for staging."""
    def __init__(self, file):
        self.file = file
        self.name = "local"


class TestRoots(PolicyTestCase):
    def test_an_absolute_pfn_is_used_as_written(self):
        """A Pegasus-derived job carries absolute submit-host paths and must keep resolving
        through path_rewrites exactly as before roots existed."""
        with tempfile.TemporaryDirectory() as tmp:
            real = script(tmp, "a.sh", "exit 0")
            runner.configure(container_runtime="none",
                             roots={"code": "/wrong/place"})
            spec = ExecutionSpec.from_dict({"path": "/srv/x", "arguments": [], "pfn": real})
            cmd, reason = runner.build_command(spec, "/w")
        self.assertEqual(cmd[0], real)

    def test_a_relative_pfn_resolves_under_the_code_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            os.makedirs(os.path.join(tmp, "bin"))
            script(os.path.join(tmp, "bin"), "analyze.py", "exit 0")
            runner.configure(container_runtime="none", roots={"code": tmp})
            spec = ExecutionSpec.from_dict(
                {"path": "/srv/x", "arguments": [], "pfn": "bin/analyze.py"})
            cmd, reason = runner.build_command(spec, "/w")
        self.assertEqual(cmd[0], os.path.join(tmp, "bin", "analyze.py"))

    def test_a_relative_pfn_with_no_root_is_refused_not_guessed(self):
        """Falling back to the process cwd would run whatever happened to sit there."""
        runner.configure(container_runtime="none", roots={})
        spec = ExecutionSpec.from_dict(
            {"path": "/srv/x", "arguments": [], "pfn": "bin/analyze.py"})
        cmd, reason = runner.build_command(spec, "/w")
        self.assertEqual(cmd, [])

    def test_a_relative_image_resolves_under_the_images_root(self):
        runner.configure(container_runtime="apptainer", roots={"images": "/imgs"})
        spec = ExecutionSpec.from_dict({
            "path": "/srv/x", "arguments": [], "pfn_type": "installed",
            "container": {"name": "c", "kind": "singularity", "image": "Soil.sif"}})
        self.assertEqual(runner.resolve_image(spec), "/imgs/Soil.sif")

    def test_a_docker_uri_is_not_treated_as_a_relative_name(self):
        runner.configure(container_runtime="docker", roots={"images": "/imgs"})
        spec = ExecutionSpec.from_dict({
            "path": "/srv/x", "arguments": [], "pfn_type": "installed",
            "container": {"name": "c", "kind": "docker", "image": "docker://repo/img:1"}})
        self.assertEqual(runner.resolve_image(spec), "docker://repo/img:1")


class TestInputStaging(PolicyTestCase):
    def test_a_declared_input_is_staged_from_the_inputs_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            inputs = os.path.join(tmp, "in"); os.makedirs(inputs)
            Path(inputs, "polygons.json").write_text("{}")
            work = os.path.join(tmp, "work"); os.makedirs(work)
            runner.configure(roots={"inputs": inputs})
            staged, refusal = runner.stage_inputs([_Node("polygons.json")], work)
            self.assertEqual(refusal, "")
            self.assertEqual(staged, ["polygons.json"])
            self.assertTrue(os.path.exists(os.path.join(work, "polygons.json")))

    def test_an_existing_file_is_never_overwritten(self):
        """It is a parent job's output. Replacing it with a stale replica corrupts the DAG in
        the most confusing way available: the parent ran, the child read something else."""
        with tempfile.TemporaryDirectory() as tmp:
            inputs = os.path.join(tmp, "in"); os.makedirs(inputs)
            Path(inputs, "shared.csv").write_text("STALE")
            work = os.path.join(tmp, "work"); os.makedirs(work)
            Path(work, "shared.csv").write_text("FRESH-FROM-PARENT")
            runner.configure(roots={"inputs": inputs})
            staged, refusal = runner.stage_inputs([_Node("shared.csv")], work)
            self.assertEqual(refusal, "")
            self.assertEqual(staged, [])
            self.assertEqual(Path(work, "shared.csv").read_text(), "FRESH-FROM-PARENT")

    def test_a_missing_input_is_refused_not_ignored(self):
        """A job without its input usually does not fail — it produces empty or default
        output, which looks like a result until someone checks the numbers."""
        with tempfile.TemporaryDirectory() as tmp:
            inputs = os.path.join(tmp, "in"); os.makedirs(inputs)
            work = os.path.join(tmp, "work"); os.makedirs(work)
            runner.configure(roots={"inputs": inputs})
            staged, refusal = runner.stage_inputs([_Node("absent.json")], work)
        self.assertIn("absent.json", refusal)

    def test_a_job_refuses_to_run_when_an_input_is_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = script(tmp, "ok.sh", "exit 0")
            runner.configure(mode="real", work_dir=os.path.join(tmp, "work"),
                             container_runtime="none", roots={"inputs": tmp})
            res = runner.run(ExecutionSpec.from_dict(
                {"path": "/u", "pfn": path, "arguments": []}), "j",
                data_in=[_Node("nope.csv")])
        self.assertTrue(res.refused)
        self.assertIn("nope.csv", res.reason)

    def test_staging_is_atomic_so_a_partial_file_is_never_visible(self):
        """The working directory is shared and several agents stage concurrently; a
        half-written file is readable and looks complete."""
        with tempfile.TemporaryDirectory() as tmp:
            inputs = os.path.join(tmp, "in"); os.makedirs(inputs)
            Path(inputs, "big.bin").write_bytes(b"x" * 4096)
            work = os.path.join(tmp, "work"); os.makedirs(work)
            runner.configure(roots={"inputs": inputs})
            seen = []
            real_link = os.link

            def watch(src, dst):
                # At the moment the name appears the destination must not already exist as
                # a partial file: the copy went to a temporary name first.
                seen.append(os.path.exists(dst))
                return real_link(src, dst)

            with patch("os.link", side_effect=watch):
                runner.stage_inputs([_Node("big.bin")], work)
            self.assertEqual(seen, [False])
            self.assertEqual(Path(work, "big.bin").read_bytes(), b"x" * 4096)
            self.assertEqual([p for p in os.listdir(work) if ".staging." in p], [])

    def test_a_parent_output_written_during_the_copy_is_not_clobbered(self):
        """The existence check is check-then-act: a parent job can finish while the copy is
        in flight. A replace would then overwrite a fresh result with a stale replica —
        the never-overwrite rule defeated through a window rather than directly."""
        with tempfile.TemporaryDirectory() as tmp:
            inputs = os.path.join(tmp, "in"); os.makedirs(inputs)
            Path(inputs, "shared.csv").write_text("STALE")
            work = os.path.join(tmp, "work"); os.makedirs(work)
            runner.configure(roots={"inputs": inputs})
            real_copy = __import__("shutil").copy2

            def racing_copy(src, dst):
                out = real_copy(src, dst)
                # the parent finishes right here, after the existence check
                Path(work, "shared.csv").write_text("FRESH-FROM-PARENT")
                return out

            with patch("shutil.copy2", side_effect=racing_copy):
                staged, refusal = runner.stage_inputs([_Node("shared.csv")], work)
            self.assertEqual(refusal, "")
            self.assertEqual(staged, [])
            self.assertEqual(Path(work, "shared.csv").read_text(), "FRESH-FROM-PARENT")
            self.assertEqual([p for p in os.listdir(work) if ".staging." in p], [])

    def test_a_site_name_is_never_treated_as_a_file(self):
        """On a DataNode `name` is the SITE (`local`, `dtn3`) and `file` is the file name.
        Falling back to `name` would try to stage a file called "dtn3" and refuse a job
        that was fine."""
        node = _Node(None)
        node.name = "dtn3"
        with tempfile.TemporaryDirectory() as tmp:
            runner.configure(roots={"inputs": tmp})
            staged, refusal = runner.stage_inputs([node], tmp)
        self.assertEqual((staged, refusal), ([], ""))

    def test_a_path_in_a_declared_name_cannot_escape_the_work_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            inputs = os.path.join(tmp, "in"); os.makedirs(inputs)
            Path(inputs, "passwd").write_text("staged-not-escaped")
            work = os.path.join(tmp, "work"); os.makedirs(work)
            runner.configure(roots={"inputs": inputs})
            staged, refusal = runner.stage_inputs([_Node("../../etc/passwd")], work)
            self.assertEqual(staged, ["passwd"])
            self.assertTrue(os.path.isfile(os.path.join(work, "passwd")))


class TestRootContainment(PolicyTestCase):
    """A job record is workflow-supplied data; naming a root means everything resolves in it."""

    def test_an_image_may_not_climb_out_of_the_images_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            outside = os.path.join(tmp, "outside"); os.makedirs(outside)
            Path(outside, "evil.sif").write_bytes(b"x")
            images = os.path.join(tmp, "images"); os.makedirs(images)
            runner.configure(container_runtime="apptainer", roots={"images": images})
            spec = ExecutionSpec.from_dict({
                "path": "/srv/x", "arguments": [], "pfn_type": "installed",
                "container": {"name": "c", "kind": "singularity",
                              "image": "../outside/evil.sif"}})
            with patch("shutil.which", return_value="/usr/bin/apptainer"):
                cmd, reason = runner.build_command(spec, "/w")
            self.assertEqual(cmd, [])
            self.assertIn("outside", reason)

    def test_a_pfn_may_not_climb_out_of_the_code_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            code = os.path.join(tmp, "code"); os.makedirs(code)
            runner.configure(container_runtime="none", roots={"code": code})
            spec = ExecutionSpec.from_dict(
                {"path": "/srv/x", "arguments": [], "pfn": "../../bin/sh"})
            cmd, reason = runner.build_command(spec, "/w")
            self.assertEqual(cmd, [])
            self.assertIn("outside", reason)

    def test_an_ordinary_nested_path_still_resolves(self):
        """Containment must not break the normal case it is wrapped around."""
        with tempfile.TemporaryDirectory() as tmp:
            os.makedirs(os.path.join(tmp, "bin"))
            script(os.path.join(tmp, "bin"), "a.sh", "exit 0")
            runner.configure(container_runtime="none", roots={"code": tmp})
            spec = ExecutionSpec.from_dict(
                {"path": "/srv/x", "arguments": [], "pfn": "bin/a.sh"})
            cmd, reason = runner.build_command(spec, "/w")
            self.assertEqual(cmd[0], os.path.join(tmp, "bin", "a.sh"))

    def test_a_bare_name_is_not_taken_from_the_agents_working_directory(self):
        """An unrelated file that happens to share the name must not be run instead of the
        image. This fall-back was reintroduced once already while fixing the sandbox case."""
        with tempfile.TemporaryDirectory() as tmp:
            os.makedirs(os.path.join(tmp, "decoy"))
            images = os.path.join(tmp, "images"); os.makedirs(images)
            runner.configure(container_runtime="apptainer", roots={"images": images})
            spec = ExecutionSpec.from_dict({
                "path": "/srv/x", "arguments": [], "pfn_type": "installed",
                "container": {"name": "c", "kind": "docker", "image": "decoy"}})
            cwd = os.getcwd()
            try:
                os.chdir(tmp)          # the decoy is now in the process's cwd
                with patch("shutil.which", return_value="/usr/bin/apptainer"):
                    cmd, _ = runner.build_command(spec, "/w")
            finally:
                os.chdir(cwd)
            self.assertIn("docker://decoy", cmd)
            self.assertNotIn(os.path.join(tmp, "decoy"), cmd)


class TestImageResolutionMatrix(PolicyTestCase):
    """The whole table, in one place.

    Image resolution has four independent dimensions — the catalog's `kind`, the form of the
    reference, the runtime, and whether a matching file exists in the images root — and
    fixing it one reported case at a time kept uncovering the next empty cell. Enumerating it
    is what makes the cases finite and reviewable.

    The rule the table encodes: **only apptainer can run an image from a path.** Docker
    resolves references against a registry or its local image store and never a file, so the
    same reference legitimately resolves differently per runtime. A bare name colliding with
    something in the images root is a local image under apptainer and a registry reference
    under docker.
    """

    # (kind, image) -> (expected under apptainer, expected under docker)
    # REFUSED means build_command returns no command.
    CASES = [
        (("docker", "ubuntu:22.04"),        "docker://ubuntu:22.04",    "ubuntu:22.04"),
        (("docker", "docker://ubuntu:1"),   "docker://ubuntu:1",        "ubuntu:1"),
        (("docker", "repo/img:1"),          "docker://repo/img:1",      "repo/img:1"),
        (("docker", "absent:1"),            "docker://absent:1",        "absent:1"),
        # collides with a directory in the images root
        (("docker", "ubuntu"),              "<root>/ubuntu",            "ubuntu"),
        (("docker", "sandbox"),             "<root>/sandbox",           "sandbox"),
        (("singularity", "Soil.sif"),       "<root>/Soil.sif",          "REFUSED"),
        (("singularity", "<root>/Soil.sif"), "<root>/Soil.sif",         "REFUSED"),
        (("singularity", "file://<root>/Soil.sif"), "<root>/Soil.sif",  "REFUSED"),
        (("singularity", "../outside.sif"), "REFUSED",                  "REFUSED"),
        (("", "Soil.sif"),                  "<root>/Soil.sif",          "REFUSED"),
    ]

    def test_every_combination(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = os.path.join(tmp, "imgs")
            os.makedirs(os.path.join(root, "sandbox"))
            os.makedirs(os.path.join(root, "ubuntu"))
            Path(root, "Soil.sif").write_bytes(b"x")
            Path(tmp, "outside.sif").write_bytes(b"x")

            def expand(s):
                return s.replace("<root>", root)

            for (kind, image), want_app, want_dock in self.CASES:
                for runtime, want in (("apptainer", want_app), ("docker", want_dock)):
                    with self.subTest(kind=kind, image=image, runtime=runtime):
                        runner.configure(container_runtime=runtime, roots={"images": root})
                        spec = ExecutionSpec.from_dict({
                            "path": "/srv/x", "arguments": [], "pfn_type": "installed",
                            "container": {"name": "c", "kind": kind,
                                          "image": expand(image)}})
                        with patch("shutil.which", lambda b: f"/usr/bin/{b}"):
                            cmd, reason = runner.build_command(spec, "/w")
                        if want == "REFUSED":
                            self.assertEqual(cmd, [], f"expected refusal, got {cmd}")
                            continue
                        self.assertTrue(cmd, f"unexpected refusal: {reason}")
                        got = (cmd[cmd.index("--entrypoint") + 2] if runtime == "docker"
                               else cmd[-2])
                        self.assertEqual(got, expand(want))

    def test_docker_is_never_handed_a_filesystem_path(self):
        """The invariant behind the table's docker column. Docker fails such a command with
        "invalid reference format", which says nothing about the cause."""
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, "img.oci").write_bytes(b"x")
            runner.configure(container_runtime="docker", roots={"images": tmp})
            spec = ExecutionSpec.from_dict({
                "path": "/srv/x", "arguments": [], "pfn_type": "installed",
                "container": {"name": "c", "kind": "docker",
                              "image": os.path.join(tmp, "img.oci")}})
            with patch("shutil.which", return_value="/usr/bin/docker"):
                cmd, reason = runner.build_command(spec, "/w")
            self.assertEqual(cmd, [])
            self.assertIn("docker", reason)


class TestOverrideResolution(PolicyTestCase):
    """An override REPLACES the catalog's image, so it goes through the same resolution.

    Returning it verbatim was the last remaining way a relative path reached the runtime
    unresolved, and so got resolved against the agent's own working directory. It matters
    because `image_overrides` is the documented answer to "the catalog names a .sif and this
    host runs docker" — writing a path there is the natural thing to do.
    """

    def test_a_relative_override_resolves_under_the_images_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            os.makedirs(os.path.join(tmp, "imgs"))
            Path(tmp, "imgs", "Soil.sif").write_bytes(b"x")
            runner.configure(container_runtime="apptainer", roots={"images": tmp},
                             image_overrides={"c": "imgs/Soil.sif"})
            spec = ExecutionSpec.from_dict({
                "path": "/srv/x", "arguments": [], "pfn_type": "installed",
                "container": {"name": "c", "kind": "singularity", "image": "Other.sif"}})
            self.assertEqual(runner.resolve_image(spec),
                             os.path.join(tmp, "imgs", "Soil.sif"))

    def test_a_registry_override_still_works(self):
        """The documented case: substituting a docker image for a singularity one. The
        override must NOT inherit the kind it is replacing, or this refuses."""
        runner.configure(container_runtime="docker", roots={"images": "/imgs"},
                         image_overrides={"c": "swarm/soil:1.0"})
        spec = ExecutionSpec.from_dict({
            "path": "/srv/x", "arguments": [], "pfn_type": "installed",
            "container": {"name": "c", "kind": "singularity", "image": "Soil.sif"}})
        self.assertEqual(runner.resolve_image(spec), "swarm/soil:1.0")
        with patch("shutil.which", return_value="/usr/bin/docker"):
            cmd, reason = runner.build_command(spec, "/w")
        self.assertEqual(reason, "")
        self.assertIn("swarm/soil:1.0", cmd)


class TestRootEdgeCases(PolicyTestCase):
    def test_a_root_of_slash_does_not_refuse_everything(self):
        """`root + os.sep` makes "//" for a root of "/", which nothing matches."""
        runner.configure(roots={"images": "/"})
        self.assertEqual(runner.resolve_under_root("etc/hosts", "images"), "/etc/hosts")

    def test_a_trailing_slash_on_a_root_is_harmless(self):
        runner.configure(roots={"images": "/imgs/"})
        self.assertEqual(runner.resolve_under_root("a.sif", "images"), "/imgs/a.sif")

    def test_containment_still_holds_for_a_root_of_slash(self):
        runner.configure(roots={"images": "/"})
        self.assertEqual(runner.resolve_under_root("../etc/hosts", "images"), "/etc/hosts")


class TestUnresolvableRelativePaths(PolicyTestCase):
    """A relative path with no root must refuse, and say which key is missing."""

    def test_a_relative_pfn_without_a_code_root_names_the_key(self):
        runner.configure(container_runtime="none", roots={})
        spec = ExecutionSpec.from_dict(
            {"path": "/srv/x", "arguments": [], "pfn": "bin/analyze.py"})
        cmd, reason = runner.build_command(spec, "/w")
        self.assertEqual(cmd, [])
        self.assertIn("roots.code", reason)
        self.assertNotIn("no pfn", reason)      # there IS one; it just cannot be resolved

    def test_a_bare_docker_tag_is_a_registry_reference_not_a_path(self):
        """`ubuntu:22.04` has no scheme and no leading slash, but it is not a file. Treating
        every bare name as a path broke every catalog that names a plain docker tag."""
        runner.configure(container_runtime="docker", roots={"images": "/imgs"})
        spec = ExecutionSpec.from_dict({
            "path": "/srv/x", "arguments": [], "pfn_type": "installed",
            "container": {"name": "c", "kind": "docker", "image": "ubuntu:22.04"}})
        self.assertEqual(runner.resolve_image(spec), "ubuntu:22.04")
        with patch("shutil.which", return_value="/usr/bin/docker"):
            cmd, reason = runner.build_command(spec, "/w")
        self.assertEqual(reason, "")
        self.assertIn("ubuntu:22.04", cmd)

    def test_a_namespaced_docker_reference_is_not_a_path(self):
        runner.configure(container_runtime="docker", roots={})
        spec = ExecutionSpec.from_dict({
            "path": "/srv/x", "arguments": [], "pfn_type": "installed",
            "container": {"name": "c", "kind": "docker", "image": "repo/img:1"}})
        self.assertEqual(runner.resolve_image(spec), "repo/img:1")

    def test_apptainer_gets_a_scheme_added_for_a_registry_reference(self):
        """The mirror of docker having its scheme stripped. Apptainer reads a bare reference
        as a LOCAL FILE NAME — handed `ubuntu:22.04` it looks for a file of that name and
        fails with an error naming a path nobody wrote."""
        runner.configure(container_runtime="apptainer", roots={})
        spec = ExecutionSpec.from_dict({
            "path": "/srv/x", "arguments": [], "pfn_type": "installed",
            "container": {"name": "c", "kind": "docker", "image": "ubuntu:22.04"}})
        with patch("shutil.which", return_value="/usr/bin/apptainer"):
            cmd, reason = runner.build_command(spec, "/w")
        self.assertEqual(reason, "")
        self.assertIn("docker://ubuntu:22.04", cmd)
        self.assertNotIn("ubuntu:22.04", cmd)

    def test_a_sandbox_directory_is_not_turned_into_a_registry_pull(self):
        """An apptainer sandbox is a directory with no extension — a real local image that
        no suffix rule can recognise. Prefixing it pulls a repository nobody published."""
        with tempfile.TemporaryDirectory() as tmp:
            os.makedirs(os.path.join(tmp, "mybox"))
            runner.configure(container_runtime="apptainer", roots={"images": tmp})
            spec = ExecutionSpec.from_dict({
                "path": "/srv/x", "arguments": [], "pfn_type": "installed",
                "container": {"name": "c", "kind": "docker", "image": "mybox"}})
            with patch("shutil.which", return_value="/usr/bin/apptainer"):
                cmd, reason = runner.build_command(spec, "/w")
            self.assertEqual(reason, "")
            self.assertIn(os.path.join(tmp, "mybox"), cmd)
            self.assertFalse(any(c.startswith("docker://") for c in cmd))

    def test_a_local_image_without_a_known_suffix_is_not_prefixed(self):
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, "image.oci").write_bytes(b"x")
            runner.configure(container_runtime="apptainer", roots={"images": tmp})
            spec = ExecutionSpec.from_dict({
                "path": "/srv/x", "arguments": [], "pfn_type": "installed",
                "container": {"name": "c", "kind": "docker", "image": "image.oci"}})
            with patch("shutil.which", return_value="/usr/bin/apptainer"):
                cmd, _ = runner.build_command(spec, "/w")
            self.assertIn(os.path.join(tmp, "image.oci"), cmd)

    def test_a_reference_that_exists_nowhere_is_still_a_registry_pull(self):
        with tempfile.TemporaryDirectory() as tmp:
            runner.configure(container_runtime="apptainer", roots={"images": tmp})
            spec = ExecutionSpec.from_dict({
                "path": "/srv/x", "arguments": [], "pfn_type": "installed",
                "container": {"name": "c", "kind": "docker", "image": "ubuntu:22.04"}})
            with patch("shutil.which", return_value="/usr/bin/apptainer"):
                cmd, _ = runner.build_command(spec, "/w")
            self.assertIn("docker://ubuntu:22.04", cmd)

    def test_apptainer_does_not_double_a_scheme_it_already_has(self):
        runner.configure(container_runtime="apptainer", roots={})
        spec = ExecutionSpec.from_dict({
            "path": "/srv/x", "arguments": [], "pfn_type": "installed",
            "container": {"name": "c", "kind": "docker", "image": "docker://repo/img:1"}})
        with patch("shutil.which", return_value="/usr/bin/apptainer"):
            cmd, _ = runner.build_command(spec, "/w")
        self.assertIn("docker://repo/img:1", cmd)
        self.assertNotIn("docker://docker://repo/img:1", cmd)

    def test_a_resolved_sif_path_is_left_alone_under_apptainer(self):
        """A .sif is an absolute path by this point; prefixing it would be nonsense."""
        runner.configure(container_runtime="apptainer", roots={"images": "/imgs"})
        spec = ExecutionSpec.from_dict({
            "path": "/srv/x", "arguments": [], "pfn_type": "installed",
            "container": {"name": "c", "kind": "singularity", "image": "Soil.sif"}})
        with patch("shutil.which", return_value="/usr/bin/apptainer"):
            cmd, _ = runner.build_command(spec, "/w")
        self.assertIn("/imgs/Soil.sif", cmd)
        self.assertNotIn("docker:///imgs/Soil.sif", cmd)

    def test_a_sif_suffix_is_a_file_even_when_the_kind_says_docker(self):
        """The suffix is the tiebreak when the catalog's kind is missing or wrong."""
        runner.configure(container_runtime="apptainer", roots={"images": "/imgs"})
        spec = ExecutionSpec.from_dict({
            "path": "/srv/x", "arguments": [], "pfn_type": "installed",
            "container": {"name": "c", "kind": "", "image": "Soil.sif"}})
        self.assertEqual(runner.resolve_image(spec), "/imgs/Soil.sif")

    def test_a_relative_image_without_an_images_root_refuses(self):
        """Returning the bare name would hand the runtime a relative path, which it
        resolves against the process's own working directory."""
        runner.configure(container_runtime="apptainer", roots={})
        spec = ExecutionSpec.from_dict({
            "path": "/srv/x", "arguments": [], "pfn_type": "installed",
            "container": {"name": "c", "kind": "singularity", "image": "Soil.sif"}})
        self.assertEqual(runner.resolve_image(spec), "")
        with patch("shutil.which", return_value="/usr/bin/apptainer"):
            cmd, reason = runner.build_command(spec, "/w")
        self.assertEqual(cmd, [])
        self.assertIn("roots.images", reason)

    def test_no_inputs_is_not_an_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            runner.configure(roots={})
            self.assertEqual(runner.stage_inputs([], tmp), ([], ""))
            self.assertEqual(runner.stage_inputs(None, tmp), ([], ""))


if __name__ == "__main__":
    unittest.main()


class TestModeHasOneDefault(unittest.TestCase):
    """`runtime.execution.mode` is resolved in exactly one place. It was briefly resolved twice
    with different defaults — the agent defaulting an absent key to `simulate`, and run_test's
    bundle validation reading absent as "assume it executes" — which refused ordinary simulated
    replays."""

    def test_absent_means_simulate(self):
        from swarm.execution.runner import resolve_mode
        self.assertEqual(resolve_mode(None), "simulate")
        self.assertEqual(resolve_mode({}), "simulate")

    def test_an_explicit_value_is_honoured_and_case_insensitive(self):
        from swarm.execution.runner import resolve_mode
        self.assertEqual(resolve_mode({"mode": "REAL"}), "real")
        self.assertEqual(resolve_mode({"mode": "simulate"}), "simulate")

    def test_an_unknown_value_raises(self):
        from swarm.execution.runner import resolve_mode
        with self.assertRaises(ValueError):
            resolve_mode({"mode": "dry-run"})
