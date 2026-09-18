"""--pegasus-jobs-dir: publishing a bundle converted somewhere else.

The flag exists so a workflow can be converted on the Pegasus submit host (the only machine
where the catalogs' absolute paths resolve) and then merely COPIED onto the shared export.
Everything pinned here is a way that could silently go wrong: the run publishing a synthetic
set instead, or `cleanup_between_runs` deleting the bundle it was pointed at.
"""
import json
import os
import sys
import types
from pathlib import Path

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.argv = ["run_test.py"]
import run_test  # noqa: E402


def _bundle(tmp_path, n=4, manifest=True, declared=None):
    for i in range(1, n + 1):
        (tmp_path / f"job_{i}.json").write_text("{}")
    if manifest:
        (tmp_path / "manifest.json").write_text("{}")
    (tmp_path / "conversion_summary.json").write_text(
        json.dumps({"total_jobs_written": n if declared is None else declared}))
    return tmp_path


class TestJobsDir:
    def test_defaults_to_the_generated_jobs_directory(self):
        assert run_test.jobs_dir(types.SimpleNamespace()) == "jobs"

    def test_the_flag_wins(self, tmp_path):
        args = types.SimpleNamespace(pegasus_jobs_dir=str(tmp_path))
        assert run_test.jobs_dir(args) == str(tmp_path)


class TestValidation:
    def test_a_directory_with_no_job_records_is_refused(self, tmp_path):
        # job_distributor.py publishes job_*.json and nothing else, so this would otherwise
        # start the whole fleet with nothing to schedule and read as a scheduling failure.
        args = types.SimpleNamespace(pegasus_jobs_dir=str(tmp_path), jobs=4)
        with pytest.raises(SystemExit):
            run_test.validate_pegasus_jobs_dir(args)

    def test_a_missing_directory_is_refused(self, tmp_path):
        args = types.SimpleNamespace(pegasus_jobs_dir=str(tmp_path / "nope"), jobs=4)
        with pytest.raises(SystemExit):
            run_test.validate_pegasus_jobs_dir(args)

    def test_a_bundle_is_accepted(self, tmp_path):
        args = types.SimpleNamespace(pegasus_jobs_dir=str(_bundle(tmp_path)), jobs=4)
        run_test.validate_pegasus_jobs_dir(args)

    def test_a_count_mismatch_warns_but_runs(self, tmp_path, capsys):
        # --jobs drives the agents' expected job count and the completion checks, not the
        # directory, so the two disagreeing is a real (recoverable) mistake.
        args = types.SimpleNamespace(pegasus_jobs_dir=str(_bundle(tmp_path)), jobs=9)
        run_test.validate_pegasus_jobs_dir(args)
        assert "WARNING: --jobs 9" in capsys.readouterr().out

    def test_an_unbundled_directory_warns(self, tmp_path, capsys):
        args = types.SimpleNamespace(
            pegasus_jobs_dir=str(_bundle(tmp_path, manifest=False)), jobs=4)
        run_test.validate_pegasus_jobs_dir(args)
        assert "no readable manifest.json" in capsys.readouterr().out


class TestCleanupLeavesTheBundleAlone:
    """`cleanup_between_runs` runs `rm -rf jobs`. Pointed at a bundle, that would delete the
    workflow — usually the copy on the shared export — rather than a regenerable synthetic set.
    """

    def _cleanup_cmds(self, monkeypatch, **overrides):
        cmds = []
        monkeypatch.setattr(run_test, "run_blocking", lambda cmd, **kw: cmds.append(cmd))
        args = types.SimpleNamespace(
            agents=5, db_host="localhost", use_config_dir=False,
            agent_hosts_file=None, pegasus_profiles=None, pegasus_jobs_dir=None)
        for k, v in overrides.items():
            setattr(args, k, v)
        run_test.cleanup_between_runs(args)
        return cmds

    def test_synthetic_jobs_are_still_cleaned(self, monkeypatch):
        assert ["rm", "-rf", "jobs"] in self._cleanup_cmds(monkeypatch)

    def test_a_pegasus_jobs_dir_is_never_cleaned(self, monkeypatch, tmp_path):
        cmds = self._cleanup_cmds(monkeypatch, pegasus_jobs_dir=str(tmp_path))
        assert not any(c[:2] == ["rm", "-rf"] and c[2] in ("jobs", str(tmp_path)) for c in cmds)

    def test_nor_is_it_when_it_IS_the_jobs_directory(self, monkeypatch):
        # The obvious thing to do with a bundle is to drop it at ./jobs.
        cmds = self._cleanup_cmds(monkeypatch, pegasus_jobs_dir="jobs")
        assert ["rm", "-rf", "jobs"] not in cmds


class TestStaleRecordsFromAnEarlierConversion:
    """The converter sweeps its own output directory; a COPY of it does not. `rsync -a`
    without --delete restages a new conversion into an old one and leaves the surplus behind,
    where the distributor publishes it as part of this run.
    """

    def test_surplus_records_are_refused(self, tmp_path):
        # 10 files on disk, a conversion that wrote 4: six belong to an earlier workflow.
        bundle = _bundle(tmp_path, n=10, declared=4)
        args = types.SimpleNamespace(pegasus_jobs_dir=str(bundle), jobs=4)
        with pytest.raises(SystemExit, match="earlier conversion"):
            run_test.validate_pegasus_jobs_dir(args)

    def test_a_matching_count_passes(self, tmp_path):
        args = types.SimpleNamespace(pegasus_jobs_dir=str(_bundle(tmp_path, n=4)), jobs=4)
        run_test.validate_pegasus_jobs_dir(args)

    def test_a_directory_with_no_summary_warns_rather_than_refusing(self, tmp_path, capsys):
        # Hand-assembled job sets are legitimate; they just cannot be checked.
        for i in range(1, 4):
            (tmp_path / f"job_{i}.json").write_text("{}")
        args = types.SimpleNamespace(pegasus_jobs_dir=str(tmp_path), jobs=3)
        run_test.validate_pegasus_jobs_dir(args)
        assert "no readable conversion_summary.json" in capsys.readouterr().out

    def test_an_unreadable_summary_warns_rather_than_refusing(self, tmp_path, capsys):
        bundle = _bundle(tmp_path, n=3)
        (bundle / "conversion_summary.json").write_text("{not json")
        args = types.SimpleNamespace(pegasus_jobs_dir=str(bundle), jobs=3)
        run_test.validate_pegasus_jobs_dir(args)
        assert "no readable conversion_summary.json" in capsys.readouterr().out


class TestNameCollisionsAreRefused:
    """Logical file names are the one thing a bundle does not namespace. Converting several
    workflows together (or several runs of one workflow) collides them, and every consequence
    is silent: a job gated on an unrelated workflow, or staged its file and succeeding on it.
    """

    CLEAN_DAG = {"gating": True, "execution_jobs": 0, "colliding_outputs": {},
                 "replica_conflicts": {}, "cross_workflow_edges": {}}

    def _bundle_with(self, tmp_path, dag=None, manifest=None, n=2):
        for i in range(1, n + 1):
            (tmp_path / f"job_{i}.json").write_text("{}")
        merged = dict(self.CLEAN_DAG)
        merged.update(dag or {})
        (tmp_path / "conversion_summary.json").write_text(
            json.dumps({"total_jobs_written": n, "dag": merged}))
        (tmp_path / "manifest.json").write_text(json.dumps(manifest or {"missing": []}))
        return types.SimpleNamespace(pegasus_jobs_dir=str(tmp_path), jobs=n)

    @staticmethod
    def _mode(monkeypatch, mode):
        """What the agents' config says — a bundle cannot know whether its jobs will execute."""
        monkeypatch.setattr(run_test, "_run_execution_mode", lambda _args: mode)

    def test_a_gated_bundle_with_two_producers_of_one_name_is_refused(self, tmp_path):
        args = self._bundle_with(tmp_path, dag={
            "colliding_outputs": {"output.csv": ["wfA_split", "wfB_extract"]}})
        with pytest.raises(SystemExit, match="produced by more than one job"):
            run_test.validate_pegasus_jobs_dir(args)

    def test_an_inert_replay_warns_instead(self, tmp_path, capsys, monkeypatch):
        # No gating and nothing executes: the names are never consulted and never touched.
        self._mode(monkeypatch, "simulate")
        args = self._bundle_with(tmp_path, dag={
            "gating": False,
            "colliding_outputs": {"output.csv": ["wfA_split", "wfB_extract"]}})
        run_test.validate_pegasus_jobs_dir(args)
        assert "Inert in this run" in capsys.readouterr().out

    def test_it_is_refused_for_a_real_run_even_without_gating(self, tmp_path, monkeypatch):
        # Without gating there is no producer map, but the working directory is still flat and
        # shared, so two jobs still write one file.
        self._mode(monkeypatch, "real")
        args = self._bundle_with(tmp_path, dag={
            "gating": False, "execution_jobs": 2,
            "colliding_outputs": {"output.csv": ["wfA_split", "wfB_extract"]}})
        with pytest.raises(SystemExit, match="produced by more than one job"):
            run_test.validate_pegasus_jobs_dir(args)

    def test_a_real_run_of_jobs_with_nothing_to_execute_is_still_inert(
            self, tmp_path, capsys, monkeypatch):
        # `mode: real` does not mean these jobs execute — a synthetic job in a real run still
        # simulates. Both halves are needed, or an ordinary replay is refused.
        self._mode(monkeypatch, "real")
        args = self._bundle_with(tmp_path, dag={
            "gating": False, "execution_jobs": 0,
            "colliding_outputs": {"output.csv": ["wfA_split", "wfB_extract"]}})
        run_test.validate_pegasus_jobs_dir(args)
        assert "Inert in this run" in capsys.readouterr().out

    def test_a_colliding_input_is_refused(self, tmp_path, monkeypatch):
        # The loser is not merely missing: the runner resolves a bare name under roots.inputs,
        # so that job would be staged the winner's bytes and succeed on the wrong data.
        self._mode(monkeypatch, "real")
        args = self._bundle_with(tmp_path, dag={"execution_jobs": 2}, manifest={"missing": [
            {"kind": "input", "lfn": "data.csv", "collision": True,
             "reason": "basename collides with '/wfA/data.csv'"}]})
        with pytest.raises(SystemExit, match="collide by basename"):
            run_test.validate_pegasus_jobs_dir(args)

    def test_a_colliding_input_is_inert_when_nothing_executes(
            self, tmp_path, capsys, monkeypatch):
        # Staging is the only thing that acts on it, and a simulated run never stages.
        self._mode(monkeypatch, "simulate")
        args = self._bundle_with(tmp_path, dag={"execution_jobs": 2}, manifest={"missing": [
            {"kind": "input", "lfn": "data.csv", "collision": True, "reason": "…"}]})
        run_test.validate_pegasus_jobs_dir(args)
        assert "Inert in this run" in capsys.readouterr().out

    def test_a_merely_missing_input_still_passes_validation(self, tmp_path, monkeypatch):
        # It fails loudly at execution time ("refuses a missing input"), which is a different
        # and much safer failure than running on another workflow's file.
        self._mode(monkeypatch, "real")
        args = self._bundle_with(tmp_path, dag={"execution_jobs": 2}, manifest={"missing": [
            {"kind": "input", "lfn": "gone.csv", "reason": "not found"}]})
        run_test.validate_pegasus_jobs_dir(args)

    def test_a_clean_bundle_passes(self, tmp_path):
        run_test.validate_pegasus_jobs_dir(self._bundle_with(tmp_path))

    def test_a_staged_input_another_workflow_produces_is_refused(self, tmp_path):
        args = self._bundle_with(tmp_path, dag={
            "replica_conflicts": {"data.csv": {"expected_by": ["wfB_load"],
                                               "produced_by": ["wfA_split"],
                                               "cross_workflow": True}}})
        with pytest.raises(SystemExit, match="another workflow"):
            run_test.validate_pegasus_jobs_dir(args)

    def test_the_same_name_inside_one_workflow_only_warns(self, tmp_path, capsys):
        # Pre-seeding an intermediate is how a job that cannot run here (an external API call)
        # is stood in for — legitimate, and not something to refuse.
        args = self._bundle_with(tmp_path, dag={
            "replica_conflicts": {"data.csv": {"expected_by": ["wfA_load"],
                                               "produced_by": ["wfA_fetch"],
                                               "cross_workflow": False}}})
        run_test.validate_pegasus_jobs_dir(args)
        assert "same workflow" in capsys.readouterr().out

    def test_a_name_one_workflow_produces_and_another_reads_is_refused(self, tmp_path):
        # The general case, and the one that needs no replica catalog to appear: B's job simply
        # reads a name A's job writes. Nothing else in the bundle looks wrong.
        args = self._bundle_with(tmp_path, dag={
            "cross_workflow_edges": {"out.csv": {"read_by": ["wfB_load"],
                                                 "produced_by": ["wfA_split"]}}})
        with pytest.raises(SystemExit, match="produced by one workflow and read by another"):
            run_test.validate_pegasus_jobs_dir(args)

    def test_a_bundle_predating_the_checks_is_not_read_as_clean(self, tmp_path, monkeypatch):
        # Silence from a check that never ran reads exactly like a clean bill of health.
        self._mode(monkeypatch, "simulate")
        args = self._bundle_with(tmp_path)
        summary = json.loads((tmp_path / "conversion_summary.json").read_text())
        del summary["dag"]["cross_workflow_edges"]
        (tmp_path / "conversion_summary.json").write_text(json.dumps(summary))
        with pytest.raises(SystemExit, match="never been checked"):
            run_test.validate_pegasus_jobs_dir(args)

    def _colliding_bundle(self, tmp_path):
        return self._bundle_with(tmp_path, dag={"gating": False, "execution_jobs": 4},
                                 manifest={"missing": [
                                     {"kind": "input", "lfn": "data.csv", "collision": True,
                                      "reason": "…"}]})

    def test_a_simulated_run_is_inert(self, tmp_path, capsys, monkeypatch):
        self._mode(monkeypatch, "simulate")
        run_test.validate_pegasus_jobs_dir(self._colliding_bundle(tmp_path))
        assert "Inert in this run" in capsys.readouterr().out

    def test_a_config_that_could_not_be_READ_is_checked_as_if_it_executes(
            self, tmp_path, monkeypatch):
        """Unknown is not absent. Absent means `simulate` and is a real answer; unreadable is
        no answer, and under --use-config-dir that run still starts — so skipping the checks
        would publish a colliding bundle into a run that does execute."""
        self._mode(monkeypatch, "unknown")
        with pytest.raises(SystemExit, match="collide by basename"):
            run_test.validate_pegasus_jobs_dir(self._colliding_bundle(tmp_path))


class TestRunExecutionMode:
    """What the agents of THIS run will do — read from the config they will actually use."""

    def _args(self, tmp_path):
        return types.SimpleNamespace(use_config_dir=True, config_dir=str(tmp_path))

    def test_an_absent_key_is_simulate(self, tmp_path):
        (tmp_path / "config_swarm_multi_1.yml").write_text("runtime:\n  results_dir: x\n")
        assert run_test._run_execution_mode(self._args(tmp_path)) == "simulate"

    def test_an_explicit_mode_is_read(self, tmp_path):
        (tmp_path / "config_swarm_multi_1.yml").write_text(
            "runtime:\n  execution:\n    mode: real\n")
        assert run_test._run_execution_mode(self._args(tmp_path)) == "real"

    def test_a_config_that_does_not_parse_is_unknown_not_simulate(self, tmp_path):
        # The reused config may well say `mode: real`; nothing here can tell, and the run
        # starts anyway because the agents read these files themselves.
        (tmp_path / "config_swarm_multi_1.yml").write_text("runtime: [unclosed\n")
        assert run_test._run_execution_mode(self._args(tmp_path)) == "unknown"

    def test_a_duplicate_key_is_unknown_too(self, tmp_path):
        # yaml_strict raises on duplicates, which is exactly the kind of config error that
        # would otherwise be read as "says nothing, so simulate".
        (tmp_path / "config_swarm_multi_1.yml").write_text(
            "runtime:\n  execution:\n    mode: real\n    mode: simulate\n")
        assert run_test._run_execution_mode(self._args(tmp_path)) == "unknown"

    def test_an_unreadable_directory_config_does_not_fall_back_to_the_base_config(
            self, tmp_path):
        """Under --use-config-dir those are the files the agents read; answering from the base
        config would describe a different run."""
        (tmp_path / "config_swarm_multi_1.yml").write_text("runtime: [unclosed\n")
        assert run_test._run_execution_mode(self._args(tmp_path)) == "unknown"

    def test_an_unknown_value_raises_rather_than_defaulting(self, tmp_path):
        (tmp_path / "config_swarm_multi_1.yml").write_text(
            "runtime:\n  execution:\n    mode: dry-run\n")
        with pytest.raises(ValueError, match="not 'simulate' or 'real'"):
            run_test._run_execution_mode(self._args(tmp_path))

    def test_one_agent_configured_real_makes_the_run_execute(self, tmp_path):
        """--use-config-dir means the files are used as they are on disk, so they need not
        agree. Reading only the first answered for a fleet rather than about it."""
        (tmp_path / "config_swarm_multi_1.yml").write_text(
            "runtime:\n  execution:\n    mode: simulate\n")
        (tmp_path / "config_swarm_multi_2.yml").write_text(
            "runtime:\n  execution:\n    mode: real\n")
        assert run_test._run_execution_mode(self._args(tmp_path)) == "real"

    def test_an_unrelated_yaml_in_the_directory_does_not_answer(self, tmp_path):
        # It sorts first and has no runtime block, so it reads as `simulate` — for configs
        # that say `real`.
        (tmp_path / "agent_profiles.yml").write_text("agents: []\n")
        (tmp_path / "config_swarm_multi_1.yml").write_text(
            "runtime:\n  execution:\n    mode: real\n")
        assert run_test._run_execution_mode(self._args(tmp_path)) == "real"

    def test_one_unreadable_config_leaves_the_answer_unknown(self, tmp_path):
        # Nothing rules out that the unreadable one says `real`.
        (tmp_path / "config_swarm_multi_1.yml").write_text(
            "runtime:\n  execution:\n    mode: simulate\n")
        (tmp_path / "config_swarm_multi_2.yml").write_text("runtime: [unclosed\n")
        assert run_test._run_execution_mode(self._args(tmp_path)) == "unknown"

    def test_a_known_real_config_wins_over_an_unreadable_one(self, tmp_path):
        (tmp_path / "config_swarm_multi_1.yml").write_text(
            "runtime:\n  execution:\n    mode: real\n")
        (tmp_path / "config_swarm_multi_2.yml").write_text("runtime: [unclosed\n")
        assert run_test._run_execution_mode(self._args(tmp_path)) == "real"

    def test_a_directory_with_no_agent_configs_is_unknown(self, tmp_path):
        assert run_test._run_execution_mode(self._args(tmp_path)) == "unknown"

    def _fleet(self, tmp_path, agents, dynamic=0, mode="remote"):
        return types.SimpleNamespace(use_config_dir=True, config_dir=str(tmp_path),
                                     agents=agents, dynamic_agents=dynamic, mode=mode)

    def _local_fleet(self, monkeypatch, tmp_path, agents, dynamic=0):
        """A local run reads ./configs literally, so move the fixture there and chdir."""
        root = tmp_path / "root"; (root / "configs").mkdir(parents=True, exist_ok=True)
        for f in tmp_path.glob("config_swarm_multi_*.yml"):
            (root / "configs" / f.name).write_text(f.read_text())
        monkeypatch.chdir(root)
        return types.SimpleNamespace(use_config_dir=True, config_dir="configs",
                                     agents=agents, dynamic_agents=dynamic, mode="local")

    def test_a_config_for_an_agent_this_run_does_not_launch_is_ignored(self, tmp_path):
        """A config directory outlives the run that generated it — reusing one is the point of
        --use-config-dir — so it routinely holds a larger fleet than this run starts."""
        for i in (1, 2):
            (tmp_path / f"config_swarm_multi_{i}.yml").write_text(
                "runtime:\n  execution:\n    mode: simulate\n")
        (tmp_path / "config_swarm_multi_5.yml").write_text(
            "runtime:\n  execution:\n    mode: real\n")
        assert run_test._run_execution_mode(self._fleet(tmp_path, agents=2)) == "simulate"

    def test_dynamic_agents_count_as_launched(self, tmp_path):
        (tmp_path / "config_swarm_multi_1.yml").write_text(
            "runtime:\n  execution:\n    mode: simulate\n")
        (tmp_path / "config_swarm_multi_2.yml").write_text(
            "runtime:\n  execution:\n    mode: real\n")
        assert run_test._run_execution_mode(
            self._fleet(tmp_path, agents=1, dynamic=1)) == "real"
        assert run_test._run_execution_mode(
            self._fleet(tmp_path, agents=1, dynamic=0)) == "simulate"

    def test_locally_a_config_that_does_not_exist_says_nothing_about_the_run(
            self, tmp_path, monkeypatch):
        """swarm-multi-start.sh iterates the configs that EXIST and skips ids outside its
        range, so a missing one means that agent never starts. Counting it as "cannot tell"
        refused runs whose every launched agent was perfectly readable."""
        (tmp_path / "config_swarm_multi_1.yml").write_text(
            "runtime:\n  execution:\n    mode: simulate\n")
        assert run_test._run_execution_mode(
            self._local_fleet(monkeypatch, tmp_path, agents=2)) == "simulate"

    def test_remotely_a_missing_config_is_not_optional(self, tmp_path):
        """start_agents_remote raises FileNotFoundError on the first missing config, so the run
        does not start — nothing can be concluded about it."""
        (tmp_path / "config_swarm_multi_1.yml").write_text(
            "runtime:\n  execution:\n    mode: simulate\n")
        assert run_test._run_execution_mode(
            self._fleet(tmp_path, agents=2, mode="remote")) == "unknown"

    def test_a_config_that_exists_but_does_not_parse_is_still_unknown(self, tmp_path):
        (tmp_path / "config_swarm_multi_1.yml").write_text(
            "runtime:\n  execution:\n    mode: simulate\n")
        (tmp_path / "config_swarm_multi_2.yml").write_text("runtime: [unclosed\n")
        assert run_test._run_execution_mode(self._fleet(tmp_path, agents=2)) == "unknown"

    def test_a_local_run_reads_the_directory_the_starter_globs(self, tmp_path, monkeypatch):
        """swarm-multi-start.sh globs `configs/` literally, so --config-dir does not name what
        a local run launches from. Reading it answered from a directory no agent opens."""
        (tmp_path / "configs").mkdir()
        (tmp_path / "configs" / "config_swarm_multi_1.yml").write_text(
            "runtime:\n  execution:\n    mode: real\n")
        elsewhere = tmp_path / "elsewhere"; elsewhere.mkdir()
        (elsewhere / "config_swarm_multi_1.yml").write_text(
            "runtime:\n  execution:\n    mode: simulate\n")
        monkeypatch.chdir(tmp_path)
        args = types.SimpleNamespace(use_config_dir=True, config_dir=str(elsewhere),
                                     agents=1, dynamic_agents=0, mode="local")
        assert run_test._run_execution_mode(args) == "real"

    def test_a_remote_run_reads_config_dir_because_that_is_what_is_copied(
            self, tmp_path, monkeypatch):
        (tmp_path / "configs").mkdir()
        (tmp_path / "configs" / "config_swarm_multi_1.yml").write_text(
            "runtime:\n  execution:\n    mode: simulate\n")
        source = tmp_path / "source"; source.mkdir()
        (source / "config_swarm_multi_1.yml").write_text(
            "runtime:\n  execution:\n    mode: real\n")
        monkeypatch.chdir(tmp_path)
        args = types.SimpleNamespace(use_config_dir=True, config_dir=str(source),
                                     agents=1, dynamic_agents=0, mode="remote")
        assert run_test._run_execution_mode(args) == "real"


class TestTheFleetCanActuallyRunTheJobs:
    """A converted workflow carries the requirements the jobs really had; the fleet is sized
    independently, and with --pegasus-jobs-dir the conversion did not know this fleet at all.
    When they do not meet, `is_job_feasible` is False for every agent and the job is never
    selected — no error, just a run that ends with jobs pending, which reads as a scheduling
    problem."""

    def _fleet(self, tmp_path, agents):
        path = tmp_path / "agent_profiles.json"
        path.write_text(json.dumps(agents))
        return str(path)

    def _jobs(self, tmp_path, *jobs, agents=0, mode="remote", config_dir=None):
        d = tmp_path / "bundle"; d.mkdir(exist_ok=True)
        for i, job in enumerate(jobs, 1):
            (d / f"job_{i}.json").write_text(json.dumps(job))
        # An empty config dir by default, so these exercise the agent_profiles.json fallback;
        # the per-agent-config path has its own tests below.
        cfgs = Path(config_dir) if config_dir else (tmp_path / "cfgs")
        cfgs.mkdir(exist_ok=True)
        return types.SimpleNamespace(pegasus_jobs_dir=str(d), mode=mode,
                                     config_dir=str(cfgs), agents=agents, dynamic_agents=0)

    @staticmethod
    def _agent(core, ram, disk, gpu=0, dtns=()):
        return {"core": core, "ram": ram, "disk": disk, "gpu": gpu,
                "dtns": [{"name": n} for n in dtns]}

    @staticmethod
    def _job(jid, core=1, ram=1, disk=1, gpu=0, dtns=()):
        return {"id": jid, "capacities": {"core": core, "ram": ram, "disk": disk, "gpu": gpu},
                "data_in": [{"name": n, "file": f"{n}.dat"} for n in dtns], "data_out": []}

    def test_a_fleet_that_fits_passes(self, tmp_path):
        profiles = self._fleet(tmp_path, {"1": self._agent(8, 32, 500)})
        args = self._jobs(tmp_path, self._job("j1", core=4, ram=16, disk=100))
        run_test.check_fleet_fits_jobs(args, profiles)

    def test_a_job_bigger_than_every_agent_is_refused(self, tmp_path):
        profiles = self._fleet(tmp_path, {"1": self._agent(4, 16, 100)})
        args = self._jobs(tmp_path, self._job("big", core=4, ram=64, disk=100))
        with pytest.raises(SystemExit, match="never be selected"):
            run_test.check_fleet_fits_jobs(args, profiles)

    def test_one_agent_must_satisfy_every_dimension_at_once(self, tmp_path):
        # Per-dimension maxima would say this fleet is fine. No single agent can run the job.
        profiles = self._fleet(tmp_path, {"1": self._agent(64, 8, 100),
                                          "2": self._agent(2, 512, 100)})
        args = self._jobs(tmp_path, self._job("both", core=32, ram=256, disk=50))
        with pytest.raises(SystemExit, match="never be selected"):
            run_test.check_fleet_fits_jobs(args, profiles)

    def test_a_dtn_no_agent_holds_is_refused_and_named(self, tmp_path):
        profiles = self._fleet(tmp_path, {"1": self._agent(8, 32, 500, dtns=["dtn1"])})
        args = self._jobs(tmp_path, self._job("j1", dtns=["dtn7"]))
        with pytest.raises(SystemExit, match="no agent holds dtn7"):
            run_test.check_fleet_fits_jobs(args, profiles)

    def test_local_is_not_a_dtn_requirement(self, tmp_path):
        # `local` means the local filesystem; is_job_feasible excludes it, and a bundle
        # converted with --dtn-names local is deliberately data-location-free.
        profiles = self._fleet(tmp_path, {"1": self._agent(8, 32, 500)})
        args = self._jobs(tmp_path, self._job("j1", dtns=["local"]))
        run_test.check_fleet_fits_jobs(args, profiles)

    def test_a_job_needing_two_dtns_needs_one_agent_holding_both(self, tmp_path):
        profiles = self._fleet(tmp_path, {"1": self._agent(8, 32, 500, dtns=["dtn1"]),
                                          "2": self._agent(8, 32, 500, dtns=["dtn2"])})
        args = self._jobs(tmp_path, self._job("j1", dtns=["dtn1", "dtn2"]))
        with pytest.raises(SystemExit, match="never be selected"):
            run_test.check_fleet_fits_jobs(args, profiles)

    def test_without_profiles_it_warns_rather_than_guessing(self, tmp_path, capsys):
        args = self._jobs(tmp_path, self._job("j1"))
        run_test.check_fleet_fits_jobs(args, str(tmp_path / "nope.json"))
        assert "cannot tell whether the jobs fit" in capsys.readouterr().out

    def _configs(self, tmp_path, **agents):
        d = tmp_path / "cfgs"; d.mkdir(exist_ok=True)
        for aid, a in agents.items():
            (d / f"config_swarm_multi_{aid[1:]}.yml").write_text(json.dumps({
                "capacities": {"core": a["core"], "ram": a["ram"], "disk": a["disk"],
                               "gpu": a.get("gpu", 0)},
                "dtns": [{"name": n} for n in a.get("dtns", [])]}))
        return str(d)

    def test_the_fleet_comes_from_the_configs_the_agents_will_read(self, tmp_path):
        # agent_profiles.json is a local artefact of the last generation; under
        # --use-config-dir it need not describe the configs this run uses at all.
        cfgs = self._configs(tmp_path, a1=self._agent(2, 4, 50))
        profiles = self._fleet(tmp_path, {"1": self._agent(64, 512, 5000)})
        args = self._jobs(tmp_path, self._job("j1", core=32, ram=256, disk=100),
                          agents=1, config_dir=cfgs)
        with pytest.raises(SystemExit, match="never be selected"):
            run_test.check_fleet_fits_jobs(args, profiles)

    def test_an_agent_this_run_does_not_launch_cannot_make_the_jobs_fit(self, tmp_path):
        """The core of it: a profiles file (or config dir) left from a 270-agent generation
        satisfied the check for a 30-agent run, which then stalled with jobs pending."""
        cfgs = self._configs(tmp_path,
                             a1=self._agent(2, 4, 50),
                             a2=self._agent(64, 512, 5000))   # generated, not launched
        args = self._jobs(tmp_path, self._job("j1", core=32, ram=256, disk=100),
                          agents=1, config_dir=cfgs)
        with pytest.raises(SystemExit, match="never be selected"):
            run_test.check_fleet_fits_jobs(args, str(tmp_path / "none.json"))
        # …and it passes once that agent is actually part of the run.
        args = self._jobs(tmp_path, self._job("j1", core=32, ram=256, disk=100),
                          agents=2, config_dir=cfgs)
        run_test.check_fleet_fits_jobs(args, str(tmp_path / "none.json"))

    def test_the_profiles_fallback_is_restricted_to_launched_ids_too(
            self, tmp_path, monkeypatch):
        # No per-agent configs at all (a local run, where their absence is not fatal), so the
        # fleet comes from agent_profiles.json — which lists every agent ever generated.
        profiles = self._fleet(tmp_path, {"1": self._agent(2, 4, 50),
                                          "2": self._agent(64, 512, 5000)})
        args = self._jobs(tmp_path, self._job("j1", core=32, ram=256, disk=100), agents=1,
                          mode="local")
        root = tmp_path / "empty"; (root / "configs").mkdir(parents=True)
        monkeypatch.chdir(root)
        with pytest.raises(SystemExit, match="never be selected"):
            run_test.check_fleet_fits_jobs(args, profiles)

    def test_a_launched_config_that_cannot_be_read_skips_the_check(self, tmp_path, capsys):
        # The agent that did not parse may be the only one that fits, so refusing and passing
        # would both be guesses; the unreadable config is the thing to report.
        cfgs = self._configs(tmp_path, a1=self._agent(2, 4, 50))
        (Path(cfgs) / "config_swarm_multi_2.yml").write_text("capacities: [unclosed\n")
        args = self._jobs(tmp_path, self._job("j1", core=32, ram=256, disk=100),
                          agents=2, config_dir=cfgs)
        run_test.check_fleet_fits_jobs(args, str(tmp_path / "none.json"))
        assert "could not be read" in capsys.readouterr().out

    def test_a_missing_config_does_not_swap_the_fleet_for_a_stale_profiles_file(
            self, tmp_path, monkeypatch):
        """Falling back wholesale was worse than useless: agent_profiles.json can describe an
        older generation, so one missing config replaced every real capacity with a guess."""
        # A local run reads ./configs literally: agent 1 is there and small, agent 2 is absent
        # (locally that just means it does not start), and the profiles file is the stale one
        # claiming both are huge.
        root = tmp_path / "root"; (root / "configs").mkdir(parents=True)
        (root / "configs" / "config_swarm_multi_1.yml").write_text(json.dumps({
            "capacities": {"core": 2, "ram": 4, "disk": 50, "gpu": 0}, "dtns": []}))
        profiles = self._fleet(tmp_path, {"1": self._agent(64, 512, 5000),
                                          "2": self._agent(64, 512, 5000)})
        args = self._jobs(tmp_path, self._job("j1", core=32, ram=256, disk=100),
                          agents=2, mode="local")
        monkeypatch.chdir(root)
        with pytest.raises(SystemExit, match="never be selected"):
            run_test.check_fleet_fits_jobs(args, profiles)

    def test_locally_an_agent_with_no_config_is_not_counted_as_fleet(
            self, tmp_path, capsys, monkeypatch):
        # A local run reads ./configs literally, so put the fixture there and chdir.
        self._configs(tmp_path, a1=self._agent(64, 512, 5000))
        root = tmp_path / "root"; (root / "configs").mkdir(parents=True)
        for f in (tmp_path / "cfgs").glob("*.yml"):
            (root / "configs" / f.name).write_text(f.read_text())
        args = self._jobs(tmp_path, self._job("j1", core=32, ram=256, disk=100),
                          agents=3, mode="local")
        monkeypatch.chdir(root)
        run_test.check_fleet_fits_jobs(args, str(tmp_path / "none.json"))
        assert "will not start" in capsys.readouterr().out

    def test_remotely_a_missing_config_stops_the_check_instead(self, tmp_path, capsys):
        """A remote run aborts on it (start_agents_remote raises), so a verdict about job sizes
        would describe a fleet that never exists."""
        cfgs = self._configs(tmp_path, a1=self._agent(64, 512, 5000))
        args = self._jobs(tmp_path, self._job("j1", core=32, ram=256, disk=100),
                          agents=3, config_dir=cfgs, mode="remote")
        run_test.check_fleet_fits_jobs(args, str(tmp_path / "none.json"))
        out = capsys.readouterr().out
        assert "cannot tell whether the jobs fit" in out and "cannot start without it" in out


class TestJobCountComesFromTheBundle:
    """--jobs drives the agents' expected total and the completion checks. A bundle knows it
    exactly, so requiring it again only creates a way to get it wrong."""

    def test_it_counts_job_records_and_nothing_else(self, tmp_path):
        for i in range(1, 4):
            (tmp_path / f"job_{i}.json").write_text("{}")
        for noise in ("conversion_summary.json", "manifest.json", "pegasus_baseline.json"):
            (tmp_path / noise).write_text("{}")
        (tmp_path / "code").mkdir()
        assert run_test.count_job_records(tmp_path) == 3

    def test_a_directory_that_is_not_there_counts_zero(self, tmp_path):
        assert run_test.count_job_records(tmp_path / "nope") == 0


class TestConversionIsNotSkippedByUseConfigDir:
    """`--pegasus-profiles` says "convert this trace and run it". `--use-config-dir` says
    "reuse the fleet on disk". They are about different things, and while the conversion sat
    inside the generation branch, asking for both silently did neither: no jobs were written,
    jobs/ was left alone (it is not cleaned when a Pegasus flag is present, precisely because a
    conversion was supposed to overwrite it), and the run published whatever an earlier run had
    left there — or nothing — without a word. The README now recommends --use-config-dir for
    every workflow run, so this combination is the common one.

    Pinned structurally: the call has to sit outside the `if not args.use_config_dir` block,
    which is a property of where it is, not of what it returns."""

    @staticmethod
    def _main_body():
        import ast
        tree = ast.parse(Path(run_test.__file__).read_text())
        return next(n for n in tree.body
                    if isinstance(n, ast.FunctionDef) and n.name == "main")

    @staticmethod
    def _calls_conversion(node):
        import ast
        return any(isinstance(n, ast.Call) and getattr(n.func, "id", None) == "convert_pegasus_jobs"
                   for n in ast.walk(node))

    def _generation_branch(self):
        import ast
        for node in ast.walk(self._main_body()):
            if not isinstance(node, ast.If):
                continue
            t = node.test
            if (isinstance(t, ast.UnaryOp) and isinstance(t.op, ast.Not)
                    and isinstance(t.operand, ast.Attribute)
                    and t.operand.attr == "use_config_dir"):
                return node
        raise AssertionError("main() no longer has an `if not args.use_config_dir` branch")

    def test_the_conversion_is_not_inside_the_generation_branch(self):
        assert not self._calls_conversion(self._generation_branch())

    def test_but_main_still_converts(self):
        assert self._calls_conversion(self._main_body())

    def test_the_dtn_pool_is_scoped_to_the_launched_fleet(self, tmp_path):
        """Why running it outside the branch is correct and not merely non-silent: the pool
        comes from the per-agent configs this run launches, so a reused fleet answers for
        itself."""
        for i, dtn in ((1, "dtn-a"), (2, "dtn-b")):
            (tmp_path / f"config_swarm_multi_{i}.yml").write_text(
                f"capacities:\n  core: 4\ndtns:\n  - name: {dtn}\n")
        args = types.SimpleNamespace(use_config_dir=True, config_dir=str(tmp_path),
                                     agents=2, dynamic_agents=0, mode="remote")
        assert run_test.launched_dtn_pool(args) == (["dtn-a", "dtn-b"], None)


class TestTheConversionPoolIsTheLaunchedFleets:
    """`agent_dtns.json` is a repo-root artefact of the last generation. Under
    --use-config-dir the configs can come from another machine, and the file routinely
    describes more agents than this run starts — so a job hashed onto a DTN only an
    unlaunched agent holds is infeasible for every agent that does start, and says nothing:
    never proposed, never failed, just pending at the end."""

    def _cfg(self, tmp_path, agent_id, dtn):
        (tmp_path / f"config_swarm_multi_{agent_id}.yml").write_text(
            f"capacities:\n  core: 4\ndtns:\n  - name: {dtn}\n")

    def _args(self, tmp_path, agents, dynamic=0, mode="remote"):
        return types.SimpleNamespace(use_config_dir=True, config_dir=str(tmp_path),
                                     agents=agents, dynamic_agents=dynamic, mode=mode)

    def test_an_unlaunched_agents_dtn_is_not_in_the_pool(self, tmp_path):
        self._cfg(tmp_path, 1, "dtn-live")
        self._cfg(tmp_path, 2, "dtn-departed")
        assert run_test.launched_dtn_pool(self._args(tmp_path, agents=1)) == (["dtn-live"], None)

    def test_dynamic_agents_are_launched_so_theirs_counts(self, tmp_path):
        self._cfg(tmp_path, 1, "dtn-a")
        self._cfg(tmp_path, 2, "dtn-b")
        pool, _ = run_test.launched_dtn_pool(self._args(tmp_path, agents=1, dynamic=1))
        assert pool == ["dtn-a", "dtn-b"]

    def test_a_stale_agent_dtns_file_does_not_answer_for_the_configs(self, tmp_path,
                                                                     monkeypatch):
        """The exact --use-config-dir shape: configs generated elsewhere, a leftover file in
        the repo root naming DTNs none of them holds."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / "agent_dtns.json").write_text(json.dumps({"1": [{"name": "dtn-stale"}]}))
        cfgs = tmp_path / "cfgs"; cfgs.mkdir()
        (cfgs / "config_swarm_multi_1.yml").write_text(
            "capacities:\n  core: 4\ndtns:\n  - name: dtn-real\n")
        args = types.SimpleNamespace(use_config_dir=True, config_dir=str(cfgs),
                                     agents=1, dynamic_agents=0, mode="remote")
        # What the pre-fix conversion would have hashed the jobs onto, held by nobody here:
        assert run_test.agent_dtn_pool() == ["dtn-stale"]
        assert run_test.launched_dtn_pool(args) == (["dtn-real"], None)

    def test_a_fleet_that_holds_no_dtns_yields_an_empty_pool_not_a_guess(self, tmp_path):
        (tmp_path / "config_swarm_multi_1.yml").write_text("capacities:\n  core: 4\n")
        assert run_test.launched_dtn_pool(self._args(tmp_path, agents=1)) == ([], None)

    def test_an_undescribable_fleet_is_reported_rather_than_guessed(self, tmp_path):
        (tmp_path / "config_swarm_multi_1.yml").write_text("capacities:\n  core: [unclosed\n")
        pool, incomplete = run_test.launched_dtn_pool(self._args(tmp_path, agents=1))
        assert pool == [] and incomplete

    def test_and_the_conversion_then_drops_to_local_and_says_so(self, tmp_path, monkeypatch,
                                                               capsys):
        """`local` is excluded from required DTNs, so the jobs stay schedulable — the run
        loses the locality dimension rather than stranding work, and is told."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / "config_swarm_multi_1.yml").write_text("capacities:\n  core: [unclosed\n")
        seen = {}

        def _fake_convert(**kw):
            seen.update(kw)
            return {"jobs_written": 0, "warnings_count": 0}

        monkeypatch.setattr(run_test, "convert_pegasus_profiles", _fake_convert, raising=False)
        import pegasus_to_swarm_converter as conv
        monkeypatch.setattr(conv, "convert_pegasus_profiles", _fake_convert)
        args = types.SimpleNamespace(
            use_config_dir=True, config_dir=str(tmp_path), agents=1, dynamic_agents=0,
            mode="remote", pegasus_dtn_names=None, pegasus_profiles="p.json",
            pegasus_input_type="json", pegasus_data_nodes="per-file",
            pegasus_bundle_source_root=None, pegasus_dag_gating=True)
        run_test.convert_pegasus_jobs(args)
        assert seen["dtn_names"] == ["local"]
        assert "cannot tell which DTNs this fleet holds" in capsys.readouterr().out


class TestRecordedCoordinatorTypeIsTheLaunchedOne:
    """`run_meta.json`'s `hierarchical_level1_agent_type` is what `evaluation/collect.py` uses
    to decide which agents were expected to run the LLM plane. Under `--use-config-dir` the
    flag is inert — the configs came from an earlier generation — so recording the flag's value
    describes a fleet this run did not build. Since the default became `resource` (2026-09-18)
    that mistake is silent and one-directional: a reused fleet of LLM coordinators would be
    recorded as analytic, dropping the whole tier from the coverage denominator, so a
    coordinator tier that went entirely dark reads as a fully measured run."""

    def _cfg(self, d, agent_id, level, agent_type):
        (d / f"config_swarm_multi_{agent_id}.yml").write_text(
            f"agent_type: {agent_type}\ntopology:\n  type: hierarchical\n  level: {level}\n")

    def _args(self, tmp_path, declared="resource", use_config_dir=True, agents=2):
        return types.SimpleNamespace(
            hierarchical_level1_agent_type=declared, use_config_dir=use_config_dir,
            config_dir=str(tmp_path), agents=agents, dynamic_agents=0, mode="remote")

    def test_a_reused_llm_coordinator_is_recorded_as_llm(self, tmp_path):
        self._cfg(tmp_path, 1, 0, "resource")
        self._cfg(tmp_path, 2, 1, "llm")
        assert run_test._effective_coordinator_type(self._args(tmp_path)) == "llm"

    def test_a_reused_resource_coordinator_is_recorded_as_resource(self, tmp_path):
        self._cfg(tmp_path, 1, 0, "resource")
        self._cfg(tmp_path, 2, 1, "resource")
        assert run_test._effective_coordinator_type(
            self._args(tmp_path, declared="llm")) == "resource"

    def test_without_use_config_dir_the_flag_is_the_answer(self, tmp_path):
        """It was forwarded to generate_configs, so it describes the fleet that was built."""
        self._cfg(tmp_path, 1, 1, "llm")
        assert run_test._effective_coordinator_type(
            self._args(tmp_path, declared="resource", use_config_dir=False)) == "resource"

    def test_an_unparseable_config_yields_unknown_not_a_guess(self, tmp_path):
        (tmp_path / "config_swarm_multi_1.yml").write_text("agent_type: [unclosed\n")
        assert run_test._effective_coordinator_type(self._args(tmp_path, agents=1)) is None

    def test_a_mixed_tier_yields_unknown(self, tmp_path):
        """No single value describes it, and collect.py handles None by refusing to attribute
        roles rather than guessing."""
        self._cfg(tmp_path, 1, 1, "llm")
        self._cfg(tmp_path, 2, 1, "resource")
        assert run_test._effective_coordinator_type(self._args(tmp_path)) is None

    def test_a_flat_fleet_keeps_the_declared_value(self, tmp_path):
        """No level-1 config at all: nothing was observed to contradict the flag."""
        self._cfg(tmp_path, 1, 0, "resource")
        self._cfg(tmp_path, 2, 0, "resource")
        assert run_test._effective_coordinator_type(self._args(tmp_path)) == "resource"

    def test_the_inert_flag_is_reported(self, tmp_path, capsys):
        self._cfg(tmp_path, 1, 1, "llm")
        run_test._effective_coordinator_type(self._args(tmp_path, agents=1))
        assert "is inert under --use-config-dir" in capsys.readouterr().out

    def test_a_config_without_an_agent_type_launches_as_resource(self, tmp_path):
        """Not a guess: swarm-multi-start.sh reads the key with 'resource' as its fallback and
        main.py does the same (its CLI value wins only when it is 'llm'). Reading the absence
        as unknown blocks a collect.py verdict over a tier whose role is well defined."""
        (tmp_path / "config_swarm_multi_1.yml").write_text(
            "topology:\n  type: hierarchical\n  level: 1\n")
        args = types.SimpleNamespace(
            hierarchical_level1_agent_type="llm", use_config_dir=True,
            config_dir=str(tmp_path), agents=1, dynamic_agents=0, mode="remote")
        assert run_test._effective_coordinator_type(args) == "resource"


class TestRecordedDelegationPolicyIsTheLaunchedOne:
    """Same rule as the coordinator type: under --use-config-dir the arm is read from the
    configs the agents launch — `./configs` locally whatever --config-dir says, --config-dir
    remotely, ids 1..agents — and from all of them. The reader used to glob
    `<config_dir>/*.yml` and take the first file, so a local run with --config-dir elsewhere
    recorded an arm from a directory no agent read, and any run recorded whichever agent
    sorted first. collect.py labels arms from this field."""

    def _cfg(self, d, agent_id, policy):
        (d / f"config_swarm_multi_{agent_id}.yml").write_text(
            f"delegation:\n  policy: {policy}\n")

    def _args(self, cfg_dir, mode="remote", agents=2, flag=None):
        return types.SimpleNamespace(delegation_policy=flag, use_config_dir=True,
                                     config_dir=str(cfg_dir), agents=agents,
                                     dynamic_agents=0, mode=mode)

    def test_the_flag_wins_when_given(self, tmp_path):
        self._cfg(tmp_path, 1, "bandit")
        assert run_test._effective_delegation_policy(
            self._args(tmp_path, agents=1, flag="llm")) == "llm"

    def test_a_local_run_reads_dot_configs_not_config_dir(self, tmp_path, monkeypatch):
        root = tmp_path / "root"; (root / "configs").mkdir(parents=True)
        alt = tmp_path / "alt"; alt.mkdir()
        self._cfg(root / "configs", 1, "bandit")
        self._cfg(alt, 1, "llm")
        monkeypatch.chdir(root)
        assert run_test._effective_delegation_policy(
            self._args(alt, mode="local", agents=1)) == "bandit"

    def test_all_launched_configs_are_read_and_disagreement_is_unknown(self, tmp_path):
        self._cfg(tmp_path, 1, "bandit")
        self._cfg(tmp_path, 2, "llm")
        assert run_test._effective_delegation_policy(self._args(tmp_path)) is None

    def test_an_unlaunched_agents_config_does_not_vote(self, tmp_path):
        self._cfg(tmp_path, 1, "bandit")
        self._cfg(tmp_path, 2, "llm")         # not launched: agents=1
        assert run_test._effective_delegation_policy(
            self._args(tmp_path, agents=1)) == "bandit"

    def test_an_unparseable_config_is_unknown(self, tmp_path):
        self._cfg(tmp_path, 1, "bandit")
        (tmp_path / "config_swarm_multi_2.yml").write_text("delegation: [unclosed\n")
        assert run_test._effective_delegation_policy(self._args(tmp_path)) is None
