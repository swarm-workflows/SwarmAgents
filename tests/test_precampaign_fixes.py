"""Regression guards for the pre-campaign fixes (FGCS_EVAL_PLAN section 0.2).

Every bug covered here was *silent*: the run completed and produced plausible numbers while a
metric measured nothing, a documented knob held a different value, or a fleet differed between
runs. Tests, not review, are what keep them fixed.
"""
import json
import os
import subprocess
import sys

import pytest
import yaml

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from swarm.models.job import Job  # noqa: E402
from swarm.utils.yaml_strict import DuplicateKeyError  # noqa: E402
from swarm.utils.yaml_strict import safe_load as strict_load  # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SHIPPED_CONFIG = os.path.join(REPO, "config_swarm_multi.yml")


class TestStrictYaml:
    """F-13: a duplicate key must raise, not silently keep the last value."""

    def test_duplicate_key_raises_and_names_both_lines(self):
        with pytest.raises(DuplicateKeyError) as exc:
            strict_load("runtime:\n  peer_expiry_seconds: 300\n  x: 1\n  peer_expiry_seconds: 45\n")
        msg = str(exc.value)
        assert "peer_expiry_seconds" in msg
        assert "line 2" in msg and "line 4" in msg

    def test_plain_yaml_would_have_kept_the_last_value(self):
        """Pins why this is needed: stock safe_load silently prefers the second definition."""
        doc = "runtime:\n  peer_expiry_seconds: 300\n  peer_expiry_seconds: 45\n"
        assert yaml.safe_load(doc)["runtime"]["peer_expiry_seconds"] == 45

    def test_nested_and_list_documents_still_load(self):
        got = strict_load("a:\n  b: 1\n  c:\n    - {d: 2}\n    - {d: 3}\n")
        assert got == {"a": {"b": 1, "c": [{"d": 2}, {"d": 3}]}}

    def test_same_key_in_sibling_mappings_is_fine(self):
        got = strict_load("one:\n  k: 1\ntwo:\n  k: 2\n")
        assert got == {"one": {"k": 1}, "two": {"k": 2}}


class TestShippedConfig:
    def test_loads_under_the_strict_loader(self):
        """The shipped config must stay free of duplicate keys."""
        assert strict_load(open(SHIPPED_CONFIG))["runtime"]

    def test_peer_expiry_matches_the_documented_value(self):
        rt = strict_load(open(SHIPPED_CONFIG))["runtime"]
        assert rt["peer_expiry_seconds"] == 300

    def test_execution_simulation_keys_present_and_faithful(self):
        rt = strict_load(open(SHIPPED_CONFIG))["runtime"]
        assert rt["wall_time_scale"] == 1.0, "shipped runs must replay real wall times"
        assert rt["wall_time_max_s"] > 0, "an uncapped tail lets one job own the makespan"


class TestExecutionSimulation:
    """F-9: `execute()` slept a flat 1 s, so makespan/throughput/utilisation measured nothing."""

    @pytest.fixture(autouse=True)
    def restore_policy(self):
        saved = (Job._WALL_TIME_SCALE, Job._WALL_TIME_MIN_S, Job._WALL_TIME_MAX_S)
        yield
        Job.configure_execution_simulation(*saved)

    def test_default_policy_replays_real_wall_time(self):
        Job.configure_execution_simulation()
        assert Job.simulated_execution_seconds(2.0) == pytest.approx(2.0)
        assert Job.simulated_execution_seconds(40.0) == pytest.approx(40.0)

    def test_distinct_wall_times_give_distinct_durations(self):
        """The actual defect: every job took the same time regardless of its duration."""
        Job.configure_execution_simulation(scale=1.0)
        durations = {Job.simulated_execution_seconds(w) for w in (0.5, 2.0, 10.0, 40.0)}
        assert len(durations) == 4

    def test_scale_compresses_proportionally(self):
        Job.configure_execution_simulation(scale=0.1, max_s=0)
        assert Job.simulated_execution_seconds(100.0) == pytest.approx(10.0)
        assert Job.simulated_execution_seconds(20.0) == pytest.approx(2.0)

    def test_cap_bounds_the_tail_and_floor_lifts_the_head(self):
        Job.configure_execution_simulation(scale=1.0, min_s=0.5, max_s=120.0)
        assert Job.simulated_execution_seconds(1992.72) == pytest.approx(120.0)
        assert Job.simulated_execution_seconds(0.1) == pytest.approx(0.5)

    def test_zero_wall_time_never_sleeps(self):
        Job.configure_execution_simulation(scale=1.0, min_s=0.5)
        assert Job.simulated_execution_seconds(0) == 0.0
        assert Job.simulated_execution_seconds(None) == 0.0

    def test_legacy_flat_policy_is_still_reachable(self):
        """scale<=0 reproduces the old behaviour, for replaying an old result only."""
        Job.configure_execution_simulation(scale=0.0)
        assert Job.simulated_execution_seconds(2.0) == 1.0
        assert Job.simulated_execution_seconds(1992.0) == 1.0
        assert Job.simulated_execution_seconds(0) == 0.0


class TestRuntimeCap:
    """F-14: --runtime was parsed and never read, so a stalled run polled forever."""

    def test_default_is_no_cap_not_the_old_ninety_seconds(self):
        """Enforcing the old default of 90 would have truncated every run that omits the flag."""
        out = subprocess.run([sys.executable, "run_test.py", "--help"],
                             cwd=REPO, capture_output=True, text=True, timeout=120)
        assert out.returncode == 0, out.stderr
        assert "--runtime" in out.stdout
        assert "0 = no cap" in out.stdout

    def test_wait_runtime_reads_the_flag(self):
        """A cap that is never consulted is the bug; assert the source actually uses it."""
        src = open(os.path.join(REPO, "run_test.py")).read()
        body = src[src.index("def wait_runtime"):src.index("def wait_with_early_exit")]
        assert "args" in body and "runtime" in body, "wait_runtime must consult --runtime"
        assert "deadline" in body


class TestAgentHostsPreserved:
    """F-6: cleanup deleted agent_hosts.txt, then generate_configs.py read that same path."""

    def test_cleanup_keeps_the_file_when_it_is_the_input(self, tmp_path, monkeypatch):
        import run_test
        monkeypatch.chdir(tmp_path)
        (tmp_path / "agent_hosts.txt").write_text("agent-1\nagent-2\n")
        (tmp_path / "agent_profiles.json").write_text("{}")
        monkeypatch.setattr(run_test, "log", lambda *a, **k: None)
        calls = []
        monkeypatch.setattr(run_test, "run_blocking",
                            lambda cmd, **k: calls.append(" ".join(map(str, cmd))))

        class A:
            agents = 2
            db_host = None
            use_config_dir = False
            pegasus_profiles = None
            agent_hosts_file = "agent_hosts.txt"

        run_test.cleanup_between_runs(A())
        assert not any("agent_hosts.txt" in c for c in calls), \
            "the run's own --agent-hosts-file input must not be deleted"
        assert (tmp_path / "agent_hosts.txt").exists()

    def test_cleanup_still_removes_a_generated_hosts_file(self, tmp_path, monkeypatch):
        import run_test
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(run_test, "log", lambda *a, **k: None)
        calls = []
        monkeypatch.setattr(run_test, "run_blocking",
                            lambda cmd, **k: calls.append(" ".join(map(str, cmd))))

        class A:
            agents = 2
            db_host = None
            use_config_dir = False
            pegasus_profiles = None
            agent_hosts_file = None

        run_test.cleanup_between_runs(A())
        assert any("agent_hosts.txt" in c for c in calls)

    def test_profile_and_dtn_state_is_always_removed(self, tmp_path, monkeypatch):
        """Reproducibility depends on generating from a clean state — never make these
        conditional (generate_configs reuses agent_dtns.json through a different RNG path)."""
        import run_test
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(run_test, "log", lambda *a, **k: None)
        calls = []
        monkeypatch.setattr(run_test, "run_blocking",
                            lambda cmd, **k: calls.append(" ".join(map(str, cmd))))

        class A:
            agents = 2
            db_host = None
            use_config_dir = False
            pegasus_profiles = None
            agent_hosts_file = "agent_hosts.txt"

        run_test.cleanup_between_runs(A())
        assert any("agent_profiles.json" in c for c in calls)
        assert any("agent_dtns.json" in c for c in calls)


class TestLlmTimeoutEnforced:
    """F-7: llm.timeout_seconds was parsed into LlmConfig and used nowhere."""

    def test_config_carries_the_deadline(self):
        from swarm.agents.llm.llm_config import LlmConfig
        assert LlmConfig.from_dict({"timeout_seconds": 6}).timeout_seconds == 6

    def test_bidder_passes_it_to_the_model_call(self):
        src = open(os.path.join(REPO, "swarm/agents/llm/llm_bidder.py")).read()
        call = src[src.index("res = self.agent.run_sync("):src.index("bid = res.output")]
        assert "timeout" in call, "the deadline must reach the model call, not just the config"


class TestFleetPrefixStability:
    """A seed alone does not make fleets comparable: flavours are percentages of fleet size."""

    def _flavors(self, num_agents, master, seed=42):
        import random
        import generate_configs as gc
        random.seed(seed)
        gen = gc.SwarmConfigGenerator.__new__(gc.SwarmConfigGenerator)
        gen.num_agents = num_agents
        gen.master_fleet_size = master or num_agents
        return [f["name"] for f in gen.assign_flavors(gc.DEFAULT_FLAVOR_PERCENTAGES)]

    def test_without_a_master_the_same_seed_gives_different_fleets(self):
        """Pins the problem the flag solves — this is current behaviour, not a bug to fix."""
        small = self._flavors(10, None)
        big = self._flavors(30, None)
        assert small != big[:10]

    def test_with_a_master_every_fleet_is_a_prefix(self):
        master = 60
        f10 = self._flavors(10, master)
        f30 = self._flavors(30, master)
        f60 = self._flavors(60, master)
        assert f60[:10] == f10
        assert f60[:30] == f30

    def test_master_smaller_than_the_fleet_is_rejected(self):
        import generate_configs as gc
        with pytest.raises(ValueError, match="master_fleet_size"):
            gc.SwarmConfigGenerator(
                num_agents=30, jobs_per_proposal=1, base_config_path=SHIPPED_CONFIG,
                output_dir="/tmp", topology="mesh", db_host="localhost",
                enable_dtns=False, master_fleet_size=10)


class TestDtnReuseIsAnnounced:
    """A silent reuse of agent_dtns.json makes --seed non-reproducible."""

    def test_reuse_warns(self, tmp_path, monkeypatch, capsys):
        import generate_configs as gc
        monkeypatch.chdir(tmp_path)
        (tmp_path / "agent_dtns.json").write_text(json.dumps({"1": [{"name": "dtn1"}]}))
        gen = gc.SwarmConfigGenerator.__new__(gc.SwarmConfigGenerator)
        got = gen._load_agent_dtns("agent_dtns.json")
        assert got == {"1": [{"name": "dtn1"}]}
        assert "WARNING" in capsys.readouterr().out

    def test_clean_state_is_silent(self, tmp_path, monkeypatch, capsys):
        import generate_configs as gc
        monkeypatch.chdir(tmp_path)
        gen = gc.SwarmConfigGenerator.__new__(gc.SwarmConfigGenerator)
        assert gen._load_agent_dtns("agent_dtns.json") == {}
        assert capsys.readouterr().out == ""


class TestHierarchicalSummaryLabel:
    """F-12: every level's summary block was labelled [no_restarts]."""

    def test_levels_are_distinguishable(self):
        assert _tag("_level0", 0, None) == "level0"
        assert _tag("_level1", 1, None) == "level1"
        assert _tag("_level2", 2, None) == "level2"
        assert len({_tag(f"_level{i}", i, None) for i in range(3)}) == 3

    def test_flat_path_labels_are_unchanged(self):
        assert _tag("", 0, None) == "all"
        assert _tag("_no_restarts", 0, {1, 2}) == "no_restarts"

    def test_a_level_that_also_excludes_restarts_says_both(self):
        assert _tag("_level1", 1, {7}) == "level1,no_restarts"


def _tag(label_suffix, level, exclude_job_ids):
    """Mirror of the labelling rule in plotting.single_run (kept in step by the test below)."""
    if level is not None and label_suffix.startswith("_level"):
        tag = f"level{level}"
        if exclude_job_ids:
            tag += ",no_restarts"
        return tag
    return "no_restarts" if exclude_job_ids else "all"


def test_label_rule_still_lives_in_the_plotting_module():
    """Guards the mirror above from drifting away from the code it stands in for."""
    src = open(os.path.join(REPO, "plotting/single_run.py")).read()
    assert 'tag = f"level{level}"' in src
    assert 'tag = "no_restarts" if exclude_job_ids else "all"' in src


class TestBatchRunnerForwarding:
    """The batch runner is how the campaign gets its repeats — it must not undo the fixes."""

    @staticmethod
    def _help():
        out = subprocess.run([sys.executable, "batch_tests_v2.py", "--help"],
                             cwd=REPO, capture_output=True, text=True, timeout=120)
        assert out.returncode == 0, out.stderr
        return out.stdout

    def test_runtime_default_is_no_cap(self):
        """It defaulted to 30 and always forwarded it. Once run_test enforces the cap, that
        stops every batch run after 30s — long before a 500-job run drains."""
        src = open(os.path.join(REPO, "batch_tests_v2.py")).read()
        assert '"--runtime", type=int, default=0' in src
        assert "0 = no cap" in self._help()

    def test_forwards_the_fleet_reproducibility_flags(self):
        help_text = self._help()
        assert "--seed" in help_text
        assert "--master-fleet-size" in help_text
        src = open(os.path.join(REPO, "batch_tests_v2.py")).read()
        build = src[src.index("# Build run_test.py command"):src.index("if args.use_config_dir:")]
        assert '"--seed"' in build, "a seed that is never forwarded does not pin anything"
        assert '"--master-fleet-size"' in build


class TestSplitPathsUseTheBudget:
    """The split-hybrid paths hardcoded a ~1s total, bypassing the wall-time policy."""

    @pytest.fixture(autouse=True)
    def restore_policy(self):
        saved = (Job._WALL_TIME_SCALE, Job._WALL_TIME_MIN_S, Job._WALL_TIME_MAX_S)
        yield
        Job.configure_execution_simulation(*saved)

    @staticmethod
    def _producer_job(wall_time, iterations):
        job = Job()
        job.from_dict({
            "id": "sp-1", "wall_time": wall_time,
            "capacities": {"core": 1, "ram": 1, "disk": 1, "gpu": 0, "qubits": 4},
            "quantum": {"qubits": 4, "circuit_depth": 2, "shots": 8,
                        "hybrid": True, "iterations": iterations},
        })
        return job

    def test_producer_sleeps_for_its_own_wall_time(self, monkeypatch):
        """Recorded rather than timed, so the assertion is exact and never flaky."""
        import swarm.models.job as job_mod
        slept = []
        monkeypatch.setattr(job_mod.time, "sleep", lambda s: slept.append(s))
        Job.configure_execution_simulation(scale=1.0, max_s=0)

        class _Layer:
            def announce_producer(self, *a, **k): pass
            def publish(self, *a, **k): return 1

        iterations = 4
        self._producer_job(20.0, iterations).execute_producer(_Layer())
        # budget/(iterations+1) per step, one prep step plus one per iteration
        assert sum(slept) == pytest.approx(20.0), slept
        assert len(slept) == iterations + 1

    def test_producer_budget_tracks_the_policy(self, monkeypatch):
        import swarm.models.job as job_mod
        slept = []
        monkeypatch.setattr(job_mod.time, "sleep", lambda s: slept.append(s))
        Job.configure_execution_simulation(scale=0.5, max_s=0)

        class _Layer:
            def announce_producer(self, *a, **k): pass
            def publish(self, *a, **k): return 1

        self._producer_job(20.0, 4).execute_producer(_Layer())
        assert sum(slept) == pytest.approx(10.0), "scale must reach the split path too"

    @staticmethod
    def _consumer_job(wall_time, total):
        job = Job()
        job.from_dict({
            "id": "sp-1-c", "wall_time": wall_time,
            "capacities": {"core": 1, "ram": 1, "disk": 1, "gpu": 0},
            "data_predicate": {"experiment_id": "exp-sp-1", "min_snapshots": 1,
                               "total_snapshots": total},
        })
        return job

    class _FeedLayer:
        """Serves `n` snapshot batches immediately, then nothing."""

        def __init__(self, n):
            self.n = n

        def read_from(self, exp, last_id="0-0", block_ms=0):
            served = int(str(last_id).split("-")[0])
            if served >= self.n:
                return []
            return [(f"{served + 1}-0", {"shots": 8})]

    @pytest.mark.parametrize("total", [1, 2, 4, 8])
    def test_consumer_spends_its_whole_budget(self, monkeypatch, total):
        """It divided by total+1 while sleeping only `total` times, so a one-snapshot
        post-processing job ran at half its wall_time and a four-snapshot one at 80%."""
        import swarm.models.job as job_mod
        slept = []
        monkeypatch.setattr(job_mod.time, "sleep", lambda s: slept.append(s))
        Job.configure_execution_simulation(scale=1.0, max_s=0)

        self._consumer_job(20.0, total).execute_consumer(self._FeedLayer(total), timeout_s=5.0)
        assert len(slept) == total, "one classical update per snapshot batch"
        assert sum(slept) == pytest.approx(20.0), slept

    def test_consumer_budget_tracks_the_policy(self, monkeypatch):
        import swarm.models.job as job_mod
        slept = []
        monkeypatch.setattr(job_mod.time, "sleep", lambda s: slept.append(s))
        Job.configure_execution_simulation(scale=0.25, max_s=0)

        self._consumer_job(20.0, 4).execute_consumer(self._FeedLayer(4), timeout_s=5.0)
        assert sum(slept) == pytest.approx(5.0)

    def test_producer_and_consumer_of_one_split_agree_on_their_budgets(self, monkeypatch):
        """Both halves must price the same wall_time the same way, or a split job and a whole
        job of equal duration contribute differently to makespan."""
        import swarm.models.job as job_mod
        Job.configure_execution_simulation(scale=1.0, max_s=0)

        class _Layer:
            def announce_producer(self, *a, **k): pass
            def publish(self, *a, **k): return 1

        prod = []
        monkeypatch.setattr(job_mod.time, "sleep", lambda s: prod.append(s))
        self._producer_job(12.0, 3).execute_producer(_Layer())

        cons = []
        monkeypatch.setattr(job_mod.time, "sleep", lambda s: cons.append(s))
        self._consumer_job(12.0, 3).execute_consumer(self._FeedLayer(3), timeout_s=5.0)

        assert sum(prod) == pytest.approx(sum(cons)) == pytest.approx(12.0)

    def test_neither_split_path_hardcodes_a_one_second_total(self):
        src = open(os.path.join(REPO, "swarm/models/job.py")).read()
        prod = src[src.index("def execute_producer"):src.index("def execute_consumer")]
        cons = src[src.index("def execute_consumer"):src.index("# ---------- Introspection")]
        assert "1.0 / (iterations + 1)" not in prod
        assert "min(0.2, 1.0 / (total + 1))" not in cons
        for body in (prod, cons):
            assert "simulated_execution_seconds" in body

