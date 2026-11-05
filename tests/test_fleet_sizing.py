"""Sizing a fleet from the jobs it will be given.

The failure this prevents is silent: a job no agent can host is never proposed, never fails, and
shows up only as still-pending at the end, which reads as a scheduling problem. The rule lives in
one module because two copies of "can this agent host this job" would drift.
"""
import json
import os
import subprocess
import sys

import pytest
import yaml

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

from swarm.utils import fleet_sizing  # noqa: E402
from swarm.utils.fleet_sizing import (  # noqa: E402
    JobRequirements, dtn_entries, fit_flavor, job_requirements, load_job_records)


def _job(core=1, ram=1, disk=1, gpu=0, dtns=()):
    return {"capacities": {"core": core, "ram": ram, "disk": disk, "gpu": gpu},
            "data_in": [{"name": n, "file": f"{n}.dat"} for n in dtns], "data_out": []}


class TestRequirements:
    def test_it_takes_the_maximum_of_each_dimension(self):
        req = job_requirements([_job(core=8, ram=2), _job(core=2, ram=64)])
        assert (req.core, req.ram) == (8.0, 64.0)
        assert req.job_count == 2

    def test_one_agent_has_to_satisfy_all_of_them(self):
        """Not "some agent is big enough in each dimension" — the same agent runs the job, so
        the requirement is one profile, not four independent ones."""
        req = job_requirements([_job(core=8, ram=2), _job(core=2, ram=64)])
        fitted = fit_flavor({"core": 2, "ram": 8, "disk": 100, "gpu": 0}, req)
        assert fitted["core"] >= 8 and fitted["ram"] >= 64

    def test_local_is_not_a_dtn(self):
        # `local` means the local filesystem; is_job_feasible excludes it, so attaching it to an
        # agent would be meaningless.
        assert job_requirements([_job(dtns=["local", "dtn3"])]).dtns == {"dtn3"}

    def test_an_empty_set_is_reported_as_empty(self):
        assert job_requirements([]).empty


class TestFitting:
    def test_it_raises_and_never_lowers(self):
        """Fitting means every agent can host the largest job. Lowering would shrink a fleet to
        match a small workflow, which is not what anyone asked for."""
        big = {"core": 64, "ram": 512, "disk": 5000, "gpu": 8}
        assert fit_flavor(big, job_requirements([_job(core=2, ram=4)])) == big

    def test_fractional_requirements_round_up(self):
        # 3.2 cores of requirement is not satisfied by 3.
        fitted = fit_flavor({"core": 1, "ram": 1, "disk": 1, "gpu": 0},
                            job_requirements([_job(core=3.2, ram=0.5)]))
        assert fitted["core"] == 4 and fitted["ram"] == 1

    def test_the_flavour_keeps_its_other_fields(self):
        fitted = fit_flavor({"name": "small", "core": 2, "ram": 8, "disk": 100, "gpu": 0},
                            job_requirements([_job(core=4)]))
        assert fitted["name"] == "small"

    def test_dtn_entries_are_shaped_like_agent_configs(self):
        entries = dtn_entries({"dtn3", "dtn1"})
        assert [e["name"] for e in entries] == ["dtn1", "dtn3"]        # stable order
        assert all({"name", "ip", "user", "connectivity_score"} <= set(e) for e in entries)


class TestLoading:
    def test_it_reads_job_records_and_ignores_everything_else(self, tmp_path):
        for i in range(1, 3):
            (tmp_path / f"job_{i}.json").write_text(json.dumps(_job()))
        (tmp_path / "manifest.json").write_text("{}")
        (tmp_path / "code").mkdir()
        assert len(load_job_records(str(tmp_path))) == 2

    def test_an_unparseable_record_is_skipped_not_fatal(self, tmp_path):
        # A directory can hold a half-written file; refusing to size a fleet over one helps
        # nobody, and the fleet-fit check at run time still sees the real set.
        (tmp_path / "job_1.json").write_text(json.dumps(_job()))
        (tmp_path / "job_2.json").write_text("{not json")
        assert len(load_job_records(str(tmp_path))) == 1


class TestGenerateConfigsSizesToJobs:
    """The end the user sees: `generate_configs.py --size-to-jobs`, including for hierarchical
    fleets, which the converter cannot generate."""

    def _run(self, tmp_path, *extra, topology="mesh", agents=3):
        jobs = tmp_path / "jobs"; jobs.mkdir(exist_ok=True)
        (jobs / "job_1.json").write_text(json.dumps(_job(core=48, ram=300, disk=2000,
                                                         dtns=["dtn9"])))
        out = tmp_path / "configs"
        cmd = [sys.executable, os.path.join(REPO, "generate_configs.py"), str(agents), "10",
               os.path.join(REPO, "config_swarm_multi.yml"), str(out), topology, "localhost",
               "0", "--skip-jobs", "--seed", "42", *extra]
        proc = subprocess.run(cmd, cwd=tmp_path, capture_output=True, text=True)
        return proc, out

    def test_every_agent_is_raised_to_the_largest_job(self, tmp_path):
        proc, out = self._run(tmp_path, "--size-to-jobs", "jobs")
        assert proc.returncode == 0, proc.stderr
        for path in out.glob("config_swarm_multi_*.yml"):
            caps = yaml.safe_load(path.read_text())["capacities"]
            assert caps["core"] >= 48 and caps["ram"] >= 300 and caps["disk"] >= 2000

    def test_every_agent_holds_the_dtns_the_jobs_name(self, tmp_path):
        # Feasibility needs ONE agent holding every DTN a job names, so a sized fleet gets them
        # whether or not --dtns assigned a pool.
        proc, out = self._run(tmp_path, "--size-to-jobs", "jobs")
        for path in out.glob("config_swarm_multi_*.yml"):
            names = {d["name"] for d in yaml.safe_load(path.read_text()).get("dtns") or []}
            assert "dtn9" in names

    def test_it_works_for_hierarchical_which_the_converter_cannot_generate(self, tmp_path):
        proc, out = self._run(tmp_path, "--size-to-jobs", "jobs",
                              "--hierarchical-level1-agent-type", "resource",
                              topology="hierarchical", agents=30)
        assert proc.returncode == 0, proc.stderr
        caps = yaml.safe_load((out / "config_swarm_multi_1.yml").read_text())["capacities"]
        assert caps["core"] >= 48

    def test_without_the_flag_the_fleet_is_the_standard_pool(self, tmp_path):
        """Sizing flattens capacity and locality heterogeneity, so it must stay opt-in: a run
        whose subject is the fleet keeps the pool it has always used."""
        proc, out = self._run(tmp_path)
        caps = [yaml.safe_load(p.read_text())["capacities"]["core"]
                for p in out.glob("config_swarm_multi_*.yml")]
        assert max(caps) < 48

    def test_a_directory_with_no_records_is_refused(self, tmp_path):
        (tmp_path / "empty").mkdir()
        proc, _ = self._run(tmp_path, "--size-to-jobs", "empty")
        assert proc.returncode != 0 and "no job_*.json" in (proc.stderr + proc.stdout)

    def test_a_path_that_is_not_a_directory_is_refused(self, tmp_path):
        proc, _ = self._run(tmp_path, "--size-to-jobs", "nope")
        assert proc.returncode != 0 and "not a directory" in (proc.stderr + proc.stdout)

    def test_the_persisted_dtn_map_matches_the_configs(self, tmp_path):
        """agent_dtns.json is what `run_test.agent_dtn_pool()` reads to decide which DTNs jobs
        may be hashed onto, and what a later generation reuses. Recording it before sizing
        attached the job DTNs left it describing a fleet its own configs contradict."""
        proc, out = self._run(tmp_path, "--size-to-jobs", "jobs", "--dtns")
        assert proc.returncode == 0, proc.stderr
        persisted = json.loads((tmp_path / "agent_dtns.json").read_text())
        for path in out.glob("config_swarm_multi_*.yml"):
            agent_id = path.stem.rsplit("_", 1)[1]
            in_config = {d["name"] for d in yaml.safe_load(path.read_text())["dtns"]}
            assert "dtn9" in in_config
            assert {d["name"] for d in persisted[agent_id]} == in_config

    def test_a_fleet_with_no_pool_still_persists_what_sizing_gave_it(self, tmp_path):
        # Hierarchical never gets --dtns, so the file used to be skipped entirely — reporting a
        # fleet that holds nothing while every config names dtn9.
        proc, _ = self._run(tmp_path, "--size-to-jobs", "jobs",
                            "--hierarchical-level1-agent-type", "resource",
                            topology="hierarchical", agents=30)
        assert proc.returncode == 0, proc.stderr
        persisted = json.loads((tmp_path / "agent_dtns.json").read_text())
        assert all("dtn9" in {d["name"] for d in v} for v in persisted.values())

    def test_regenerating_from_the_map_keeps_the_sized_dtns(self, tmp_path):
        """The reuse path: a second generation reads agent_dtns.json instead of drawing a pool.
        With the map stale, the sized DTNs vanished from the regenerated fleet."""
        self._run(tmp_path, "--size-to-jobs", "jobs", "--dtns")
        proc, out = self._run(tmp_path, "--dtns")          # no sizing flag this time
        assert proc.returncode == 0, proc.stderr
        names = {d["name"]
                 for p in out.glob("config_swarm_multi_*.yml")
                 for d in yaml.safe_load(p.read_text())["dtns"]}
        assert "dtn9" in names

    def test_shrinking_the_fleet_drops_agents_that_no_longer_exist(self, tmp_path):
        """agent_dtns.json is read as "which DTNs this fleet holds". Carrying entries for
        agents 11-30 into a 10-agent run advertises DTNs no live agent has, and jobs hashed
        onto them are infeasible everywhere — the silent stall, arrived at from a new angle."""
        self._run(tmp_path, "--dtns", agents=30)
        before = json.loads((tmp_path / "agent_dtns.json").read_text())
        assert len(before) == 30

        proc, _ = self._run(tmp_path, "--dtns", agents=10)
        assert proc.returncode == 0, proc.stderr
        after = json.loads((tmp_path / "agent_dtns.json").read_text())
        assert sorted(int(k) for k in after) == list(range(1, 11))

    def test_a_fleet_with_no_dtns_at_all_leaves_no_stale_file(self, tmp_path):
        # The base config used here lists DTNs, so force the case with one that does not.
        self._run(tmp_path, "--dtns", agents=5)
        assert (tmp_path / "agent_dtns.json").exists()

        base = tmp_path / "bare.yml"
        cfg = yaml.safe_load(open(os.path.join(REPO, "config_swarm_multi.yml")))
        cfg.pop("dtns", None)
        base.write_text(yaml.safe_dump(cfg))
        out = tmp_path / "configs2"
        proc = subprocess.run(
            [sys.executable, os.path.join(REPO, "generate_configs.py"), "5", "10", str(base),
             str(out), "mesh", "localhost", "0", "--skip-jobs", "--seed", "42"],
            cwd=tmp_path, capture_output=True, text=True)
        assert proc.returncode == 0, proc.stderr
        assert not (tmp_path / "agent_dtns.json").exists()


class TestVariabilityWithoutInfeasibility:
    """The point of sizing is that EVERY agent can run EVERY job — that is what makes a failure
    test mean anything, since a dead agent's work has to be pickable-up by someone. A uniform
    fleet is not the point, and does not follow: feasibility tests DTN *names* and job
    capacities, while the cost model tests `connectivity_score`, so locality can vary freely
    without ever stranding a job."""

    def test_every_agent_holds_every_name(self):
        names = {"dtn1", "dtn2", "dtn3"}
        bases = fleet_sizing.dtn_base_scores(names)
        fleet = [fleet_sizing.dtn_entries(names, base_scores=bases) for _ in range(10)]
        assert all({e["name"] for e in agent} == names for agent in fleet)

    def test_but_their_connectivity_differs(self):
        bases = fleet_sizing.dtn_base_scores({"dtn1"})
        scores = {fleet_sizing.dtn_entries({"dtn1"}, base_scores=bases)[0]["connectivity_score"]
                  for _ in range(50)}
        assert len(scores) > 1, "every agent scored the same: locality is flat again"

    def test_a_name_keeps_one_base_across_the_fleet(self):
        """Jitter around a shared base, not an independent draw per agent — otherwise the DTN
        name carries no locality for selection to learn."""
        bases = fleet_sizing.dtn_base_scores({"dtn1"})
        scores = [fleet_sizing.dtn_entries({"dtn1"}, base_scores=bases)[0]["connectivity_score"]
                  for _ in range(200)]
        assert max(scores) - min(scores) <= 2 * fleet_sizing.DTN_JITTER + 0.01

    def test_scores_stay_in_range(self):
        names = {f"dtn{i}" for i in range(1, 11)}
        bases = fleet_sizing.dtn_base_scores(names)
        for _ in range(20):
            for e in fleet_sizing.dtn_entries(names, base_scores=bases):
                assert 0.0 <= e["connectivity_score"] <= 1.0

    def test_it_is_not_pinned_at_the_best_possible_value(self):
        """1.0 made a bolted-on DTN look better connected than any agent's own assignment
        (drawn from 0.6-0.95), which is variability pointing the wrong way."""
        names = {f"dtn{i}" for i in range(1, 21)}
        bases = fleet_sizing.dtn_base_scores(names)
        entries = fleet_sizing.dtn_entries(names, base_scores=bases)
        assert not all(e["connectivity_score"] == 1.0 for e in entries)
        assert min(e["connectivity_score"] for e in entries) < 1.0

    def test_capacity_is_raised_to_a_floor_not_levelled(self):
        """The other half of the same rule: an agent already bigger than the largest job keeps
        its flavour, so the fleet is not made uniform by being made feasible."""
        req = JobRequirements(core=4, ram=14, disk=7, gpu=1, job_count=1)
        small = fleet_sizing.fit_flavor({"core": 2, "ram": 8, "disk": 100, "gpu": 0}, req)
        big = fleet_sizing.fit_flavor({"core": 32, "ram": 128, "disk": 1000, "gpu": 4}, req)
        assert (small["core"], small["ram"]) == (4, 14)
        assert (big["core"], big["ram"]) == (32, 128)
        assert big["core"] > small["core"]


class TestOneBasePerDtnName:
    """A DTN's connectivity has to describe the DTN, not how an agent came by it. `--dtns` draws
    a pool with a `base_connectivity_score` per name; sizing used to draw a SECOND, unrelated
    base for names the jobs referenced, so a name present in both had two populations — the
    agents assigned it scattered around one base, the agents given it by sizing around another.
    Measured before the fix on a 20-agent fleet: dtn2 spanning 0.59-0.98 against a +/-0.05
    jitter. Selection then learns a locality that is an artefact of the generator."""

    def _fleet(self, tmp_path, agents=12, dtn="dtn9"):
        jobs = tmp_path / "jobs"; jobs.mkdir(exist_ok=True)
        (jobs / "job_1.json").write_text(json.dumps(_job(core=2, ram=4, disk=10, dtns=[dtn])))
        out = tmp_path / "configs"
        cmd = [sys.executable, os.path.join(REPO, "generate_configs.py"), str(agents), "10",
               os.path.join(REPO, "config_swarm_multi.yml"), str(out), "mesh", "localhost",
               "0", "--skip-jobs", "--seed", "42", "--dtns", "--size-to-jobs", "jobs"]
        proc = subprocess.run(cmd, cwd=tmp_path, capture_output=True, text=True)
        assert proc.returncode == 0, proc.stderr
        scores = {}
        for path in out.glob("config_swarm_multi_*.yml"):
            for d in yaml.safe_load(path.read_text()).get("dtns") or []:
                scores.setdefault(d["name"], []).append(d["connectivity_score"])
        return scores

    def test_a_name_in_both_the_pool_and_the_jobs_has_one_base(self, tmp_path):
        scores = self._fleet(tmp_path)
        assert len(scores["dtn9"]) == 12, "every agent should hold the job's DTN"
        spread = max(scores["dtn9"]) - min(scores["dtn9"])
        assert spread <= 2 * fleet_sizing.DTN_JITTER + 0.011, (
            f"dtn9 spans {spread:.2f}: sized and pool-assigned agents are on different bases")

    def test_every_name_stays_within_one_jitter_band(self, tmp_path):
        for name, vals in self._fleet(tmp_path).items():
            spread = max(vals) - min(vals)
            assert spread <= 2 * fleet_sizing.DTN_JITTER + 0.011, f"{name} spans {spread:.2f}"

    def test_the_known_bases_come_from_the_pool_not_a_fresh_draw(self):
        """Unit-level: the pool's own base is what sizing reuses."""
        import generate_configs
        gen = generate_configs.SwarmConfigGenerator.__new__(generate_configs.SwarmConfigGenerator)
        gen.agent_dtns_map = {}
        pool = [{"name": "dtn1", "base_connectivity_score": 0.72}]
        assert gen._known_dtn_bases(pool) == {"dtn1": 0.72}

    def test_a_reused_assignment_yields_a_base_too(self):
        """With no pool (agent_dtns.json reused) the base is the mean of what agents hold, so a
        regeneration does not re-scatter a name around a new centre."""
        import generate_configs
        gen = generate_configs.SwarmConfigGenerator.__new__(generate_configs.SwarmConfigGenerator)
        gen.agent_dtns_map = {"1": [{"name": "dtn1", "connectivity_score": 0.70}],
                              "2": [{"name": "dtn1", "connectivity_score": 0.80}]}
        assert gen._known_dtn_bases(None) == {"dtn1": 0.75}


class TestCoordinatorTypeDefault:
    """`--hierarchical-level1-agent-type` defaults to `resource` (changed 2026-09-18 from
    `llm`). `LlmAgent.__init__` builds its bidder eagerly and needs OPENAI_API_KEY, which only
    some hosts carry — so under the old default a coordinator died at startup depending on
    where placement put it: two log lines, then silence, with the run looking busy until the
    cap expired. Every launcher script in the repo already passed `resource` explicitly, so the
    default was only ever reached by accident.

    It deliberately does NOT follow `--agent-type`: an all-LLM hierarchy has to ask for both."""

    @staticmethod
    def _default(module_path, flag="--hierarchical-level1-agent-type"):
        import ast
        tree = ast.parse(open(os.path.join(REPO, module_path)).read())
        for node in ast.walk(tree):
            if (isinstance(node, ast.Call) and getattr(node.func, "attr", None) == "add_argument"
                    and node.args and getattr(node.args[0], "value", None) == flag):
                for kw in node.keywords:
                    if kw.arg == "default":
                        return kw.value.value
        raise AssertionError(f"{flag} not found in {module_path}")

    @pytest.mark.parametrize("module", ["run_test.py", "generate_configs.py",
                                        "batch_tests_v2.py"])
    def test_every_entry_point_defaults_to_resource(self, module):
        assert self._default(module) == "resource"

    def test_the_three_agree(self):
        """run_test forwards its value to generate_configs unconditionally, so a disagreement
        would make the launcher and the config generator describe different fleets."""
        defaults = {m: self._default(m) for m in
                    ("run_test.py", "generate_configs.py", "batch_tests_v2.py")}
        assert len(set(defaults.values())) == 1, defaults

    def test_the_generator_constructor_agrees_with_its_cli(self):
        import inspect, generate_configs
        sig = inspect.signature(generate_configs.SwarmConfigGenerator.__init__)
        assert sig.parameters["hierarchical_level1_agent_type"].default == "resource"

    def test_a_generated_coordinator_is_a_resource_agent_by_default(self, tmp_path):
        out = tmp_path / "configs"
        cmd = [sys.executable, os.path.join(REPO, "generate_configs.py"), "30", "10",
               os.path.join(REPO, "config_swarm_multi.yml"), str(out), "hierarchical",
               "localhost", "0", "--skip-jobs", "--seed", "42"]
        proc = subprocess.run(cmd, cwd=tmp_path, capture_output=True, text=True)
        assert proc.returncode == 0, proc.stderr
        types = {yaml.safe_load(p.read_text()).get("agent_type")
                 for p in out.glob("config_swarm_multi_*.yml")}
        assert "llm" not in types, f"a coordinator still defaults to llm: {types}"


class TestGroupsPerCoordinatorDefault:
    """Default 2 (changed 2026-09-18 from 1). At 1 a coordinator parents a single group, so it
    never chooses: the MAB and `delegation.policy: llm` are both inert and every delegation
    records as `trivial` — a fleet built with the old default cannot produce the measurement
    the hierarchy exists for.

    A default may not break a command that worked, so the two cases differ: a three-level
    hierarchy (100, 990, 1000) REFUSES an explicit >1, because the request cannot be honoured,
    but steps the default down to 1 and says so."""

    def _gen(self, tmp_path, agents, *extra):
        out = tmp_path / f"configs{agents}"
        cmd = [sys.executable, os.path.join(REPO, "generate_configs.py"), str(agents), "10",
               os.path.join(REPO, "config_swarm_multi.yml"), str(out), "hierarchical",
               "localhost", "0", "--skip-jobs", "--seed", "42", *extra]
        proc = subprocess.run(cmd, cwd=tmp_path, capture_output=True, text=True)
        return proc, out

    @staticmethod
    def _fan_out(out):
        widths = []
        for p in out.glob("config_swarm_multi_*.yml"):
            topo = yaml.safe_load(p.read_text()).get("topology") or {}
            if topo.get("level") == 1:
                widths.append(len(topo.get("children") or []))
        return max(widths) if widths else None

    def test_a_two_level_fleet_gets_two_groups_per_coordinator(self, tmp_path):
        proc, out = self._gen(tmp_path, 30)
        assert proc.returncode == 0, proc.stderr
        assert self._fan_out(out) == 2

    def test_a_three_level_fleet_steps_the_default_down_and_still_generates(self, tmp_path):
        proc, out = self._gen(tmp_path, 100)
        assert proc.returncode == 0, proc.stderr
        assert "does not support; using 1" in proc.stdout
        assert self._fan_out(out) == 1

    def test_but_an_explicit_request_is_still_refused_there(self, tmp_path):
        proc, _ = self._gen(tmp_path, 100, "--groups-per-coordinator", "2")
        assert proc.returncode != 0
        assert "only supported for two-level hierarchies" in (proc.stdout + proc.stderr)

    def test_an_explicit_one_is_honoured(self, tmp_path):
        proc, out = self._gen(tmp_path, 30, "--groups-per-coordinator", "1")
        assert proc.returncode == 0, proc.stderr
        assert self._fan_out(out) == 1

    @pytest.mark.parametrize("module", ["run_test.py", "generate_configs.py",
                                        "batch_tests_v2.py"])
    def test_the_cli_default_is_the_none_sentinel(self, module):
        """None, not 2: the launcher cannot tell whether a topology can honour 2, so it must
        pass the question on rather than answer it."""
        assert TestCoordinatorTypeDefault._default(module, "--groups-per-coordinator") is None

    @pytest.mark.parametrize("module", ["run_test.py", "batch_tests_v2.py"])
    def test_an_explicit_value_is_forwarded_even_when_it_is_one(self, module):
        """Forwarding only when > 1 would let an explicit 1 reach generate_configs as "not
        given", which now means 2 — the launcher and the fleet would disagree."""
        src = open(os.path.join(REPO, module)).read()
        assert "if getattr(args, 'groups_per_coordinator', None) is not None:" in src
        assert "if getattr(args, 'groups_per_coordinator', 1) > 1:" not in src


class TestExplicitCoParentsOutrankTheDefaultFanOut:
    """A wider fan-out means FEWER coordinators (`ceil(num_groups / G)`), and co-parents are
    capped at the coordinator count — so making G=2 the default silently shrank an explicitly
    requested `--co-parents`. On Hier-30: 5 groups, G=2 leaves 3 coordinators, so `--co-parents
    4` became 3 with nothing said, and a failover test would have run with less redundancy than
    it asked for.

    An explicit request outranks a default, so the default fan-out gives way. Two explicit
    values that cannot both hold are refused rather than reconciled."""

    def _gen(self, tmp_path, *extra, agents=30):
        out = tmp_path / "configs"
        cmd = [sys.executable, os.path.join(REPO, "generate_configs.py"), str(agents), "10",
               os.path.join(REPO, "config_swarm_multi.yml"), str(out), "hierarchical",
               "localhost", "0", "--skip-jobs", "--seed", "42", *extra]
        return subprocess.run(cmd, cwd=tmp_path, capture_output=True, text=True), out

    @staticmethod
    def _co_parents(out):
        widths = []
        for p in out.glob("config_swarm_multi_*.yml"):
            topo = yaml.safe_load(p.read_text()).get("topology") or {}
            if topo.get("level") == 0 and topo.get("co_parents"):
                widths.append(len(topo["co_parents"]))
        return max(widths) if widths else None

    def test_the_default_fan_out_gives_way_to_an_explicit_co_parents(self, tmp_path):
        proc, out = self._gen(tmp_path, "--co-parents", "4")
        assert proc.returncode == 0, proc.stderr
        assert "so the requested failover redundancy holds" in proc.stdout
        assert self._co_parents(out) == 4, "the explicit redundancy was still reduced"

    def test_two_explicit_values_that_cannot_both_hold_are_refused(self, tmp_path):
        proc, _ = self._gen(tmp_path, "--co-parents", "4", "--groups-per-coordinator", "2")
        assert proc.returncode != 0
        out = proc.stdout + proc.stderr
        assert "but --co-parents 4 needs 4" in out
        assert "--groups-per-coordinator 1 is the widest" in out, "refusal names no way forward"

    def test_a_request_the_fan_out_can_honour_keeps_the_default(self, tmp_path):
        proc, out = self._gen(tmp_path, "--co-parents", "2")
        assert proc.returncode == 0, proc.stderr
        assert "coordinator(s) x 2 group(s) each" in proc.stdout
        assert self._co_parents(out) == 2

    def test_more_co_parents_than_coordinators_is_reported(self, tmp_path):
        """Pre-dates the default change and stays a warning, since campaign scripts rely on the
        capped behaviour — but it is no longer silent."""
        proc, _ = self._gen(tmp_path, "--co-parents", "9", "--groups-per-coordinator", "1")
        assert proc.returncode == 0, proc.stderr
        assert "exceeds the 5 coordinator(s)" in proc.stdout

    def test_the_refusal_names_the_widest_fan_out_that_works(self, tmp_path):
        """`num_groups // K` is always valid but not always widest — 9 groups with K=3 gives 3
        when 4 works — so the refusal sent the reader to a narrower fleet than they needed.
        The condition ceil(n/G) >= K is exactly n > G*(K-1), so the answer is (n-1)//(K-1)."""
        proc, _ = self._gen(tmp_path, "--co-parents", "3", "--groups-per-coordinator", "5",
                            agents=90)
        assert proc.returncode != 0
        assert "--groups-per-coordinator 4 is the widest" in (proc.stdout + proc.stderr)

    def test_and_that_value_really_generates(self, tmp_path):
        """A refusal that names an unusable number is worse than one that names none."""
        proc, out = self._gen(tmp_path, "--co-parents", "3", "--groups-per-coordinator", "4",
                              agents=90)
        assert proc.returncode == 0, proc.stderr
        assert self._co_parents(out) == 3

    def test_the_formula_matches_brute_force(self):
        """The closed form, against every (groups, K) pair up to 40."""
        import math
        for n in range(2, 41):
            for k in range(2, n + 1):
                widest = max(1, min(n, (n - 1) // (k - 1)))
                brute = max([g for g in range(1, n + 1) if math.ceil(n / g) >= k] or [1])
                assert widest == brute, (n, k, widest, brute)


class TestConverterGeneratesConfigsWithABaseConfig:
    """`pegasus_to_swarm_converter.py --generate-agent-configs --base-config <file>` writes
    per-agent YAML as well as agent_profiles.json. That branch had no test, and on 2026-09-18
    it acquired a NameError (a variable renamed in the profiles loop, not in the YAML loop) that
    the whole suite passed over. Found by a review pass, not by a test — so now it is a test."""

    def _convert(self, tmp_path):
        import json as _json
        prof = tmp_path / "profiles.json"
        prof.write_text(_json.dumps([{
            "job_id": "j1", "wf_uuid": "u", "wf_name": "w", "transformation": "t",
            "duration": 1.0, "exit_code": 0, "cores": 1, "memory_mb": 256,
            "inputs": [{"lfn": "a.dat", "size": 10}], "outputs": [{"lfn": "b.dat", "size": 10}],
        }]))
        out = tmp_path / "bundle"
        cmd = [sys.executable, os.path.join(REPO, "pegasus_to_swarm_converter.py"),
               "--input", str(prof), "--input-type", "json", "--output-dir", str(out),
               "--no-bundle", "--dtn-names", "dtn1,dtn2", "--dtn-scope", "job",
               "--generate-agent-configs", "--num-agents", "3",
               "--base-config", os.path.join(REPO, "config_swarm_multi.yml"),
               "--topology", "mesh", "--db-host", "localhost"]
        return subprocess.run(cmd, cwd=tmp_path, capture_output=True, text=True), out

    def test_it_runs_and_writes_one_config_per_agent(self, tmp_path):
        proc, out = self._convert(tmp_path)
        assert proc.returncode == 0, proc.stderr[-2000:]
        assert "NameError" not in proc.stderr
        cfgs = sorted((out / "configs").glob("config_swarm_multi_*.yml"))
        assert len(cfgs) == 3, proc.stdout[-1000:]

    def test_each_config_carries_the_same_dtns_as_its_profile(self, tmp_path):
        """The fleet-fit check reads the config and the DTN pool reads the profile; if they
        disagreed they would describe two different agents."""
        import json as _json
        proc, out = self._convert(tmp_path)
        assert proc.returncode == 0, proc.stderr[-2000:]
        profiles = _json.loads((out / "agent_profiles.json").read_text())
        for path in (out / "configs").glob("config_swarm_multi_*.yml"):
            aid = path.stem.rsplit("_", 1)[1]
            cfg_dtns = yaml.safe_load(path.read_text())["dtns"]
            assert cfg_dtns == profiles[aid]["dtns"], aid
