"""run_test.convert_pegasus_jobs must only place jobs on DTNs some agent holds.

generate_configs.assign_agent_dtns gives each agent 1-4 random picks from a 10-name pool, so a
small fleet leaves pool names with no holder. Feasibility requires an agent to hold every DTN a
job references, so a converted job hashed onto an unheld name could never run. The pool the
converter hashes over is therefore derived from agent_dtns.json, not from the pool it was drawn
from — and that requires the conversion to run after config generation.
"""
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import run_test  # noqa: E402
from pegasus_to_swarm_converter import make_dtn_resolver  # noqa: E402


def _write_assignments(tmp_path, assigned: dict) -> str:
    p = tmp_path / "agent_dtns.json"
    p.write_text(json.dumps(assigned))
    return str(p)


class TestAgentDtnPool:
    def test_union_of_held_names_sorted(self, tmp_path):
        path = _write_assignments(tmp_path, {
            "1": [{"name": "dtn7"}, {"name": "dtn2"}],
            "2": [{"name": "dtn2"}],
            "3": [{"name": "dtn10"}],
        })
        assert run_test.agent_dtn_pool(path) == ["dtn10", "dtn2", "dtn7"]

    def test_unheld_pool_names_are_excluded(self, tmp_path):
        # Ten-name pool, but the fleet only drew three of them.
        path = _write_assignments(tmp_path, {str(i): [{"name": f"dtn{i}"}] for i in (1, 4, 9)})
        pool = run_test.agent_dtn_pool(path)
        assert pool == ["dtn1", "dtn4", "dtn9"]
        assert "dtn3" not in pool

    def test_missing_file_means_no_dtns(self, tmp_path):
        assert run_test.agent_dtn_pool(str(tmp_path / "absent.json")) == []

    def test_agents_without_dtns_contribute_nothing(self, tmp_path):
        path = _write_assignments(tmp_path, {"1": [], "2": None, "3": [{"name": "dtn5"}]})
        assert run_test.agent_dtn_pool(path) == ["dtn5"]

    def test_accepts_bare_string_entries(self, tmp_path):
        path = _write_assignments(tmp_path, {"1": ["dtn3", {"name": "dtn1"}]})
        assert run_test.agent_dtn_pool(path) == ["dtn1", "dtn3"]


class TestConvertedJobsLandOnHeldDtns:
    def test_every_job_hashes_onto_a_held_dtn(self, tmp_path):
        """The invariant the fix exists for: no job references a DTN no agent has."""
        held = run_test.agent_dtn_pool(_write_assignments(tmp_path, {
            "1": [{"name": "dtn2"}, {"name": "dtn5"}],
            "2": [{"name": "dtn5"}, {"name": "dtn8"}],
        }))
        resolve = make_dtn_resolver(dtn_names=held, dtn_scope="job")
        for n in range(500):
            assert resolve("local", f"f{n}.dat", group_key=f"job-{n}") in set(held)

    def test_job_scope_keeps_all_files_on_one_dtn(self, tmp_path):
        held = ["dtn2", "dtn5", "dtn8"]
        resolve = make_dtn_resolver(dtn_names=held, dtn_scope="job")
        placements = {resolve("local", f"in{i}.dat", group_key="job-42") for i in range(20)}
        assert len(placements) == 1


class TestConvertPegasusJobsPoolSelection:
    """convert_pegasus_jobs chooses the pool; the converter itself is stubbed out."""

    @pytest.fixture
    def capture(self, monkeypatch):
        calls = {}

        def fake_convert(**kwargs):
            calls.update(kwargs)
            return {"jobs_written": 0, "warnings_count": 0}

        import pegasus_to_swarm_converter
        monkeypatch.setattr(pegasus_to_swarm_converter, "convert_pegasus_profiles", fake_convert)
        monkeypatch.setattr(run_test, "log", lambda *a, **k: None)
        return calls

    @staticmethod
    def _args(dtn_names=None):
        class A:
            pegasus_profiles = "profiles.txt"
            pegasus_input_type = "text"
            pegasus_data_nodes = "per-file"
            pegasus_dtn_names = dtn_names
        return A()

    def test_default_pool_is_what_the_fleet_holds(self, tmp_path, monkeypatch, capture):
        monkeypatch.chdir(tmp_path)
        _write_assignments(tmp_path, {"1": [{"name": "dtn9"}], "2": [{"name": "dtn3"}]})
        run_test.convert_pegasus_jobs(self._args())
        assert capture["dtn_names"] == ["dtn3", "dtn9"]
        assert capture["dtn_scope"] == "job"

    def test_no_dtns_anywhere_falls_back_to_local(self, tmp_path, monkeypatch, capture):
        """Hierarchical runs do not pass --dtns; 'local' is excluded from required DTNs."""
        monkeypatch.chdir(tmp_path)
        run_test.convert_pegasus_jobs(self._args())
        assert capture["dtn_names"] == ["local"]

    def test_explicit_names_win(self, tmp_path, monkeypatch, capture):
        monkeypatch.chdir(tmp_path)
        _write_assignments(tmp_path, {"1": [{"name": "dtn1"}]})
        run_test.convert_pegasus_jobs(self._args("dtn1, dtn2"))
        assert capture["dtn_names"] == ["dtn1", "dtn2"]
