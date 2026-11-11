"""Code review 2026-10-05 §57: the Pegasus extractor's fail-opens.

Each turned "unknown" into a plausible value that the converter then trusted: arguments `[]`
(runnable, so the job ran with none), a dangling or unparseable container as "no container"
(so the job ran on the host's libraries), the first non-local catalog site instead of the one
the job ran on, and unknown sizes and exit codes as 0.
"""
import os
import sys
import tempfile
from pathlib import Path

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from pegasus_profile_extractor import (  # noqa: E402
    CATALOG_ERROR_KEY, catalog_entry_for, container_for, resolve_arguments)
from pegasus_to_swarm_converter import map_profile  # noqa: E402


class TestArguments:
    def test_nothing_declared_and_nothing_recorded_is_unknown(self):
        assert resolve_arguments(False, [], False, "", []) == (None, "")

    def test_a_declared_empty_list_stays_empty(self):
        """The job's abstract id resolved and declares no arguments: genuinely none."""
        assert resolve_arguments(False, [], True, "", []) == ([], "")

    def test_declaration_wins(self):
        assert resolve_arguments(False, ["-i", "a"], True, "", []) == (["-i", "a"], "-i a")

    def test_a_recorded_argv_is_kept_without_a_workflow(self):
        assert resolve_arguments(False, [], False, "-x 1", ["-x", "1"]) == (["-x", "1"], "-x 1")

    def test_a_cluster_is_unknown(self):
        assert resolve_arguments(True, ["-i"], True, "", []) == (None, "")

    def test_a_command_line_passed_as_one_argument_is_split_as_pegasus_runs_it(self):
        """tnseq passes `add_args(f"--input {a} --output {b}")`: one declared argument that
        Pegasus writes unquoted into a shell line. Kept as one argv entry, every tnseq job
        failed on its usage message."""
        argv, raw = resolve_arguments(
            False, ["--input mapped/WT1_count.txt --operation sum"], True, "", [])
        assert argv == ["--input", "mapped/WT1_count.txt", "--operation", "sum"]
        assert raw == "--input mapped/WT1_count.txt --operation sum"

    def test_separately_declared_arguments_are_joined_then_split(self):
        assert resolve_arguments(False, ["--day", "6", "--a b"], True, "", [])[0] == \
            ["--day", "6", "--a", "b"]

    def test_quotes_group_as_the_shell_would(self):
        assert resolve_arguments(False, ["--name 'a b'"], True, "", [])[0] == ["--name", "a b"]

    def test_a_line_the_shell_would_expand_is_unknown_not_literal(self):
        """A variable, glob or redirection makes the real argv unknowable from the line."""
        for arg in ("--in $HOME/x", "--in *.fq", "--out a > b", "--url http://h/?a=1&b=2"):
            assert resolve_arguments(False, [arg], True, "", [])[0] is None, arg

    def test_an_unbalanced_quote_is_unknown(self):
        assert resolve_arguments(False, ["--name 'a"], True, "", [])[0] is None


class TestCatalog:
    TRANSFORMATIONS = {"t": {
        "name": "t", "pfn": "/a/t", "type": "stageable", "site": "condorpool", "container": "c1",
        "by_site": {
            "condorpool": {"name": "t", "pfn": "/a/t", "type": "stageable", "site": "condorpool",
                           "container": "c1"},
            "osg": {"name": "t", "pfn": "/b/t", "type": "stageable", "site": "osg",
                    "container": "c2"}}}}

    def test_the_entry_is_the_site_the_job_ran_on(self):
        assert catalog_entry_for(self.TRANSFORMATIONS, "t", "osg")["pfn"] == "/b/t"

    def test_an_unknown_site_falls_back_to_the_default_choice(self):
        e = catalog_entry_for(self.TRANSFORMATIONS, "t", "elsewhere")
        assert e["pfn"] == "/a/t" and "by_site" not in e

    def test_a_dangling_container_name_is_imageless_not_none(self):
        c = container_for({"container": "missing"}, {})
        assert c == {"name": "missing", "type": None, "image": None, "image_site": None}

    def test_no_container_is_none(self):
        assert container_for({"container": None}, {}) is None


def _profile(**over):
    base = {"job_name": "j1", "run_name": "r", "wall_time_sec": 10.0, "exitcode_db": 0,
            "input_files_db": [], "output_files_db": [],
            "executable_db": "/bin/tool", "argv_db": [], "pfn_db": "/code/tool",
            "pfn_type_db": "stageable", "container_db": None}
    base.update(over)
    return base


class TestConverterSide:
    def _convert(self, **over):
        return map_profile(_profile(**over), 1)

    def test_an_unknown_exit_code_is_warned(self):
        job, warnings = self._convert(exitcode_db=None)
        assert job["should_fail"] is False
        assert any("outcome unknown" in w for w in warnings)

    def test_a_known_exit_code_is_not(self):
        _job, warnings = self._convert(exitcode_db=0)
        assert not any("outcome unknown" in w for w in warnings)

    def test_an_unreadable_catalog_refuses_execution(self):
        job, warnings = self._convert(catalog_error_db="tc.yml: bad yaml")
        assert job["execution"]["container"]["image"] == ""
        assert any("catalog unreadable" in w for w in warnings)
        assert any("NOT EXECUTABLE" in w for w in warnings)
