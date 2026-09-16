# MIT License
#
# Copyright (c) 2024 swarm-workflows
#
# Author: Komal Thareja(kthare10@renci.org)
"""Carrying a real workflow's executables and containers through to a Job.

`Job.execute()` sleeps for a job's wall time, which is enough to study ordering and placement
and is where every number so far comes from. Running the same workflow Pegasus ran needs three
things the tree did not carry: the executable, its arguments, and the container to run them
in. These tests pin the path that collects them — stampede DB + transformation catalog →
profile → job record → `Job` — and, more importantly, the places where it must **refuse**.

The refusals matter more than the happy path. The point of this pipeline is an
apples-to-apples comparison against Pegasus, and a job that runs *something* when it should
have run nothing produces a number that looks like a result and is not one.
"""
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from pegasus_profile_extractor import (  # noqa: E402
    load_transformation_catalog, load_workflow_uses,
)
from pegasus_to_swarm_converter import _map_execution  # noqa: E402
from swarm.models.execution import ContainerSpec, ExecutionSpec  # noqa: E402
from swarm.models.job import Job  # noqa: E402

CATALOG = """
pegasus: 5.0.4
transformations:
- name: analyze_moisture
  sites:
  - name: condorpool
    pfn: /wf/bin/analyze_moisture.py
    type: stageable
    container: soil_container
- name: installed_tool
  sites:
  - name: condorpool
    pfn: /usr/bin/installed_tool
    type: installed
containers:
- name: soil_container
  type: singularity
  image: file:///wf/Apptainer/Soil.sif
  image.site: local
"""


def write_catalog(root: Path, relpath: str, body: str = CATALOG) -> Path:
    path = root / relpath
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body)
    return path


# --------------------------------------------------------------------------------------
# The catalog is where the executable actually lives.
# --------------------------------------------------------------------------------------

class TestTransformationCatalog(unittest.TestCase):
    def test_transformations_and_containers_are_parsed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_catalog(root, "catalogs/transformations.yml")
            transformations, containers = load_transformation_catalog(str(root))
        self.assertEqual(transformations["analyze_moisture"]["pfn"],
                         "/wf/bin/analyze_moisture.py")
        self.assertEqual(transformations["analyze_moisture"]["container"], "soil_container")
        self.assertEqual(containers["soil_container"]["type"], "singularity")
        self.assertEqual(containers["soil_container"]["image"], "file:///wf/Apptainer/Soil.sif")

    def test_the_planned_copy_wins_over_the_workflow_root(self):
        """The run dir's catalog is the one Pegasus planned with, so it describes the run
        that happened; the workflow-root copy can have been edited since."""
        edited = CATALOG.replace("/wf/bin/analyze_moisture.py", "/wf/bin/EDITED_LATER.py")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_catalog(root, "catalogs/transformations.yml")       # planned
            write_catalog(root, "transformations.yml", edited)        # edited afterwards
            transformations, _ = load_transformation_catalog(str(root))
        self.assertEqual(transformations["analyze_moisture"]["pfn"],
                         "/wf/bin/analyze_moisture.py")

    def test_a_non_local_site_is_preferred(self):
        """`local` is the submit host, whose pfn is often a wrapper rather than the science
        code. Site order in the catalog is not meaningful, so first-wins would be a coin flip."""
        body = CATALOG.replace(
            "  - name: condorpool\n    pfn: /wf/bin/analyze_moisture.py",
            "  - name: local\n    pfn: /wf/wrapper.sh\n    type: stageable\n"
            "  - name: condorpool\n    pfn: /wf/bin/analyze_moisture.py")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_catalog(root, "catalogs/transformations.yml", body)
            transformations, _ = load_transformation_catalog(str(root))
        self.assertEqual(transformations["analyze_moisture"]["pfn"],
                         "/wf/bin/analyze_moisture.py")

    def test_a_missing_catalog_is_empty_not_an_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            transformations, containers = load_transformation_catalog(tmp)
        self.assertEqual((transformations, containers), ({}, {}))

    def test_a_malformed_catalog_does_not_abort_the_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_catalog(root, "catalogs/transformations.yml", "{{{ not yaml")
            transformations, containers = load_transformation_catalog(str(root))
        self.assertEqual((transformations, containers), ({}, {}))

    def test_a_transformation_with_no_container_has_none(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_catalog(root, "catalogs/transformations.yml")
            transformations, _ = load_transformation_catalog(str(root))
        self.assertIsNone(transformations["installed_tool"]["container"])


# --------------------------------------------------------------------------------------
# Arguments live in the abstract workflow, not in the recorded argv.
# --------------------------------------------------------------------------------------

WORKFLOW = """
pegasus: 5.0.4
jobs:
- id: fetch_field1
  name: fetch_soil_data
  arguments: ['--fetch', '--polygon-id', 'field1', '--output', 'field1_soil_data.csv']
  uses:
  - {lfn: polygons.json, type: input}
  - {lfn: field1_soil_data.csv, type: output}
- id: analyze_field1
  name: analyze_moisture
  arguments: ['--input', {lfn: field1_soil_data.csv}, '--threshold', 0.5]
  uses:
  - {lfn: field1_soil_data.csv, type: input}
- id: bare
  name: no_args
  uses: []
"""


class TestWorkflowArguments(unittest.TestCase):
    """`invocation.argv` is empty for every compute job in these workflows — measured on the
    soilmoisture run — while the abstract workflow declares the real command line. A job
    re-run from argv alone gets no arguments and dies on its own usage message, which is
    exactly how this was found: the container ran, the code ran, and it printed
    `error: the following arguments are required: --polygons-file`.
    """

    def _load(self, body=WORKFLOW):
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, "workflow.yml").write_text(body)
            return load_workflow_uses(tmp)

    def test_arguments_are_parsed_per_abstract_job(self):
        uses = self._load()
        self.assertEqual(uses["fetch_field1"]["arguments"][:2], ["--fetch", "--polygon-id"])
        self.assertIn("field1_soil_data.csv", uses["fetch_field1"]["arguments"])

    def test_a_file_object_argument_renders_as_its_lfn(self):
        """The Pegasus API permits a File where a string would do; it round-trips through
        YAML as a mapping, and str() on it would put a dict repr on the command line."""
        uses = self._load()
        self.assertIn("field1_soil_data.csv", uses["analyze_field1"]["arguments"])
        self.assertFalse(any("{" in a for a in uses["analyze_field1"]["arguments"]))

    def test_non_string_scalars_survive(self):
        uses = self._load()
        self.assertIn("0.5", uses["analyze_field1"]["arguments"])

    def test_a_job_with_no_arguments_gets_an_empty_list(self):
        self.assertEqual(self._load()["bare"]["arguments"], [])

    def test_uses_still_parses_alongside(self):
        """The arguments were added to an existing loader; its original job must not regress."""
        uses = self._load()
        self.assertEqual(uses["fetch_field1"]["input"], ["polygons.json"])
        self.assertEqual(uses["fetch_field1"]["output"], ["field1_soil_data.csv"])


class TestClusteredJobs(unittest.TestCase):
    """A clustered Condor job bundles several tasks, which Pegasus runs as separate
    sequential invocations.

    There is no single (executable, argv) pair that describes one: the executable is taken
    from the first task, so merging every task's arguments would run task A's binary with
    A's and B's flags together, the later overriding the earlier. That executes, produces
    output, and is a job that never existed — the worst possible outcome for a comparison.
    """

    def test_a_clustered_job_refuses_rather_than_merging_command_lines(self):
        merged = ExecutionSpec.from_dict({
            "path": "/srv/a", "arguments": None,      # what the extractor now emits
            "container": {"name": "c", "kind": "docker", "image": "docker://x:1"}})
        self.assertFalse(merged.runnable())

    def test_merging_would_have_produced_a_contradictory_command_line(self):
        """Documents the shape of the bug so it is recognisable if it returns: two tasks
        chained by a file, concatenated, give one invocation with two --input flags."""
        a = ["--input", "a.csv", "--output", "b.json"]
        b = ["--input", "b.json", "--output", "c.json"]
        merged = a + b
        self.assertEqual(merged.count("--input"), 2)
        self.assertEqual(merged.count("--output"), 2)


# --------------------------------------------------------------------------------------
# What may and may not be executed.
# --------------------------------------------------------------------------------------

class TestRunnable(unittest.TestCase):
    def test_a_complete_spec_is_runnable(self):
        spec = ExecutionSpec.from_dict({
            "path": "/srv/analyze", "arguments": [], "pfn": "/wf/bin/analyze.py",
            "container": {"name": "c", "kind": "singularity", "image": "file:///wf/x.sif"}})
        self.assertTrue(spec.runnable())

    def test_no_path_is_not_runnable(self):
        self.assertFalse(ExecutionSpec.from_dict({"pfn": "/wf/bin/analyze.py"}).runnable())

    def test_unparseable_arguments_are_not_runnable(self):
        """`None` means the recorded argv could not be parsed. Treating it as "no arguments"
        would invoke a different command than the one being compared against."""
        self.assertFalse(
            ExecutionSpec.from_dict({"path": "/srv/x", "arguments": None}).runnable())

    def test_empty_arguments_are_runnable(self):
        """Most Pegasus jobs pass data by file, not by flag. Empty is not unknown."""
        self.assertTrue(
            ExecutionSpec.from_dict({"path": "/srv/x", "arguments": []}).runnable())

    def test_a_container_without_an_image_is_not_runnable(self):
        """Falling back to the host's own installation is exactly the variable the container
        exists to remove."""
        spec = ExecutionSpec.from_dict({
            "path": "/srv/x", "arguments": [], "container": {"name": "c", "kind": "docker"}})
        self.assertFalse(spec.runnable())

    def test_an_installed_transformation_needs_no_pfn(self):
        """`installed` code is already in the image; there is nothing to stage."""
        spec = ExecutionSpec.from_dict({
            "path": "/usr/bin/tool", "arguments": [], "pfn_type": "installed",
            "container": {"name": "c", "kind": "docker", "image": "docker://tool:1"}})
        self.assertTrue(spec.runnable())


# --------------------------------------------------------------------------------------
# Round trip. A spec that loses its refusal on the way through Redis is worse than no spec.
# --------------------------------------------------------------------------------------

class TestRoundTrip(unittest.TestCase):
    def test_spec_survives_serialization(self):
        raw = {"transformation": "analyze_moisture", "path": "/srv/analyze",
               "pfn": "/wf/bin/analyze.py", "pfn_type": "stageable",
               "container": {"name": "c", "kind": "singularity",
                             "image": "file:///wf/x.sif", "image_site": "local"}}
        spec = ExecutionSpec.from_dict(raw)
        back = ExecutionSpec.from_dict(spec.to_dict())
        self.assertEqual(back.path, "/srv/analyze")
        self.assertEqual(back.pfn, "/wf/bin/analyze.py")
        self.assertEqual(back.container.image, "file:///wf/x.sif")
        self.assertEqual(back.container.kind, "singularity")

    def test_empty_arguments_are_dropped_and_restored_as_empty(self):
        spec = ExecutionSpec.from_dict({"path": "/srv/x", "arguments": []})
        self.assertNotIn("arguments", spec.to_dict())
        self.assertEqual(ExecutionSpec.from_dict(spec.to_dict()).arguments, [])

    def test_unknown_arguments_survive_and_keep_refusing(self):
        """The one value that must not be normalised away in transit."""
        spec = ExecutionSpec.from_dict({"path": "/srv/x", "arguments": None})
        back = ExecutionSpec.from_dict(spec.to_dict())
        self.assertIsNone(back.arguments)
        self.assertFalse(back.runnable())

    def test_container_is_nested_not_flattened(self):
        spec = ExecutionSpec.from_dict({
            "path": "/srv/x", "container": {"name": "c", "kind": "docker",
                                            "image": "docker://x:1"}})
        self.assertIsInstance(spec.to_dict()["container"], dict)
        self.assertIsInstance(spec.container, ContainerSpec)

    def test_job_round_trips_the_spec(self):
        job = Job()
        job.from_dict({"id": "j1", "wall_time": 1.0,
                       "execution": {"path": "/srv/x", "pfn": "/wf/x.py",
                                     "container": {"name": "c", "kind": "docker",
                                                   "image": "docker://x:1"}}})
        self.assertTrue(job.execution.runnable())
        back = Job()
        back.from_dict(job.to_dict())
        self.assertEqual(back.execution.path, "/srv/x")
        self.assertEqual(back.execution.container.image, "docker://x:1")

    def test_a_job_without_execution_stays_none(self):
        """Every job converted before this existed, and every synthetic job, takes this path.
        Simulation must be completely untouched."""
        job = Job()
        job.from_dict({"id": "j1", "wall_time": 1.0})
        self.assertIsNone(job.execution)
        self.assertIsNone(job.to_dict()["execution"])


# --------------------------------------------------------------------------------------
# The converter boundary.
# --------------------------------------------------------------------------------------

class TestConverterPassthrough(unittest.TestCase):
    def test_a_profile_without_an_executable_gets_no_execution_block(self):
        """Profiles written before the extractor recorded these keys must convert exactly as
        they did, or the existing converted corpora become half-runnable."""
        self.assertIsNone(_map_execution({"transformation_db": "analyze"}))

    def test_fields_are_carried_across(self):
        out = _map_execution({
            "transformation_db": "analyze_moisture",
            "executable_db": "/srv/analyze_moisture",
            "argv_db": ["--verbose"],
            "pfn_db": "/wf/bin/analyze_moisture.py",
            "pfn_type_db": "stageable",
            "container_db": {"name": "soil", "type": "singularity",
                             "image": "file:///wf/x.sif", "image_site": "local"}})
        self.assertEqual(out["path"], "/srv/analyze_moisture")
        self.assertEqual(out["arguments"], ["--verbose"])
        self.assertEqual(out["pfn"], "/wf/bin/analyze_moisture.py")

    def test_the_catalogs_type_becomes_the_models_kind(self):
        out = _map_execution({"executable_db": "/srv/x",
                              "container_db": {"name": "c", "type": "docker",
                                               "image": "docker://x:1"}})
        self.assertEqual(out["container"]["kind"], "docker")
        self.assertNotIn("type", out["container"])

    def test_unknown_argv_is_not_turned_into_empty(self):
        """The whole point of the None/[] distinction, at the boundary where it would be
        easiest to erase."""
        out = _map_execution({"executable_db": "/srv/x", "argv_db": None})
        self.assertIsNone(out["arguments"])
        self.assertFalse(ExecutionSpec.from_dict(out).runnable())


if __name__ == "__main__":
    unittest.main()
