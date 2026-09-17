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
    cluster_task_ids, load_transformation_catalog, load_workflow_uses, ordered_task_ids,
)
from pegasus_to_swarm_converter import (  # noqa: E402
    _map_execution, bundle_payload, clear_previous_output, rewrite_job_for_bundle,
)
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

    def test_clustering_is_detected_from_the_db_not_the_workflow_map(self):
        """The signal must not depend on workflow.yml. Counting the ids that resolved
        against the abstract workflow FAILS OPEN: with no workflow.yml that list is empty
        for every job, so a cluster is not recognised and executes the first task's argv as
        though it described the whole thing. Missing metadata means we know less about a
        job, which can only make running it less safe."""
        # (transformation, abs_task_id, executable, argv) — as selected from `invocation`.
        main_tasks = [("analyze", "t1", "/srv/a", ""), ("merge", "t2", "/srv/b", "")]
        self.assertEqual(len(cluster_task_ids(main_tasks)), 2)

    def test_a_single_task_job_is_not_clustered(self):
        self.assertEqual(len(cluster_task_ids([("analyze", "t1", "/srv/a", "")])), 1)

    def test_repeated_rows_for_one_task_are_not_a_cluster(self):
        self.assertEqual(
            len(cluster_task_ids([("a", "t1", "/srv/a", ""), ("a", "t1", "/srv/a", "")])), 1)

    def test_rows_without_a_task_id_are_ignored(self):
        self.assertEqual(len(cluster_task_ids([("a", None, "/srv/a", ""),
                                               ("a", "", "/srv/a", "")])), 0)

    def test_duplicate_invocation_rows_do_not_double_the_command_line(self):
        """One job instance can carry several invocation rows for the same abstract task.
        Arguments are accumulated per entry and deliberately not de-duplicated (order
        matters, repeats are legitimate), so a duplicate row would double the command line
        — and because it is still ONE distinct task, nothing flags it as a cluster and it
        runs. De-duplication has to happen at the id list."""
        rows = [("analyze", "t1", "/srv/a", ""), ("analyze", "t1", "/srv/a", "")]
        self.assertEqual(ordered_task_ids(rows), ["t1"])
        self.assertEqual(len(cluster_task_ids(rows)), 1)   # correctly not a cluster

    def test_invocation_order_is_preserved(self):
        """For a real cluster this is the order Pegasus ran the tasks in, and the only
        ordering information there is."""
        rows = [("b", "t2", "/srv/b", ""), ("a", "t1", "/srv/a", "")]
        self.assertEqual(ordered_task_ids(rows), ["t2", "t1"])

    def test_ids_absent_from_the_workflow_map_are_dropped_when_asked(self):
        rows = [("a", "t1", "/srv/a", ""), ("b", "t2", "/srv/b", "")]
        self.assertEqual(ordered_task_ids(rows, known={"t1"}), ["t1"])

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


# --------------------------------------------------------------------------------------
# Self-contained bundles: copy the directory, run the jobs.
# --------------------------------------------------------------------------------------

class TestBundling(unittest.TestCase):
    def _fixture(self, tmp):
        """A tiny workflow tree plus the (job, profile) pairs a conversion would produce."""
        src = os.path.join(tmp, "wf")
        os.makedirs(os.path.join(src, "bin"))
        Path(src, "bin", "analyze.py").write_text("#!/usr/bin/env python3\n")
        Path(src, "bin", "train.py").write_text("#!/usr/bin/env python3\n")
        Path(src, "seed.json").write_text("{}")
        pairs = []
        for name, script_name in (("analyze", "analyze.py"), ("train", "train.py")):
            job = {"id": f"j-{name}",
                   "execution": {"transformation": name, "path": f"/srv/{name}",
                                 "pfn": os.path.join(src, "bin", script_name),
                                 "arguments": [],
                                 "container": {"name": "c", "kind": "singularity",
                                               "image": "file:///nowhere/x.sif"}}}
            profile = {"transformation_db": name,
                       "replicas_db": {"seed.json": os.path.join(src, "seed.json")}}
            pairs.append((job, profile))
        return src, pairs

    def test_executables_and_root_inputs_are_copied_in(self):
        with tempfile.TemporaryDirectory() as tmp:
            src, pairs = self._fixture(tmp)
            out = os.path.join(tmp, "out"); os.makedirs(out)
            manifest = bundle_payload(pairs, out)
            self.assertTrue(os.path.isfile(os.path.join(out, "code", "analyze", "analyze.py")))
            self.assertTrue(os.path.isfile(os.path.join(out, "inputs", "seed.json")))
            self.assertEqual(manifest["missing"], [])

    def test_the_job_points_at_the_code_root_not_the_bundle(self):
        """`roots.code` already names code/, so a bundle-relative pfn puts it in the path
        twice and every job refuses. Caught end to end, not by construction."""
        with tempfile.TemporaryDirectory() as tmp:
            src, pairs = self._fixture(tmp)
            out = os.path.join(tmp, "out"); os.makedirs(out)
            manifest = bundle_payload(pairs, out)
            job = rewrite_job_for_bundle(pairs[0][0], manifest)
            self.assertEqual(job["execution"]["pfn"], os.path.join("analyze", "analyze.py"))
            self.assertFalse(job["execution"]["pfn"].startswith("code"))

    def test_one_copy_per_transformation_not_per_job(self):
        """A workflow runs the same executable many times; the DAG's shape must not decide
        how many copies a bundle carries."""
        with tempfile.TemporaryDirectory() as tmp:
            src, pairs = self._fixture(tmp)
            pairs = pairs + [pairs[0]] * 5          # same transformation, many jobs
            out = os.path.join(tmp, "out"); os.makedirs(out)
            manifest = bundle_payload(pairs, out)
            self.assertEqual(len(manifest["code"]), 2)
            self.assertEqual(len(os.listdir(os.path.join(out, "code", "analyze"))), 1)

    def test_same_basename_in_two_transformations_does_not_collide(self):
        """Two transformations may both ship a run.py; a flat directory silently keeps one."""
        with tempfile.TemporaryDirectory() as tmp:
            src = os.path.join(tmp, "wf")
            os.makedirs(os.path.join(src, "a")); os.makedirs(os.path.join(src, "b"))
            Path(src, "a", "run.py").write_text("A")
            Path(src, "b", "run.py").write_text("B")
            pairs = [({"execution": {"transformation": t, "pfn": os.path.join(src, d, "run.py")}},
                      {}) for t, d in (("ta", "a"), ("tb", "b"))]
            out = os.path.join(tmp, "out"); os.makedirs(out)
            bundle_payload(pairs, out)
            self.assertEqual(Path(out, "code", "ta", "run.py").read_text(), "A")
            self.assertEqual(Path(out, "code", "tb", "run.py").read_text(), "B")

    def test_a_missing_executable_is_reported_not_silently_dropped(self):
        """A bundle that looks whole and fails per job, later, on whichever agent draws it,
        is the worst way to find out."""
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, "out"); os.makedirs(out)
            pairs = [({"execution": {"transformation": "gone", "pfn": "/nowhere/x.py"}}, {})]
            manifest = bundle_payload(pairs, out)
            self.assertEqual(len(manifest["missing"]), 1)
            self.assertEqual(manifest["missing"][0]["kind"], "code")

    def test_a_job_that_could_not_be_bundled_keeps_its_original_path(self):
        """Still a faithful description; rewriting it to a path that does not exist is worse."""
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, "out"); os.makedirs(out)
            job = {"execution": {"transformation": "gone", "pfn": "/nowhere/x.py"}}
            manifest = bundle_payload([(job, {})], out)
            self.assertEqual(rewrite_job_for_bundle(job, manifest)["execution"]["pfn"],
                             "/nowhere/x.py")

    def test_images_are_referenced_not_copied_by_default(self):
        with tempfile.TemporaryDirectory() as tmp:
            src, pairs = self._fixture(tmp)
            out = os.path.join(tmp, "out"); os.makedirs(out)
            manifest = bundle_payload(pairs, out)
            self.assertIn("c", manifest["images"])
            self.assertIsNone(manifest["images"]["c"]["bundled"])
            self.assertFalse(os.path.exists(os.path.join(out, "images")))

    def test_every_copied_file_is_checksummed(self):
        """A path says where code was, not which code it was: edit a script after a run and
        a replayed job silently executes a different program than the baseline's."""
        with tempfile.TemporaryDirectory() as tmp:
            src, pairs = self._fixture(tmp)
            out = os.path.join(tmp, "out"); os.makedirs(out)
            manifest = bundle_payload(pairs, out)
            for entry in list(manifest["code"].values()) + list(manifest["inputs"].values()):
                self.assertEqual(len(entry["sha256"]), 64)

    def test_a_transformation_name_cannot_write_outside_the_bundle(self):
        """A transformation name is workflow-supplied and becomes a directory name, so it is
        untrusted input on a WRITE path: `../../` escapes the output directory and the
        bundler then creates directories and copies files there."""
        with tempfile.TemporaryDirectory() as tmp:
            src = os.path.join(tmp, "wf"); os.makedirs(src)
            Path(src, "x.py").write_text("payload")
            out = os.path.join(tmp, "out"); os.makedirs(out)
            escape = os.path.join(tmp, "ESCAPED")
            pairs = [({"execution": {"transformation": "../../ESCAPED/pwn",
                                     "pfn": os.path.join(src, "x.py")}}, {})]
            bundle_payload(pairs, out)
            self.assertFalse(os.path.exists(escape), "wrote outside the bundle")
            written = [p for p, _d, f in os.walk(out) for _ in f]
            for path, _dirs, files in os.walk(out):
                for f in files:
                    self.assertTrue(os.path.realpath(os.path.join(path, f))
                                    .startswith(os.path.realpath(out)))

    def test_two_workflows_sharing_a_transformation_name_do_not_share_code(self):
        """Converting several runs at once can put two different `process` executables in one
        bundle. Keying by name bundled whichever came first and handed it to both."""
        with tempfile.TemporaryDirectory() as tmp:
            a = os.path.join(tmp, "a"); os.makedirs(a)
            b = os.path.join(tmp, "b"); os.makedirs(b)
            Path(a, "process.py").write_text("WORKFLOW-A")
            Path(b, "process.py").write_text("WORKFLOW-B")
            out = os.path.join(tmp, "out"); os.makedirs(out)
            job_a = {"execution": {"transformation": "process",
                                   "pfn": os.path.join(a, "process.py")}}
            job_b = {"execution": {"transformation": "process",
                                   "pfn": os.path.join(b, "process.py")}}
            manifest = bundle_payload([(job_a, {}), (job_b, {})], out)
            self.assertEqual(len(manifest["code"]), 2)
            rewrite_job_for_bundle(job_a, manifest)
            rewrite_job_for_bundle(job_b, manifest)
            read = lambda j: Path(out, "code", j["execution"]["pfn"]).read_text()
            self.assertEqual(read(job_a), "WORKFLOW-A")
            self.assertEqual(read(job_b), "WORKFLOW-B")

    def test_a_unique_transformation_keeps_a_readable_directory_name(self):
        """Disambiguation only where it is needed; the common case stays legible."""
        with tempfile.TemporaryDirectory() as tmp:
            src, pairs = self._fixture(tmp)
            out = os.path.join(tmp, "out"); os.makedirs(out)
            bundle_payload(pairs, out)
            self.assertTrue(os.path.isdir(os.path.join(out, "code", "analyze")))

    def test_source_root_is_a_prefix_map_not_a_basename_search(self):
        """The first version tried shorter and shorter suffixes and took the first that
        existed — matching on BASENAME at its last step, so two workflows' `process.py`
        resolved to the same file. Distinct manifest keys, identical wrong content, which
        silently undoes keying by pfn."""
        with tempfile.TemporaryDirectory() as tmp:
            root = os.path.join(tmp, "src"); os.makedirs(root)
            Path(root, "process.py").write_text("THE-WRONG-ONE")
            out = os.path.join(tmp, "out"); os.makedirs(out)
            pairs = [({"execution": {"transformation": "ta", "pfn": "/wf-a/bin/process.py"}}, {}),
                     ({"execution": {"transformation": "tb", "pfn": "/wf-b/bin/process.py"}}, {})]
            manifest = bundle_payload(pairs, out, source_root=root)
            self.assertEqual(manifest["code"], {})
            self.assertEqual(len(manifest["missing"]), 2)

    def test_source_root_maps_a_real_tree(self):
        """The case it exists for: paths spread over more than one directory, so their
        common parent is the workflow root — which is what soilmoisture looks like."""
        with tempfile.TemporaryDirectory() as tmp:
            local = os.path.join(tmp, "local")
            os.makedirs(os.path.join(local, "bin"))
            Path(local, "bin", "analyze.py").write_text("RIGHT")
            Path(local, "fetch.py").write_text("ALSO-RIGHT")
            out = os.path.join(tmp, "out"); os.makedirs(out)
            pairs = [({"execution": {"transformation": "analyze",
                                     "pfn": "/submit/wf/bin/analyze.py"}}, {}),
                     ({"execution": {"transformation": "fetch",
                                     "pfn": "/submit/wf/fetch.py"}}, {})]
            manifest = bundle_payload(pairs, out, source_root=local)
            self.assertEqual(len(manifest["code"]), 2)
            self.assertEqual(Path(out, "code", "analyze", "analyze.py").read_text(), "RIGHT")

    def test_a_single_directory_is_ambiguous_and_says_so(self):
        """With one source directory the common parent IS that directory, so there is no way
        to tell whether the root stands for it or for its parent. Rather than guess, the
        mapping is deterministic and the refusal names the explicit form."""
        with tempfile.TemporaryDirectory() as tmp:
            local = os.path.join(tmp, "local", "bin"); os.makedirs(local)
            Path(local, "analyze.py").write_text("RIGHT")
            out = os.path.join(tmp, "out"); os.makedirs(out)
            pairs = [({"execution": {"transformation": "analyze",
                                     "pfn": "/submit/wf/bin/analyze.py"}}, {})]
            manifest = bundle_payload(pairs, out,
                                      source_root=os.path.join(tmp, "local"))
            self.assertEqual(manifest["code"], {})
            self.assertIn("OLD=NEW", manifest["missing"][0]["reason"])
            # and the explicit form resolves it
            manifest = bundle_payload(pairs, out,
                                      source_root=f"/submit/wf={os.path.join(tmp, 'local')}")
            self.assertEqual(len(manifest["code"]), 1)

    def test_an_explicit_old_equals_new_mapping_works(self):
        with tempfile.TemporaryDirectory() as tmp:
            local = os.path.join(tmp, "here"); os.makedirs(local)
            Path(local, "x.py").write_text("RIGHT")
            out = os.path.join(tmp, "out"); os.makedirs(out)
            pairs = [({"execution": {"transformation": "t", "pfn": "/there/x.py"}}, {})]
            manifest = bundle_payload(pairs, out, source_root=f"/there={local}")
            self.assertEqual(len(manifest["code"]), 1)

    def test_a_dot_leading_component_is_not_mangled_into_another_path(self):
        """`lstrip("./")` strips a CHARACTER SET, not a prefix, so `.venv/bin/tool` became
        `venv/bin/tool` — a different path. If something existed there the bundler copied a
        completely unrelated file while the manifest recorded the original source: the
        failure looks correct and is wrong."""
        with tempfile.TemporaryDirectory() as tmp:
            root = os.path.join(tmp, "local")
            os.makedirs(os.path.join(root, "venv", "bin"))
            os.makedirs(os.path.join(root, "bin"))
            Path(root, "venv", "bin", "tool").write_text("DECOY")
            Path(root, "bin", "other.py").write_text("x")
            out = os.path.join(tmp, "out"); os.makedirs(out)
            pairs = [({"execution": {"transformation": "t1",
                                     "pfn": "/wf/.venv/bin/tool"}}, {}),
                     ({"execution": {"transformation": "t2",
                                     "pfn": "/wf/bin/other.py"}}, {})]
            manifest = bundle_payload(pairs, out, source_root=root)
            self.assertNotIn("/wf/.venv/bin/tool", manifest["code"])
            self.assertIn("/wf/.venv/bin/tool",
                          [m["path"] for m in manifest["missing"]])
            self.assertIn("/wf/bin/other.py", manifest["code"])

    def test_a_dot_directory_that_really_exists_resolves(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = os.path.join(tmp, "local")
            os.makedirs(os.path.join(root, ".venv", "bin"))
            os.makedirs(os.path.join(root, "bin"))
            Path(root, ".venv", "bin", "tool").write_text("RIGHT")
            Path(root, "bin", "other.py").write_text("x")
            out = os.path.join(tmp, "out"); os.makedirs(out)
            pairs = [({"execution": {"transformation": "t1",
                                     "pfn": "/wf/.venv/bin/tool"}}, {}),
                     ({"execution": {"transformation": "t2",
                                     "pfn": "/wf/bin/other.py"}}, {})]
            manifest = bundle_payload(pairs, out, source_root=root)
            entry = manifest["code"]["/wf/.venv/bin/tool"]
            self.assertEqual(Path(out, entry["bundled"]).read_text(), "RIGHT")

    def test_an_explicit_mapping_beats_a_file_at_the_submit_host_path(self):
        """Checking the original path first meant an incidental file at the submit-host
        location won over the tree the caller explicitly named — so converting on any
        machine that happens to have `/home/ubuntu/...` bundled that instead."""
        with tempfile.TemporaryDirectory() as tmp:
            submit = os.path.join(tmp, "submit", "wf", "bin"); os.makedirs(submit)
            Path(submit, "a.py").write_text("STALE-LOCAL-COPY")
            local = os.path.join(tmp, "local", "bin"); os.makedirs(local)
            Path(local, "a.py").write_text("ASKED-FOR")
            out = os.path.join(tmp, "out"); os.makedirs(out)
            pfn = os.path.join(submit, "a.py")
            manifest = bundle_payload(
                [({"execution": {"transformation": "t", "pfn": pfn}}, {})], out,
                source_root=f"{os.path.join(tmp, 'submit', 'wf')}={os.path.join(tmp, 'local')}")
            self.assertEqual(Path(out, manifest["code"][pfn]["bundled"]).read_text(),
                             "ASKED-FOR")

    def test_without_a_mapping_the_recorded_path_is_still_used(self):
        """The common case: converting on the submit host, where the paths resolve."""
        with tempfile.TemporaryDirectory() as tmp:
            d = os.path.join(tmp, "wf", "bin"); os.makedirs(d)
            Path(d, "a.py").write_text("LOCAL")
            out = os.path.join(tmp, "out"); os.makedirs(out)
            manifest = bundle_payload(
                [({"execution": {"transformation": "t",
                                 "pfn": os.path.join(d, "a.py")}}, {})], out)
            self.assertEqual(len(manifest["code"]), 1)

    def test_the_bare_root_anchor_covers_images_too(self):
        """Leaving images out of the anchor made it too DEEP as well as missing them: with
        all code in `/wf/bin` the common parent was `/wf/bin`, so the image fell outside it
        and the code resolved one directory too high."""
        with tempfile.TemporaryDirectory() as tmp:
            local = os.path.join(tmp, "local")
            os.makedirs(os.path.join(local, "bin"))
            os.makedirs(os.path.join(local, "Apptainer"))
            for n in ("a.py", "b.py"):
                Path(local, "bin", n).write_text("code")
            Path(local, "Apptainer", "x.sif").write_text("IMAGE")
            out = os.path.join(tmp, "out"); os.makedirs(out)
            pairs = [({"execution": {
                "transformation": f"t{i}", "pfn": f"/wf/bin/{n}",
                "container": {"name": "c", "kind": "singularity",
                              "image": "file:///wf/Apptainer/x.sif"}}}, {})
                for i, n in enumerate(("a.py", "b.py"))]
            manifest = bundle_payload(pairs, out, source_root=local)
            self.assertEqual(len(manifest["code"]), 2)
            self.assertIsNotNone(manifest["images"]["c"]["sha256"])

    def test_two_inputs_sharing_a_basename_are_reported_not_overwritten(self):
        """The working directory is flat so both genuinely cannot be staged. Copying the
        second over the first while claiming both are bundled hands a job the wrong file."""
        with tempfile.TemporaryDirectory() as tmp:
            a = os.path.join(tmp, "a"); os.makedirs(a)
            b = os.path.join(tmp, "b"); os.makedirs(b)
            Path(a, "data.csv").write_text("FROM-A")
            Path(b, "data.csv").write_text("FROM-B")
            out = os.path.join(tmp, "out"); os.makedirs(out)
            profile = {"replicas_db": {"a/data.csv": os.path.join(a, "data.csv"),
                                       "b/data.csv": os.path.join(b, "data.csv")}}
            manifest = bundle_payload([({"id": "j"}, profile)], out)
            self.assertEqual(len(manifest["inputs"]), 1)
            self.assertEqual(len(manifest["missing"]), 1)
            self.assertIn("collides", manifest["missing"][0]["reason"])
            self.assertEqual(Path(out, "inputs", "data.csv").read_text(), "FROM-A")

    def test_the_same_file_listed_twice_is_not_a_collision(self):
        with tempfile.TemporaryDirectory() as tmp:
            a = os.path.join(tmp, "a"); os.makedirs(a)
            Path(a, "data.csv").write_text("X")
            out = os.path.join(tmp, "out"); os.makedirs(out)
            profile = {"replicas_db": {"data.csv": os.path.join(a, "data.csv"),
                                       "also/data.csv": os.path.join(a, "data.csv")}}
            manifest = bundle_payload([({"id": "j"}, profile)], out)
            self.assertEqual(manifest["missing"], [])

    def test_a_previous_conversions_jobs_do_not_survive(self):
        """Writing a new set of job files does not replace the old one. A 4-job workflow
        written over a 400-job synthetic run left 406 files; the distributor pushed all of
        them and the agents spent the run scheduling a campaign that had finished days
        earlier. Nothing said so — the run simply looked busy."""
        with tempfile.TemporaryDirectory() as tmp:
            for i in range(1, 21):
                Path(tmp, f"job_{i}.json").write_text("{}")
            Path(tmp, "conversion_summary.json").write_text("{}")
            os.makedirs(os.path.join(tmp, "code", "old"))
            Path(tmp, "code", "old", "old.py").write_text("STALE")
            Path(tmp, "keep_me.txt").write_text("not ours")
            removed = clear_previous_output(tmp)
            self.assertGreaterEqual(removed, 21)
            self.assertEqual([f for f in os.listdir(tmp) if f.startswith("job_")], [])
            self.assertFalse(os.path.exists(os.path.join(tmp, "code")))
            # Files the converter does not own are left alone.
            self.assertTrue(os.path.exists(os.path.join(tmp, "keep_me.txt")))

    def test_a_failed_conversion_leaves_the_previous_bundle_intact(self):
        """Clearing first destroyed a working bundle whenever the conversion after it
        raised, leaving neither the new one nor the old — and a bundle is the thing you
        copy to a fleet and run. The payload is staged and only promoted once everything
        that can fail has succeeded."""
        import pegasus_to_swarm_converter as conv
        from unittest.mock import patch as _patch
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, "out")
            os.makedirs(os.path.join(out, "code", "good"))
            Path(out, "code", "good", "g.py").write_text("PREVIOUS")
            Path(out, "job_1.json").write_text('{"id": "previous"}')

            real = conv.bundle_payload

            def boom(*a, **kw):
                real(*a, **kw)          # builds into staging
                raise RuntimeError("failure after the old destructive point")

            profiles = os.path.join(tmp, "profiles.json")
            Path(profiles).write_text(json.dumps([{
                "run_name": "r", "job_name": "j", "transformation_db": "t",
                "kickstart_sec_stats": 1.0, "exitcode_db": 0,
                "input_files_db": [], "output_files_db": [],
                "request_memory_mb_db": 1024, "request_cpus_db": 1,
            }]))
            with _patch.object(conv, "bundle_payload", side_effect=boom):
                with self.assertRaises(RuntimeError):
                    conv.convert_pegasus_profiles(
                        input_path=profiles, input_type="json", output_dir=out)
            # Previous bundle untouched. Staging debris may remain — it is swept by the
            # next successful conversion, and leaving it is strictly better than damaging
            # the only working copy of the output.
            self.assertEqual(Path(out, "code", "good", "g.py").read_text(), "PREVIOUS")
            self.assertTrue(os.path.exists(os.path.join(out, "job_1.json")))

    def test_a_failure_while_writing_job_files_also_leaves_the_bundle_intact(self):
        """Staging only the payload moved the hole one step along rather than closing it:
        the job files were still written straight to the output directory, after the
        previous one had been cleared. Everything is staged now, so the replacement is the
        last thing that happens."""
        import pegasus_to_swarm_converter as conv
        from unittest.mock import patch as _patch
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, "out")
            os.makedirs(os.path.join(out, "code", "good"))
            Path(out, "code", "good", "g.py").write_text("PREVIOUS")
            Path(out, "job_1.json").write_text('{"id": "previous"}')
            profiles = os.path.join(tmp, "profiles.json")
            Path(profiles).write_text(json.dumps([{
                "run_name": "r", "job_name": "j", "transformation_db": "t",
                "kickstart_sec_stats": 1.0, "exitcode_db": 0,
                "input_files_db": [], "output_files_db": [],
                "request_memory_mb_db": 1024, "request_cpus_db": 1,
            }]))
            real_dump = json.dump

            def fail_on_job_file(obj, fh, **kw):
                if getattr(fh, "name", "").endswith(".json") and "job_" in getattr(fh, "name", ""):
                    raise OSError("no space left on device")
                return real_dump(obj, fh, **kw)

            with _patch.object(conv.json, "dump", side_effect=fail_on_job_file):
                with self.assertRaises(OSError):
                    conv.convert_pegasus_profiles(
                        input_path=profiles, input_type="json", output_dir=out)
            self.assertEqual(Path(out, "code", "good", "g.py").read_text(), "PREVIOUS")
            self.assertTrue(os.path.exists(os.path.join(out, "job_1.json")))

    def test_a_failure_during_promotion_itself_rolls_back(self):
        """The window that survived two previous fixes. Promotion moves several entries and
        can fail between them, so clearing the old output before it — at the start, or just
        before promoting — still left neither the new output nor the old. Nothing is removed
        until the replacement is installed, each entry displaces its predecessor atomically,
        and a failure puts everything back."""
        import shutil as _shutil
        import pegasus_to_swarm_converter as conv
        from unittest.mock import patch as _patch
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, "out")
            os.makedirs(os.path.join(out, "code", "good"))
            Path(out, "code", "good", "g.py").write_text("PREVIOUS")
            Path(out, "job_1.json").write_text('{"id": "previous"}')
            Path(out, "job_99.json").write_text('{"id": "surplus"}')
            src = os.path.join(tmp, "wf"); os.makedirs(src)
            Path(src, "a.py").write_text("code")
            profiles = os.path.join(tmp, "profiles.json")
            Path(profiles).write_text(json.dumps([{
                "run_name": "r", "job_name": "j", "transformation_db": "t",
                "kickstart_sec_stats": 1.0, "exitcode_db": 0,
                "input_files_db": [], "output_files_db": [],
                "request_memory_mb_db": 1024, "request_cpus_db": 1,
            }]))
            real_move = _shutil.move
            calls = {"n": 0}

            def flaky(s, d, *a, **k):
                calls["n"] += 1
                if calls["n"] == 2:
                    raise OSError("no space left on device")
                return real_move(s, d, *a, **k)

            with _patch.object(conv.shutil, "move", side_effect=flaky):
                with self.assertRaises(OSError):
                    conv.convert_pegasus_profiles(
                        input_path=profiles, input_type="json", output_dir=out)
            self.assertEqual(Path(out, "code", "good", "g.py").read_text(), "PREVIOUS")
            self.assertTrue(os.path.exists(os.path.join(out, "job_1.json")))
            self.assertTrue(os.path.exists(os.path.join(out, "job_99.json")))
            self.assertEqual([d for d in os.listdir(out) if ".replacing-" in d], [])

    def _promote_fixture(self, tmp):
        out = os.path.join(tmp, "out"); os.makedirs(out)
        stag = os.path.join(tmp, "stag"); os.makedirs(stag)
        for n in ("a.json", "b.json"):
            Path(stag, n).write_text("NEW")
            Path(out, n).write_text("OLD")
        return out, stag

    def test_an_interrupt_during_promotion_still_rolls_back(self):
        """`except Exception` does not catch KeyboardInterrupt, and an interrupt is the
        likeliest way a long conversion stops. It walked out with the output half installed
        and the previous copies stranded as `.replacing-*`."""
        import shutil as _shutil
        import pegasus_to_swarm_converter as conv
        from unittest.mock import patch as _patch
        with tempfile.TemporaryDirectory() as tmp:
            out, stag = self._promote_fixture(tmp)
            real = _shutil.move
            calls = {"n": 0}

            def interrupt(s, d, *a, **k):
                calls["n"] += 1
                if calls["n"] == 2:
                    raise KeyboardInterrupt()
                return real(s, d, *a, **k)

            with _patch.object(conv.shutil, "move", side_effect=interrupt):
                with self.assertRaises(KeyboardInterrupt):
                    conv.promote_staged_output(stag, out)
            self.assertEqual(Path(out, "a.json").read_text(), "OLD")
            self.assertEqual([n for n in os.listdir(out) if ".replacing-" in n], [])

    def test_a_failure_during_rollback_neither_stops_it_nor_hides_the_cause(self):
        """A rollback that aborts on its first problem leaves the half-state it exists to
        prevent, and one that raises replaces the real cause with its own."""
        import shutil as _shutil
        import pegasus_to_swarm_converter as conv
        from unittest.mock import patch as _patch
        with tempfile.TemporaryDirectory() as tmp:
            out, stag = self._promote_fixture(tmp)
            real = _shutil.move
            calls = {"n": 0}

            def boom(s, d, *a, **k):
                calls["n"] += 1
                if calls["n"] == 2:
                    raise OSError("primary failure")
                return real(s, d, *a, **k)

            with _patch.object(conv.shutil, "move", side_effect=boom), \
                 _patch.object(conv.os, "remove", side_effect=OSError("cleanup failed")):
                with self.assertRaises(OSError) as caught:
                    conv.promote_staged_output(stag, out)
            self.assertIn("primary failure", str(caught.exception))
            self.assertEqual(Path(out, "a.json").read_text(), "OLD")
            self.assertEqual([n for n in os.listdir(out) if ".replacing-" in n], [])

    def test_the_sweep_never_touches_files_it_does_not_own(self):
        """The output directory is shared with whatever else the user keeps there, and this
        function deletes things. A substring test for ".replacing-" removed a user's
        `notes.replacing-the-old-plan.txt`, `data.replacing-v2.csv` and an
        `experiments.replacing-baseline/` directory — silently."""
        with tempfile.TemporaryDirectory() as tmp:
            user_files = ["notes.replacing-the-old-plan.txt", "data.replacing-v2.csv",
                          "keep_me.txt",
                          # owned BASE name but not a pid suffix: still the user's
                          "code.replacing-notes.txt"]
            for n in user_files:
                Path(tmp, n).write_text("USER DATA")
            os.makedirs(os.path.join(tmp, "experiments.replacing-baseline"))
            os.makedirs(os.path.join(tmp, ".convert-staging-notes"))
            clear_previous_output(tmp)
            for n in user_files:
                self.assertTrue(os.path.exists(os.path.join(tmp, n)), f"deleted {n}")
            self.assertTrue(os.path.isdir(os.path.join(tmp, "experiments.replacing-baseline")))
            self.assertTrue(os.path.isdir(os.path.join(tmp, ".convert-staging-notes")))

    def test_the_sweep_does_remove_what_it_owns(self):
        """The other half: the guard must not be so strict that debris survives."""
        with tempfile.TemporaryDirectory() as tmp:
            owned = ["job_1.json", "manifest.json", "code.replacing-9999",
                     "inputs.replacing-123"]
            for n in owned:
                Path(tmp, n).write_text("owned")
            os.makedirs(os.path.join(tmp, ".convert-staging-4242"))
            clear_previous_output(tmp)
            for n in owned + [".convert-staging-4242"]:
                self.assertFalse(os.path.exists(os.path.join(tmp, n)), f"kept {n}")

    def test_stranded_displaced_copies_are_swept(self):
        """Nothing else ever removed these, so a rollback that could not finish left debris
        accumulating in the output directory forever."""
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, "code.replacing-9999").write_text("stranded")
            os.makedirs(os.path.join(tmp, "inputs.replacing-9999"))
            clear_previous_output(tmp)
            self.assertEqual([n for n in os.listdir(tmp) if ".replacing-" in n], [])

    def test_the_sweep_never_removes_what_was_just_installed(self):
        """The sweep runs AFTER promotion now, so it has to tell the new output from the
        surplus it exists to remove."""
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, "job_1.json").write_text("new")
            Path(tmp, "job_2.json").write_text("new")
            Path(tmp, "job_50.json").write_text("surplus")
            clear_previous_output(tmp, installed={"job_1.json", "job_2.json"})
            remaining = sorted(f for f in os.listdir(tmp) if f.startswith("job_"))
            self.assertEqual(remaining, ["job_1.json", "job_2.json"])

    def test_staging_debris_is_swept_by_the_next_conversion(self):
        with tempfile.TemporaryDirectory() as tmp:
            os.makedirs(os.path.join(tmp, ".convert-staging-99999"))
            Path(tmp, ".convert-staging-99999", "leftover").write_text("x")
            clear_previous_output(tmp)
            self.assertEqual([d for d in os.listdir(tmp)
                              if d.startswith(".convert-staging")], [])

    def test_clearing_an_empty_or_missing_directory_is_harmless(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(clear_previous_output(tmp), 0)
            self.assertEqual(clear_previous_output(os.path.join(tmp, "nope")), 0)

    def test_a_published_job_keeps_the_fields_that_make_it_a_workflow(self):
        """job_distributor built a Job field by field and dropped everything added since:
        data_predicate (DAG gating), should_fail (failure replay) and execution (real
        execution). A converted workflow reached Redis without the halves that make it one,
        and the run looked healthy. Pinned here because the loss is silent."""
        import job_distributor  # noqa: F401 - import is part of the assertion
        from swarm.models.job import Job as _Job
        record = {
            "id": "j1", "wall_time": 1.0,
            "capacities": {"core": 1.0, "ram": 1.0, "disk": 1.0},
            "data_in": [{"name": "local", "file": "in.csv"}],
            "data_out": [{"name": "local", "file": "out.csv"}],
            "should_fail": True,
            "data_predicate": {"kind": "files", "files": ["in.csv"]},
            "execution": {"transformation": "t", "path": "/srv/t", "pfn": "t/t.py",
                          "arguments": ["--x"]},
        }
        job = _Job()
        job.from_dict(record)
        self.assertIsNotNone(job.execution, "execution dropped")
        self.assertEqual(job.data_predicate, {"kind": "files", "files": ["in.csv"]})
        self.assertTrue(job.to_dict()['should_fail'])
        self.assertEqual([n.file for n in job.data_in], ["in.csv"])
        # and it survives the trip through Redis serialisation
        back = _Job()
        back.from_dict(job.to_dict())
        self.assertIsNotNone(back.execution)
        self.assertEqual(back.execution.arguments, ["--x"])

    def test_a_simulated_job_bundles_cleanly_with_nothing_to_copy(self):
        """Jobs with no execution block are the default case and must still convert."""
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, "out"); os.makedirs(out)
            manifest = bundle_payload([({"id": "j1", "wall_time": 1.0}, {})], out)
            self.assertEqual(manifest["missing"], [])
            self.assertEqual(manifest["code"], {})


if __name__ == "__main__":
    unittest.main()
