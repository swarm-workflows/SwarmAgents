"""merge_workflow_jobs.py: a real workflow bundle merged into a simulated set (E1′ mixed rung, T-6).

The simulated replay set duplicates recorded Pegasus runs, including soilmoisture's, so it shares
file names with the real soilmoisture bundle (6 on the slice, 2026-11-09) and with itself. In a
run that executes, `run_test.py` refuses any shared name. The merge renames only the simulated
jobs' file names, which the scheduler never prices, and leaves the real jobs untouched.
"""
import argparse
import json
import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

import merge_workflow_jobs as mw  # noqa: E402
import run_test  # noqa: E402


def _dn(f, size=10, node="local"):
    return {"name": node, "file": f, "size_bytes": size}


def _write(d, name, obj):
    d.mkdir(parents=True, exist_ok=True)
    (d / name).write_text(json.dumps(obj))


def _fixture(tmp_path):
    syn = tmp_path / "jobs_3"
    # Two replayed soilmoisture-like jobs sharing an output name with each other AND with the
    # real bundle — the shape found on the slice — plus an ordinary one.
    _write(syn, "job_1.json", {"id": 1, "wall_time": 3.0, "capacities": {"core": 1},
                               "data_in": [_dn("field1_soil_data.csv", 7, "dtn3")],
                               "data_out": [_dn("field1_analysis.json")]})
    _write(syn, "job_2.json", {"id": 2, "wall_time": 4.0, "capacities": {"core": 2},
                               "data_in": [], "data_out": [_dn("field1_analysis.json")]})
    _write(syn, "job_3.json", {"id": 3, "wall_time": 5.0, "capacities": {"core": 1},
                               "data_in": [_dn("x.dat")], "data_out": []})
    bun = tmp_path / "bundle"
    _write(bun, "job_1.json", {"id": "soil_run1_analyze", "workflow": "soilmoisture",
                               "execution": {"transformation": "analyze"},
                               "data_in": [_dn("field1_soil_data.csv")],
                               "data_out": [_dn("field1_analysis.json")]})
    _write(bun, "job_2.json", {"id": "soil_run1_predict", "workflow": "soilmoisture",
                               "execution": {"transformation": "predict"},
                               "data_predicate": {"kind": "files", "files": ["field1_analysis.json"]},
                               "data_in": [_dn("field1_analysis.json")],
                               "data_out": [_dn("field1_prediction.json")]})
    _write(bun, "manifest.json", {"code": ["code/analyze.py"], "missing": []})
    _write(bun, "conversion_summary.json", {"total_jobs_written": 2, "dag": {
        "gating": True, "edges": 1, "colliding_outputs": {}, "cross_workflow_edges": {},
        "replica_conflicts": {}, "execution_jobs": 2}})
    (bun / "code").mkdir()
    (bun / "code" / "analyze.py").write_text("print('hi')\n")
    return syn, bun


def test_the_unmerged_sets_would_be_refused_and_the_merge_is_clean(tmp_path):
    syn, bun = _fixture(tmp_path)
    raw = [dict(json.loads(p.read_text()), id=str(json.loads(p.read_text())["id"]))
           for p in mw._job_files(syn) + mw._job_files(bun)]
    assert mw.colliding_outputs(raw)                    # field1_analysis.json, three producers
    out = tmp_path / "mixed"
    s = mw.merge(syn, bun, out)
    assert s["dag"]["colliding_outputs"] == {} and s["dag"]["cross_workflow_edges"] == {}
    assert s["total_jobs_written"] == 5 and s["dag"]["execution_jobs"] == 2
    assert s["dag"]["gating"] is True


def test_simulated_jobs_keep_everything_the_scheduler_reads(tmp_path):
    syn, bun = _fixture(tmp_path)
    out = tmp_path / "mixed"
    mw.merge(syn, bun, out)
    before = json.loads((syn / "job_1.json").read_text())
    after = json.loads((out / "job_1.json").read_text())
    assert after["data_in"][0]["file"] == "replay1__field1_soil_data.csv"
    for k in ("id", "wall_time", "capacities"):
        assert after[k] == before[k]
    for a, b in zip(after["data_in"] + after["data_out"], before["data_in"] + before["data_out"]):
        assert (a["name"], a["size_bytes"]) == (b["name"], b["size_bytes"])


def test_real_jobs_are_unchanged_and_arrive_after_every_simulated_job(tmp_path):
    syn, bun = _fixture(tmp_path)
    out = tmp_path / "mixed"
    mw.merge(syn, bun, out)
    assert [p.name for p in mw._job_files(out)] == [f"job_{i}.json" for i in range(1, 6)]
    assert json.loads((out / "job_4.json").read_text()) == json.loads((bun / "job_1.json").read_text())
    assert json.loads((out / "job_5.json").read_text()) == json.loads((bun / "job_2.json").read_text())
    assert (out / "code" / "analyze.py").is_file() and (out / "manifest.json").is_file()


def test_run_tests_own_bundle_gate_accepts_the_merge_for_an_executing_run(tmp_path, monkeypatch):
    syn, bun = _fixture(tmp_path)
    out = tmp_path / "mixed"
    mw.merge(syn, bun, out)
    monkeypatch.setattr(run_test, "_run_execution_mode", lambda a: "real")
    run_test.validate_pegasus_jobs_dir(argparse.Namespace(pegasus_jobs_dir=str(out), jobs=5))

    # The same gate refuses the unrenamed union — the reason the merge exists.
    raw = tmp_path / "raw"
    raw.mkdir()
    for p in mw._job_files(syn):
        (raw / p.name).write_text(p.read_text())
    for i, p in enumerate(mw._job_files(bun), start=4):
        (raw / f"job_{i}.json").write_text(p.read_text())
    (raw / "manifest.json").write_text((bun / "manifest.json").read_text())
    jobs = [dict(j, id=str(j["id"])) for j in (json.loads(p.read_text()) for p in mw._job_files(raw))]
    (raw / "conversion_summary.json").write_text(json.dumps({"total_jobs_written": 5, "dag": {
        "gating": True, "colliding_outputs": mw.colliding_outputs(jobs),
        "cross_workflow_edges": mw.cross_workflow_edges(jobs), "replica_conflicts": {},
        "execution_jobs": 2}}))
    with pytest.raises(SystemExit):
        run_test.validate_pegasus_jobs_dir(argparse.Namespace(pegasus_jobs_dir=str(raw), jobs=5))


def test_an_unclean_merge_is_refused_with_a_message_not_a_crash(tmp_path, monkeypatch):
    syn, bun = _fixture(tmp_path)
    monkeypatch.setattr(mw, "_rename", lambda entries, job_id: list(entries or []))  # no rename
    with pytest.raises(SystemExit, match="not clean"):
        mw.merge(syn, bun, tmp_path / "mixed")
    assert not (tmp_path / "mixed").exists()


def test_refusals(tmp_path):
    syn, bun = _fixture(tmp_path)
    out = tmp_path / "mixed"
    out.mkdir()
    with pytest.raises(SystemExit):
        mw.merge(syn, bun, out)                       # never merges into an existing dir
    (bun / "manifest.json").unlink()
    with pytest.raises(SystemExit):
        mw.merge(syn, bun, tmp_path / "other")        # not a converted bundle


# --------------------------------------------------------------------------- several workflows
def _bundle(tmp_path, name, wf, files, image="img", code="code/a.py"):
    b = tmp_path / name
    jobs = []
    for i, (fin, fout) in enumerate(files, start=1):
        j = {"id": f"{wf}_{i}", "workflow": wf, "execution": {"transformation": f"t{i}"},
             "data_in": [_dn(f) for f in fin], "data_out": [_dn(f) for f in fout]}
        _write(b, f"job_{i}.json", j)
        jobs.append(j)
    (b / Path(code).parent).mkdir(parents=True, exist_ok=True)
    (b / code).write_text(f"# {wf}\n")
    _write(b, "manifest.json", {"code": {f"/src/{wf}/{code}": {"bundled": code, "sha256": wf}},
                                "inputs": {}, "images": {image: {"sha256": image}}, "missing": []})
    _write(b, "conversion_summary.json", {"total_jobs_written": len(jobs), "dag": {
        "gating": True, "edges": 1, "roots": [jobs[0]["id"]], "colliding_outputs": {},
        "cross_workflow_edges": {}, "replica_conflicts": {}, "execution_jobs": len(jobs)}})
    _write(b, "pegasus_baseline.json", {"metadata": {}, "runs": {f"{wf}_run1": {"makespan_sec": 1}}})
    return b


from pathlib import Path  # noqa: E402


def test_two_workflows_merge_in_order_with_unioned_bundles(tmp_path, monkeypatch):
    syn, _ = _fixture(tmp_path)
    a = _bundle(tmp_path, "a", "cper", [([], ["x.csv"]), (["x.csv"], ["y.csv"])],
                image="cper_img", code="code/ca/ca.py")
    b = _bundle(tmp_path, "b", "eht", [([], ["u.fits"])], image="eht_img", code="code/eb/eb.py")
    out = tmp_path / "mixed"
    s = mw.merge(syn, [a, b], out)
    files = [r["file"] for r in s["merged"]["real_jobs"]]
    assert files == ["job_4.json", "job_5.json", "job_6.json"]
    assert [r["workflow"] for r in s["merged"]["real_jobs"]] == ["cper", "cper", "eht"]
    m = json.loads((out / "manifest.json").read_text())
    assert set(m["images"]) == {"cper_img", "eht_img"} and len(m["code"]) == 2
    assert (out / "code/ca/ca.py").is_file() and (out / "code/eb/eb.py").is_file()
    assert set(json.loads((out / "pegasus_baseline.json").read_text())["runs"]) == {"cper_run1", "eht_run1"}
    assert s["dag"]["edges"] == 2 and s["dag"]["execution_jobs"] == 3
    monkeypatch.setattr(run_test, "_run_execution_mode", lambda a: "real")
    run_test.validate_pegasus_jobs_dir(argparse.Namespace(pegasus_jobs_dir=str(out), jobs=6))


def test_a_name_one_workflow_produces_and_another_reads_is_refused(tmp_path):
    syn, _ = _fixture(tmp_path)
    a = _bundle(tmp_path, "a", "cper", [([], ["shared.csv"])], code="code/ca/ca.py")
    b = _bundle(tmp_path, "b", "eht", [(["shared.csv"], ["u.fits"])], image="i2", code="code/eb/eb.py")
    with pytest.raises(SystemExit, match="not clean"):
        mw.merge(syn, [a, b], tmp_path / "mixed")
    assert not (tmp_path / "mixed").exists()


def test_two_bundles_placing_different_code_at_one_path_are_refused(tmp_path):
    syn, _ = _fixture(tmp_path)
    a = _bundle(tmp_path, "a", "cper", [([], ["x.csv"])], code="code/run/run.py")
    b = _bundle(tmp_path, "b", "eht", [([], ["u.fits"])], image="i2", code="code/run/run.py")
    with pytest.raises(SystemExit, match="different"):
        mw.merge(syn, [a, b], tmp_path / "mixed")


def test_the_same_bundle_twice_is_refused(tmp_path):
    syn, bun = _fixture(tmp_path)
    with pytest.raises(SystemExit, match="twice"):
        mw.merge(syn, [bun, bun], tmp_path / "mixed")


def test_bundles_alone_for_the_standalone_pilot(tmp_path, monkeypatch):
    a = _bundle(tmp_path, "a", "cper", [([], ["x.csv"]), (["x.csv"], ["y.csv"])], code="code/ca/ca.py")
    b = _bundle(tmp_path, "b", "eht", [([], ["u.fits"])], image="i2", code="code/eb/eb.py")
    out = tmp_path / "wf_only"
    s = mw.merge(None, [a, b], out)
    assert [p.name for p in mw._job_files(out)] == ["job_1.json", "job_2.json", "job_3.json"]
    assert s["merged"]["synthetic_jobs"] == 0 and s["merged"]["synthetic"] is None
    monkeypatch.setattr(run_test, "_run_execution_mode", lambda a: "real")
    run_test.validate_pegasus_jobs_dir(argparse.Namespace(pegasus_jobs_dir=str(out), jobs=3))
