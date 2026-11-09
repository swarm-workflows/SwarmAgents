#!/usr/bin/env python3
"""Merge a converted workflow bundle into a simulated job set: the E1′ mixed rung (T-6).

    python3.11 merge_workflow_jobs.py --synthetic /root/workloads/jobs_1800 \
        --bundle /export/swarm-wf/jobs2 --out /export/swarm-wf/mixed_1800_soil

The output is ONE directory that `run_test.py --pegasus-jobs-dir` publishes, and that the mixed
cells' `runtime.execution.bundle` points at: the simulated jobs, then the bundle's real jobs,
then the bundle's `code/`, `inputs/`, `manifest.json` and `pegasus_baseline.json`, and a
`conversion_summary.json` whose file-name checks are computed over the merged records by the
converter's own functions, so `run_test.py` validates the merge exactly as it validates a bundle.

What it does to each part, and why:

* **Simulated jobs keep everything except their file names.** Each `data_in`/`data_out` file
  becomes `replay<id>__<basename>`. The replay set duplicates recorded Pegasus runs, including
  soilmoisture's, so it shares names with the real bundle (6 on 2026-11-09) and with itself (the
  same outputs recur across copies). In a run that executes, every name is live — the readiness
  registry and the per-run working directory are keyed by it — and `run_test.py` refuses such a
  set. A simulated job never executes and carries no `data_predicate`, and scheduling prices a
  job by its data node (`name`) and `size_bytes`, never by the file name, so the rename changes
  nothing the scheduler sees. Records keep their `job_<n>.json` names and ids, so their arrival
  order and times are those of the unmerged set.
* **Real jobs are copied unchanged** — their file names are on their command lines — and
  numbered after the simulated ones (`job_<N+1>.json` …), so they arrive last.
* Refuses rather than overwrites: an existing `--out`, a bundle without a manifest or summary,
  a real job id that collides with a simulated one, or a merged set whose checks are not clean.
"""
import argparse
import json
import os
import shutil
import sys
from pathlib import Path

from pegasus_to_swarm_converter import colliding_outputs, cross_workflow_edges


def _job_files(d: Path):
    def key(p: Path):
        stem = p.name[len("job_"):-len(".json")]
        return (0, int(stem), "") if stem.isdigit() else (1, 0, stem)
    return sorted((p for p in d.glob("job_*.json") if p.is_file()), key=key)


def _rename(entries, job_id):
    out = []
    for dn in entries or []:
        dn = dict(dn)
        f = dn.get("file")
        if f:
            dn["file"] = f"replay{job_id}__{os.path.basename(str(f))}"
        out.append(dn)
    return out


def merge(synthetic: Path, bundle: Path, out: Path) -> dict:
    if out.exists():
        raise SystemExit(f"{out} exists; refusing to merge into it (delete it first)")
    for need in ("manifest.json", "conversion_summary.json"):
        if not (bundle / need).is_file():
            raise SystemExit(f"{bundle} has no {need}; it is not a converted bundle")
    syn_files = _job_files(synthetic)
    real_files = _job_files(bundle)
    if not syn_files or not real_files:
        raise SystemExit(f"no job_*.json in {synthetic if not syn_files else bundle}")
    bsum = json.loads((bundle / "conversion_summary.json").read_text())

    syn = [json.loads(p.read_text()) for p in syn_files]
    real = [json.loads(p.read_text()) for p in real_files]
    clash = {str(j["id"]) for j in syn} & {str(j["id"]) for j in real}
    if clash:
        raise SystemExit(f"job ids in both sets: {sorted(clash)[:5]}")
    for j in syn:
        j["data_in"] = _rename(j.get("data_in"), j["id"])
        j["data_out"] = _rename(j.get("data_out"), j["id"])

    merged = syn + real
    # The converter's checks sort job ids; simulated ids are ints and converted ones strings,
    # so they are checked on string ids (the records themselves keep theirs).
    checked = [dict(j, id=str(j["id"])) for j in merged]
    collisions = colliding_outputs(checked)
    foreign = cross_workflow_edges(checked)
    if collisions or foreign:
        raise SystemExit(f"merged set is not clean: {len(collisions)} colliding outputs, "
                         f"{len(foreign)} cross-workflow names, e.g. "
                         f"{sorted(collisions or foreign)[:3]}")

    out.mkdir(parents=True)
    try:
        for p, j in zip(syn_files, syn):
            (out / p.name).write_text(json.dumps(j))
        base = len(syn_files)
        written_real = []
        for i, (p, j) in enumerate(zip(real_files, real), start=1):
            name = f"job_{base + i}.json"
            shutil.copyfile(p, out / name)
            written_real.append({"file": name, "from": p.name, "id": j["id"]})
        for d in ("code", "inputs"):
            if (bundle / d).is_dir():
                shutil.copytree(bundle / d, out / d, symlinks=True)
        for f in ("manifest.json", "pegasus_baseline.json"):
            if (bundle / f).is_file():
                shutil.copyfile(bundle / f, out / f)
        dag = dict(bsum.get("dag") or {})
        dag.update({"colliding_outputs": collisions, "cross_workflow_edges": foreign,
                    "replica_conflicts": (bsum.get("dag") or {}).get("replica_conflicts") or {},
                    "execution_jobs": sum(1 for j in merged if j.get("execution"))})
        summary = dict(bsum)
        summary.update({
            "total_jobs_written": len(merged),
            "dag": dag,
            "merged": {"tool": "merge_workflow_jobs.py", "synthetic": str(synthetic),
                       "synthetic_jobs": len(syn), "bundle": str(bundle),
                       "real_jobs": written_real,
                       "renamed": "simulated jobs' file names -> replay<id>__<basename>"},
        })
        (out / "conversion_summary.json").write_text(json.dumps(summary, indent=2))
    except BaseException:
        shutil.rmtree(out, ignore_errors=True)
        raise
    return summary


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--synthetic", required=True, type=Path)
    ap.add_argument("--bundle", required=True, type=Path)
    ap.add_argument("--out", required=True, type=Path)
    a = ap.parse_args(argv)
    s = merge(a.synthetic, a.bundle, a.out)
    m = s["merged"]
    print(f"{a.out}: {m['synthetic_jobs']} simulated + {len(m['real_jobs'])} real "
          f"({', '.join(r['file'] for r in m['real_jobs'])}); names clean")


if __name__ == "__main__":
    sys.exit(main())
