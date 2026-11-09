#!/usr/bin/env python3
"""Merge converted workflow bundles into a simulated job set: the E1′ mixed rung (T-6).

    python3.11 merge_workflow_jobs.py --synthetic /root/workloads/jobs_1800 \
        --bundle cper_bundle --bundle eht_bundle --out /export/swarm-wf/mixed_1800_wf

Several bundles may be merged (one per workflow): their real jobs follow the simulated ones in
the order given, their code/inputs/manifests are unioned, and any name two workflows share —
a produced-and-read file, a code or input path, an image — is refused unless it is the same file.

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


def _merge_manifests(manifests):
    """Union the bundles' manifests. A code path, input name or image name that two bundles
    both declare is allowed only when it is the same file (same sha256); anything else would
    be two workflows reading one name, which the flat working directory cannot keep apart."""
    out = {"code": {}, "inputs": {}, "images": {}, "missing": []}
    for b, m in manifests:
        for kind in ("code", "inputs", "images"):
            section = m.get(kind) or {}
            if isinstance(section, list):            # older manifests list bundled paths only
                section = {str(x): {"bundled": str(x)} for x in section}
            for key, entry in section.items():
                prev = out[kind].get(key)
                if prev is not None and prev.get("sha256") != entry.get("sha256"):
                    raise SystemExit(f"{kind} {key!r} is declared by two bundles with different "
                                     f"content (one of them {b}); refusing to merge")
                out[kind][key] = entry
        out["missing"].extend(m.get("missing") or [])
    bundled = {}
    for kind in ("code", "inputs"):
        for key, entry in out[kind].items():
            rel = entry.get("bundled")
            if rel and bundled.setdefault(rel, entry.get("sha256")) != entry.get("sha256"):
                raise SystemExit(f"two bundles place different files at {rel}; refusing to merge")
    return out


def merge(synthetic, bundles, out: Path) -> dict:
    """Merge one or more converted bundles into the simulated set `synthetic`, writing `out`.
    `synthetic` None merges the bundles alone (the §10 standalone pilot: real jobs only)."""
    bundles = [Path(b) for b in (bundles if isinstance(bundles, (list, tuple)) else [bundles])]
    if out.exists():
        raise SystemExit(f"{out} exists; refusing to merge into it (delete it first)")
    if len({b.resolve() for b in bundles}) != len(bundles):
        raise SystemExit("the same bundle is given twice")
    for bundle in bundles:
        for need in ("manifest.json", "conversion_summary.json"):
            if not (bundle / need).is_file():
                raise SystemExit(f"{bundle} has no {need}; it is not a converted bundle")
    syn_files = _job_files(Path(synthetic)) if synthetic is not None else []
    if synthetic is not None and not syn_files:
        raise SystemExit(f"no job_*.json in {synthetic}")
    per_bundle = []
    for bundle in bundles:
        files = _job_files(bundle)
        if not files:
            raise SystemExit(f"no job_*.json in {bundle}")
        per_bundle.append((bundle, files, [json.loads(p.read_text()) for p in files],
                           json.loads((bundle / "conversion_summary.json").read_text()),
                           json.loads((bundle / "manifest.json").read_text())))

    syn = [json.loads(p.read_text()) for p in syn_files]
    real = [j for _, _, jobs, _, _ in per_bundle for j in jobs]
    ids = [str(j["id"]) for j in syn] + [str(j["id"]) for j in real]
    if len(set(ids)) != len(ids):
        dup = sorted({i for i in ids if ids.count(i) > 1})
        raise SystemExit(f"job ids appear more than once: {dup[:5]}")
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
    manifest = _merge_manifests([(b, m) for b, _, _, _, m in per_bundle])

    out.mkdir(parents=True)
    try:
        for p, j in zip(syn_files, syn):
            (out / p.name).write_text(json.dumps(j))
        n = len(syn_files)
        written_real = []
        for bundle, files, jobs, _, _ in per_bundle:
            for p, j in zip(files, jobs):
                n += 1
                name = f"job_{n}.json"
                shutil.copyfile(p, out / name)
                written_real.append({"file": name, "bundle": str(bundle), "from": p.name,
                                     "id": j["id"], "workflow": j.get("workflow")})
            for d in ("code", "inputs"):
                if (bundle / d).is_dir():
                    shutil.copytree(bundle / d, out / d, symlinks=True, dirs_exist_ok=True)
        (out / "manifest.json").write_text(json.dumps(manifest, indent=2))
        runs = {}
        for bundle, *_ in per_bundle:
            f = bundle / "pegasus_baseline.json"
            if f.is_file():
                runs.update(json.loads(f.read_text()).get("runs") or {})
        (out / "pegasus_baseline.json").write_text(json.dumps(
            {"metadata": {"merged_from": [str(b) for b in bundles], "total_runs": len(runs)},
             "runs": runs}, indent=2))

        sums = [s for _, _, _, s, _ in per_bundle]
        dag = dict(sums[0].get("dag") or {})
        replica_conflicts = {}
        for s in sums:
            replica_conflicts.update((s.get("dag") or {}).get("replica_conflicts") or {})
        dag.update({
            "gating": any((s.get("dag") or {}).get("gating") for s in sums),
            "edges": sum(int((s.get("dag") or {}).get("edges") or 0) for s in sums),
            "roots": [r for s in sums for r in ((s.get("dag") or {}).get("roots") or [])],
            "colliding_outputs": collisions, "cross_workflow_edges": foreign,
            "replica_conflicts": replica_conflicts,
            "execution_jobs": sum(1 for j in merged if j.get("execution"))})
        summary = dict(sums[0])
        summary.update({
            "total_jobs_written": len(merged),
            "dag": dag,
            "merged": {"tool": "merge_workflow_jobs.py",
                       "synthetic": str(synthetic) if synthetic is not None else None,
                       "synthetic_jobs": len(syn), "bundles": [str(b) for b in bundles],
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
    ap.add_argument("--synthetic", type=Path, default=None,
                    help="the simulated job set; omit to merge the bundles alone")
    ap.add_argument("--bundle", required=True, type=Path, action="append",
                    help="a converted bundle; repeat for several workflows")
    ap.add_argument("--out", required=True, type=Path)
    a = ap.parse_args(argv)
    s = merge(a.synthetic, a.bundle, a.out)
    m = s["merged"]
    per = {}
    for r in m["real_jobs"]:
        per.setdefault(r.get("workflow") or r["bundle"], []).append(r["file"])
    print(f"{a.out}: {m['synthetic_jobs']} simulated + {len(m['real_jobs'])} real; names clean")
    for wf, files in per.items():
        print(f"  {wf}: {len(files)} jobs ({files[0]} .. {files[-1]})")


if __name__ == "__main__":
    sys.exit(main())
