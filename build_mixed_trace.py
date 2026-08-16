#!/usr/bin/env python3
"""
Build a frozen, reproducible mixed Pegasus job trace for the Chaos Jungle experiments.

Combines two extracted profile sets:
  * cpegasus  — carries per-file data nodes, so DTN/connectivity terms are exercised
  * pegasus   — 10 distinct workflows with a much heavier resource/duration tail,
                but no file information (older extraction), so no data nodes

Jobs are sampled stratified by workflow (dax_label) so rare workflows survive the
sample instead of being swamped by the biggest one, and the sample is seeded so the
trace is byte-identical on every rebuild.

Output is a merged *profile* JSON. Convert it with pegasus_to_swarm_converter.py,
passing --dtn-names so job data-node sites match the agents' DTN pool.

    python3 build_mixed_trace.py --cpegasus <profiles.json> --pegasus <profiles.json> \
        --n-cpegasus 200 --n-pegasus 100 --seed 42 --output mixed_profile_300.json
"""
from __future__ import annotations

import argparse
import collections
import json
import random


def load(path: str) -> list:
    with open(path) as fh:
        d = json.load(fh)
    return d if isinstance(d, list) else d.get("jobs", d)


def stratified_sample(jobs: list, n: int, rng: random.Random) -> list:
    """Sample n jobs spread across workflows, round-robin from shuffled per-workflow pools."""
    if n >= len(jobs):
        return list(jobs)

    buckets = collections.defaultdict(list)
    for j in jobs:
        buckets[j.get("dax_label_db") or "unknown"].append(j)
    for b in buckets.values():
        rng.shuffle(b)

    # Round-robin across workflows so every workflow contributes before any repeats a draw.
    picked, labels = [], sorted(buckets)
    while len(picked) < n:
        progressed = False
        for label in labels:
            if buckets[label] and len(picked) < n:
                picked.append(buckets[label].pop())
                progressed = True
        if not progressed:
            break
    return picked


def summarize(tag: str, jobs: list) -> None:
    counts = collections.Counter(j.get("dax_label_db") or "unknown" for j in jobs)
    with_files = sum(1 for j in jobs if j.get("input_files_db") or j.get("output_files_db"))
    print(f"  {tag}: n={len(jobs)} with_file_info={with_files} workflows={len(counts)}")
    for label, c in counts.most_common():
        print(f"      {label}: {c}")


def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--cpegasus", required=True, help="cpegasus all_runs_jobs_profile.json")
    p.add_argument("--pegasus", required=True, help="pegasus all_runs_jobs_profile.json")
    p.add_argument("--n-cpegasus", type=int, default=200)
    p.add_argument("--n-pegasus", type=int, default=100)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--output", required=True)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    rng = random.Random(args.seed)

    cp = stratified_sample(load(args.cpegasus), args.n_cpegasus, rng)
    pg = stratified_sample(load(args.pegasus), args.n_pegasus, rng)

    print("Sampled:")
    summarize("cpegasus", cp)
    summarize("pegasus", pg)

    merged = cp + pg
    # Deterministic order regardless of how the two samples were drawn.
    merged.sort(key=lambda j: (j.get("dax_label_db") or "", str(j.get("job_name") or ""),
                               str(j.get("job_id_db") or "")))

    with open(args.output, "w") as fh:
        json.dump(merged, fh)

    with_files = sum(1 for j in merged if j.get("input_files_db") or j.get("output_files_db"))
    print(f"\nWrote {len(merged)} profiles -> {args.output}")
    print(f"  {with_files} ({100*with_files//len(merged)}%) carry file info (become data/DTN nodes)")
    print(f"  seed={args.seed} — rebuild is byte-identical")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
