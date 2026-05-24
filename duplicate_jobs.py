#!/usr/bin/env python3
"""
Duplicate Pegasus job files with new IDs for weak scaling tests.

Usage:
    python3 duplicate_jobs.py <src_dir> <dst_dir> <target_count>

Example:
    # Create 1094 jobs from 547 source jobs (each source job duplicated ~2x)
    python3 duplicate_jobs.py jobs/ jobs_1094/ 1094

    # Create 2188 jobs from 547 source jobs (each source job duplicated ~4x)
    python3 duplicate_jobs.py jobs/ jobs_2188/ 2188

The script cycles through the source jobs, assigning new sequential IDs.
All other job attributes (wall_time, capacities, etc.) are preserved from
the source jobs. This ensures realistic workload distributions for weak
scaling experiments.
"""
import json
import sys
from pathlib import Path


def duplicate_jobs(src_dir: str, dst_dir: str, target_count: int) -> None:
    src = Path(src_dir)
    dst = Path(dst_dir)
    dst.mkdir(parents=True, exist_ok=True)

    # Load all source jobs
    jobs = []
    for f in sorted(src.glob("job_*.json")):
        with open(f) as fh:
            jobs.append(json.load(fh))

    if not jobs:
        print(f"ERROR: No job_*.json files found in {src}")
        sys.exit(1)

    print(f"Loaded {len(jobs)} source jobs, target: {target_count}")

    # Write jobs with new sequential IDs
    for i in range(1, target_count + 1):
        # Cycle through source jobs
        src_job = jobs[(i - 1) % len(jobs)]
        new_job = dict(src_job)
        new_job["id"] = str(i)
        new_job["capacities"] = dict(src_job["capacities"])

        out_path = dst / f"job_{i}.json"
        with open(out_path, "w") as fh:
            json.dump(new_job, fh, indent=2)

    print(f"Written {target_count} jobs to {dst}")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python3 duplicate_jobs.py <src_dir> <dst_dir> <target_count>")
        sys.exit(1)

    duplicate_jobs(sys.argv[1], sys.argv[2], int(sys.argv[3]))
