#!/usr/bin/env python3.11
import argparse, os, re, subprocess, time
from datetime import datetime

LINE_RE = re.compile(r"^state:\d+:\d+:(\d+):\s*\{([^}]*)\}\s*$")

def log(msg: str):
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)

def run_blocking(cmd, log_file=None):
    """
    Run a command to completion. If log_file is provided, write stdout/stderr there.
    Otherwise, discard output cleanly.
    """
    if log_file:
        with open(log_file, "w") as f:
            return subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT, text=True)
    else:
        # Use a real file handle to /dev/null as a context manager
        with open(os.devnull, "w") as devnull:
            return subprocess.run(cmd, stdout=devnull, stderr=subprocess.STDOUT, text=True)

def run_once(cmd):
    p = subprocess.run(cmd, capture_output=True, text=True, check=False)
    return (p.stdout or "") + (p.stderr or "")

def parse_bucket_set_count(text: str, bucket: int) -> int:
    for raw_line in text.splitlines():
        line = raw_line.strip()
        m = LINE_RE.match(line)
        if not m:
            continue
        b = int(m.group(1))
        if b != bucket:
            continue
        inside = m.group(2).strip()
        if not inside:
            return 0
        items = [x.strip() for x in inside.split(",") if x.strip()]
        return len(items)
    return None

def touch(path: str):
    with open(path, "a"):
        os.utime(path, None)

def main():
    ap = argparse.ArgumentParser(
        description="Run swarm, job distributor (blocking), then shutdown + plot."
    )
    ap.add_argument("--agent-type", choices=["resource", "llm"], default="resource",
                    help="Agent kind to launch via swarm-multi-start.sh (default: resource)")
    ap.add_argument("--agents", type=int, required=True)
    ap.add_argument("--topology", default="mesh")
    ap.add_argument("--jobs", type=int, required=True)
    ap.add_argument("--db-host", default="database")
    ap.add_argument("--jobs-per-interval", type=int, default=20)
    ap.add_argument("--run-dir", required=True)
    ap.add_argument("--watch-bucket", type=int, default=1)
    ap.add_argument("--threshold", type=int, default=5)
    ap.add_argument("--stable-seconds", type=int, default=90)
    ap.add_argument("--check-interval", type=float, default=5.0)
    ap.add_argument("--grace-seconds", type=int, default=30)
    ap.add_argument("--max-misses", type=int, default=10)
    ap.add_argument("--log-dir", default=None)
    # Optional pass-throughs to the start script
    ap.add_argument("--use-config-dir", action="store_true",
                    help="Pass --use-config-dir to swarm-multi-start.sh")
    ap.add_argument("--debug", action="store_true",
                    help="Pass --debug to swarm-multi-start.sh")
    # Optional: jobs per proposal (kept default=10 as in your script)
    ap.add_argument("--jobs-per-proposal", type=int, default=10)
    args = ap.parse_args()

    if args.log_dir:
        os.makedirs(args.log_dir, exist_ok=True)

    # 1) Run swarm start (blocking) — NOTE: agent_type is first positional arg now
    swarm_cmd = [
        "./swarm-multi-start.sh",
        args.agent_type,
        str(args.agents),
        args.topology,
        str(args.jobs),
        args.db_host,
        str(args.jobs_per_proposal),
    ]
    if args.use_config_dir:
        swarm_cmd.append("--use-config-dir")
    if args.debug:
        swarm_cmd.append("--debug")

    swarm_log = os.path.join(args.log_dir, "swarm.log") if args.log_dir else None
    log(f"Launching swarm: {' '.join(swarm_cmd)}")
    rc = run_blocking(swarm_cmd, swarm_log)
    if rc.returncode != 0:
        log(f"ERROR: swarm-multi-start.sh exited {rc.returncode}")
        return

    # 2) Run job distributor (blocking)
    job_dist_cmd = [
        "python3.11", "job_distributor.py",
        "--redis-host", args.db_host,
        "--jobs-dir", "jobs",
        "--jobs-per-interval", str(args.jobs_per_interval),
    ]
    job_log = os.path.join(args.log_dir, "job_dist.log") if args.log_dir else None
    log(f"Starting job distributor: {' '.join(job_dist_cmd)}")
    rc = run_blocking(job_dist_cmd, job_log)
    if rc.returncode != 0:
        log(f"ERROR: job_distributor.py exited {rc.returncode}")
        return

    # 3) Wait for Redis state bucket condition
    low_since = None
    consecutive_misses = 0
    while True:
        out = run_once(["python3.11", "dump_db.py", "--host", args.db_host, "--type", "redis", "--key", "state"])
        size = parse_bucket_set_count(out, args.watch_bucket)
        if size is None:
            consecutive_misses += 1
            log(f"WARN: state:*:*:{args.watch_bucket} not found (miss {consecutive_misses}/{args.max_misses})")
            if consecutive_misses > args.max_misses:
                log("Bucket missing too often → treating as done.")
                break
        else:
            cond = size < args.threshold
            log(f"Bucket {args.watch_bucket} size={size} (thr={args.threshold}) → {'LOW' if cond else 'OK'}")
            if cond:
                if low_since is None:
                    low_since = time.time()
                elif time.time() - low_since >= args.stable_seconds:
                    log("Condition stable → proceed.")
                    break
            else:
                low_since = None
        time.sleep(args.check_interval)

    # 4) Touch shutdown
    touch("shutdown")
    log("Touched shutdown")

    # 5) Grace
    if args.grace_seconds > 0:
        log(f"Sleeping {args.grace_seconds}s …")
        time.sleep(args.grace_seconds)

    # 6) Plot
    plot_cmd = [
        "python3.11", "plot_latency_jobs.py",
        "--output_dir", args.run_dir,
        "--agents", str(args.agents),
        "--db_host", args.db_host,
    ]
    log(f"Running plot: {' '.join(plot_cmd)}")
    subprocess.run(plot_cmd, check=False)

    log("Done.")

if __name__ == "__main__":
    main()
