#!/usr/bin/env python3.11
import argparse, os, re, subprocess, time
from datetime import datetime

LINE_RE = re.compile(r"^state:\d+:\d+:(\d+):\s*\{([^}]*)\}\s*$")

def log(msg: str):
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)

def start_proc(cmd, log_file=None):
    log(f"Starting: {' '.join(cmd)}")
    if log_file:
        return subprocess.Popen(cmd, stdout=open(log_file, "w"), stderr=subprocess.STDOUT)
    return subprocess.Popen(cmd)

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
        if not inside or inside == "":
            return 0
        items = [x.strip() for x in inside.split(",") if x.strip()]
        return len(items)
    return None

def touch(path: str):
    with open(path, "a"):
        os.utime(path, None)

def main():
    ap = argparse.ArgumentParser(description="Start swarm, run job distributor, watch state:*:*:1: size, shutdown, plot.")
    ap.add_argument("--agents", type=int, required=True)
    ap.add_argument("--topology", default="mesh")
    ap.add_argument("--jobs", type=int, required=True)
    ap.add_argument("--db-host", default="database")
    ap.add_argument("--jobs-per-interval", type=int, default=20)
    ap.add_argument("--run-dir", required=True, help="Output dir for plots")
    ap.add_argument("--watch-bucket", type=int, default=1)
    ap.add_argument("--threshold", type=int, default=5)
    ap.add_argument("--stable-seconds", type=int, default=90)
    ap.add_argument("--check-interval", type=int, default=5)
    ap.add_argument("--grace-seconds", type=int, default=30)
    ap.add_argument("--max-misses", type=int, default=10, help="Consecutive times bucket not found before forcing shutdown")
    ap.add_argument("--keep-running", action="store_true")
    ap.add_argument("--log-dir", default=None)
    args = ap.parse_args()

    os.makedirs(args.log_dir, exist_ok=True) if args.log_dir else None

    # 1) Start swarm
    swarm_cmd = ["./swarm-multi-start.sh", str(args.agents), args.topology, str(args.jobs), args.db_host, "10"]
    swarm = start_proc(swarm_cmd, os.path.join(args.log_dir, "swarm.log") if args.log_dir else None)

    # 2) Start job distributor
    job_dist_cmd = [
        "python3.11", "job_distributor.py",
        "--redis-host", args.db_host,
        "--jobs-dir", "jobs",
        "--jobs-per-interval", str(args.jobs_per_interval),
    ]
    job_dist = start_proc(job_dist_cmd, os.path.join(args.log_dir, "job_dist.log") if args.log_dir else None)

    # 3) Watch bucket set size, with miss counter
    low_since = None
    consecutive_misses = 0
    while True:
        out = run_once(["python3.11", "dump_db.py", "--host", args.db_host, "--type", "redis", "--key", "state"])
        size = parse_bucket_set_count(out, args.watch_bucket)

        if size is None:
            consecutive_misses += 1
            log(f"WARN: state:*:*:{args.watch_bucket}: not found (miss {consecutive_misses}/{args.max_misses})")
            if consecutive_misses > args.max_misses:
                log(f"Bucket not found for > {args.max_misses} consecutive checks. Forcing shutdown path.")
                break
            low_since = None
        else:
            consecutive_misses = 0
            cond = size < args.threshold
            log(f"state:*:*:{args.watch_bucket}: size={size} → {'LOW' if cond else 'OK'} (threshold {args.threshold})")
            if cond:
                if low_since is None:
                    low_since = time.time()
                elif time.time() - low_since >= args.stable_seconds:
                    log(f"Condition stable for {args.stable_seconds}s. Proceeding to shutdown.")
                    break
            else:
                low_since = None

        time.sleep(args.check_interval)

    # 4) Touch shutdown
    touch("shutdown")
    log("Touched 'shutdown'")

    # 5) Grace period
    if args.grace_seconds > 0:
        log(f"Waiting {args.grace_seconds}s for graceful stop …")
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

    # 7) Cleanup
    if not args.keep_running:
        for proc, name in [(job_dist, "job_distributor"), (swarm, "swarm")]:
            try:
                proc.terminate()
                log(f"Terminated {name}")
            except Exception as e:
                log(f"WARN: failed to terminate {name}: {e}")

    log("Done.")

if __name__ == "__main__":
    main()
