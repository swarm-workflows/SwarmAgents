#!/usr/bin/env python3.11
import argparse, os, re, subprocess, sys, time
from datetime import datetime
from pathlib import Path

def log(msg: str):
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)

def shell_out(cmd: list[str]) -> tuple[int, str]:
    p = subprocess.run(cmd, capture_output=True, text=True, check=False)
    return p.returncode, (p.stdout or "") + (p.stderr or "")

def wait_for_ports_clear(regex: str, timeout: int, interval: float) -> bool:
    """
    Wait until no sockets matching `regex` are visible in `ss -tan`.
    If `ss` is missing, use `netstat -tan`.
    Returns True if clear, False on timeout.
    """
    pattern = re.compile(regex)
    # pick tool
    rc, _ = shell_out(["which", "ss"])
    use_ss = (rc == 0)

    start = time.time()
    while True:
        if use_ss:
            rc, out = shell_out(["ss", "-tan"])
        else:
            rc, out = shell_out(["netstat", "-tan"])
            if rc != 0:  # if netstat also missing, consider clear to avoid deadlock
                return True

        lines = [ln.strip() for ln in out.splitlines()]
        hits = []
        for ln in lines:
            # Line can contain local addr:port in various columns. Match anywhere.
            if pattern.search(ln):
                hits.append(ln)
        if not hits:
            return True

        if time.time() - start >= timeout:
            log("WARNING: port(s) still present after timeout:")
            for h in hits[:10]:
                log("  " + h)
            return False

        time.sleep(interval)

def touch(path: str):
    with open(path, "a"):
        os.utime(path, None)

def main():
    ap = argparse.ArgumentParser(description="Run repeated Swarm tests, archive per-run results, and ensure 50xxx ports are clear between runs.")
    ap.add_argument("--runs", type=int, default=50, help="Number of runs (default: 50)")
    ap.add_argument("--agents", type=int, required=True)
    ap.add_argument("--jobs", type=int, required=True)
    ap.add_argument("--topology", default="mesh")
    ap.add_argument("--db-host", default="database")
    ap.add_argument("--jobs-per-interval", type=int, default=20)
    ap.add_argument("--base-out", required=True, help="Base output directory (e.g., runs/30/100)")
    ap.add_argument("--watch-bucket", type=int, default=1)
    ap.add_argument("--threshold", type=int, default=5)
    ap.add_argument("--stable-seconds", type=int, default=90)
    ap.add_argument("--check-interval", type=float, default=5)
    ap.add_argument("--grace-seconds", type=int, default=30)
    ap.add_argument("--max-misses", type=int, default=10)
    ap.add_argument("--wait-port-regex", default=r":50[0-9]{3}\b", help="Regex to detect sockets to avoid (default matches :50000–:50999 etc.)")
    ap.add_argument("--wait-timeout", type=int, default=120, help="Max seconds to wait for ports to clear between runs")
    ap.add_argument("--wait-interval", type=float, default=2.0, help="Polling interval while waiting for ports to clear")
    ap.add_argument("--tar-after", action="store_true", help="Tar.gz each run directory after completion")
    ap.add_argument("--sleep-between", type=float, default=2.0, help="Extra sleep seconds after ports clear (default 2)")
    args = ap.parse_args()

    base_out = Path(args.base_out)
    base_out.mkdir(parents=True, exist_ok=True)

    for i in range(1, args.runs + 1):
        run_name = f"run{i:02d}"
        run_dir = base_out / run_name
        logs_dir = run_dir / "logs"
        run_dir.mkdir(parents=True, exist_ok=True)
        logs_dir.mkdir(parents=True, exist_ok=True)

        # Ensure we don't inherit old shutdown flag
        shutdown_file = Path("shutdown")
        if shutdown_file.exists():
            shutdown_file.unlink(missing_ok=True)

        # Wait for ports like :50xxx to disappear
        log(f"[{run_name}] Waiting for ports to clear (regex={args.wait_port_regex!r}, timeout={args.wait_timeout}s)…")
        cleared = wait_for_ports_clear(args.wait_port_regex, args.wait_timeout, args.wait_interval)
        if not cleared:
            log(f"[{run_name}] WARNING: ports still present; proceeding anyway (you asked to block until absent).")

        if args.sleep_between > 0:
            time.sleep(args.sleep_between)

        # Build run_test.py command
        cmd = [
            "python3.11", "run_test.py",
            "--agents", str(args.agents),
            "--topology", args.topology,
            "--jobs", str(args.jobs),
            "--db-host", args.db_host,
            "--jobs-per-interval", str(args.jobs_per_interval),
            "--run-dir", str(run_dir),
            "--watch-bucket", str(args.watch_bucket),
            "--threshold", str(args.threshold),
            "--stable-seconds", str(args.stable_seconds),
            "--check-interval", str(args.check_interval),
            "--grace-seconds", str(args.grace_seconds),
            "--max-misses", str(args.max_misses),
            "--log-dir", str(logs_dir)
        ]

        log(f"[{run_name}] Starting test…")
        with open(logs_dir / "batch_runner.log", "w") as f:
            f.write(f"CMD: {' '.join(cmd)}\nSTART: {datetime.now().isoformat()}\n")

        # Run the test synchronously
        rc, out = shell_out(cmd)
        (logs_dir / "run_test.stdout.log").write_text(out)

        if rc != 0:
            log(f"[{run_name}] ERROR: run_test.py exit code {rc}. See logs in {logs_dir}. Continuing to next run.")
        else:
            log(f"[{run_name}] Completed successfully.")

        # Optional: tar.gz the entire run directory
        if args.tar_after:
            log(f"[{run_name}] Creating tar.gz archive…")
            tar_path = base_out / f"{run_name}.tar.gz"
            # python-based tar for portability
            import tarfile
            with tarfile.open(tar_path, "w:GZ") as tar:
                tar.add(run_dir, arcname=run_name)
            log(f"[{run_name}] Archived to {tar_path}")

    log("All runs complete.")

if __name__ == "__main__":
    sys.exit(main() or 0)
