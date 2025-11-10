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
        hits = [ln for ln in lines if pattern.search(ln)]
        if not hits:
            return True

        if time.time() - start >= timeout:
            log("WARNING: port(s) still present after timeout:")
            for h in hits[:10]:
                log("  " + h)
            return False

        time.sleep(interval)

def main():
    ap = argparse.ArgumentParser(
        description="Batch wrapper for run_test_v2.py with per-run isolation, port clearing, and tar archives."
    )
    # Batch controls
    ap.add_argument("--runs", type=int, default=50, help="Number of runs (default: 50)")
    ap.add_argument("--base-out", required=True, help="Base output directory (e.g., runs/30/100)")
    ap.add_argument("--tar-after", action="store_true", help="Tar.gz each run directory after completion")
    ap.add_argument("--sleep-between", type=float, default=2.0, help="Extra sleep seconds after ports clear")

    # Port wait
    ap.add_argument("--wait-port-regex", default=r":20[0-9]{3}\b",
                    help="Regex to detect sockets to avoid (default matches :20000–:20999)")
    ap.add_argument("--wait-timeout", type=int, default=120, help="Max seconds to wait for ports to clear")
    ap.add_argument("--wait-interval", type=float, default=2.0, help="Polling interval while waiting for ports to clear")

    # Path to the v2 runner
    ap.add_argument("--runner", default="run_test_v2.py",
                    help="Path to run_test_v2.py (default: run_test_v2.py)")

    # ---- Passthrough to run_test_v2.py (required / common) ----
    ap.add_argument("--mode", choices=["local", "remote"], default="local",
                    help="Where to start agents (v2 required)")
    ap.add_argument("--agent-type", choices=["resource", "llm"], default="resource",
                    help="Agent type for v2 runner")
    ap.add_argument("--agents", type=int, required=True, help="Total number of agents")
    ap.add_argument("--topology", required=True, choices=["mesh", "ring", "star", "hierarchical"])
    ap.add_argument("--jobs", type=int, required=True)
    ap.add_argument("--db-host", required=True)

    # v2 knobs that map to old script semantics
    ap.add_argument("--jobs-per-interval", type=int, default=20)
    ap.add_argument("--watch-bucket", type=int, default=1)
    ap.add_argument("--threshold", type=int, default=5)
    ap.add_argument("--stable-seconds", type=int, default=90)
    ap.add_argument("--check-interval", type=float, default=5.0)
    ap.add_argument("--grace-seconds", type=int, default=30)
    ap.add_argument("--max-misses", type=int, default=10)

    # Additional v2 options you might want to use
    ap.add_argument("--jobs-per-proposal", type=int, default=10)
    ap.add_argument("--runtime", type=int, default=90, help="Seconds to keep the test running (v2)")
    ap.add_argument("--job-interval", type=float, default=0.5, help="Seconds between job bursts (v2)")

    # Config / starter
    ap.add_argument("--starter", default="./swarm-multi-start.sh",
                    help="Path to swarm-multi-start.sh (v2)")
    ap.add_argument("--config-dir", default="configs", help="Where to write generated configs (v2)")
    ap.add_argument("--use-config-dir", action="store_true",
                    help="Tell starters to use pre-generated configs (v2)")
    ap.add_argument("--agents-per-host", type=int, default=1,
                    help="Remote mode shard size (v2)")
    ap.add_argument("--groups", type=int, default=None)
    ap.add_argument("--group-size", type=int, default=None)

    # Remote host options (v2)
    ap.add_argument("--agent-hosts", default=None, help="Comma-separated hostnames")
    ap.add_argument("--agent-hosts-file", default=None, help="File with one hostname per line")
    ap.add_argument("--remote-repo-dir", default="/root/SwarmAgents", help="Remote repo root")

    # Logging/output structure per run
    ap.add_argument("--log-subdir-name", default="logs", help="Subdir name for logs inside each run dir")
    ap.add_argument("--debug", action="store_true")

    args = ap.parse_args()

    base_out = Path(args.base_out)
    base_out.mkdir(parents=True, exist_ok=True)

    for i in range(1, args.runs + 1):
        run_name = f"run{i:02d}"
        run_dir = base_out / run_name
        logs_dir = run_dir / (args.log_subdir_name or "logs")
        run_dir.mkdir(parents=True, exist_ok=True)
        logs_dir.mkdir(parents=True, exist_ok=True)

        # Ensure we don't inherit old shutdown flag used by some runners
        shutdown_file = Path("shutdown")
        if shutdown_file.exists():
            shutdown_file.unlink(missing_ok=True)

        # Wait for target ports to disappear
        log(f"[{run_name}] Waiting for ports to clear (regex={args.wait_port_regex!r}, timeout={args.wait_timeout}s)…")
        cleared = wait_for_ports_clear(args.wait_port_regex, args.wait_timeout, args.wait_interval)
        if not cleared:
            log(f"[{run_name}] WARNING: ports still present; proceeding anyway.")

        if args.sleep_between > 0:
            time.sleep(args.sleep_between)

        # Build run_test_v2.py command
        cmd = [
            "python3.11", args.runner,
            "--mode", args.mode,
            "--agent-type", args.agent_type,
            "--agents", str(args.agents),
            "--topology", args.topology,
            "--jobs", str(args.jobs),
            "--db-host", args.db_host,
            "--jobs-per-interval", str(args.jobs_per_interval),
            "--jobs-per-proposal", str(args.jobs_per_proposal),
            "--runtime", str(args.runtime),
            "--job-interval", str(args.job_interval),
            "--run-dir", str(run_dir),
            "--watch-bucket", str(args.watch_bucket),
            "--threshold", str(args.threshold),
            "--stable-seconds", str(args.stable_seconds),
            "--check-interval", str(args.check_interval),
            "--grace-seconds", str(args.grace_seconds),
            "--max-misses", str(args.max_misses),
            "--log-dir", str(logs_dir),
            "--starter", args.starter,
            "--config-dir", args.config_dir,
        ]

        if args.use_config_dir:
            cmd.append("--use-config-dir")
        if args.groups is not None:
            cmd += ["--groups", str(args.groups)]
        if args.group_size is not None:
            cmd += ["--group-size", str(args.group_size)]
        if args.agents_per_host is not None:
            cmd += ["--agents-per-host", str(args.agents_per_host)]
        if args.agent_hosts:
            cmd += ["--agent-hosts", args.agent_hosts]
        if args.agent_hosts_file:
            cmd += ["--agent-hosts-file", args.agent_hosts_file]
        if args.remote_repo_dir:
            cmd += ["--remote-repo-dir", args.remote_repo_dir]
        if args.debug:
            cmd.append("--debug")

        log(f"[{run_name}] Starting test…")
        with open(logs_dir / "batch_runner.log", "w") as f:
            f.write(f"CMD: {' '.join(cmd)}\nSTART: {datetime.now().isoformat()}\n")

        rc, out = shell_out(cmd)
        (logs_dir / "run_test.stdout.log").write_text(out)

        if rc != 0:
            log(f"[{run_name}] ERROR: {args.runner} exit code {rc}. See logs in {logs_dir}. Continuing.")
        else:
            log(f"[{run_name}] Completed successfully.")

        if args.tar_after:
            log(f"[{run_name}] Creating tar.gz archive…")
            tar_path = base_out / f"{run_name}.tar.gz"
            import tarfile
            with tarfile.open(tar_path, "w:GZ") as tar:
                tar.add(run_dir, arcname=run_name)
            log(f"[{run_name}] Archived to {tar_path}")

    log("All runs complete.")

if __name__ == "__main__":
    sys.exit(main() or 0)
