#!/usr/bin/env python3.11
import argparse, os, re, subprocess, sys, time
import shlex
from datetime import datetime
from pathlib import Path

def log(msg: str):
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)

def run_blocking(cmd: list[str] | str, log_file: str | None = None, check: bool = False) -> tuple[int, str]:
    """
    Run a command to completion, streaming stdout/stderr to the console in real time
    and (optionally) to a log file. Returns (returncode, combined_output).
    """
    import shlex, subprocess
    from pathlib import Path

    if isinstance(cmd, str):
        shell = True
        printable = cmd
        popen_cmd = cmd
    else:
        shell = False
        printable = " ".join(shlex.quote(c) for c in cmd)
        popen_cmd = cmd

    log(f"$ {printable}")

    # open log file if requested
    f = None
    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        f = open(log_file, "w", buffering=1)  # line-buffered

    try:
        proc = subprocess.Popen(
            popen_cmd,
            shell=shell,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,  # line-buffered
        )

        all_out = []
        assert proc.stdout is not None
        for line in proc.stdout:
            # print to console immediately
            print(line, end="")
            # append to in-memory buffer
            all_out.append(line)
            # also write to log file if provided
            if f:
                f.write(line)

        proc.wait()
        combined = "".join(all_out)

        if check and proc.returncode != 0:
            raise subprocess.CalledProcessError(proc.returncode, popen_cmd, combined)

        return proc.returncode, combined
    finally:
        if f:
            f.close()

def main():
    ap = argparse.ArgumentParser(
        description="Batch wrapper for run_test_v2.py with per-run isolation and archives."
    )
    # Batch controls
    ap.add_argument("--runs", type=int, default=50, help="Number of runs (default: 50)")
    ap.add_argument("--base-out", required=True, help="Base output directory (e.g., runs/30/100)")
    ap.add_argument("--sleep-between", type=float, default=2.0, help="Extra sleep seconds between runs")
    ap.add_argument("--tar-after", action="store_true", help="Tar.gz each run directory after completion")

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
    ap.add_argument("--hierarchical-level1-agent-type", type=str,
                    choices=["llm", "resource"], default="llm",
                    help="Agent type for level 1 (parent) agents in hierarchical topology (default: llm)")
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

    # Additional v2 options
    ap.add_argument("--jobs-per-proposal", type=int, default=10)
    ap.add_argument("--runtime", type=int, default=30, help="Seconds to keep the test running (v2)")
    ap.add_argument("--job-interval", type=float, default=0.5, help="Seconds between job bursts (v2)")

    # Dynamic agent addition (v2)
    ap.add_argument("--dynamic-agents", type=int, default=0,
                    help="Number of agents to add dynamically during test (0 = disabled)")
    ap.add_argument("--dynamic-trigger", choices=["time", "bucket", "jobs-completed"], default="time",
                    help="Trigger type for adding dynamic agents")
    ap.add_argument("--dynamic-delay", type=int, default=30,
                    help="Seconds to wait before adding agents (for 'time' trigger)")
    ap.add_argument("--dynamic-trigger-bucket", type=int, default=1,
                    help="Bucket to monitor (for 'bucket' trigger)")
    ap.add_argument("--dynamic-trigger-threshold", type=int, default=50,
                    help="Threshold value for bucket/jobs trigger")
    ap.add_argument("--dynamic-trigger-jobs", type=int, default=50,
                    help="Number of completed jobs to wait for (for 'jobs-completed' trigger)")

    # Test shutdown control (v2)
    ap.add_argument("--shutdown-after-seconds", type=int, default=0,
                    help="Shutdown test after N seconds (0 = use default wait_runtime behavior)")

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

    # Auto-generate agent hosts if needed (only for remote mode)
    autogenerated_hosts = None
    if args.mode == "remote" and not args.agent_hosts and not args.agent_hosts_file:
        # e.g., agents=30 -> "agent-1,agent-2,...,agent-30"
        count = int (args.agents / args.agents_per_host)
        autogenerated_hosts = ",".join(f"agent-{i}" for i in range(1, count + 1))
        log(f"[defaults] Auto-generated --agent-hosts: {autogenerated_hosts}")

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

        if args.sleep_between > 0 and i > 1:
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
        if args.topology == "hierarchical":
            cmd += ["--hierarchical-level1-agent-type", args.hierarchical_level1_agent_type]
        if args.groups is not None:
            cmd += ["--groups", str(args.groups)]
        if args.group_size is not None:
            cmd += ["--group-size", str(args.group_size)]
        if args.agents_per_host is not None:
            cmd += ["--agents-per-host", str(args.agents_per_host)]
        if args.mode == "remote":
            if args.agent_hosts_file:
                cmd += ["--agent-hosts-file", args.agent_hosts_file]
            elif args.agent_hosts:
                cmd += ["--agent-hosts", args.agent_hosts]
            elif autogenerated_hosts:
                cmd += ["--agent-hosts", autogenerated_hosts]
        if args.remote_repo_dir:
            cmd += ["--remote-repo-dir", args.remote_repo_dir]
        if args.debug:
            cmd.append("--debug")

        # Dynamic agent addition parameters
        if args.dynamic_agents > 0:
            cmd += ["--dynamic-agents", str(args.dynamic_agents)]
            cmd += ["--dynamic-trigger", args.dynamic_trigger]
            if args.dynamic_trigger == "time":
                cmd += ["--dynamic-delay", str(args.dynamic_delay)]
            elif args.dynamic_trigger == "bucket":
                cmd += ["--dynamic-trigger-bucket", str(args.dynamic_trigger_bucket)]
                cmd += ["--dynamic-trigger-threshold", str(args.dynamic_trigger_threshold)]
            elif args.dynamic_trigger == "jobs-completed":
                cmd += ["--dynamic-trigger-jobs", str(args.dynamic_trigger_jobs)]

        # Shutdown timer parameter
        if args.shutdown_after_seconds > 0:
            cmd += ["--shutdown-after-seconds", str(args.shutdown_after_seconds)]

        log(f"[{run_name}] Starting test…")
        (logs_dir / "batch_runner.log").write_text(
            f"CMD: {' '.join(shlex.quote(c) for c in cmd)}\nSTART: {datetime.now().isoformat()}\n"
        )

        rc, out = run_blocking(cmd)
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
