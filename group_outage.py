#!/usr/bin/env python3
# MIT License
#
# Copyright (c) 2024 swarm-workflows
#
# Author: Komal Thareja(kthare10@renci.org)
"""Take one leaf group out of a running cell and bring it back (E2b, F5).

What happens: every agent of one level-0 group is killed with **SIGKILL** at once, the group stays
down for `--duration` seconds, and then every one of them is restarted, the way the run started it
(same config, same `SWARM_RUN_ID`), so the group **rejoins**. Its coordinator should stop
delegating to it while it is down (liveness gate, delegation timeouts) and re-adopt it after it
returns. F5 is the time to re-adoption, measured from `rejoined_at` in the record this driver
writes. `--no-rejoin` leaves the group down for the rest of the run.

Run on the `database` node as root, as a campaign `companion:` (started with the attempt,
SIGTERMed when it ends):

    python3.11 group_outage.py run --hosts-file agent_hosts.txt --agents 90 --group 4 \\
        --duration 180 --start-after-log {run_dir}.log --delay 120 --run-dir {run_dir} \\
        --out {run_dir}/outage.json

**Who is killed.** The members of `--group` are read from the run's generated configs
(`--config-dir`, `configs/` by default: `topology.level == 0` and `topology.group == G`). Each one's
host is placed by the hosts file exactly as `run_test.py` places it: host index
`(id - 1) // agents_per_host`. A group with no members, or one with a coordinator in it, is refused.

**Verified, not assumed.** After the kill, every member's process must be gone (`pgrep`). After the
restart, every member's process must be up 5 s later. The record carries the members, hosts,
`killed_at`, `rejoined_at`, the verification on each host and `held_s`.

**Declare the kill.** A SIGKILLed agent writes no metrics for its first life. With rejoin, the new
process writes metrics at the end of the run as usual, but its `executed_jobs` covers its second
life only, so `jobs_executed_twice` is a lower bound for those agents. With `--no-rejoin` the
members never report, so the cell must pass `--expect-silent-agents <ids>` (the ids are in the
record), or the run's completeness gate fails it.

**Stopped early** (SIGTERM, SIGINT or SIGHUP, for example because the run ended): the driver does
**not** restart anyone. A restart after the run has stopped its agents would leave a stray agent
running into the next cell. It records what happened and exits 3.

**Exit status:** 0 the group went down on every host, was verified down, held for the full duration
and (unless `--no-rejoin`) came back verified on every host; 1 a host failed to kill or restart, or a
verification disagreed; 2 the run never started; 3 the outage never happened, or was held for less
than planned, or the driver was stopped before the rejoin.
"""
from __future__ import annotations

import argparse
import json
import shlex
import signal
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, List, Tuple

SSH_OPTS = ["-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
            "-o", "StrictHostKeyChecking=accept-new"]
REMOTE_REPO = "/root/SwarmAgents"


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def ssh_run(host: str, script: str, timeout: float = 60.0) -> Tuple[int, str]:
    proc = subprocess.run(["ssh", *SSH_OPTS, host, script], capture_output=True, text=True,
                          timeout=timeout)
    return proc.returncode, (proc.stdout + proc.stderr).strip()


def read_lines(path: str) -> List[str]:
    return [l.strip() for l in Path(path).read_text().splitlines() if l.strip()]


def group_members(config_dir: str, group: int) -> Dict[int, str]:
    """{agent_id: agent_type} for every level-0 agent of `group` in the generated configs."""
    import yaml
    members = {}
    for path in Path(config_dir).glob("config_swarm_multi_*.yml"):
        stem = path.stem.rsplit("_", 1)[-1]
        if not stem.isdigit():
            continue
        cfg = yaml.safe_load(path.read_text()) or {}
        topo = cfg.get("topology") or {}
        if int(topo.get("level", 0) or 0) == 0 and int(topo.get("group", -1)) == int(group):
            members[int(stem)] = str(cfg.get("agent_type", "resource") or "resource")
    if not members:
        raise SystemExit(f"group {group} has no level-0 members in {config_dir}")
    return dict(sorted(members.items()))


def place(members: Dict[int, str], hosts: List[str], agents_per_host: int) -> Dict[int, str]:
    out = {}
    for aid in members:
        idx = (aid - 1) // agents_per_host
        if idx >= len(hosts):
            raise SystemExit(f"agent {aid} needs host #{idx + 1}; the hosts file has {len(hosts)}")
        out[aid] = hosts[idx]
    return out


def _pattern(aid: int) -> str:
    """The agent's process, and nothing else, for `pgrep -f` / `pkill -f`.

    Anchored at the START of the command line on the interpreter: the remote `bash -c` that runs
    our own kill or restart script also has `main.py <id>` in its command line (the restart script
    launches it literally), so an unanchored pattern matched that shell — the restart guard then
    reported ALREADY for every agent and the post-launch check reported UP whether or not the
    agent started (stop-time review). The id is anchored on a space or the end of the line, so
    `main.py 5` never matches agent 50. An interpreter path (`/usr/bin/python3.11`) is allowed."""
    return f"^(\\S*/)?python3(\\.[0-9]+)? main\\.py {aid}( |$)"


def kill_script(aid: int) -> str:
    p = shlex.quote(_pattern(aid))
    return f"pkill -9 -f {p}; sleep 1; if pgrep -f {p} >/dev/null; then echo ALIVE; else echo GONE; fi"


def restart_script(aid: int, agent_type: str, run_id: str) -> str:
    """Start the agent again, unless a process for it is already running: a second `main.py <id>`
    would be a duplicate agent with the same id, advertising the same address (stop-time review).
    ALREADY is reported, never UP, so a caller cannot read it as a successful rejoin."""
    p = shlex.quote(_pattern(aid))
    launch = (f"source ~/.profile >/dev/null 2>&1; export SWARM_RUN_ID={shlex.quote(run_id)} && "
              f"cd {REMOTE_REPO} && "
              f"(nohup python3.11 main.py {aid} --agent-type {shlex.quote(agent_type)} "
              f">> agent_{aid}_rejoin.log 2>&1 < /dev/null &)")
    return (f"if pgrep -f {p} >/dev/null; then echo ALREADY; else "
            f"{launch}; sleep 5; if pgrep -f {p} >/dev/null; then echo UP; else echo DOWN; fi; fi")


def wait_for_log(path: str, marker: str, timeout_s: float) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            if marker in Path(path).read_text(errors="replace"):
                return True
        except OSError:
            pass
        time.sleep(2.0)
    return False


def _fanout(fn, items):
    with ThreadPoolExecutor(max_workers=max(1, min(32, len(items)))) as pool:
        return list(pool.map(fn, items))


class _Stop(Exception):
    pass


def cmd_run(args) -> int:
    hosts = read_lines(args.hosts_file)
    members = group_members(args.config_dir, args.group)
    if any(aid > args.agents for aid in members):
        raise SystemExit(f"group {args.group} has members beyond --agents {args.agents}: {list(members)}")
    placed = place(members, hosts, args.agents_per_host)
    record = {"group": args.group, "members": list(members), "hosts": {str(a): h for a, h in placed.items()},
              "duration_s": args.duration, "rejoin": not args.no_rejoin,
              "expect_silent_agents": ",".join(str(a) for a in members) if args.no_rejoin else ""}
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    def write():
        out.write_text(json.dumps(record, indent=2))

    def _term(*_):
        raise _Stop()

    for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(sig, _term)

    status, held_from, killed = 0, None, False
    run_id = args.run_id
    try:
        if args.start_after_log:
            log(f"waiting for {args.start_marker!r} in {args.start_after_log}")
            if not wait_for_log(args.start_after_log, args.start_marker, args.start_timeout):
                record["outcome"] = "run_never_started"
                write()
                return 2
        # Read after the run has started: run_test.py writes run_meta.json at launch, which can be
        # after this companion was started.
        if not run_id and args.run_dir:
            meta = Path(args.run_dir) / "run_meta.json"
            if meta.exists():
                run_id = json.loads(meta.read_text()).get("run_id", "")
        record["run_id"] = run_id
        if not args.no_rejoin and not run_id:
            record["outcome"] = "no_run_id"
            log("no run id (pass --run-id, or --run-dir holding run_meta.json): a rejoined agent "
                "without SWARM_RUN_ID writes metrics the run cannot match; not starting the outage")
            write()
            return 3
        if args.delay:
            log(f"outage in {args.delay:.0f} s")
            time.sleep(args.delay)
        log(f"killing group {args.group}: agents {list(members)} on {sorted(set(placed.values()))}")
        killed = True
        record["killed_at"] = time.time()
        results = _fanout(lambda a: (a, ssh_run(placed[a], kill_script(a))), list(members))
        record["kill"] = {str(a): {"rc": rc, "verdict": outp.splitlines()[-1] if outp else ""}
                          for a, (rc, outp) in results}
        down = all(v["verdict"] == "GONE" for v in record["kill"].values())
        if not down:
            log(f"kill NOT verified: {record['kill']}")
            status = 1
        held_from = time.time()
        write()
        time.sleep(args.duration)
        if not args.no_rejoin:
            # Only members whose kill was VERIFIED are restarted. One whose process survived the
            # kill (or whose kill could not be checked) is still running, and starting it again
            # would put two agents with one id in the run (stop-time review).
            dead = [a for a in members if record["kill"][str(a)]["verdict"] == "GONE"]
            skipped = [a for a in members if a not in dead]
            if skipped:
                record["rejoin_skipped_alive"] = skipped
                log(f"not restarting {skipped}: their kill was not verified")
            log(f"restarting group {args.group}: {dead}")
            record["rejoined_at"] = time.time()
            results = _fanout(lambda a: (a, ssh_run(placed[a], restart_script(a, members[a], run_id))),
                              dead)
            record["rejoin_check"] = {str(a): {"rc": rc, "verdict": outp.splitlines()[-1] if outp else ""}
                                      for a, (rc, outp) in results}
            if skipped or not all(v["verdict"] == "UP" for v in record["rejoin_check"].values()):
                log(f"rejoin NOT verified: {record['rejoin_check']}")
                status = 1
            else:
                log("group back up and verified")
    except _Stop:
        log("stopped early; NOT restarting anyone")
        record["stopped_early"] = True
    finally:
        held = round(max(0.0, time.time() - held_from), 1) if held_from is not None else 0.0
        if "rejoined_at" in record and held_from is not None:
            held = round(record["rejoined_at"] - held_from, 1)
        record["held_s"] = held
        if not killed:
            record["outcome"] = record.get("outcome", "not_applied")
            status = status or 3
        elif held + 1.0 < args.duration:
            record["outcome"] = "held_short"
            status = status or 3
        elif not args.no_rejoin and "rejoin_check" not in record:
            record["outcome"] = "not_rejoined"
            status = status or 3
        record.setdefault("outcome", "ok" if status == 0 else "problems")
        write()
        if status:
            log(f"outage {record['outcome']}: held {held} s of {args.duration:.0f}; exit {status}")
    return status


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="kill one leaf group, hold, restart it")
    r.add_argument("--hosts-file", required=True)
    r.add_argument("--agents", type=int, required=True)
    r.add_argument("--agents-per-host", type=int, default=1)
    r.add_argument("--group", type=int, required=True, help="level-0 group index to take down")
    r.add_argument("--config-dir", default="configs", help="the run's generated configs")
    r.add_argument("--duration", type=float, default=180.0, help="seconds the group stays down")
    r.add_argument("--no-rejoin", action="store_true", help="leave the group down for the rest of the run")
    r.add_argument("--delay", type=float, default=0.0, help="seconds after the start marker")
    r.add_argument("--start-after-log", default="")
    r.add_argument("--start-marker", default="Job distribution started")
    r.add_argument("--start-timeout", type=float, default=1800.0)
    r.add_argument("--run-dir", default="", help="the run's directory (run_meta.json gives the run id)")
    r.add_argument("--run-id", default="", help="SWARM_RUN_ID for the restarted agents")
    r.add_argument("--out", required=True, help="outage record (JSON)")
    args = ap.parse_args(argv)
    return cmd_run(args)


if __name__ == "__main__":
    sys.exit(main())
