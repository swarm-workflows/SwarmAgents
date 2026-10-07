#!/usr/bin/env python3
# MIT License
#
# Copyright (c) 2024 swarm-workflows
#
# Author: Komal Thareja(kthare10@renci.org)
"""Run an evaluation campaign unattended: cells x repeats, gated, classified, resumable.

Run on the `database` node as root. One campaign file lists the cells; this driver runs each
repeat through `run_test.py` (or `baselines/run_sparrow.py`), one at a time, and records every
attempt in `<out>/campaign_state.json`, so a re-invocation resumes where the last one stopped.

    python campaign.py campaign.yml               # run (or resume)
    python campaign.py campaign.yml --dry-run     # print every command, run nothing
    python campaign.py campaign.yml --only e0-hier30,e1-hier90-snow

Campaign file (YAML or JSON)::

    out: runs/campaign-1
    defaults:                       # merged under every cell
      runner: run_test.py           # or baselines/run_sparrow.py
      repeats: 5
      retries: 1                    # extra attempts after a failed one
      timeout_s: 7200               # wall-clock kill for one attempt
      args:                         # --key value; true -> --key; false/null -> omitted
        mode: remote
        agent-type: resource
        db-host: database
        agent-hosts-file: agent_hosts.txt
        master-fleet-size: 270
        seed: 42
    gate:
      redis: {host: database, port: 6379}
      clock_check: ./fix_slice_clocks.sh --check     # "" to skip
      retries: 3                    # gate attempts before the repeat is skipped
      wait_s: 300                   # between gate attempts
    cells:
      - name: e0-hier30-pbft
        args: {agents: 30, jobs: 600, topology: hierarchical, groups-per-coordinator: 1,
               hierarchical-level1-agent-type: resource, runtime: 3600}
        extra: ["--some-flag"]      # appended verbatim
        # Optional: a command run beside each attempt ({run_dir} is substituted), in its own
        # session, SIGTERMed when the attempt ends. E6 uses it for partition.py. A companion
        # that exits non-zero makes the attempt `companion_failed` (retried): the cell did not
        # measure what it says it measured.
        companion: "python3.11 partition.py run --hosts-file agent_hosts.txt ... --out {run_dir}/partition.json"

**Before each repeat, a health gate.** Every host the cell needs (the first
`ceil(agents / agents_per_host)` of its hosts file) must answer ssh — probed with
`StrictHostKeyChecking=accept-new`, so a rebuilt host is not misread as down — Redis must answer,
and the clock check must exit 0. A failing gate is retried after `wait_s`; if it still fails,
the repeat is recorded `gate_failed` and skipped, rather than burning a night on a broken fleet.

**Each attempt is classified from the runner's exit status and `drain.json`:**

| outcome | meaning | next |
|---|---|---|
| `ok` | exit 0, drained / all terminal | done |
| `ok_on_cap` | exit 0, ended on `--runtime` or the timer | done — a collapse cell ends this way; read its `drain.json` |
| `ok_infeasible` | exit 0, stopped as infeasible | done, flagged |
| `ok_unverified` | exit 0, no `drain.json` | done, flagged |
| `refused` | exit 2: the runner refused the launch (a configuration error) | **campaign stops**; it would repeat on every retry |
| `metrics_shortfall` | exit 3 | retried |
| `unmeasurable` | exit 4: Redis unreadable or the job producer failed | retried |
| `timeout` | killed after `timeout_s` | retried; the next run reaps its agents |
| `failed` | any other status | retried |
| `companion_failed` | the run was fine but its companion (e.g. the partition) exited non-zero | retried |

A failed attempt's run directory is kept, renamed `<run>.attempt<k>`, for the post-mortem.
Exit status: 0 when every repeat ended `ok*`, 1 when any did not, 2 when stopped on a refusal.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import shlex
import signal
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, List, Optional

ROOT = Path(__file__).resolve().parent

DONE = {"ok", "ok_on_cap", "ok_infeasible", "ok_unverified"}
RETRY = {"metrics_shortfall", "unmeasurable", "timeout", "failed", "companion_failed"}


def log(msg: str, logfile: Optional[Path] = None) -> None:
    line = f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}"
    print(line, flush=True)
    if logfile is not None:
        with open(logfile, "a") as fh:
            fh.write(line + "\n")


# --------------------------------------------------------------------------- campaign file

def load_campaign(path: Path) -> dict:
    text = path.read_text()
    if path.suffix == ".json":
        spec = json.loads(text)
    else:
        from swarm.utils.yaml_strict import safe_load
        spec = safe_load(text)
    if not isinstance(spec, dict) or not spec.get("cells"):
        raise SystemExit(f"{path}: a campaign needs a non-empty `cells` list")
    names = [c.get("name") for c in spec["cells"]]
    if any(not n for n in names) or len(set(names)) != len(names):
        raise SystemExit(f"{path}: every cell needs a unique `name`")
    return spec


def cell_config(spec: dict, cell: dict) -> dict:
    """The cell with the campaign defaults merged under it (args merged key by key)."""
    defaults = spec.get("defaults") or {}
    merged = {**defaults, **cell}
    merged["args"] = {**(defaults.get("args") or {}), **(cell.get("args") or {})}
    merged["extra"] = list(defaults.get("extra") or []) + list(cell.get("extra") or [])
    merged.setdefault("runner", "run_test.py")
    merged.setdefault("repeats", 1)
    merged.setdefault("retries", 1)
    merged.setdefault("timeout_s", 7200)
    return merged


def cell_argv(cfg: dict, run_dir: Path, python: str = sys.executable) -> List[str]:
    argv = [python, str(ROOT / cfg["runner"])]
    for key, value in cfg["args"].items():
        if key == "run-dir":
            raise SystemExit(f"cell {cfg['name']}: run-dir is set by the driver, not the cell")
        if value is None or value is False:
            continue
        if value is True:
            argv.append(f"--{key}")
        else:
            argv += [f"--{key}", str(value)]
    argv += ["--run-dir", str(run_dir)]
    argv += [str(x) for x in cfg["extra"]]
    return argv


def hosts_needed(cfg: dict) -> List[str]:
    a = cfg["args"]
    if a.get("mode", "remote") != "remote" or not a.get("agent-hosts-file"):
        return []
    path = Path(a["agent-hosts-file"])
    if not path.is_absolute():
        path = ROOT / path
    hosts = [h.strip() for h in path.read_text().splitlines()
             if h.strip() and not h.startswith("#")]
    agents = int(a.get("agents", 0)) + int(a.get("dynamic-agents", 0) or 0)
    need = math.ceil(agents / int(a.get("agents-per-host", 1) or 1))
    return hosts[:need] if need <= len(hosts) else hosts + [f"<missing-{i}>"
                                                             for i in range(need - len(hosts))]


# --------------------------------------------------------------------------- health gate

def ssh_up(host: str) -> bool:
    if host.startswith("<missing-"):
        return False
    proc = subprocess.run(
        ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=6",
         "-o", "StrictHostKeyChecking=accept-new", host, "hostname"],
        capture_output=True, text=True, timeout=30)
    return proc.returncode == 0


class Gate:
    def __init__(self, spec: dict, ssh_check: Callable[[str], bool] = ssh_up,
                 redis_check: Optional[Callable[[], bool]] = None,
                 clock_check: Optional[Callable[[], bool]] = None):
        g = spec.get("gate") or {}
        self.retries = int(g.get("retries", 3))
        self.wait_s = float(g.get("wait_s", 300))
        self.ssh_check = ssh_check
        self.redis_cfg = g.get("redis")
        self.clock_cmd = g.get("clock_check", "./fix_slice_clocks.sh --check")
        self.redis_check = redis_check or self._redis
        self.clock_check = clock_check or self._clock

    def _redis(self) -> bool:
        if not self.redis_cfg:
            return True
        try:
            import redis
            return bool(redis.StrictRedis(host=self.redis_cfg.get("host", "localhost"),
                                          port=int(self.redis_cfg.get("port", 6379)),
                                          socket_timeout=5).ping())
        except Exception:
            return False

    def _clock(self) -> bool:
        if not self.clock_cmd:
            return True
        return subprocess.call(shlex.split(self.clock_cmd), cwd=str(ROOT),
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) == 0

    def check(self, hosts: List[str]) -> List[str]:
        """Problems found, empty when the gate passes."""
        problems = []
        if hosts:
            with ThreadPoolExecutor(max_workers=40) as pool:
                down = [h for h, ok in zip(hosts, pool.map(self.ssh_check, hosts)) if not ok]
            if down:
                problems.append(f"{len(down)} host(s) down: {', '.join(down[:8])}"
                                + ("…" if len(down) > 8 else ""))
        if not self.redis_check():
            problems.append("redis not answering")
        if not self.clock_check():
            problems.append("clock check failed")
        return problems


# --------------------------------------------------------------------------- classification

def classify(rc: Optional[int], run_dir: Path, timed_out: bool) -> str:
    if timed_out:
        return "timeout"
    if rc == 2:
        return "refused"
    if rc == 3:
        return "metrics_shortfall"
    if rc == 4:
        return "unmeasurable"
    if rc != 0:
        return "failed"
    try:
        status = json.loads((run_dir / "drain.json").read_text()).get("status")
    except (OSError, ValueError):
        return "ok_unverified"
    if status in ("drained", "all_terminal"):
        return "ok"
    if status in ("cap", "timer"):
        return "ok_on_cap"
    if status == "infeasible":
        return "ok_infeasible"
    if status in ("redis_unreadable", "producer_failed", "startup_refused"):
        return "unmeasurable"
    return "ok_unverified"


def kill_session(pgid: int, grace_s: float = 60.0, proc=None) -> None:
    """SIGTERM a run's whole session, then SIGKILL what is left. Silent when it is gone.

    *proc*, when this driver started the leader, is reaped while waiting: an exited but
    unreaped leader still answers `killpg(pgid, 0)`, which would read as alive for the whole
    grace period."""
    for sig, wait in ((signal.SIGTERM, grace_s), (signal.SIGKILL, 5.0)):
        try:
            os.killpg(pgid, sig)
        except (ProcessLookupError, PermissionError):
            return
        deadline = time.time() + wait
        while time.time() < deadline:
            if proc is not None:
                proc.poll()
            if not session_alive(pgid):
                return
            time.sleep(0.2)


def session_alive(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def start_companion(command: str, run_dir: Path) -> subprocess.Popen:
    run_dir.parent.mkdir(parents=True, exist_ok=True)
    argv = shlex.split(command.replace("{run_dir}", str(run_dir)))
    return subprocess.Popen(argv, cwd=str(ROOT), start_new_session=True,
                            stdout=open(f"{run_dir}.companion.log", "w"),
                            stderr=subprocess.STDOUT)


def stop_companion(proc: Optional[subprocess.Popen], grace_s: float = 180.0) -> Optional[int]:
    """SIGTERM the companion (partition.py heals on it) and wait; its exit status."""
    if proc is None:
        return None
    if proc.poll() is None:
        try:
            os.killpg(proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            proc.wait(timeout=grace_s)
        except subprocess.TimeoutExpired:
            kill_session(proc.pid, grace_s=5.0, proc=proc)
            proc.wait()
    return proc.returncode


def run_attempt(argv: List[str], run_dir: Path, timeout_s: float,
                on_start: Optional[Callable[[int], None]] = None) -> tuple:
    """Run one attempt; returns (exit status or None, timed out?).

    The run gets its own session so a timeout can kill everything it started — which also means
    it would OUTLIVE this driver. So the session is killed on every way out of here, an
    interrupt or SIGTERM to the driver included, and its id is handed to *on_start* at once so
    a driver that dies harder than that leaves a record the next one can act on. A run left
    behind would share Redis with the run a resumed campaign starts next.
    """
    run_dir.parent.mkdir(parents=True, exist_ok=True)
    with open(f"{run_dir}.log", "w") as out:
        proc = subprocess.Popen(argv, cwd=str(ROOT), stdout=out, stderr=subprocess.STDOUT,
                                start_new_session=True)
        try:
            if on_start is not None:
                on_start(proc.pid)
            return proc.wait(timeout=timeout_s), False
        except subprocess.TimeoutExpired:
            # The whole session: run_test.py and anything it started locally. Remote agents
            # are reaped by the next run_test.py before it flushes Redis.
            kill_session(proc.pid, proc=proc)
            proc.wait()
            return None, True
        finally:
            if proc.poll() is None:          # interrupted: never leave the run behind
                kill_session(proc.pid, grace_s=30.0, proc=proc)
                proc.wait()


# --------------------------------------------------------------------------- driver

def keep_failed_attempt(run_dir: Path) -> Optional[Path]:
    """Move a previous attempt's directory AND its log aside under the first free
    `<run>.attempt<k>` name, so no attempt overwrites another's evidence."""
    log_path = Path(f"{run_dir}.log")
    if not run_dir.exists() and not log_path.exists():
        return None
    k = 1
    while (run_dir.with_name(f"{run_dir.name}.attempt{k}").exists()
           or Path(f"{run_dir.with_name(f'{run_dir.name}.attempt{k}')}.log").exists()):
        k += 1
    kept = run_dir.with_name(f"{run_dir.name}.attempt{k}")
    if run_dir.exists():
        run_dir.rename(kept)
    if log_path.exists():
        log_path.rename(Path(f"{kept}.log"))
    return kept


class _Terminated(Exception):
    """SIGTERM or SIGHUP to the driver, raised in the main thread so `finally` blocks run."""


def process_start(pid: int) -> Optional[str]:
    """The process's start time as `ps` reports it, or None when there is no such process."""
    try:
        out = subprocess.run(["ps", "-o", "lstart=", "-p", str(int(pid))], capture_output=True,
                             text=True, timeout=10).stdout.strip()
    except Exception:
        return None
    return out or None


def session_is_ours(pgid: int, run_dir: str, started: Optional[str]) -> bool:
    """Is session *pgid* still led by the exact process this driver started for *run_dir*?

    All three must hold: the leader (pid == pgid) exists, its start time is the one recorded at
    launch — a pid reused since has a different one — and its command line carries this
    attempt's run dir. Anything weaker kills other people's work: matching on campaign-looking
    commands would take out another run_test.py, or an agent started by hand. A session whose
    leader is gone cannot be proven ours and is left alone; the cell's stop path, which runs
    either way, is what handles its agents.
    """
    if not run_dir or not started:
        return False
    if process_start(pgid) != started:
        return False
    try:
        cmd = subprocess.run(["ps", "-o", "command=", "-p", str(int(pgid))], capture_output=True,
                             text=True, timeout=10).stdout
    except Exception:
        return False
    return run_dir in cmd


def stop_cell_agents(cfg: dict) -> None:
    """Stop what a killed attempt started beyond its own session.

    Killing the runner's session stops it and everything it started locally, not the agents it
    launched over ssh: those keep running, keep the fleet busy and keep writing to Redis. The
    next run_test.py reaps them before it flushes, but nothing does when there is no next run —
    an interrupted campaign, or the last cell of one. So every killed attempt is followed by the
    runner's own stop path for that cell's hosts.
    """
    a = cfg["args"]
    mode = a.get("mode", "remote")
    if cfg["runner"].endswith("run_sparrow.py"):
        pattern = shlex.quote("sparrow_node.py")
        hosts = [h for h in hosts_needed(cfg) if not h.startswith("<missing-")]
        if mode != "remote" or not hosts:
            subprocess.call(["pkill", "-TERM", "-f", "sparrow_node.py"],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            return
        with ThreadPoolExecutor(max_workers=40) as pool:
            list(pool.map(lambda h: subprocess.call(
                ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=6",
                 "-o", "StrictHostKeyChecking=accept-new", h,
                 f"pkill -TERM -f {pattern}; sleep 3; pkill -KILL -f {pattern}; true"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=60), hosts))
        return
    cmd = ["bash", "stop_agents_v2.sh", "--mode", mode]
    if mode == "remote":
        cmd += ["--agent-hosts-file", str(a.get("agent-hosts-file", "agent_hosts.txt")),
                "--remote-repo-dir", str(a.get("remote-repo-dir", "/root/SwarmAgents"))]
    subprocess.call(cmd, cwd=str(ROOT), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                    timeout=600)


class Campaign:
    def __init__(self, spec: dict, out: Path, gate: Gate, dry_run: bool = False,
                 runner: Callable = run_attempt, sleep: Callable[[float], None] = time.sleep,
                 python: str = sys.executable, only: Optional[List[str]] = None,
                 continue_on_refusal: bool = False,
                 stopper: Callable[[dict], None] = stop_cell_agents,
                 companion_starter: Callable = start_companion,
                 ours: Callable[[int, str, Optional[str]], bool] = session_is_ours):
        self.spec, self.out, self.gate = spec, out, gate
        self.dry_run, self.runner, self.sleep, self.python = dry_run, runner, sleep, python
        self.only = set(only) if only else None
        self.continue_on_refusal = continue_on_refusal
        self.stopper, self.ours = stopper, ours
        self.companion_starter = companion_starter
        self.state_path = out / "campaign_state.json"
        self.logfile = out / "campaign.log"
        self.state = self._load_state()

    def _load_state(self) -> dict:
        try:
            return json.loads(self.state_path.read_text())
        except (OSError, ValueError):
            return {}

    def _save_state(self) -> None:
        tmp = self.state_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.state, indent=2))
        os.replace(tmp, self.state_path)

    def _log(self, msg: str) -> None:
        log(msg, None if self.dry_run else self.logfile)

    def run(self) -> int:
        if self.dry_run:
            return self._run()
        self.out.mkdir(parents=True, exist_ok=True)
        import fcntl
        lock = open(self.out / "campaign.lock", "w")
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self._log(f"REFUSED: another campaign driver holds {self.out / 'campaign.lock'}")
            return 2
        # SIGHUP as well as SIGTERM: a driver started in an ssh session to `database` gets a
        # SIGHUP when that session drops, and the default action ends it with no cleanup.
        previous = {}

        def _term(*_):
            raise _Terminated()

        for sig in (signal.SIGTERM, signal.SIGHUP):
            try:
                previous[sig] = signal.signal(sig, _term)
            except ValueError:                 # not the main thread (tests): no handler
                pass
        try:
            self._recover_interrupted()
            return self._run()
        except (_Terminated, KeyboardInterrupt):
            self._log("Driver interrupted; the running attempt was stopped and recorded")
            return 130
        finally:
            for sig, handler in previous.items():
                signal.signal(sig, handler)
            lock.close()

    def _recover_interrupted(self) -> None:
        """An attempt recorded as running belongs to a driver that died without cleaning up
        (the lock says no other driver is alive). Stop its run if it is still going — it would
        share Redis with the next one — and record the attempt as interrupted."""
        cells = {c["name"]: cell_config(self.spec, c) for c in self.spec["cells"]}
        for key, entry in self.state.items():
            running = entry.pop("running", None)
            if not running:
                continue
            pgid = int(running.get("pgid") or 0)
            if pgid and session_alive(pgid):
                if self.ours(pgid, str(running.get("run_dir") or ""),
                             running.get("leader_start")):
                    self._log(f"{key}: stopping the run a dead driver left behind "
                              f"(session {pgid})")
                    kill_session(pgid)
                else:
                    self._log(f"{key}: session {pgid} is alive but its leader is not the "
                              f"process this campaign started (id reused, or leader gone); "
                              f"not killed")
            # Its remote agents outlive the session either way.
            cfg = cells.get(key.split("/")[0])
            if cfg is not None:
                self._stop_agents(cfg, key)
            entry.setdefault("attempts", []).append(
                {"outcome": "interrupted", "at": running.get("at")})
            entry["outcome"] = "interrupted"
        self._save_state()

    def _stop_agents(self, cfg: dict, key: str) -> None:
        try:
            self.stopper(cfg)
        except Exception as exc:
            self._log(f"{key}: WARNING could not confirm the cell's agents stopped: {exc}")

    def _selected_keys(self) -> List[str]:
        keys = []
        for cell in self.spec["cells"]:
            cfg = cell_config(self.spec, cell)
            if self.only and cfg["name"] not in self.only:
                continue
            keys += [f"{cfg['name']}/run{r:02d}" for r in range(1, int(cfg["repeats"]) + 1)]
        return keys

    def _run(self) -> int:
        stopped = False
        for cell in self.spec["cells"]:
            cfg = cell_config(self.spec, cell)
            if self.only and cfg["name"] not in self.only:
                continue
            for rep in range(1, int(cfg["repeats"]) + 1):
                key = f"{cfg['name']}/run{rep:02d}"
                if self.state.get(key, {}).get("outcome") in DONE:
                    continue
                outcome = self._repeat(cfg, key, self.out / cfg["name"] / f"run{rep:02d}")
                if outcome == "refused" and not self.continue_on_refusal:
                    self._log(f"STOPPING: {key} was refused — fix the cell's configuration "
                              f"(see {self.out / cfg['name']}/run{rep:02d}.log) and re-run; "
                              f"finished repeats are kept")
                    stopped = True
                    break
            if stopped:
                break
        return self._summary(stopped)

    def _repeat(self, cfg: dict, key: str, run_dir: Path) -> str:
        argv = cell_argv(cfg, run_dir, self.python)
        if self.dry_run:
            print(f"{key}: {' '.join(shlex.quote(a) for a in argv)}")
            return "ok"
        entry = self.state.setdefault(key, {"attempts": []})
        hosts = hosts_needed(cfg)
        outcome = "failed"
        for attempt in range(len(entry["attempts"]), len(entry["attempts"]) + 1 + int(cfg["retries"])):
            problems = []
            for g in range(self.gate.retries):
                problems = self.gate.check(hosts)
                if not problems:
                    break
                self._log(f"{key}: gate failed ({'; '.join(problems)}); "
                          f"retry {g + 1}/{self.gate.retries} in {self.gate.wait_s:.0f}s")
                self.sleep(self.gate.wait_s)
            if problems:
                outcome = "gate_failed"
                entry["attempts"].append({"outcome": outcome, "problems": problems,
                                          "at": time.time()})
                break
            keep_failed_attempt(run_dir)
            self._log(f"{key}: attempt {attempt + 1} — {' '.join(argv[1:3])} …")
            started = time.time()

            def _started(pgid, entry=entry, started=started):
                # Persisted before the run does anything, so a driver killed hard leaves a
                # record the next one uses to stop this run (see _recover_interrupted).
                entry["running"] = {"pgid": pgid, "at": started, "run_dir": str(run_dir),
                                    "leader_start": process_start(pgid)}
                self._save_state()

            companion = (self.companion_starter(cfg["companion"], run_dir)
                         if cfg.get("companion") else None)
            try:
                rc, timed_out = self.runner(argv, run_dir, float(cfg["timeout_s"]),
                                            on_start=_started)
            except BaseException:
                stop_companion(companion)
                # The session is already dead (run_attempt's finally); its remote agents are
                # not. Stop them before giving up the attempt, then record it.
                self._stop_agents(cfg, key)
                entry.pop("running", None)
                entry["attempts"].append({"outcome": "interrupted", "at": started})
                entry["outcome"] = "interrupted"
                self._save_state()
                raise
            if timed_out:
                self._stop_agents(cfg, key)
            entry.pop("running", None)
            companion_rc = stop_companion(companion)
            outcome = classify(rc, run_dir, timed_out)
            # Exactly 0, nothing else: an unknown status is not a companion that did its job.
            if companion is not None and companion_rc != 0 and outcome in DONE:
                outcome = "companion_failed"
            attempt_rec = {"outcome": outcome, "exit": rc, "at": started,
                           "duration_s": round(time.time() - started, 1)}
            if companion is not None:
                attempt_rec["companion_exit"] = companion_rc
            entry["attempts"].append(attempt_rec)
            self._log(f"{key}: {outcome} (exit {rc}, {time.time() - started:.0f}s)")
            if outcome not in RETRY:
                break
        entry["outcome"] = outcome
        entry["run_dir"] = str(run_dir)
        self._save_state()
        return outcome

    def _summary(self, stopped: bool) -> int:
        if self.dry_run:
            return 0
        # Over the repeats THIS invocation was asked for — not cells --only excluded or a
        # campaign file no longer lists — and a selected repeat never attempted is not done.
        keys = self._selected_keys()
        outcomes = [self.state.get(k, {}).get("outcome", "not_run") for k in keys]
        counts: Dict[str, int] = {}
        for o in outcomes:
            counts[o] = counts.get(o, 0) + 1
        self._log("SUMMARY: " + ", ".join(f"{k} {v}" for k, v in sorted(counts.items())))
        if stopped:
            return 2
        return 0 if all(o in DONE for o in outcomes) else 1


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("campaign", type=Path)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only", default="", help="Comma-separated cell names to run.")
    ap.add_argument("--continue-on-refusal", action="store_true",
                    help="Record a refused cell and go on instead of stopping the campaign.")
    ap.add_argument("--python", default=sys.executable,
                    help="Interpreter for the runner (default: this one).")
    args = ap.parse_args(argv)
    sys.path.insert(0, str(ROOT))
    spec = load_campaign(args.campaign)
    out = Path(spec.get("out") or f"runs/{args.campaign.stem}")
    if not out.is_absolute():
        out = ROOT / out
    campaign = Campaign(spec, out, Gate(spec), dry_run=args.dry_run, python=args.python,
                        only=[c for c in args.only.split(",") if c],
                        continue_on_refusal=args.continue_on_refusal)
    return campaign.run()


if __name__ == "__main__":
    raise SystemExit(main())
