"""Code review 2026-10-05 §47-§52: ways a run exited 0 for a fleet or workload it never measured.

§47 `--debug` was forwarded to job_distributor.py, which has no such flag: argparse exited 2 in
    a check=False daemon thread, zero jobs were published, the empty pool "drained", exit 0.
§48 The drain read an unreachable Redis as an empty pool (the miss branch was dead), and
    watched PENDING only, so the last wave still RUNNING was cut off with nothing saying so.
§49 Every launch pkilled every main.py on the host, dynamic additions included — in local mode
    that killed the whole initial fleet. The local start log was never written either.
§50 Agents left over from an earlier run on hosts outside this run's hosts file survived the
    flush, re-registered and joined the run.
§51 Nothing compared a config's advertised grpc.host with where the launcher put the agent —
    and for dynamic agents the two never agreed, because the launcher restarted them from the
    top of the host list while generate_configs.py numbered them on.
§52 Local mode launches from ./configs whatever --config-dir says.
"""
import argparse
import os
import sys
from pathlib import Path

import pytest
import yaml

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

import run_test  # noqa: E402


def _args(tmp_path, **over):
    base = dict(mode="remote", agents=4, dynamic_agents=0, agents_per_host=1,
                config_dir=str(tmp_path / "cfg"), run_dir=str(tmp_path / "run"),
                db_host="localhost", jobs_per_interval=10, jobs=10, topology="mesh",
                split_hybrid=False, debug=True, check_interval=0, inflight_drain_max_s=0,
                starter="swarm-multi-start.sh", remote_repo_dir="/root/SwarmAgents",
                agent_type="resource", jobs_per_proposal=10, groups=None, group_size=None)
    base.update(over)
    return argparse.Namespace(**base)


@pytest.fixture(autouse=True)
def _fresh_state():
    run_test._PRODUCER_RC.clear()
    run_test._DRAIN.clear()
    yield
    run_test._PRODUCER_RC.clear()
    run_test._DRAIN.clear()


# --------------------------------------------------------------------------- §47
class TestProducer:
    def test_debug_is_not_forwarded_to_the_distributor(self, tmp_path, monkeypatch):
        seen = []

        class _P:
            returncode = 0
        monkeypatch.setattr(run_test, "run_blocking", lambda cmd, **k: seen.append(cmd) or _P())
        monkeypatch.setattr(run_test, "jobs_dir", lambda a: "jobs")
        run_test.produce_jobs(_args(tmp_path, debug=True))
        assert "--debug" not in seen[0]

    def test_the_distributor_accepts_every_flag_it_is_given(self, tmp_path, monkeypatch):
        """The defect class, not the instance: every flag produce_jobs can pass must exist."""
        seen = []

        class _P:
            returncode = 0
        monkeypatch.setattr(run_test, "run_blocking", lambda cmd, **k: seen.append(cmd) or _P())
        monkeypatch.setattr(run_test, "jobs_dir", lambda a: "jobs")
        run_test.produce_jobs(_args(tmp_path, topology="hierarchical", agents=30,
                                    split_hybrid=True, debug=True))
        src = open(os.path.join(REPO, "job_distributor.py")).read()
        for tok in seen[0]:
            if tok.startswith("--"):
                assert f'"{tok}"' in src, f"job_distributor.py has no {tok}"

    def test_a_failed_producer_fails_the_run(self, tmp_path, monkeypatch):
        class _P:
            returncode = 2
        monkeypatch.setattr(run_test, "run_blocking", lambda cmd, **k: _P())
        monkeypatch.setattr(run_test, "jobs_dir", lambda a: "jobs")
        run_test.produce_jobs(_args(tmp_path))
        assert run_test.producer_failed()
        assert "exited 2" in run_test.run_failed_to_measure()


# --------------------------------------------------------------------------- §48
class TestDrain:
    def test_unreachable_redis_is_unknown_not_zero(self):
        text = "Error connecting to Redis at nohost:6379: Error -2 connecting"
        assert run_test.parse_bucket_set_count(text, 1) is None
        assert run_test.parse_bucket_set_count("", 1) is None
        assert run_test.parse_bucket_set_count("Traceback (most recent call last):\n x", 1) is None

    def test_reachable_and_empty_is_zero(self):
        assert run_test.parse_bucket_set_count("No objects found in queue 'state'.", 1) == 0

    def test_counts_are_summed_over_levels_and_groups(self):
        text = ("state:0:0:1: {'job:a', 'job:b'}\n"
                "state:1:0:1: {'job:c'}\n"
                "state:0:0:5: {'job:d'}\n")
        assert run_test.parse_bucket_set_count(text, 1) == 3
        assert run_test.parse_bucket_set_count(text, 5) == 1

    def test_unreadable_redis_ends_the_wait_as_a_failure(self, tmp_path, monkeypatch):
        monkeypatch.setattr(run_test, "run_once", lambda cmd: "Error connecting to Redis")
        monkeypatch.setattr(run_test.time, "sleep", lambda s: None)
        args = _args(tmp_path, runtime=0, watch_bucket=1, threshold=5, stable_seconds=0,
                     max_misses=2, grace_seconds=0)
        run_test.wait_runtime(args)
        assert run_test._DRAIN["status"] == "redis_unreadable"
        assert run_test.run_failed_to_measure()
        assert (Path(args.run_dir) / "drain.json").exists()

    def test_inflight_jobs_are_waited_for_and_a_truncation_recorded(self, tmp_path, monkeypatch):
        counts = iter([3, 1, 0])
        monkeypatch.setattr(run_test, "_inflight_count", lambda a: next(counts))
        monkeypatch.setattr(run_test.time, "sleep", lambda s: None)
        run_test._wait_for_inflight(_args(tmp_path, inflight_drain_max_s=60))
        assert run_test._DRAIN["inflight_at_stop"] == 0

        run_test._DRAIN.clear()
        monkeypatch.setattr(run_test, "_inflight_count", lambda a: 4)
        run_test._wait_for_inflight(_args(tmp_path, inflight_drain_max_s=0))
        assert run_test._DRAIN["inflight_at_stop"] == 4


# --------------------------------------------------------------------------- §49
class TestDynamicLaunchDoesNotKill:
    def test_the_starter_guards_the_kill(self):
        src = open(os.path.join(REPO, "swarm-multi-start.sh")).read()
        guard = src.index('if [[ "$add" != true ]]; then')
        assert src.index('pkill -f "python3', guard) < src.index("\nfi\n", guard)
        assert src.index("rm -f shutdown", guard) < src.index("\nfi\n", guard)
        assert src.count("pkill -f") == 1

    def test_local_dynamic_passes_add_and_initial_does_not(self, tmp_path, monkeypatch):
        calls = []
        monkeypatch.setattr(run_test, "run_blocking",
                            lambda cmd, **k: calls.append((cmd, k.get("log_file"))))
        args = _args(tmp_path, mode="local", starter=os.path.join(REPO, "swarm-multi-start.sh"))
        run_test.start_agents_local(args)
        run_test.start_agents_local(args, agent_count=2, start_offset=4)
        (initial, ilog), (dynamic, dlog) = calls
        assert "--add" not in initial and "--add" in dynamic
        # The redirect is a real log file now, not literal argv.
        for cmd in (initial, dynamic):
            assert ">" not in cmd and "&" not in cmd and "2>&1" not in cmd
        assert ilog == "local_agents_initial_start.log" and dlog == "local_agents_dynamic_start.log"


# --------------------------------------------------------------------------- §51
def _write_configs(cfg_dir: Path, hosts_by_agent: dict):
    cfg_dir.mkdir(parents=True, exist_ok=True)
    for i, host in hosts_by_agent.items():
        (cfg_dir / f"{run_test.CFG_PREFIX}{i}.yml").write_text(
            yaml.safe_dump({"grpc": {"host": host, "port": 20000 + i}}))


class TestRemotePlacement:
    def _launch(self, tmp_path, monkeypatch, args, hosts, start_offset=0, count=None):
        placed = {}
        monkeypatch.setattr(run_test, "ssh_check", lambda host, cmd, **k: None)
        monkeypatch.setattr(run_test, "scp_to",
                            lambda host, src, dst: placed.setdefault(
                                int(Path(src).stem.rsplit("_", 1)[1]), host))
        run_test.start_agents_remote(args, hosts, agent_count=count, start_offset=start_offset)
        return placed

    def test_dynamic_agents_run_where_their_configs_say(self, tmp_path, monkeypatch):
        """4 initial + 2 dynamic, 1 per host: agents 5 and 6 belong on hosts 5 and 6. The
        launcher put them on hosts 1 and 2 — already occupied, and not what they advertise."""
        hosts = [f"agent-{i}" for i in range(1, 7)]
        args = _args(tmp_path, agents=4, dynamic_agents=2)
        _write_configs(Path(args.config_dir), {i: hosts[i - 1] for i in range(1, 7)})
        placed = self._launch(tmp_path, monkeypatch, args, hosts)
        placed.update(self._launch(tmp_path, monkeypatch, args, hosts, start_offset=4, count=2))
        assert placed == {i: hosts[i - 1] for i in range(1, 7)}

    def test_uneven_blocks_follow_the_config_arithmetic(self, tmp_path, monkeypatch):
        """2 per host, 3 initial + 2 dynamic: agent 4 shares host 2 with agent 3."""
        hosts = ["h1", "h2", "h3"]
        args = _args(tmp_path, agents=3, dynamic_agents=2, agents_per_host=2)
        _write_configs(Path(args.config_dir), {i: hosts[(i - 1) // 2] for i in range(1, 6)})
        placed = self._launch(tmp_path, monkeypatch, args, hosts)
        placed.update(self._launch(tmp_path, monkeypatch, args, hosts, start_offset=3, count=2))
        assert placed == {1: "h1", 2: "h1", 3: "h2", 4: "h2", 5: "h3"}

    def test_remote_dynamic_passes_add(self, tmp_path, monkeypatch):
        cmds = []
        hosts = ["h1", "h2"]
        args = _args(tmp_path, agents=1, dynamic_agents=1)
        _write_configs(Path(args.config_dir), {1: "h1", 2: "h2"})
        monkeypatch.setattr(run_test, "ssh_check", lambda host, cmd, **k: cmds.append(cmd))
        monkeypatch.setattr(run_test, "scp_to", lambda *a: None)
        run_test.start_agents_remote(args, hosts)
        run_test.start_agents_remote(args, hosts, agent_count=1, start_offset=1)
        starts = [c for c in cmds if "nohup bash" in c]
        assert "--add" not in starts[0] and "--add" in starts[1]


class TestLaunchCheck:
    def test_matching_configs_pass(self, tmp_path):
        args = _args(tmp_path, agents=2, dynamic_agents=1)
        hosts = ["h1", "h2", "h3"]
        _write_configs(Path(args.config_dir), {1: "h1", 2: "h2", 3: "h3"})
        run_test.check_launch_matches_configs(args, hosts)

    def test_a_wildcard_is_refused(self, tmp_path):
        args = _args(tmp_path, agents=2)
        _write_configs(Path(args.config_dir), {1: "h1", 2: "0.0.0.0"})
        with pytest.raises(SystemExit, match="wildcard"):
            run_test.check_launch_matches_configs(args, ["h1", "h2"])

    def test_a_shifted_hosts_file_is_refused(self, tmp_path):
        """One node dropped from the hosts file shifts every later agent."""
        args = _args(tmp_path, agents=3)
        _write_configs(Path(args.config_dir), {1: "h1", 2: "h2", 3: "h3"})
        with pytest.raises(SystemExit, match="advertises h2, launcher places it on h3"):
            run_test.check_launch_matches_configs(args, ["h1", "h3", "h4"])

    def test_a_different_agents_per_host_is_refused(self, tmp_path):
        args = _args(tmp_path, agents=2, agents_per_host=2)
        _write_configs(Path(args.config_dir), {1: "h1", 2: "h2"})
        with pytest.raises(SystemExit):
            run_test.check_launch_matches_configs(args, ["h1", "h2"])

    def test_local_mode_refuses_a_config_dir_it_will_not_launch_from(self, tmp_path):
        with pytest.raises(SystemExit, match="./configs"):
            run_test.check_launch_matches_configs(
                _args(tmp_path, mode="local", config_dir="cfg-arm-b"), [])

    def test_local_mode_with_the_default_dir_passes(self, tmp_path):
        run_test.check_launch_matches_configs(_args(tmp_path, mode="local", config_dir="configs"), [])


# --------------------------------------------------------------------------- §50
class _FakeRedis:
    def __init__(self, live):
        self.kv = {"agent:0:0:7": '{"host": "agent-77"}', "agent:0:0:8": '{"host": "agent-78"}'}
        self.live = set(live)            # keys whose writer is still running

    def ping(self):
        return True

    def scan_iter(self, match="*", count=None):
        return [k for k in list(self.kv) if k.startswith(match.rstrip("*"))]

    def get(self, k):
        return self.kv.get(k)

    def delete(self, k):
        self.kv.pop(k, None)
        if k in self.live:                # a live agent writes its key again next tick
            self.kv[k] = '{"host": "agent-77"}'


class TestLeftoverAgents:
    def _redis(self, monkeypatch, fake):
        import redis
        monkeypatch.setattr(redis, "StrictRedis", lambda **k: fake)

    def test_a_live_leftover_refuses_the_launch(self, tmp_path, monkeypatch):
        self._redis(monkeypatch, _FakeRedis(live={"agent:0:0:7"}))
        with pytest.raises(SystemExit, match="agent-77"):
            run_test.refuse_if_agents_still_registering(_args(tmp_path), settle_s=0)

    def test_dead_records_inside_their_ttl_do_not(self, tmp_path, monkeypatch):
        """A crashed agent's record lives on for its TTL; it is deleted, not mistaken for a
        live agent."""
        self._redis(monkeypatch, _FakeRedis(live=set()))
        run_test.refuse_if_agents_still_registering(_args(tmp_path), settle_s=0)
