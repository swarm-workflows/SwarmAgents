"""T-2: the RTT matrix captured at run start, and its per-job join in the collector.

Pins: the probe measures from each agent's placement host to every advertised address, takes
the median, and records an unanswered pair as null (never 0); the collector bins a job by the
RTT between its executor and its delegating coordinator (hierarchical) or the executor's median
peer RTT (flat), refuses to bin what it cannot attribute, and emits nothing but the status for a
run whose matrix failed; run_test.py captures it before launch without ever failing the run, and
a refused launch exits 2, not 1.
"""
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

import rtt_matrix  # noqa: E402
from evaluation import rtt  # noqa: E402

PING = """PING {d} ({d}) 56(84) bytes of data.
64 bytes from {d}: icmp_seq=1 ttl=64 time={a} ms
64 bytes from {d}: icmp_seq=2 ttl=64 time={b} ms
64 bytes from {d}: icmp_seq=3 ttl=64 time={c} ms
"""


def _probe_output(values):
    return "".join(f"### {d}\n" + (PING.format(d=d, a=v[0], b=v[1], c=v[2]) if v else
                                   f"PING {d}\n--- 100% packet loss\n")
                   for d, v in values.items())


# --------------------------------------------------------------------------- probe

def test_the_probe_takes_the_median_and_reports_loss():
    parsed = rtt_matrix.parse_probe_output(
        _probe_output({"h2": (10.0, 30.0, 20.0), "h3": None}), samples=4)
    assert parsed["h2"]["rtt_ms"] == 20.0
    assert parsed["h2"]["loss"] == 0.25
    assert parsed["h3"]["rtt_ms"] is None and parsed["h3"]["loss"] == 1.0


def test_the_probe_script_pings_every_target_in_parallel_into_separate_files():
    script = rtt_matrix.probe_script(["a", "b"], samples=20, interval_s=0.2)
    assert "ping -n -c 20 -i 0.2" in script and "& done; wait" in script
    assert '"$tmp/$d"' in script


def test_measure_runs_each_row_on_the_placement_host_and_nulls_unanswered_pairs():
    calls = []
    table = {"h1": {"h2": (1, 1, 1), "h3": (95, 95, 95)},
             "h2": {"h1": (1, 1, 1), "h3": None},
             "h3": {"h1": (95, 95, 95), "h2": (50, 50, 50)}}

    def fake_ssh(host, script, timeout):
        calls.append(host)
        src = {"mgmt-1": "h1", "mgmt-2": "h2", "mgmt-3": "h3"}[host]
        return _probe_output(table[src])

    result = rtt_matrix.measure({1: "h1", 2: "h2", 3: "h3", 4: "h3"},
                                {1: "mgmt-1", 2: "mgmt-2", 3: "mgmt-3", 4: "mgmt-3"},
                                samples=3, ssh=fake_ssh, local=lambda s, t: "",
                                include_database=True)
    assert sorted(calls) == ["mgmt-1", "mgmt-2", "mgmt-3"], "one row per advertised host"
    assert result["rtt_ms"]["h1"]["h3"] == 95.0
    assert result["rtt_ms"]["h2"]["h3"] is None, "an unanswered pair is null, never 0"
    assert result["rtt_ms"]["h3"]["h3"] == 0.0
    assert result["status"] == "partial" and result["pairs_missing"] == 1
    assert result["agents"]["4"] == "h3"


def test_an_unreachable_source_is_a_null_row_not_a_failure_of_the_whole_capture():
    def fake_ssh(host, script, timeout):
        if host == "h2":
            raise RuntimeError("timed out")
        return _probe_output({"h1": (1, 1, 1), "h2": (2, 2, 2)})

    result = rtt_matrix.measure({1: "h1", 2: "h2"}, {1: "h1", 2: "h2"}, samples=3,
                                ssh=fake_ssh, include_database=False)
    assert result["rtt_ms"]["h2"]["h1"] is None
    assert "h2" in result["unreachable_sources"]
    assert result["status"] == "partial"


# --------------------------------------------------------------------------- collector

def _write_matrix(run: Path, agents, rows, status="captured"):
    (run / "rtt_matrix.json").write_text(json.dumps(
        {"status": status, "agents": {str(k): v for k, v in agents.items()},
         "rtt_ms": rows, "pairs_missing": 0}))


def _jobs(records):
    return pd.DataFrame(records)


def test_bins_are_contiguous():
    assert [rtt.rtt_bin(v) for v in (0.0, 4.9, 5.0, 39.9, 40.0, 89.9, 90.0, 200)] == [
        "lan", "lan", "regional", "regional", "continental", "continental",
        "transatlantic", "transatlantic"]
    assert rtt.rtt_bin(None) is None


def test_a_hierarchical_job_is_binned_by_executor_to_coordinator_rtt():
    with tempfile.TemporaryDirectory() as tmp:
        run = Path(tmp)
        _write_matrix(run, {1: "h1", 2: "h2", 30: "h30"},
                      {"h1": {"h2": 1.0, "h30": 95.0}, "h2": {"h1": 1.0, "h30": None},
                       "h30": {"h1": 95.0, "h2": 20.0}})
        (run / "level1_jobs.csv").write_text(
            "job_id,submitted_at,selection_started_at,assigned_at,started_at,completed_at,"
            "exit_status,leader_id\n"
            "a,1,1,3,0,0,0,30\n"
            "b,1,1,2,0,0,0,30\n"
            "c,1,1,2,0,0,0,\n")
        jobs = _jobs([
            {"job_id": "a", "leader_id": 1, "selection_started_at": 3, "assigned_at": 4,
             "completed_at": 9},
            {"job_id": "b", "leader_id": 2, "selection_started_at": 2, "assigned_at": 2.5,
             "completed_at": 0},
            {"job_id": "c", "leader_id": 1, "selection_started_at": 2, "assigned_at": 3,
             "completed_at": 5},
        ])
        rows = {r["job_id"]: r for r in rtt.job_rows(run, jobs)}
        assert rows["a"]["rtt_ms"] == 95.0 and rows["a"]["rtt_bin"] == "transatlantic"
        assert rows["a"]["l1_selection_s"] == 2.0 and rows["a"]["selection_s"] == 1.0
        assert rows["b"]["rtt_ms"] == 20.0, "a pair measured one way uses that direction"
        assert rows["c"]["rtt_bin"] is None, "no coordinator: unattributed, not guessed"
        cols = rtt.run_metrics(run, list(rows.values()))
        assert cols["rtt_jobs_unattributed"] == 1
        assert cols["rtt_transatlantic_jobs"] == 1 and cols["rtt_lan_jobs"] == 0
        assert cols["rtt_regional_completed_share"] == 0.0
        assert cols["rtt_kind"] == "coordinator"


def test_a_flat_job_uses_the_executors_median_peer_rtt():
    with tempfile.TemporaryDirectory() as tmp:
        run = Path(tmp)
        _write_matrix(run, {1: "h1", 2: "h2", 3: "h3"},
                      {"h1": {"h2": 10.0, "h3": 100.0}, "h2": {"h1": 10.0, "h3": 30.0},
                       "h3": {"h1": 100.0, "h2": 30.0}})
        jobs = _jobs([{"job_id": "a", "leader_id": 2, "selection_started_at": 1,
                       "assigned_at": 2, "completed_at": 3}])
        (row,) = rtt.job_rows(run, jobs)
        assert row["rtt_kind"] == "peer_median" and row["rtt_ms"] == 20.0


def test_a_failed_matrix_yields_only_its_status():
    with tempfile.TemporaryDirectory() as tmp:
        run = Path(tmp)
        (run / "rtt_matrix.json").write_text(json.dumps({"status": "failed", "error": "x"}))
        jobs = _jobs([{"job_id": "a", "leader_id": 1, "completed_at": 1}])
        assert rtt.job_rows(run, jobs) == []
        assert rtt.run_metrics(run, []) == {"rtt_matrix_status": "failed"}
        assert rtt.run_metrics(Path(tmp) / "nope", []) == {}


def test_the_collector_writes_job_rtt_csv_and_the_run_columns():
    from evaluation.collect import main as collect_main
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        run = root / "mesh-2" / "run01"
        run.mkdir(parents=True)
        (run / "all_jobs.csv").write_text(
            "job_id,submitted_at,selection_started_at,assigned_at,started_at,completed_at,"
            "exit_status,leader_id,reasoning_time,scheduling_latency\n"
            "a,1,1,2,2,9,0,1,0,1\n")
        _write_matrix(run, {1: "h1", 2: "h2"}, {"h1": {"h2": 12.0}, "h2": {"h1": 12.0}})
        sys_argv = sys.argv
        sys.argv = ["collect.py", "--root", str(root), "--out", str(root / "out")]
        try:
            assert collect_main() == 0
        finally:
            sys.argv = sys_argv
        wide = pd.read_csv(root / "out" / "runs_wide.csv")
        assert wide.loc[0, "rtt_regional_jobs"] == 1
        per_job = pd.read_csv(root / "out" / "job_rtt.csv")
        assert per_job.loc[0, "rtt_bin"] == "regional" and per_job.loc[0, "rtt_ms"] == 12.0


# --------------------------------------------------------------------------- run_test hook

def test_run_test_captures_before_launch_and_never_fails_the_run(tmp_path, monkeypatch):
    import run_test
    cfg = tmp_path / "configs"
    cfg.mkdir()
    for i, host in ((1, "agent-1"), (2, "agent-2")):
        (cfg / f"{run_test.CFG_PREFIX}{i}.yml").write_text(f"grpc:\n  host: {host}\n")
    args = SimpleNamespace(mode="remote", run_dir=str(tmp_path), config_dir=str(cfg),
                           agents=2, dynamic_agents=0, agents_per_host=1,
                           rtt_samples=5, rtt_interval=0.2)
    seen = {}

    def fake_measure(adv, placement, samples, interval_s):
        seen.update(adv=adv, placement=placement, samples=samples)
        return {"status": "captured", "pairs": 2, "pairs_missing": 0, "duration_s": 0.1}

    monkeypatch.setattr(rtt_matrix, "measure", fake_measure)
    run_test.capture_rtt_matrix(args, ["agent-1", "agent-2"])
    assert seen["adv"] == {1: "agent-1", 2: "agent-2"} and seen["samples"] == 5
    assert json.loads((tmp_path / "rtt_matrix.json").read_text())["status"] == "captured"

    def boom(*a, **k):
        raise RuntimeError("ssh mesh down")

    monkeypatch.setattr(rtt_matrix, "measure", boom)
    run_test.capture_rtt_matrix(args, ["agent-1", "agent-2"])     # must not raise
    assert json.loads((tmp_path / "rtt_matrix.json").read_text())["status"] == "failed"


def test_a_refused_launch_exits_2_not_1(tmp_path):
    """`--delegation-policy` with `--use-config-dir` is refused at the top of main()."""
    proc = subprocess.run(
        [sys.executable, "run_test.py", "--mode", "local", "--agent-type", "resource",
         "--agents", "1", "--jobs", "1",
         "--topology", "mesh", "--db-host", "localhost", "--run-dir", str(tmp_path / "r"),
         "--delegation-policy", "bandit", "--use-config-dir"],
        cwd=REPO, capture_output=True, text=True, timeout=60)
    assert proc.returncode == 2, proc.stderr[-500:]
    assert "--delegation-policy has no effect" in proc.stderr


# --------------------------------------------------------------------------- fleet DTNs

@pytest.mark.parametrize("topology", ["hierarchical", "mesh", "ring"])
def test_every_topology_gets_dtns_unless_asked_not_to(tmp_path, monkeypatch, topology):
    """Hierarchical fleets were generated with no DTNs, so every replay-golden job naming one
    was infeasible on every leaf. The generator is topology-blind; run_test.py now is too."""
    import run_test
    captured = []
    monkeypatch.setattr(run_test, "run_blocking", lambda cmd, check=True: captured.append(cmd))
    monkeypatch.setattr(sys, "argv", ["run_test.py", "--mode", "local", "--agent-type",
                                      "resource", "--agents", "30", "--jobs", "60",
                                      "--topology", topology, "--db-host", "localhost",
                                      "--run-dir", str(tmp_path / "r"),
                                      "--config-dir", str(tmp_path / "cfg"),
                                      "--agent-hosts-file", str(tmp_path / "hosts")])
    args = run_test.parse_args()
    run_test.generate_configs(args, ["localhost"])
    assert "--dtns" in captured[-1]
    args.no_dtns = True
    run_test.generate_configs(args, ["localhost"])
    assert "--dtns" not in captured[-1]
