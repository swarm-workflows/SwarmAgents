"""Tests for the figure-D ablation plumbing in `scenarios/helpers.py`.

The ablation (`llm.disable_fallback: true`) is the one experiment in the campaign that is
*invisible when it fails to apply*. A run whose configs never got the flag produces an ordinary
S05: same fault, same 503s, plausible numbers, and no marker anywhere saying the ablation did not
happen. That is how the first S05 100% no-fallback attempt ended up unreadable.

So these pin the two guards that make it visible:

1. `set_disable_fallback()` writes the flag into the configs the agents actually load — the
   per-agent `configs/` files, not the base YAML, which `--use-config-dir` never reads — and
   refuses a partial application rather than reporting it as a smaller blast radius.
2. `collect()` counts `LLM_COST_NO_BID`, because under the flag a failed call leaves *no* trace
   in `llm_fallback`: the run reports "fallback rate 0.0%", which is also what a healthy fleet and
   an inert ablation report.

Plus one for `snapshot_agent_logs()`, whose job is to get the per-agent evidence off the hosts
before the next `cleanup()` deletes it.
"""
from __future__ import annotations

import os
import sys

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO_ROOT, "scenarios"))

helpers = pytest.importorskip("helpers", reason="scenarios/helpers.py not importable")

# The population check compares against the fleet the run says it launched, never against the
# module's AGENTS — so a fixture orchestrator log has to carry the line that states it.
LAUNCHED = "[18:25:47] Launched {n} initial 'llm' agents across {n} remote host(s).\n"

# Trimmed to the shape that matters: a top-level `llm:` block with siblings around it, so a
# misanchored insert lands somewhere provably wrong.
CONFIG = """\
runtime:
  jobs_per_proposal: 10
llm:
  enabled: true
  provider: ollama
  model: "gpt-oss-20b"
  use_for_selection: true
  prompts:
    cost: 'score this job'
topology:
  peer_agents: 30
"""


@pytest.fixture
def fake_repo(tmp_path, monkeypatch):
    """A throwaway repo with `n` per-agent configs, standing in for the frozen fleet."""
    monkeypatch.setattr(helpers, "REPO", str(tmp_path))

    def build(n: int = 3, body: str = CONFIG) -> list[str]:
        cfg = tmp_path / "configs"
        cfg.mkdir(exist_ok=True)
        paths = []
        for i in range(1, n + 1):
            p = cfg / f"config_swarm_multi_{i}.yml"
            p.write_text(body)
            paths.append(str(p))
        return paths

    return build


def _flag_lines(path: str) -> list[str]:
    return [ln for ln in open(path).read().splitlines()
            if ln.strip().startswith("disable_fallback:")]


def test_flag_is_written_into_the_llm_block_of_every_config(fake_repo):
    paths = fake_repo(3)
    assert helpers.set_disable_fallback(True) == 3
    for p in paths:
        lines = open(p).read().splitlines()
        assert _flag_lines(p) == ["  disable_fallback: true"]
        # Directly under `llm:`, at the block's indentation. Landing under `runtime:` or inside
        # `prompts:` would parse fine and be ignored, which is the silent-no-op case.
        assert lines[lines.index("llm:") + 1] == "  disable_fallback: true"


def test_turning_it_off_strips_the_flag_so_a_plain_s05_cannot_inherit_it(fake_repo):
    fake_repo(3)
    helpers.set_disable_fallback(True)
    assert helpers.set_disable_fallback(False) == 0
    for p in sorted(os.listdir(os.path.join(helpers.REPO, "configs"))):
        assert _flag_lines(os.path.join(helpers.REPO, "configs", p)) == []


def test_reapplying_does_not_stack_duplicate_keys(fake_repo):
    paths = fake_repo(2)
    helpers.set_disable_fallback(True)
    helpers.set_disable_fallback(True)
    for p in paths:
        assert _flag_lines(p) == ["  disable_fallback: true"]


def test_a_config_without_an_llm_block_is_refused_not_silently_skipped(fake_repo):
    fake_repo(1, body="runtime:\n  jobs_per_proposal: 10\n")
    with pytest.raises(SystemExit):
        helpers.set_disable_fallback(True)


def test_no_configs_at_all_is_refused_when_arming(tmp_path, monkeypatch):
    monkeypatch.setattr(helpers, "REPO", str(tmp_path))
    with pytest.raises(SystemExit):
        helpers.set_disable_fallback(True)


def test_clearing_with_no_configs_is_a_no_op_not_a_failure(tmp_path, monkeypatch):
    """clear_faults.py clears the flag before asserting, so this path must never raise —
    otherwise the recovery tool fails on a slice whose fleet has not been generated yet."""
    monkeypatch.setattr(helpers, "REPO", str(tmp_path))
    assert helpers.set_disable_fallback(False) == 0


def test_assert_clean_refuses_a_leaked_ablation(fake_repo, monkeypatch):
    """The leak this guards is silent and cross-scenario: a leftover `true` is invisible in every
    host-side probe, and the next S01 or baseline run would measure the ablation as if it were
    the fault. Only the *config* check can see it."""
    fake_repo(3)
    monkeypatch.setattr(helpers, "hosts", lambda: ["agent-1"])
    monkeypatch.setattr(helpers, "_fan_out", lambda *a, **k: "0 0 0\n")

    helpers.assert_clean()  # clean fleet, flag absent -> passes

    helpers.set_disable_fallback(True)
    with pytest.raises(SystemExit, match="disable_fallback"):
        helpers.assert_clean()

    # And the documented way out actually works, rather than leaving the operator stuck between
    # a scenario that refuses to run and a teardown that refuses to clear.
    helpers.set_disable_fallback(False)
    helpers.assert_clean()


def test_no_bid_is_counted_and_kept_separate_from_fallback(tmp_path, monkeypatch):
    """The load-bearing metric: 352 refused bids must not read as a fault-free fleet.

    `fallback_rate` stays 0 here — correctly, nothing fell back — so if `llm_no_bid` were not
    collected, this run and a healthy one would be identical in every reported number.
    """
    monkeypatch.setattr(helpers, "REPO", str(tmp_path))
    run = "runs/nofb"
    (tmp_path / run / "agent-1").mkdir(parents=True)
    (tmp_path / run / "agent-1" / "agent-1.log").write_text(
        "[LLM_COST_NO_BID] Job=1 Agent=1 FallbackDisabled\n"
        "[LLM_COST_NO_BID] Job=2 Agent=1 FallbackDisabled\n"
        "[LLM_COST_COMPLETE] Job=3 Agent=1 Score=42.0 ReasoningTime=4.100s\n"
    )
    (tmp_path / "runs_nofb.log").write_text(LAUNCHED.format(n=1) +
        "\n[all] Jobs per agent:\n  Agent 1: 1 jobs\n")

    m = helpers.collect(run)
    assert m["llm_no_bid"] == 2
    assert m["llm_fallback"] == 0
    assert m["fallback_rate"] == 0.0
    # 2 refusals out of 3 attempts — the denominator includes the successful call, so this is
    # readable as "a third of bids got through", not as a bare count.
    assert m["no_bid_rate"] == round(2 / 3, 4)


def test_the_log_population_is_reported_as_a_metric(tmp_path, monkeypatch):
    """A failed collection must not publish a partial sum as a fleet total.

    Only 2 of 30 logs are present here, so `llm_no_bid` is short by 28 agents' worth. The count
    of logs read has to travel with the numbers, because the table gets quoted on its own.
    """
    monkeypatch.setattr(helpers, "REPO", str(tmp_path))
    run = "runs/partial"
    for host in ("agent-1", "agent-2"):
        (tmp_path / run / host).mkdir(parents=True)
        (tmp_path / run / host / f"{host}.log").write_text("[LLM_COST_NO_BID] Job=1\n")
    (tmp_path / "runs_partial.log").write_text(LAUNCHED.format(n=30) +
        "\n[all] Jobs per agent:\n  Agent 1: 1 jobs\n")

    m = helpers.collect(run)
    assert m["agent_logs"] == 2
    assert m["fleet_size"] == helpers.AGENTS
    assert m["agent_logs_missing"] == helpers.AGENTS - 2
    assert m["agent_logs_extra"] == 0
    # metrics.json is absent here, so `restarts` fell back to counting log lines and is NOT
    # independent of log collection. report() has to place it accordingly.
    assert m["restarts_source"] == "agent logs"


def test_a_matching_log_count_does_not_pass_as_a_matching_population(tmp_path, monkeypatch,
                                                                     capsys):
    """The parity trap: 2 logs for a 2-agent fleet, but they are agent 1 twice and agent 2 never.

    File counts cancel out — `fleet - len(logs)` is 0 — while every sum double-counts agent 1 and
    omits agent 2, and load_split() silently keeps only one of the duplicates. Only an ID check
    sees it.
    """
    monkeypatch.setattr(helpers, "REPO", str(tmp_path))
    monkeypatch.setattr(helpers, "AGENTS", 2)
    run = "runs/dup"
    for host in ("host-a", "host-b"):
        (tmp_path / run / host).mkdir(parents=True)
        (tmp_path / run / host / "agent-1.log").write_text("[LLM_COST_NO_BID] Job=1\n")
    (tmp_path / "runs_dup.log").write_text(LAUNCHED.format(n=2) +
        "\n[all] Jobs per agent:\n  Agent 1: 1 jobs\n")

    m = helpers.collect(run)
    assert m["agent_logs"] == 2 and m["fleet_size"] == 2      # counts match...
    assert m["agent_ids_missing"] == [2]                       # ...population does not
    assert m["agent_ids_duplicated"] == [1]
    assert m["agent_logs_missing"] == 1 and m["agent_logs_extra"] == 1
    assert m["llm_no_bid"] == 2                                # agent 1 counted twice

    helpers.report("S05", "test", {"llm_complete": 1}, m, ["nothing"])
    out = capsys.readouterr().out
    assert "no log for agent(s): 2" in out
    assert "more than one log for agent(s): 1" in out
    assert "double-counted" in out and "deduplicated in load_split()" in out
    # Short AND over-counted at once, so the sums bound the truth from neither side. Calling them
    # lower bounds here would be the same false claim as calling a mean one.
    assert "wrong in BOTH directions" in out and "not a bound in either direction" in out
    assert "lower bounds" not in out


def test_more_logs_than_agents_is_reported_not_clamped_to_zero_missing(tmp_path, monkeypatch,
                                                                       capsys):
    """`max(0, fleet - logs)` would print "0 missing" while every sum silently includes an agent
    outside the fleet — a stray log from a larger run, or a host that started two agents."""
    monkeypatch.setattr(helpers, "REPO", str(tmp_path))
    monkeypatch.setattr(helpers, "AGENTS", 2)
    run = "runs/extra"
    for i in (1, 2, 3):
        (tmp_path / run / f"agent-{i}").mkdir(parents=True)
        (tmp_path / run / f"agent-{i}" / f"agent-{i}.log").write_text("[LLM_COST_NO_BID] Job=1\n")
    (tmp_path / "runs_extra.log").write_text(LAUNCHED.format(n=2) +
        "\n[all] Jobs per agent:\n  Agent 1: 1 jobs\n")

    m = helpers.collect(run)
    assert m["agent_logs"] == 3 and m["fleet_size"] == 2
    assert m["agent_logs_missing"] == 0 and m["agent_logs_extra"] == 1

    helpers.report("S05", "test", {"llm_complete": 1}, m, ["nothing"])
    out = capsys.readouterr().out
    assert "3 agent log(s) for a fleet of 2, covering 2 of 2 agents" in out
    assert "log(s) for agent(s) outside the fleet: 3" in out
    assert "over-counted" in out and "inflated" in out


def test_identity_comes_from_the_body_not_the_filename(tmp_path, monkeypatch, capsys):
    """A filename is a label applied by whoever copied the file; the agent writes its own id into
    every line. When they disagree the file was fetched into the wrong place, and crediting the
    filename certifies a population that is not the one being summed."""
    monkeypatch.setattr(helpers, "REPO", str(tmp_path))
    monkeypatch.setattr(helpers, "AGENTS", 2)
    run = "runs/mislabel"
    (tmp_path / run / "agent-1").mkdir(parents=True)
    (tmp_path / run / "agent-1" / "agent-1.log").write_text(
        "2026-08-22 18:24:43,245 - agent-1 - INFO - [LLM_COST_NO_BID] Job=1\n")
    (tmp_path / run / "agent-2").mkdir(parents=True)
    # Named for agent 2, written by agent 1 — a wrong-dir fetch.
    (tmp_path / run / "agent-2" / "agent-2.log").write_text(
        "2026-08-22 18:24:43,245 - agent-1 - INFO - [LLM_COST_NO_BID] Job=2\n")
    (tmp_path / "runs_mislabel.log").write_text(LAUNCHED.format(n=2) +
        "\n[all] Jobs per agent:\n  Agent 1: 1 jobs\n")

    m = helpers.collect(run)
    assert m["agent_ids_mislabelled"] == ["agent-2.log written by agent-1"]
    assert m["agent_ids_missing"] == [2]          # agent 2 really has no log
    assert m["agent_logs_extra"] == 1             # and the mislabelled one is not credited to it
    assert m["llm_no_bid"] == 2                   # its contents are still in the sums

    helpers.report("S05", "test", {"llm_complete": 1}, m, ["nothing"])
    out = capsys.readouterr().out
    assert "names a different agent than their filename" in out
    assert "their contents ARE in every sum above" in out


def test_an_unknown_fleet_withholds_the_verdict_instead_of_guessing(tmp_path, monkeypatch, capsys):
    """`fleet_size` comes from the orchestrator log. Without one, comparing against the module's
    AGENTS would validate a 10- or 60-agent run against 30 — this repo has both, from the
    fleet-size sweep. Unverifiable must not render as clean."""
    monkeypatch.setattr(helpers, "REPO", str(tmp_path))
    run = "runs/noorchestrator"
    (tmp_path / run / "agent-1").mkdir(parents=True)
    (tmp_path / run / "agent-1" / "agent-1.log").write_text(
        "2026-08-22 18:24:43,245 - agent-1 - INFO - [LLM_COST_NO_BID] Job=1\n")

    m = helpers.collect(run)
    assert m["agent_population_verified"] is False
    assert m["agent_logs"] == 1
    for key in ("agent_ids_missing", "agent_logs_missing", "agent_logs_extra"):
        assert m[key] is None                      # blank in the table, not a reassuring zero

    helpers.report("S05", "test", {"llm_complete": 1}, m, ["nothing"])
    out = capsys.readouterr().out
    assert "population could not be checked" in out
    assert "blank rather than zero" in out


def test_a_log_removed_mid_read_is_counted_not_raised(tmp_path, monkeypatch, capsys):
    """collect() runs after a ~20 minute run; losing the whole report to one disappearing file is
    worse than reporting a smaller population."""
    monkeypatch.setattr(helpers, "REPO", str(tmp_path))
    monkeypatch.setattr(helpers, "AGENTS", 1)
    run = "runs/racy"
    (tmp_path / run / "agent-1").mkdir(parents=True)
    (tmp_path / run / "agent-1" / "agent-1.log").write_text("x\n")
    (tmp_path / "runs_racy.log").write_text(LAUNCHED.format(n=1) +
        "\n[all] Jobs per agent:\n  Agent 1: 1 jobs\n")

    real_open = open

    def vanishing(path, *a, **k):
        if str(path).endswith("agent-1.log"):
            raise FileNotFoundError(path)
        return real_open(path, *a, **k)

    monkeypatch.setitem(__builtins__ if isinstance(__builtins__, dict) else vars(__builtins__),
                        "open", vanishing)
    try:
        m = helpers.collect(run)
    finally:
        monkeypatch.undo()

    assert m["agent_logs_vanished"] == 1
    helpers.report("S05", "test", {"llm_complete": 1}, m, ["nothing"])
    assert "disappeared between being listed and being read" in capsys.readouterr().out


def test_metrics_json_backed_restarts_are_declared_independent_of_the_logs(capsys):
    """The mirror of the fallback case: with metrics.json present the row survives a partial
    collection, and saying otherwise would understate what the run still measured."""
    fault = {"agent_logs": 22, "agent_logs_missing": 8, "agent_logs_extra": 0, "fleet_size": 30,
             "restarts": 0, "restarts_source": "metrics.json"}
    helpers.report("S05", "test", {"llm_complete": 1}, fault, ["nothing"])
    banner = capsys.readouterr().out.split("agent log(s) for a fleet")[1]
    sums, independent = banner.split("independent of log collection")
    assert "job restarts" not in sums          # not demoted to a log sum...
    assert "restart/conflict counts" in independent and "metrics.json" in independent

    # ...and the fallback case says the opposite, in the sums clause where it belongs.
    fault["restarts_source"] = "agent logs"
    helpers.report("S05", "test", {"llm_complete": 1}, fault, ["nothing"])
    banner = capsys.readouterr().out.split("agent log(s) for a fleet")[1]
    sums, independent = banner.split("independent of log collection")
    assert "job restarts" in sums and "no metrics.json" in sums
    assert "restart/conflict counts" not in independent


def test_report_refuses_to_present_partial_sums_as_fleet_totals(capsys):
    """The warning belongs next to the numbers it qualifies, and it must not over-claim.

    Counts over fewer logs are lower bounds. Rates and means are NOT — they are another
    population's statistics, wrong in whichever direction the absent hosts differed, and a missing
    slow bidder pulls the latency mean down. Calling those floors would be a new false claim in
    place of the old silent one.
    """
    fault = {"llm_complete": 400, "latency_mean_s": 4.1, "fallback_rate": 0.0,
             "agent_logs": 22, "agent_logs_missing": 8, "fleet_size": 30}
    helpers.report("S05", "test", {"llm_complete": 1123}, fault, ["nothing"])
    out = capsys.readouterr().out
    assert "agent logs read" in out and "agents with no log" in out
    assert "22 agent log(s) for a fleet of 30, covering 22 of 30 agents" in out

    # Search the banner, not the table above it — the row labels repeat the metric names.
    out = out[out.index("agent log(s) for a fleet"):]
    lower, biased = out.index("lower bounds"), out.index("UNKNOWN direction")
    # The sums are named as bounds; the rates and means are named as biased, and each label sits in
    # its own clause rather than one blanket sentence over both.
    for count_metric in ("LLM calls OK", "LLM no-bids", "restart/reselect log lines"):
        assert lower < out.index(count_metric) < biased
    for derived in ("fallback rate", "bid latency mean/p95", "LLM score mean/sd"):
        assert out.index(derived) > biased
    assert "not floors, not ceilings" in out
    # And an unverifiable baseline population is said out loud rather than assumed complete.
    assert "records no log population" in out

    helpers.report("S05", "test", {"llm_complete": 1123, "agent_logs": 30},
                   {"llm_complete": 1100, "agent_logs": 30, "agent_logs_missing": 0}, ["nothing"])
    quiet = capsys.readouterr().out
    assert "agent logs are missing" not in quiet and "records no log population" not in quiet


def test_zero_attempts_does_not_divide_by_zero(tmp_path, monkeypatch):
    monkeypatch.setattr(helpers, "REPO", str(tmp_path))
    (tmp_path / "runs" / "quiet" / "agent-1").mkdir(parents=True)
    (tmp_path / "runs" / "quiet" / "agent-1" / "agent-1.log").write_text("nothing to see\n")
    m = helpers.collect("runs/quiet")
    assert m["no_bid_rate"] == 0.0 and m["fallback_rate"] == 0.0


@pytest.fixture
def s05(monkeypatch, fake_repo):
    """The S05 scenario module, with every remote-touching helper stubbed out.

    `set_disable_fallback` is deliberately NOT stubbed — these tests are about whether the real
    thing gets called on the paths that can fail, so it runs against the fake repo's configs and
    the assertion is the state left on disk.
    """
    import importlib.util

    fake_repo(3)
    calls = {"stop_fault": 0, "cleanup": 0}
    for name, fn in (
        ("hosts", lambda: [f"agent-{i}" for i in range(1, 31)]),
        ("assert_clean", lambda *a, **k: None),
        ("health_gate", lambda *a, **k: None),
        ("start_fault", lambda *a, **k: None),
        ("run_swarm", lambda *a, **k: None),
        ("collect", lambda *a, **k: {}),
        ("load_reference", lambda *a, **k: {}),
        ("report", lambda *a, **k: None),
        ("load_split", lambda *a, **k: {}),
        ("print_split", lambda *a, **k: None),
    ):
        monkeypatch.setattr(helpers, name, fn)
    monkeypatch.setattr(helpers, "cleanup",
                        lambda *a, **k: calls.__setitem__("cleanup", calls["cleanup"] + 1))
    monkeypatch.setattr(helpers, "stop_fault",
                        lambda *a, **k: calls.__setitem__("stop_fault", calls["stop_fault"] + 1))
    monkeypatch.setenv("CJ_DISABLE_FALLBACK", "1")
    # main() reads its blast radius from argv, and pytest's argv is not a fraction.
    monkeypatch.setattr(sys, "argv", ["s05_unavailable.py", "1.0", "nofb"])

    path = os.path.join(REPO_ROOT, "scenarios", "api", "s05_unavailable.py")
    spec = importlib.util.spec_from_file_location("s05_unavailable_under_test", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod, calls


def _armed(n: int = 3) -> int:
    return sum(1 for p in sorted(glob_configs()) if _flag_lines(p))


def glob_configs() -> list[str]:
    from glob import glob
    return glob(os.path.join(helpers.REPO, "configs", "config_swarm_multi_*.yml"))


def test_the_scenario_no_longer_owns_the_ablation_lifecycle(s05, monkeypatch):
    """Arming moved into run_swarm() so all four scenarios share one lifecycle. The scenario must
    not mutate the configs itself — a second writer is a second chance to leak one."""
    mod, calls = s05
    monkeypatch.setattr(helpers, "cleanup",
                        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("fan-out timed out")))
    with pytest.raises(RuntimeError):
        mod.main()
    assert _armed() == 0          # never armed by the scenario at all
    assert calls["stop_fault"] == 1


def test_a_failed_run_disarms_and_still_stops_the_fault(s05, monkeypatch):
    mod, calls = s05
    monkeypatch.setattr(helpers, "run_swarm", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("run died")))
    with pytest.raises(RuntimeError):
        mod.main()
    assert _armed() == 0
    assert calls["stop_fault"] == 1


def test_a_failing_run_still_stops_the_fault(s05, monkeypatch):
    """stop_fault() must run even when the run raises: a leaked proxy plus OLLAMA_BASE_URL is the
    worse leak, since it silently faults every later run."""
    mod, calls = s05
    monkeypatch.setattr(helpers, "run_swarm",
                        lambda *a, **k: (_ for _ in ()).throw(SystemExit("run died")))
    with pytest.raises(SystemExit):
        mod.main()
    assert calls["stop_fault"] == 1


def test_a_clean_run_leaves_the_frozen_fleet_unarmed(s05):
    mod, calls = s05
    assert mod.main() == 0
    assert _armed() == 0
    assert calls["stop_fault"] == 1 and calls["cleanup"] == 1


@pytest.fixture
def snap(tmp_path, monkeypatch):
    """A run dir plus a recorder for the fan-out the snapshot would issue."""
    monkeypatch.setattr(helpers, "REPO", str(tmp_path))
    monkeypatch.setattr(helpers, "hosts", lambda: ["agent-1", "agent-2"])
    seen = {"cmd": ""}
    monkeypatch.setattr(helpers, "_sh",
                        lambda cmd, timeout=900: seen.__setitem__("cmd", cmd) or "")

    def write(host: str, body: str, mtime: float) -> str:
        d = tmp_path / "runs" / "x" / host
        d.mkdir(parents=True, exist_ok=True)
        p = d / f"{host}.log"
        p.write_text(body)
        os.utime(p, (mtime, mtime))
        return str(p)

    return tmp_path, seen, write


def test_a_log_from_an_earlier_run_is_not_republished_as_this_runs(snap):
    """The staleness hole: run dirs are reused, and every reader globs the whole directory.

    A log left by an earlier run into the same dir would be counted by collect(), load_split() and
    _restarts_and_conflicts() as this run's evidence, mixing two runs with nothing on screen to
    say so. Presence is not freshness.
    """
    tmp_path, seen, write = snap
    old = write("agent-1", "previous run's bids\n", mtime=1000.0)
    fresh = write("agent-2", "this run's bids\n", mtime=3000.0)

    helpers.snapshot_agent_logs("runs/x", since=2000.0)

    # Set aside, not deleted — and out of the `agent-*.log` glob the readers use.
    assert not os.path.exists(old)
    assert open(old + ".stale").read() == "previous run's bids\n"
    assert helpers.glob(f"{tmp_path}/runs/x/**/agent-*.log", recursive=True) == [fresh]
    # The stale host is treated as having nothing, so a fresh copy is attempted for it.
    assert "agent-1:" in seen["cmd"] and "agent-2:" not in seen["cmd"]


def test_a_fresh_log_is_left_alone(snap):
    """run_test collects these too and wins the race; re-copying only adds a way to replace a
    complete log with a partial one."""
    tmp_path, seen, write = snap
    write("agent-1", "collected by run_test\n", mtime=3000.0)
    write("agent-2", "collected by run_test\n", mtime=3000.0)

    helpers.snapshot_agent_logs("runs/x", since=2000.0)

    assert seen["cmd"] == ""            # no transfer issued at all
    assert (tmp_path / "runs" / "x" / "agent-1" / "agent-1.log").read_text() == \
        "collected by run_test\n"


def test_partial_transfers_cannot_truncate_an_already_collected_log(snap):
    """Transfers land in .incoming and are moved only on arrival."""
    tmp_path, seen, _ = snap
    helpers.snapshot_agent_logs("runs/x", since=2000.0)
    cmd = seen["cmd"]
    for host in ("agent-1", "agent-2"):
        assert f"{tmp_path}/runs/x/{host}/.incoming/" in cmd
        assert f"scp -q -o ConnectTimeout=10" in cmd
        assert f"mv {tmp_path}/runs/x/{host}/.incoming/agent-*.log " \
               f"{tmp_path}/runs/x/{host}/" in cmd


def test_missing_and_stale_hosts_are_both_reported(snap, capsys):
    """Silence would read as complete evidence: a run missing a host's log computes every
    per-agent metric over a smaller population than the fleet it divides by."""
    tmp_path, seen, write = snap
    write("agent-1", "previous run\n", mtime=1000.0)
    helpers.snapshot_agent_logs("runs/x", since=2000.0)
    out = capsys.readouterr().out
    assert "0/2 hosts collected" in out
    assert "predate this run" in out and "agent-1/agent-1.log" in out
    assert "no log collected from 2 host(s)" in out


def test_a_failed_refresh_reports_a_missing_host_and_never_falls_back_to_the_stale_file(snap):
    """"We could not collect this, so here is last time's" is the original bug in a new hat.

    The stubbed `_sh` delivers nothing, which is what an unreachable host or an already-deleted
    log looks like. The host must end up EMPTY and reported, not quietly restored to the file it
    displaced.
    """
    tmp_path, seen, write = snap
    old = write("agent-1", "previous run's bids\n", mtime=1000.0)

    helpers.snapshot_agent_logs("runs/x", since=2000.0)

    assert not os.path.exists(old)                       # displaced, and not put back
    assert os.path.exists(old + ".stale")                # but not destroyed either
    assert helpers.glob(f"{tmp_path}/runs/x/**/agent-*.log", recursive=True) == []


def test_the_move_is_gated_on_the_transfer_succeeding(snap):
    """scp exits nonzero on a half-finished transfer, leaving a truncated file in .incoming. An
    unconditional `mv` would publish that as this run's complete log."""
    _, seen, _ = snap
    helpers.snapshot_agent_logs("runs/x", since=2000.0)
    assert "2>/dev/null && mv" in seen["cmd"]
    assert "2>/dev/null; mv" not in seen["cmd"]


def test_since_is_required(snap):
    """Every weaker rule than "compare against the run's start" ends up publishing another run's
    log as this one's, so there is no default to fall back to."""
    with pytest.raises(TypeError):
        helpers.snapshot_agent_logs("runs/x")


def test_a_suspicious_run_dir_is_refused(monkeypatch, tmp_path):
    monkeypatch.setattr(helpers, "REPO", str(tmp_path))
    for bad in ("", "runs/../.."):
        with pytest.raises(SystemExit):
            helpers.snapshot_agent_logs(bad, since=0.0)


def test_a_log_holding_two_agents_is_not_attributed_to_the_first_one(tmp_path, monkeypatch,
                                                                    capsys):
    """Two agents sharing a log path, or a stale log appended across runs — a documented hazard in
    this repo's runbook. Trusting the first `- agent-N -` line hands the whole file, and all its
    counts, to whichever agent wrote first."""
    monkeypatch.setattr(helpers, "REPO", str(tmp_path))
    run = "runs/mixed"
    (tmp_path / run / "agent-1").mkdir(parents=True)
    (tmp_path / run / "agent-1" / "agent-1.log").write_text(
        "2026-08-22 18:24:43,245 - agent-1 - INFO - [LLM_COST_NO_BID] Job=1\n"
        "2026-08-22 18:24:44,245 - agent-2 - INFO - [LLM_COST_NO_BID] Job=2\n")
    (tmp_path / "runs_mixed.log").write_text(
        LAUNCHED.format(n=2) + "\n[all] Jobs per agent:\n  Agent 1: 1 jobs\n")

    m = helpers.collect(run)
    assert m["agent_ids_mixed"] == ["agent-1.log holds agents 1, 2"]
    # Credited to neither: both agents read as having no log of their own.
    assert m["agent_ids_missing"] == [1, 2]
    assert m["llm_no_bid"] == 2                       # but both lines are still counted

    helpers.report("S05", "test", {"llm_complete": 1}, m, ["nothing"])
    out = capsys.readouterr().out
    assert "MORE THAN ONE agent" in out and "every line in it is counted" in out


def test_the_check_uses_the_runs_own_launched_count_not_the_module_constant(tmp_path, monkeypatch):
    """`fleet_size` is max(AGENTS, highest placing id), so on a 14-agent run it reports 30 and a
    check against it invents 16 phantom missing agents. This repo has 10-, 14- and 60-agent sweep
    runs, so the constant is this campaign's fleet, not any given run's."""
    monkeypatch.setattr(helpers, "REPO", str(tmp_path))
    monkeypatch.setattr(helpers, "AGENTS", 30)
    run = "runs/small"
    for i in (1, 2):
        (tmp_path / run / f"agent-{i}").mkdir(parents=True)
        (tmp_path / run / f"agent-{i}" / f"agent-{i}.log").write_text(
            f"2026-08-22 18:24:43,245 - agent-{i} - INFO - x\n")
    (tmp_path / "runs_small.log").write_text(
        LAUNCHED.format(n=2) + "\n[all] Jobs per agent:\n  Agent 1: 1 jobs\n")

    m = helpers.collect(run)
    assert m["fleet_configured"] == 2
    assert m["fleet_size"] == 30            # unchanged: the published fairness denominator
    assert m["agent_ids_missing"] == []     # ...but the population check uses the launched 2
    assert m["agent_logs_missing"] == 0 and m["agent_logs_extra"] == 0


def test_dynamic_agent_batches_are_added_to_the_launched_fleet(tmp_path, monkeypatch):
    monkeypatch.setattr(helpers, "REPO", str(tmp_path))
    run = "runs/dyn"
    (tmp_path / run / "agent-1").mkdir(parents=True)
    (tmp_path / run / "agent-1" / "agent-1.log").write_text("- agent-1 - INFO - x\n")
    (tmp_path / "runs_dyn.log").write_text(
        "Launched 10 initial 'llm' agents across 10 remote host(s).\n"
        "Launched 5 dynamic 'llm' agents across 5 remote host(s).\n"
        "\n[all] Jobs per agent:\n  Agent 1: 1 jobs\n")

    m = helpers.collect(run)
    assert m["fleet_configured"] == 15
    assert m["agent_logs_missing"] == 14


def test_no_launched_line_withholds_the_verdict(tmp_path, monkeypatch, capsys):
    """An orchestrator log alone is not enough — it has to state the fleet."""
    monkeypatch.setattr(helpers, "REPO", str(tmp_path))
    run = "runs/nofleet"
    (tmp_path / run / "agent-1").mkdir(parents=True)
    (tmp_path / run / "agent-1" / "agent-1.log").write_text("- agent-1 - INFO - x\n")
    (tmp_path / "runs_nofleet.log").write_text("\n[all] Jobs per agent:\n  Agent 1: 1 jobs\n")

    m = helpers.collect(run)
    assert m["agent_population_verified"] is False
    assert m.get("fleet_configured") is None and m["agent_logs_missing"] is None

    helpers.report("S05", "test", {"llm_complete": 1}, m, ["nothing"])
    out = capsys.readouterr().out
    assert "no \"Launched N … agents\" line" in out
    assert f"NOT assumed to be {helpers.AGENTS}" in out


def _dated(epoch: float, agent: int = 1, marker: str = "[LLM_COST_NO_BID] Job=1") -> str:
    from datetime import datetime
    return (f"{datetime.fromtimestamp(epoch).strftime('%Y-%m-%d %H:%M:%S')},123 - "
            f"agent-{agent} - INFO - {marker}\n")


def _window(tmp_path, run: str, started: float, ended: float, ablation: bool = False) -> None:
    import json
    (tmp_path / run).mkdir(parents=True, exist_ok=True)
    json.dump({"started": started, "ended": ended, "disable_fallback": ablation},
              open(tmp_path / run / ".run_window", "w"))


def test_a_fresh_copy_of_an_old_log_is_stale_content_not_current_evidence(snap):
    """The deepest version of this bug: mtime is the COPY time (scp without -p), so a log an
    earlier run left on a host and this run fetched arrives looking brand new. Only its dated
    lines disagree, so content has to decide and the copy time cannot."""
    tmp_path, seen, _ = snap
    d = tmp_path / "runs" / "x" / "agent-1"
    d.mkdir(parents=True)
    p = d / "agent-1.log"
    p.write_text(_dated(1_000_000))          # content from long before the run
    os.utime(p, (9_000_000, 9_000_000))      # ...but copied just now

    helpers.snapshot_agent_logs("runs/x", since=8_000_000)

    assert not p.exists()                     # not certified as this run's
    assert (str(p) + ".stale") and os.path.exists(str(p) + ".stale")
    assert "agent-1:" in seen["cmd"]          # and a real copy is attempted instead


def test_a_log_dated_entirely_before_the_run_is_reported(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(helpers, "REPO", str(tmp_path))
    monkeypatch.setattr(helpers, "AGENTS", 1)
    run = "runs/old"
    _window(tmp_path, run, started=9_000_000, ended=9_001_000)
    (tmp_path / run / "agent-1").mkdir(parents=True)
    (tmp_path / run / "agent-1" / "agent-1.log").write_text(_dated(1_000_000))
    (tmp_path / "runs_old.log").write_text(
        LAUNCHED.format(n=1) + "\n[all] Jobs per agent:\n  Agent 1: 1 jobs\n")

    m = helpers.collect(run)
    assert m["agent_logs_outside_run"] == ["agent-1.log"]
    assert m["run_window_known"] is True
    helpers.report("S05", "test", {"llm_complete": 1}, m, ["nothing"])
    out = capsys.readouterr().out
    assert "dated entirely BEFORE this run" in out


def test_a_log_appended_across_runs_is_reported(tmp_path, monkeypatch, capsys):
    """cleanup() is supposed to delete host logs between runs; when it silently fails, the agent
    appends and the file holds both runs. Every count above then includes pre-run lines."""
    monkeypatch.setattr(helpers, "REPO", str(tmp_path))
    monkeypatch.setattr(helpers, "AGENTS", 1)
    run = "runs/appended"
    _window(tmp_path, run, started=9_000_000, ended=9_001_000)
    (tmp_path / run / "agent-1").mkdir(parents=True)
    (tmp_path / run / "agent-1" / "agent-1.log").write_text(
        _dated(1_000_000) + _dated(9_000_500))
    (tmp_path / "runs_appended.log").write_text(
        LAUNCHED.format(n=1) + "\n[all] Jobs per agent:\n  Agent 1: 1 jobs\n")

    m = helpers.collect(run)
    assert m["agent_logs_predating_run"] == ["agent-1.log"]
    assert m["llm_no_bid"] == 2                     # both lines counted, one of them not ours
    helpers.report("S05", "test", {"llm_complete": 1}, m, ["nothing"])
    assert "appended across runs" in capsys.readouterr().out


def test_no_run_window_reports_unknown_rather_than_clean(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(helpers, "REPO", str(tmp_path))
    monkeypatch.setattr(helpers, "AGENTS", 1)
    run = "runs/nowindow"
    (tmp_path / run / "agent-1").mkdir(parents=True)
    (tmp_path / run / "agent-1" / "agent-1.log").write_text(_dated(9_000_500))
    (tmp_path / "runs_nowindow.log").write_text(
        LAUNCHED.format(n=1) + "\n[all] Jobs per agent:\n  Agent 1: 1 jobs\n")

    m = helpers.collect(run)
    assert m["run_window_known"] is False
    # None, not [] — an empty list would read as "checked, nothing wrong".
    for key in ("agent_logs_predating_run", "agent_logs_outside_run", "agent_logs_undated"):
        assert m[key] is None
    helpers.report("S05", "test", {"llm_complete": 1}, m, ["nothing"])
    assert "no .run_window" in capsys.readouterr().out


def test_the_ablation_state_is_recorded_as_run_provenance(tmp_path, monkeypatch):
    """A reference built under the ablation must not be indistinguishable from a normal one."""
    monkeypatch.setattr(helpers, "REPO", str(tmp_path))
    monkeypatch.setattr(helpers, "AGENTS", 1)
    run = "runs/prov"
    _window(tmp_path, run, started=9_000_000, ended=9_001_000, ablation=True)
    (tmp_path / run / "agent-1").mkdir(parents=True)
    (tmp_path / run / "agent-1" / "agent-1.log").write_text(_dated(9_000_500))
    (tmp_path / "runs_prov.log").write_text(
        LAUNCHED.format(n=1) + "\n[all] Jobs per agent:\n  Agent 1: 1 jobs\n")

    assert helpers.collect(run)["ablation_disable_fallback"] is True


def test_cleanup_refuses_to_start_when_stale_host_logs_survive(monkeypatch, capsys):
    """The upstream fix: if a host keeps last run's log, it will be fetched during this run with a
    fresh copy time. Nothing downstream can detect that, so cleanup must not proceed."""
    monkeypatch.setattr(helpers, "hosts", lambda: ["agent-1", "agent-2"])
    monkeypatch.setattr(helpers, "_sh", lambda *a, **k: "")
    monkeypatch.setattr(helpers, "_fan_out",
                        lambda hosts_, cmd, **k: "agent-1 1\nagent-2 0\n" if "wc -l" in cmd else "")
    with pytest.raises(SystemExit, match="survived cleanup"):
        helpers.cleanup()
    assert "retrying" in capsys.readouterr().out

    # And it passes once the hosts come back clean.
    monkeypatch.setattr(helpers, "_fan_out",
                        lambda hosts_, cmd, **k: "agent-1 0\nagent-2 0\n" if "wc -l" in cmd else "")
    helpers.cleanup()
