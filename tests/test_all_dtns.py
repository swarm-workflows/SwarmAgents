"""--all-dtns: every agent holds every pool DTN name, capacities untouched (2026-10-07).

replay-golden jobs name up to 8 DTNs, and the default 1-4 random draw from 10 left 82 of a
600-job rung unplaceable — the first slice pilot was refused for it. --size-to-jobs fixed
feasibility but raised 18 of 30 agents to a 4-core / 8 GB / 1-GPU floor, taking the fleet off
the standard flavour pool. --all-dtns keeps the flavour pool and carries locality in
connectivity_score alone.
"""
import json
import os
import subprocess
import sys
from pathlib import Path

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GEN = os.path.join(REPO, "generate_configs.py")
BASE = os.path.join(REPO, "config_swarm_multi.yml")


def _generate(cwd: Path, *extra):
    cwd.mkdir(parents=True, exist_ok=True)
    proc = subprocess.run([sys.executable, GEN, "12", "10", BASE, str(cwd / "cfg"), "mesh",
                           "localhost", "24", "--seed", "7", "--skip-jobs", *extra],
                          cwd=cwd, capture_output=True, text=True, timeout=120)
    return proc, (json.loads((cwd / "agent_profiles.json").read_text())
                  if (cwd / "agent_profiles.json").exists() else None)


def _caps(p):
    return tuple(p.get(k) for k in ("core", "ram", "disk", "gpu"))


def test_every_agent_holds_every_pool_dtn_and_capacities_do_not_move(tmp_path):
    plain_proc, plain = _generate(tmp_path / "plain", "--dtns")
    all_proc, every = _generate(tmp_path / "all", "--dtns", "--all-dtns")
    assert plain_proc.returncode == 0 and all_proc.returncode == 0, all_proc.stderr[-400:]
    assert {k: _caps(p) for k, p in plain.items()} == {k: _caps(p) for k, p in every.items()}
    assert any(len(p["dtns"]) < 10 for p in plain.values()), "the default draw is a subset"
    names = {d["name"] for p in every.values() for d in p["dtns"]}
    assert len(names) == 10
    for p in every.values():
        assert sorted(d["name"] for d in p["dtns"]) == sorted(names)
    # One DTN, one population: every agent's score for a name sits in one jitter band.
    for name in names:
        scores = [d["connectivity_score"] for p in every.values() for d in p["dtns"]
                  if d["name"] == name]
        assert max(scores) - min(scores) <= 0.1 + 0.011, (name, scores)
    assert len({d["connectivity_score"] for p in every.values() for d in p["dtns"]}) > 10, \
        "locality must still vary across agents"


def test_all_dtns_without_a_pool_is_refused(tmp_path):
    proc, _ = _generate(tmp_path / "nodtns", "--all-dtns")
    assert proc.returncode != 0 and "--all-dtns needs --dtns" in (proc.stdout + proc.stderr)


def test_run_test_forwards_it(tmp_path, monkeypatch):
    sys.path.insert(0, REPO)
    import run_test
    captured = []
    monkeypatch.setattr(run_test, "run_blocking", lambda cmd, check=True: captured.append(cmd))
    monkeypatch.setattr(sys, "argv", ["run_test.py", "--mode", "local", "--agent-type",
                                      "resource", "--agents", "3", "--jobs", "6", "--topology",
                                      "hierarchical", "--db-host", "localhost", "--all-dtns",
                                      "--run-dir", str(tmp_path / "r"),
                                      "--config-dir", str(tmp_path / "cfg"),
                                      "--agent-hosts-file", str(tmp_path / "h")])
    run_test.generate_configs(run_test.parse_args(), ["localhost"])
    assert "--dtns" in captured[-1] and "--all-dtns" in captured[-1]
