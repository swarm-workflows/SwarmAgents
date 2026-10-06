"""Code review 2026-10-05 §69-§70: job isolation and the data service.

§69 Apptainer ran without `--containall`, so $HOME and the host /tmp were bound — on a root-run
    agent, /root/.ssh and the root mesh key were readable by workflow code — and the staged code
    bundle was bound read-write, where docker binds it read-only.
§70 The data service was insecure gRPC with no authentication: any host could read a run's files
    and PUT any (run, name), and with first-wins storage a pre-seeded upload became the copy.
"""
import argparse
import os
import shutil
import sys
from unittest.mock import patch

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)
sys.path.insert(0, os.path.join(REPO, "tests"))

from swarm.execution import runner, staging  # noqa: E402
from swarm.models.execution import ExecutionSpec  # noqa: E402
from test_staging import _serve, _store, _write  # noqa: E402

TRUE_BIN = shutil.which("true") or sys.executable


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    monkeypatch.delenv(staging.TOKEN_ENV, raising=False)
    runner.configure()
    staging.configure()
    staging.set_context()
    yield
    runner.configure()
    staging.configure()
    staging.set_context()


def _apptainer_cmd(**policy):
    runner.configure(container_runtime="apptainer", **policy)
    spec = ExecutionSpec.from_dict({
        "path": "/srv/analyze", "arguments": [], "pfn": TRUE_BIN, "pfn_type": "stageable",
        "container": {"name": "c", "kind": "singularity", "image": "/img.sif"}})
    with patch("shutil.which", return_value="/usr/bin/apptainer"):
        cmd, reason = runner.build_command(spec, "/work")
    assert reason == ""
    return cmd


# --------------------------------------------------------------------------- §69
class TestApptainerIsolation:
    def test_containall_by_default(self):
        cmd = _apptainer_cmd()
        assert cmd[:3] == ["apptainer", "exec", "--containall"]

    def test_staged_code_is_bound_read_only(self):
        cmd = _apptainer_cmd()
        code_binds = [cmd[i + 1] for i, a in enumerate(cmd) if a == "--bind"
                      and cmd[i + 1].endswith("/srv/analyze:ro")]
        assert code_binds, cmd

    def test_the_work_dir_stays_writable(self):
        cmd = _apptainer_cmd()
        assert "/work:/work" in cmd

    def test_the_escape_hatch(self):
        assert "--containall" not in _apptainer_cmd(apptainer_containall=False)

    def test_the_agent_reads_the_key_with_the_same_default(self):
        src = open(os.path.join(REPO, "swarm/agents/agent_grpc.py")).read()
        assert 'apptainer_containall=bool(cfg.get("apptainer_containall", True))' in src


# --------------------------------------------------------------------------- §70
class TestDataServiceToken:
    def test_a_fetch_without_the_token_is_refused(self, tmp_path, monkeypatch):
        src = _write(str(tmp_path / "p" / "r.json"), b"{}")
        monkeypatch.setenv(staging.TOKEN_ENV, "s3cret")
        server, loc = _serve(tmp_path, {"r.json": src})
        try:
            monkeypatch.delenv(staging.TOKEN_ENV)             # the client has none
            dest = str(tmp_path / "c")
            os.makedirs(dest)
            out = staging.fetch("r.json", loc, dest, run_id="run-1")
            assert not out.ok and "token" in out.reason
            monkeypatch.setenv(staging.TOKEN_ENV, "wrong")
            assert not staging.fetch("r.json", loc, dest, run_id="run-1").ok
            monkeypatch.setenv(staging.TOKEN_ENV, "s3cret")
            assert staging.fetch("r.json", loc, dest, run_id="run-1").ok
        finally:
            server.stop(0)

    def test_a_put_without_the_token_is_refused(self, tmp_path, monkeypatch):
        monkeypatch.setenv(staging.TOKEN_ENV, "s3cret")
        server, port, store_dir = _store(tmp_path)
        try:
            staging.configure(enabled=True, store_host="127.0.0.1", store_port=port)
            f = _write(str(tmp_path / "w" / "out.txt"), b"forged")
            monkeypatch.delenv(staging.TOKEN_ENV)
            res = staging.put("out.txt", f, run_id="run-1")
            assert not res.ok and "token" in res.reason
            assert not os.path.exists(os.path.join(store_dir, "run-1", "out.txt"))
            monkeypatch.setenv(staging.TOKEN_ENV, "s3cret")
            assert staging.put("out.txt", f, run_id="run-1").ok
        finally:
            server.stop(0)

    def test_without_a_token_configured_the_service_is_open(self, tmp_path):
        """Unchanged default; the agent warns at startup instead."""
        src = _write(str(tmp_path / "p" / "r.json"), b"{}")
        server, loc = _serve(tmp_path, {"r.json": src})
        try:
            dest = str(tmp_path / "c")
            os.makedirs(dest)
            assert staging.fetch("r.json", loc, dest, run_id="run-1").ok
        finally:
            server.stop(0)

    def test_the_runner_forwards_the_token_to_remote_agents(self, tmp_path, monkeypatch):
        import run_test
        import yaml
        cfg = tmp_path / "cfg"
        cfg.mkdir()
        (cfg / f"{run_test.CFG_PREFIX}1.yml").write_text(yaml.safe_dump({"grpc": {"host": "h1"}}))
        args = argparse.Namespace(
            agents=1, agents_per_host=1, config_dir=str(cfg), starter="swarm-multi-start.sh",
            remote_repo_dir="/root/SwarmAgents", groups=None, group_size=None, debug=False,
            agent_type="resource", topology="mesh", jobs=1, db_host="database",
            jobs_per_proposal=1)
        cmds = []
        monkeypatch.setattr(run_test, "ssh_check", lambda host, cmd: cmds.append(cmd))
        monkeypatch.setattr(run_test, "scp_to", lambda *a: None)
        monkeypatch.setenv("SWARM_STAGING_TOKEN", "s3cret")
        run_test.start_agents_remote(args, ["h1"])
        start = [c for c in cmds if "nohup bash" in c][0]
        assert "export SWARM_STAGING_TOKEN=s3cret && " in start
