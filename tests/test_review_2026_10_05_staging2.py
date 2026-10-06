"""Code review 2026-10-05 §71-§73: the remaining staging and execution items.

§71 A verifying receiver accepted a stream with no digest, and an upload that ended without its
    final chunk was stored as the durable copy.
§72 A docker image was pulled inside the job's timed duration (a pull failure read as the job
    failing), and `auto` silently ran a docker container under apptainer.
§73 Smaller: publish errors skipped the completion write; an empty work_dir served the agent's
    cwd; the environment denylist missed ssh-agent, *_PASS, *_AUTH and URL credentials; logs of
    two attempts (or of `a/b` and `a_b`) shared a file; a wildcard grpc.host was only logged.
"""
import os
import shutil
import sys
from unittest.mock import MagicMock, patch

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)
sys.path.insert(0, os.path.join(REPO, "tests"))

from swarm.comm import consensus_pb2  # noqa: E402
from swarm.execution import runner, staging  # noqa: E402
from swarm.models.execution import ExecutionSpec  # noqa: E402
from test_staging import _Job, _agent, _serve, _write  # noqa: E402


@pytest.fixture(autouse=True)
def _clean():
    runner.configure()
    staging.configure()
    staging.set_context()
    yield
    runner.configure()
    staging.configure()
    staging.set_context()


# --------------------------------------------------------------------------- §71
class TestVerificationIsNotOptionalAtTheFarEnd:
    def test_a_stream_without_a_digest_is_refused_when_verifying(self, tmp_path):
        src = _write(str(tmp_path / "p" / "r.json"), b"{}" * 100)
        staging.configure(verify=False)                    # the SERVER does not digest
        server, loc = _serve(tmp_path, {"r.json": src})
        try:
            staging.configure(verify=True)                 # this receiver does
            dest = str(tmp_path / "c")
            os.makedirs(dest)
            out = staging.fetch("r.json", loc, dest, run_id="run-1")
            assert not out.ok and "carries no digest" in out.reason
            assert not os.path.exists(os.path.join(dest, "r.json"))
        finally:
            server.stop(0)

    def _put(self, tmp_path, chunks, verify=True):
        staging.configure(verify=verify)
        svc = staging._Servicer(staging.PublishedFiles(), "", store_dir=str(tmp_path / "store"))
        ctx = MagicMock()
        ctx.invocation_metadata.return_value = ()
        return svc.Put(iter(chunks), ctx), str(tmp_path / "store" / "run-1" / "x.txt")

    def test_an_upload_without_its_final_chunk_is_not_stored(self, tmp_path):
        ack, dest = self._put(tmp_path, [consensus_pb2.PutChunk(
            name="x.txt", run_id="run-1", content=b"half a file", last=False)], verify=False)
        assert not ack.ok and "final chunk" in ack.error
        assert not os.path.exists(dest)

    def test_an_undigested_upload_is_refused_by_a_verifying_store(self, tmp_path):
        ack, dest = self._put(tmp_path, [
            consensus_pb2.PutChunk(name="x.txt", run_id="run-1", content=b"data", last=False),
            consensus_pb2.PutChunk(name="x.txt", run_id="run-1", last=True)])
        assert not ack.ok and "no digest" in ack.error
        assert not os.path.exists(dest)


# --------------------------------------------------------------------------- §72
def _docker_spec():
    return ExecutionSpec.from_dict({
        "path": "/srv/x", "arguments": [], "pfn_type": "installed",
        "container": {"name": "c", "kind": "docker", "image": "docker://repo/img:1"}})


class TestRuntimeAndPull:
    def test_auto_does_not_run_a_docker_container_under_apptainer(self):
        runner.configure(container_runtime="auto")
        only_apptainer = lambda b: "/usr/bin/apptainer" if b == "apptainer" else None
        with patch("shutil.which", side_effect=only_apptainer):
            cmd, reason = runner.build_command(_docker_spec(), "/w")
        assert cmd == [] and reason

    def test_substitution_when_explicitly_allowed(self):
        runner.configure(container_runtime="auto", allow_runtime_substitution=True)
        only_apptainer = lambda b: "/usr/bin/apptainer" if b == "apptainer" else None
        with patch("shutil.which", side_effect=only_apptainer):
            cmd, reason = runner.build_command(_docker_spec(), "/w")
        assert cmd and cmd[0] == "apptainer"

    def _docker_cmd(self):
        return ["docker", "run", "--rm", "--entrypoint", "/srv/x", "repo/img:1"]

    def test_a_local_image_is_not_pulled(self, monkeypatch):
        calls = []
        monkeypatch.setattr(runner.subprocess, "run",
                            lambda cmd, **k: calls.append(cmd) or MagicMock(returncode=0))
        assert runner._ensure_docker_image(self._docker_cmd(), runner.policy())[1] == ""
        assert calls == [["docker", "image", "inspect", "repo/img:1"]]

    def test_a_missing_image_is_pulled_before_the_clock(self, monkeypatch):
        def fake(cmd, **k):
            return MagicMock(returncode=1 if cmd[1] == "image" else 0, stderr="")
        monkeypatch.setattr(runner.subprocess, "run", fake)
        _s, refusal = runner._ensure_docker_image(self._docker_cmd(), runner.policy())
        assert refusal == ""

    def test_a_failed_pull_is_a_transient_refusal_not_a_job_failure(self, tmp_path, monkeypatch):
        runner.configure(mode="real", work_dir=str(tmp_path / "w"), container_runtime="docker")
        monkeypatch.setattr(runner, "_ensure_docker_image",
                            lambda cmd, pol: (2.0, "could not pull image repo/img:1: denied"))
        with patch("shutil.which", return_value="/usr/bin/docker"):
            res = runner.run(_docker_spec(), "j1")
        assert res.refused and res.transient and res.pull_s == 2.0
        assert "could not pull" in res.reason


# --------------------------------------------------------------------------- §73
class TestSmaller:
    def test_the_environment_drops_ssh_agent_passwords_and_url_credentials(self):
        env = runner.job_environment({
            "PATH": "/bin", "HOME": "/root", "SSH_AUTH_SOCK": "/tmp/agent",
            "DB_PASS": "x", "PROXY_AUTH": "y",
            "REDIS_URL": "redis://user:pw@host:6379", "MIRROR": "https://example.org/x"})
        assert set(env) == {"PATH", "HOME", "MIRROR"}

    def test_each_attempt_and_each_id_gets_its_own_log(self, tmp_path):
        script = tmp_path / "ok.sh"
        script.write_text("#!/bin/sh\necho hi\n")
        script.chmod(0o755)
        runner.configure(mode="real", work_dir=str(tmp_path / "w"))
        spec = ExecutionSpec(path=str(script), arguments=[], pfn=str(script))
        paths = {runner.run(spec, jid).stdout_path for jid in ("a/b", "a_b", "a_b")}
        assert len(paths) == 3

    def test_an_empty_work_dir_publishes_nothing(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        _write(str(tmp_path / "out.json"), b"from the agent's cwd")
        staging.configure(enabled=True)
        runner.configure(mode="simulate", work_dir="")
        assert _agent(tmp_path)._publish_locations(_Job(), ["out.json"]) == {}

    def test_a_wildcard_host_with_staging_is_refused(self):
        src = open(os.path.join(REPO, "swarm/agents/agent_grpc.py")).read()
        body = src[src.index("    def _configure_staging"):]
        i = body.index('if str(self.grpc_host) in ("0.0.0.0", "::", ""):')
        branch = body[i:i + 900]
        assert "raise ValueError(" in branch and "wildcard cannot be dialled" in branch
        assert "self.logger.error(" not in branch

    def test_a_publish_error_does_not_skip_the_completion(self):
        src = open(os.path.join(REPO, "swarm/agents/resource_agent.py")).read()
        i = src.index("            locations = self._publish_locations(job, produced)")
        assert src[i - 400:i].rstrip().endswith("try:")
