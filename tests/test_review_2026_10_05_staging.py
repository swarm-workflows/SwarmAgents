"""Code review 2026-10-05 §64-§67: data staging correctness.

§64 The producer registered a raw LFN, the consumer looked up its basename: a name with a
    directory component missed and fell through to the inputs root.
§65 The store acknowledged a different body under an existing (run, name), and nothing tied a
    served file to the producer's: two copies could both "verify".
§66 A failed stage-out still published the name (peer-only), and the deadline was flat over
    the whole stream, so large outputs always lost durability.
§67 A declared output already in the shared work dir was published as a job's output even when
    the job did not rewrite it.
"""
import os
import sys
from unittest.mock import MagicMock

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)
sys.path.insert(0, os.path.join(REPO, "tests"))

from swarm.execution import runner, staging  # noqa: E402
from test_staging import _Job, _agent, _serve, _store, _write  # noqa: E402


@pytest.fixture(autouse=True)
def _clean_policy():
    runner.configure()
    staging.configure()
    staging.set_context()
    yield
    runner.configure()
    staging.configure()
    staging.set_context()


class _Node:
    def __init__(self, file):
        self.file = file
        self.name = "local"


# --------------------------------------------------------------------------- §64
class TestOneNameRule:
    """§64 first refused names with a directory component; they are SUPPORTED now, keyed and
    placed by one rule (`safe_relpath`) on every side, with `..` and absolute paths refused."""

    def test_safe_relpath(self):
        assert staging.safe_relpath("out.csv") == "out.csv"
        assert staging.safe_relpath("runA/./out.csv") == "runA/out.csv"
        assert staging.safe_relpath("runA//b/out.csv") == "runA/b/out.csv"
        for bad in ("", ".", "..", "/etc/passwd", "a/../../b", "../x", None):
            assert staging.safe_relpath(bad) is None
        assert staging.plain_name is staging.safe_relpath

    def test_a_nested_name_moves_end_to_end(self, tmp_path):
        """Producer publishes runA/out.csv at <work>/runA/out.csv; a consumer fetches it to the
        same relative path in its own work dir."""
        src = _write(str(tmp_path / "prod" / "runA" / "out.csv"), b"nested")
        server, loc = _serve(tmp_path, {"runA/out.csv": src})
        try:
            staging.configure(enabled=True)
            runner.configure(mode="real")
            work = str(tmp_path / "cons")
            os.makedirs(work)
            staged, refusal = runner.stage_inputs(
                [_Node("runA/out.csv")], work, locator=lambda n: {"runA/out.csv": [loc]},
                run_id="run-1")
            assert refusal == "" and staged == ["runA/out.csv"]
            assert open(os.path.join(work, "runA", "out.csv"), "rb").read() == b"nested"
        finally:
            server.stop(0)

    def test_a_producer_publishes_a_nested_output_at_its_real_path(self, tmp_path):
        work = str(tmp_path / "work")
        path = _write(os.path.join(work, "runA", "out.csv"), b"x")
        staging.configure(enabled=True)
        runner.configure(mode="real", work_dir=work)
        a = _agent(tmp_path)
        locs = a._publish_locations(_Job(), ["runA/out.csv"])
        assert locs["runA/out.csv"][0]["sha256"] == staging.file_sha256(path)
        assert a.staged_files.path_for("runA/out.csv") == path

    def test_an_escaping_name_is_still_refused(self, tmp_path):
        work = str(tmp_path / "w")
        os.makedirs(work)
        staging.configure(enabled=True)
        runner.configure(mode="real")
        _s, refusal = runner.stage_inputs([_Node("../outside.csv")], work,
                                          locator=lambda n: {}, run_id="r1")
        assert "not a contained relative name" in refusal

    def test_staging_off_is_unchanged(self, tmp_path):
        work = str(tmp_path / "w")
        os.makedirs(work)
        _write(os.path.join(work, "out.csv"), b"x")
        _s, refusal = runner.stage_inputs([_Node("runA/out.csv")], work)
        assert refusal == ""

    def test_the_store_keeps_nested_names(self, tmp_path):
        server, port, store_dir = _store(tmp_path)
        try:
            staging.configure(enabled=True, store_host="127.0.0.1", store_port=port)
            f = _write(str(tmp_path / "w" / "o.csv"), b"stored")
            assert staging.put("runA/o.csv", f, run_id="run-1").ok
            assert open(os.path.join(store_dir, "run-1", "runA", "o.csv"), "rb").read() == b"stored"
        finally:
            server.stop(0)


# --------------------------------------------------------------------------- §65
class TestTheServedFileIsTheProducers:
    def test_every_location_carries_the_producers_digest(self, tmp_path):
        work = str(tmp_path / "work")
        path = _write(os.path.join(work, "out.json"), b"{}")
        staging.configure(enabled=True)
        runner.configure(mode="real", work_dir=work)
        entries = _agent(tmp_path)._publish_locations(_Job(), ["out.json"])["out.json"]
        assert all(e["sha256"] == staging.file_sha256(path) for e in entries)

    def test_a_copy_that_does_not_match_is_rejected_and_the_next_is_tried(self, tmp_path,
                                                                         monkeypatch):
        good = b"the producer's bytes"
        want = __import__("hashlib").sha256(good).hexdigest()
        served = {"peer": b"a stale copy", "store": good}

        def fake_fetch(name, loc, dest_dir, **k):
            _write(os.path.join(dest_dir, name), served[loc["agent_id"]])
            return staging.FetchResult(True)
        monkeypatch.setattr(staging, "fetch", fake_fetch)
        dest = str(tmp_path / "d")
        os.makedirs(dest)
        res = staging.fetch_any("x.bin", [{"agent_id": "peer", "sha256": want},
                                          {"agent_id": "store", "sha256": want}], dest, "r1")
        assert res.ok
        assert open(os.path.join(dest, "x.bin"), "rb").read() == good

    def test_no_matching_copy_fails(self, tmp_path, monkeypatch):
        def fake_fetch(name, loc, dest_dir, **k):
            _write(os.path.join(dest_dir, name), b"wrong")
            return staging.FetchResult(True)
        monkeypatch.setattr(staging, "fetch", fake_fetch)
        dest = str(tmp_path / "d")
        os.makedirs(dest)
        res = staging.fetch_any("x.bin", [{"agent_id": "peer", "sha256": "0" * 64}], dest, "r1")
        assert not res.ok and "does not match the producer's digest" in res.reason
        assert not os.path.exists(os.path.join(dest, "x.bin"))

    def test_a_location_without_a_digest_is_accepted(self, tmp_path, monkeypatch):
        """Locations written before the digest existed still fetch."""
        monkeypatch.setattr(staging, "fetch", lambda name, loc, dest_dir, **k: (
            _write(os.path.join(dest_dir, name), b"x"), staging.FetchResult(True))[1])
        dest = str(tmp_path / "d")
        os.makedirs(dest)
        assert staging.fetch_any("x.bin", [{"agent_id": "peer"}], dest, "r1").ok


# --------------------------------------------------------------------------- §66
class TestStageOutRetry:
    def test_the_deadline_scales_with_size(self):
        staging.configure(store_timeout_s=120.0, store_min_rate_bps=1_000_000.0)
        assert staging.put_deadline_s(0) == pytest.approx(120.0)
        assert staging.put_deadline_s(500_000_000) == pytest.approx(620.0)

    def _queued(self, tmp_path, monkeypatch, ok):
        staging.configure(enabled=True, store_host="store", store_port=21000)
        a = _agent(tmp_path)
        a._pending_stage_out = [{"job_id": "j1", "name": "out.json", "path": "/x",
                                 "peer": {"agent_id": "7"}, "sha256": "ab", "attempts": 1}]
        monkeypatch.setattr(staging, "put", lambda *a_, **k: staging.PutResult(ok, "" if ok
                                                                               else "down"))
        a._retry_stage_out()
        return a

    def test_a_successful_retry_publishes_name_and_location_together(self, tmp_path,
                                                                     monkeypatch):
        a = self._queued(tmp_path, monkeypatch, ok=True)
        names, locs = a.repository.publish_data.call_args.args
        assert names == ["out.json"]
        assert [e["agent_id"] for e in locs["out.json"]] == ["7", "store"]
        assert a._pending_stage_out == []

    def test_a_failing_retry_stays_queued_and_unpublished(self, tmp_path, monkeypatch):
        a = self._queued(tmp_path, monkeypatch, ok=False)
        a.repository.publish_data.assert_not_called()
        assert a._pending_stage_out[0]["attempts"] == 2

    def test_publish_data_is_one_transaction(self):
        from swarm.database.repository import Repository
        from test_failed_agent_reassignment import _FakeRedis

        class _R(_FakeRedis):
            def hset(self, key, mapping=None):
                self.kv.setdefault(key, {}).update(mapping or {})

        r = _R()
        pipe = r.pipeline()
        ops = []
        orig = r.pipeline
        r.pipeline = lambda: _Pipe(ops, orig())

        class _Pipe:
            def __init__(self, ops, inner):
                self.ops, self.inner = ops, inner

            def multi(self):
                self.ops.append("multi")

            def sadd(self, *a):
                self.ops.append("sadd")

            def hset(self, *a, **k):
                self.ops.append("hset")

            def execute(self):
                self.ops.append("execute")
        repo = Repository(r, run_id="t")
        repo.publish_data(["out.json"], {"out.json": [{"agent_id": "7"}]})
        assert ops == ["multi", "sadd", "hset", "execute"]


# --------------------------------------------------------------------------- §67
def test_a_job_that_does_not_rewrite_its_output_does_not_publish_a_stale_one(tmp_path):
    from swarm.models.execution import ExecutionSpec
    work = str(tmp_path / "w")
    stale = _write(os.path.join(work, "out.csv"), b"last week")
    script = tmp_path / "noop.sh"
    script.write_text("#!/bin/sh\nexit 0\n")
    script.chmod(0o755)
    runner.configure(mode="real", work_dir=work)
    res = runner.run(ExecutionSpec(path=str(script), arguments=[], pfn=str(script)), "j1",
                     data_out=[_Node("out.csv")])
    assert res.exit_status == 0
    assert not os.path.exists(stale)


# --------------------------------------------------------------------------- §67 (inputs)
class TestAStaleLocalInputIsReplaced:
    def _locator(self, sha):
        return lambda names: {"in.csv": [{"agent_id": "1", "host": "127.0.0.1", "port": 1,
                                          "sha256": sha}]}

    def test_a_matching_local_copy_is_used(self, tmp_path):
        work = str(tmp_path / "w")
        path = _write(os.path.join(work, "in.csv"), b"the producer's bytes")
        staging.configure(enabled=True)
        runner.configure(mode="real")
        staged, refusal = runner.stage_inputs([_Node("in.csv")], work,
                                              locator=self._locator(staging.file_sha256(path)),
                                              run_id="r1")
        assert refusal == "" and staged == []

    def test_a_mismatching_local_copy_is_removed_and_fetched(self, tmp_path, monkeypatch):
        work = str(tmp_path / "w")
        _write(os.path.join(work, "in.csv"), b"a stale copy")
        good = b"the producer's bytes"
        want = __import__("hashlib").sha256(good).hexdigest()
        staging.configure(enabled=True)
        runner.configure(mode="real")
        monkeypatch.setattr(staging, "fetch", lambda name, loc, dest_dir, **k: (
            _write(os.path.join(dest_dir, name), good), staging.FetchResult(True))[1])
        staged, refusal = runner.stage_inputs([_Node("in.csv")], work,
                                              locator=self._locator(want), run_id="r1")
        assert refusal == "" and staged == ["in.csv"]
        assert open(os.path.join(work, "in.csv"), "rb").read() == good
