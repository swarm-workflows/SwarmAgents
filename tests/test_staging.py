"""Moving a workflow's files between agents (`docs/STAGING_DESIGN.md`).

Until this existed, "staging" was a local copy from a root the agent could already see — on the
slice, one NFS export mounted at the same path everywhere. That works, and it is what validated
real execution, but it makes every agent equidistant from every file, which is exactly the
variable the scheduler's DTN penalties price. Nothing moved data between agents.

The tests that matter here are the ones that fail *quietly* if the design is wrong:

* a produced name must beat a same-named file in the inputs root (workflow names are a flat
  namespace; 62 colliding names were measured in the shipped profile),
* a truncated transfer must be refused, not left as a short file a job reads happily,
* a fetch must never overwrite a file that appeared while it was in flight,
* the server must serve only what it published, with no directory join to traverse,
* and with staging off, every path must behave exactly as it did before.
"""
import hashlib
import os
import sys
import threading

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from swarm.execution import runner, staging  # noqa: E402


@pytest.fixture(autouse=True)
def _clean_policy():
    """Every test starts from the shipped defaults and leaves them that way."""
    runner.configure()
    staging.configure()
    staging.set_context()
    yield
    runner.configure()
    staging.configure()
    staging.set_context()


class _Node:
    """The two fields `stage_inputs` reads off a DataNode."""

    def __init__(self, file, name="local"):
        self.file = file
        self.name = name


def _write(path, content=b"payload"):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as fh:
        fh.write(content)
    return path


def _serve(tmp_path, mapping, run_id="run-1", port=0):
    """Start a real transfer server on a free port; returns (server, location-dict-factory)."""
    published = staging.PublishedFiles()
    published.publish_all(mapping)
    # Port 0 lets the OS pick, but the gRPC server needs the real one to build a location.
    import socket
    if port == 0:
        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
        s.close()
    server = staging.TransferServer(published, "127.0.0.1", port, run_id)
    server.start()
    return server, {"agent_id": "7", "host": "127.0.0.1", "port": port}


# --------------------------------------------------------------------------------------------
# end to end over a real server
# --------------------------------------------------------------------------------------------

def test_a_file_moves_between_agents_and_verifies(tmp_path):
    src = _write(str(tmp_path / "producer" / "result.json"), b'{"answer": 42}' * 1000)
    dest_dir = str(tmp_path / "consumer")
    os.makedirs(dest_dir)
    server, loc = _serve(tmp_path, {"result.json": src})
    try:
        out = staging.fetch("result.json", loc, dest_dir, run_id="run-1", requester="9")
        assert out.ok, out.reason
        assert out.size == os.path.getsize(src)
        assert out.bytes_received == out.size
        with open(os.path.join(dest_dir, "result.json"), "rb") as fh:
            assert fh.read() == open(src, "rb").read()
    finally:
        server.stop(0)


def test_a_multi_chunk_file_arrives_whole(tmp_path):
    """The chunk boundary is where a streaming bug lives: off-by-one at the last read, or a
    digest taken over one chunk instead of the stream."""
    staging.configure(enabled=True, chunk_bytes=1024)
    payload = os.urandom(1024 * 7 + 13)      # not a chunk multiple, on purpose
    src = _write(str(tmp_path / "p" / "big.bin"), payload)
    dest_dir = str(tmp_path / "c")
    os.makedirs(dest_dir)
    server, loc = _serve(tmp_path, {"big.bin": src})
    try:
        out = staging.fetch("big.bin", loc, dest_dir, run_id="run-1")
        assert out.ok, out.reason
        got = open(os.path.join(dest_dir, "big.bin"), "rb").read()
        assert got == payload
        assert hashlib.sha256(got).hexdigest() == hashlib.sha256(payload).hexdigest()
    finally:
        server.stop(0)


def test_the_server_serves_only_what_it_published(tmp_path):
    """A name it did not publish has no path at all — there is no join to traverse."""
    _write(str(tmp_path / "p" / "secret.txt"), b"not yours")
    src = _write(str(tmp_path / "p" / "public.txt"), b"fine")
    dest_dir = str(tmp_path / "c")
    os.makedirs(dest_dir)
    server, loc = _serve(tmp_path, {"public.txt": src})
    try:
        # A name it did not publish: refused by the server, by name.
        out = staging.fetch("secret.txt", loc, dest_dir, run_id="run-1")
        assert not out.ok and "not produced by this agent" in out.reason

        # A name that is not a plain file name: refused by the CLIENT, before any connection,
        # because the local path is built from it. The server refusing is not enough — the temp
        # file is opened first, so a traversing name would write outside dest_dir on the way to
        # being told no.
        for name in ("../p/secret.txt", "/etc/passwd", "..", "sub/dir.txt"):
            out = staging.fetch(name, loc, dest_dir, run_id="run-1")
            assert not out.ok, f"{name} should be refused"
            assert "not a plain file name" in out.reason, out.reason

        assert not os.path.exists(os.path.join(dest_dir, "secret.txt"))
        assert os.listdir(dest_dir) == [], "no debris from any refused fetch"
    finally:
        server.stop(0)


def test_a_fetch_for_another_run_is_refused(tmp_path):
    """A stale location record from a previous run must not be answered with this run's file of
    the same name — the same guard the run-scoped registry key gives."""
    src = _write(str(tmp_path / "p" / "f.txt"), b"this run")
    dest_dir = str(tmp_path / "c")
    os.makedirs(dest_dir)
    server, loc = _serve(tmp_path, {"f.txt": src}, run_id="run-2")
    try:
        out = staging.fetch("f.txt", loc, dest_dir, run_id="run-1")
        assert not out.ok and "run" in out.reason
    finally:
        server.stop(0)


def test_a_published_but_deleted_file_names_the_workflow_fault(tmp_path):
    src = _write(str(tmp_path / "p" / "gone.txt"), b"x")
    dest_dir = str(tmp_path / "c")
    os.makedirs(dest_dir)
    server, loc = _serve(tmp_path, {"gone.txt": src})
    os.unlink(src)
    try:
        out = staging.fetch("gone.txt", loc, dest_dir, run_id="run-1")
        assert not out.ok and "no longer on disk" in out.reason
    finally:
        server.stop(0)


def test_an_unreachable_producer_is_reported_not_raised(tmp_path):
    """The dead-producer case. It must come back as a refusal naming the target, because under
    per-agent storage this is the one failure that has no local remedy
    (`docs/STAGING_DESIGN.md` §6)."""
    dest_dir = str(tmp_path / "c")
    os.makedirs(dest_dir)
    staging.configure(enabled=True, timeout_s=1.0)
    out = staging.fetch("x.txt", {"agent_id": "3", "host": "127.0.0.1", "port": 1},
                        dest_dir, run_id="run-1")
    assert not out.ok
    assert "could not reach" in out.reason and "127.0.0.1:1" in out.reason
    assert os.listdir(dest_dir) == [], "a failed fetch must leave no debris"


def test_a_location_without_host_or_port_is_refused(tmp_path):
    dest_dir = str(tmp_path / "c")
    os.makedirs(dest_dir)
    out = staging.fetch("x", {"agent_id": "3"}, dest_dir, run_id="r")
    assert not out.ok and "no host/port" in out.reason


def test_a_fetch_never_overwrites_a_file_that_appeared_meanwhile(tmp_path):
    """`os.link`, not `os.replace`: a parent job can finish and write this very file while the
    transfer is in flight, and a replace would clobber a fresh result with the older copy."""
    src = _write(str(tmp_path / "p" / "race.txt"), b"fetched")
    dest_dir = str(tmp_path / "c")
    os.makedirs(dest_dir)
    _write(os.path.join(dest_dir, "race.txt"), b"local parent output")
    server, loc = _serve(tmp_path, {"race.txt": src})
    try:
        out = staging.fetch("race.txt", loc, dest_dir, run_id="run-1")
        assert out.ok                        # the transfer itself succeeded
        assert open(os.path.join(dest_dir, "race.txt"), "rb").read() == b"local parent output"
    finally:
        server.stop(0)


# --------------------------------------------------------------------------------------------
# stage_inputs ordering — the part that goes quietly wrong
# --------------------------------------------------------------------------------------------

def test_a_produced_name_beats_a_same_named_file_in_the_inputs_root(tmp_path):
    """Workflow file names are a flat namespace. A name in the location registry was produced
    *by this run*; a file of the same name in the inputs root is a collision, not a copy. Taking
    the root would feed a child last week's file and look entirely healthy."""
    produced = _write(str(tmp_path / "p" / "shared.txt"), b"THIS RUN")
    inputs_root = str(tmp_path / "inputs")
    _write(os.path.join(inputs_root, "shared.txt"), b"stale from another workflow")
    work = str(tmp_path / "work")
    os.makedirs(work)

    staging.configure(enabled=True)
    runner.configure(mode="real", roots={"inputs": inputs_root})
    server, loc = _serve(tmp_path, {"shared.txt": produced})
    try:
        staged, refusal = runner.stage_inputs(
            [_Node("shared.txt")], work,
            locator=lambda names: {"shared.txt": loc}, run_id="run-1")
        assert refusal == "", refusal
        assert staged == ["shared.txt"]
        assert open(os.path.join(work, "shared.txt"), "rb").read() == b"THIS RUN"
    finally:
        server.stop(0)


def test_the_inputs_root_still_serves_a_name_nobody_produced(tmp_path):
    """Root inputs of the DAG are not produced by any job; that path is unchanged."""
    inputs_root = str(tmp_path / "inputs")
    _write(os.path.join(inputs_root, "seed.csv"), b"a,b\n1,2\n")
    work = str(tmp_path / "work")
    os.makedirs(work)
    staging.configure(enabled=True)
    runner.configure(mode="real", roots={"inputs": inputs_root})

    staged, refusal = runner.stage_inputs(
        [_Node("seed.csv")], work, locator=lambda names: {}, run_id="run-1")
    assert refusal == "" and staged == ["seed.csv"]
    assert open(os.path.join(work, "seed.csv"), "rb").read() == b"a,b\n1,2\n"


def test_a_produced_file_that_cannot_be_fetched_refuses_rather_than_falling_back(tmp_path):
    """Falling back to a same-named file elsewhere is exactly how a child silently reads the
    wrong input, so an unfetchable produced file is a refusal — and it names the producer,
    because the usual cause is that the producer died."""
    inputs_root = str(tmp_path / "inputs")
    _write(os.path.join(inputs_root, "shared.txt"), b"stale")
    work = str(tmp_path / "work")
    os.makedirs(work)
    staging.configure(enabled=True, timeout_s=1.0)
    runner.configure(mode="real", roots={"inputs": inputs_root})

    staged, refusal = runner.stage_inputs(
        [_Node("shared.txt")], work,
        locator=lambda names: {"shared.txt": {"agent_id": "5", "host": "127.0.0.1", "port": 1}},
        run_id="run-1")
    assert staged == []
    assert "produced by agent 5" in refusal and "could not be staged" in refusal
    assert not os.path.exists(os.path.join(work, "shared.txt"))


def test_a_file_already_in_the_work_dir_is_never_fetched(tmp_path):
    """A parent that ran on this agent. The lookup may happen; the transfer must not."""
    work = str(tmp_path / "work")
    _write(os.path.join(work, "parent.out"), b"already here")
    staging.configure(enabled=True)
    runner.configure(mode="real")

    calls = []

    def _locator(names):
        calls.append(names)
        return {"parent.out": {"agent_id": "1", "host": "127.0.0.1", "port": 1}}

    staged, refusal = runner.stage_inputs([_Node("parent.out")], work,
                                          locator=_locator, run_id="run-1")
    assert refusal == "" and staged == []
    assert open(os.path.join(work, "parent.out"), "rb").read() == b"already here"


def test_a_lookup_failure_falls_back_to_local_sources_rather_than_failing_the_job(tmp_path):
    """Redis being briefly unreachable is not a reason to fail a job whose input is on disk."""
    inputs_root = str(tmp_path / "inputs")
    _write(os.path.join(inputs_root, "seed.csv"), b"x")
    work = str(tmp_path / "work")
    os.makedirs(work)
    staging.configure(enabled=True)
    runner.configure(mode="real", roots={"inputs": inputs_root})

    def _boom(names):
        raise RuntimeError("redis down")

    staged, refusal = runner.stage_inputs([_Node("seed.csv")], work,
                                          locator=_boom, run_id="run-1")
    assert refusal == "" and staged == ["seed.csv"]


# --------------------------------------------------------------------------------------------
# staging off: nothing changes
# --------------------------------------------------------------------------------------------

def test_with_staging_off_the_context_is_not_consulted(tmp_path):
    """The shipped default. An existing run must behave exactly as it did."""
    inputs_root = str(tmp_path / "inputs")
    _write(os.path.join(inputs_root, "seed.csv"), b"x")
    work = str(tmp_path / "work")
    os.makedirs(work)
    runner.configure(mode="real", roots={"inputs": inputs_root})

    calls = []
    staging.configure(enabled=False)
    staging.set_context(locator=lambda n: calls.append(n) or {}, run_id="run-1", agent_id="2")

    staged, refusal = runner.stage_inputs([_Node("seed.csv")], work)
    assert refusal == "" and staged == ["seed.csv"]
    assert calls == [], "staging off must not reach the locator"


def test_the_context_supplies_the_locator_when_the_caller_cannot(tmp_path):
    """`Job.execute()` is rebuilt from Redis and has no route back to the agent, so the
    process-wide context is how it reaches the lookup at all."""
    produced = _write(str(tmp_path / "p" / "x.txt"), b"from peer")
    work = str(tmp_path / "work")
    os.makedirs(work)
    staging.configure(enabled=True)
    runner.configure(mode="real")
    server, loc = _serve(tmp_path, {"x.txt": produced})
    try:
        staging.set_context(locator=lambda names: {"x.txt": loc}, run_id="run-1", agent_id="9")
        staged, refusal = runner.stage_inputs([_Node("x.txt")], work)
        assert refusal == "" and staged == ["x.txt"]
        assert open(os.path.join(work, "x.txt"), "rb").read() == b"from peer"
    finally:
        server.stop(0)


# --------------------------------------------------------------------------------------------
# config
# --------------------------------------------------------------------------------------------

def test_an_unknown_staging_key_is_refused_not_ignored():
    """A misspelled key that silently did nothing would produce a run that looks staged and is
    not — the failure `resolve_mode` exists to prevent for `mode` itself."""
    with pytest.raises(ValueError) as exc:
        staging.configure(enabled=True, chunk_size=4096)
    assert "chunk_size" in str(exc.value)


def test_the_data_port_is_derived_from_the_consensus_port():
    staging.configure(enabled=True, port_offset=1000)
    assert staging.data_port(20001) == 21001
    staging.configure(enabled=True, port_offset=7)
    assert staging.data_port(20001) == 20008


def test_server_stats_count_what_was_served(tmp_path):
    src = _write(str(tmp_path / "p" / "a.txt"), b"12345")
    dest_dir = str(tmp_path / "c")
    os.makedirs(dest_dir)
    server, loc = _serve(tmp_path, {"a.txt": src})
    try:
        staging.fetch("a.txt", loc, dest_dir, run_id="run-1")
        staging.fetch("nope.txt", loc, dest_dir, run_id="run-1")
        stats = server.stats()
        assert stats["served"] == 1 and stats["bytes_served"] == 5
        assert stats["refused"] == 1 and stats["published"] == 1
    finally:
        server.stop(0)


# --------------------------------------------------------------------------------------------
# the agent seam: publish on one agent, locate through the registry, fetch on another
# --------------------------------------------------------------------------------------------

def _agent(tmp_path, agent_id=7, port=20007):
    """A ResourceAgent with only what the publish path touches."""
    from unittest.mock import MagicMock

    from swarm.agents.resource_agent import ResourceAgent
    a = ResourceAgent.__new__(ResourceAgent)
    a.logger = MagicMock()
    a.agent_id = agent_id
    a.grpc_config = {"host": "127.0.0.1", "port": port}
    a.topology = MagicMock(level=0, group=0)
    a.repository = MagicMock()
    a.staged_files = staging.PublishedFiles()
    a._unpersisted_completions = {}
    a._unpublished_lock = threading.RLock()
    return a


class _Job:
    def __init__(self, job_id="j1"):
        self.job_id = job_id

    def to_dict(self):
        return {"id": self.job_id, "exit_status": 0}


def test_publishing_records_where_each_output_is(tmp_path):
    work = str(tmp_path / "work")
    _write(os.path.join(work, "out.json"), b"{}")
    staging.configure(enabled=True, port_offset=1000)
    runner.configure(mode="real", work_dir=work)
    a = _agent(tmp_path, agent_id=7, port=20007)

    locations = a._publish_locations(_Job(), ["out.json"])

    assert set(locations) == {"out.json"}
    loc = locations["out.json"]
    assert loc["agent_id"] == "7"
    assert loc["host"] == "127.0.0.1"
    assert loc["port"] == 21007, "the data port is the consensus port plus the offset"
    assert a.staged_files.path_for("out.json") == os.path.join(work, "out.json")


def test_with_staging_off_nothing_is_published(tmp_path):
    work = str(tmp_path / "work")
    _write(os.path.join(work, "out.json"), b"{}")
    runner.configure(mode="real", work_dir=work)
    staging.configure(enabled=False)
    a = _agent(tmp_path)

    assert a._publish_locations(_Job(), ["out.json"]) == {}
    assert len(a.staged_files) == 0


def test_a_declared_output_that_was_never_written_is_reported_at_the_producer(tmp_path):
    """The job exited 0 and said it wrote this. Here is where that is visible; the location is
    still recorded so a consumer's refusal names this agent and this path."""
    work = str(tmp_path / "work")
    os.makedirs(work)
    staging.configure(enabled=True)
    runner.configure(mode="real", work_dir=work)
    a = _agent(tmp_path)

    locations = a._publish_locations(_Job(), ["missing.json"])

    assert "missing.json" in locations
    logged = " ".join(str(c.args) for c in a.logger.error.call_args_list)
    assert "missing.json" in logged and "does not exist" in logged


def test_locations_ride_the_completion_transaction(tmp_path):
    """The same one-write rule the names follow. A descendant released by a name it cannot
    locate would refuse to stage — a torn write that reads as a staging bug."""
    work = str(tmp_path / "work")
    _write(os.path.join(work, "out.json"), b"{}")
    staging.configure(enabled=True)
    runner.configure(mode="real", work_dir=work)
    a = _agent(tmp_path)
    locations = a._publish_locations(_Job(), ["out.json"])

    a._persist_completion(_Job(), ["out.json"], locations)

    kwargs = a.repository.save.call_args.kwargs
    assert kwargs["produced_data"] == ["out.json"]
    assert kwargs["produced_locations"] == locations
    assert kwargs["produced_locations"]["out.json"]["port"] == 21007


def test_the_whole_loop_producer_to_registry_to_consumer(tmp_path):
    """End to end across two agents with a real server and a real transfer: A produces and
    publishes, the registry carries the location, B stages it into its own work dir."""
    import socket
    s = socket.socket(); s.bind(("127.0.0.1", 0)); free = s.getsockname()[1]; s.close()

    work_a = str(tmp_path / "a")
    _write(os.path.join(work_a, "parent.out"), b"produced by A")
    staging.configure(enabled=True, port_offset=0)       # data port == the port we bound
    runner.configure(mode="real", work_dir=work_a)
    producer = _agent(tmp_path, agent_id=3, port=free)
    locations = producer._publish_locations(_Job("parent"), ["parent.out"])

    server = staging.TransferServer(producer.staged_files, "127.0.0.1", free, run_id="run-1")
    server.start()
    try:
        # The registry is the only thing that crosses between them.
        registry = dict(locations)
        work_b = str(tmp_path / "b")
        os.makedirs(work_b)
        runner.configure(mode="real", work_dir=work_b)
        staged, refusal = runner.stage_inputs(
            [_Node("parent.out")], work_b,
            locator=lambda names: {n: registry[n] for n in names if n in registry},
            run_id="run-1", requester="9")

        assert refusal == "", refusal
        assert staged == ["parent.out"]
        assert open(os.path.join(work_b, "parent.out"), "rb").read() == b"produced by A"
    finally:
        server.stop(0)


# --------------------------------------------------------------------------------------------
# the location registry (fakeredis, no server needed)
# --------------------------------------------------------------------------------------------

def _repo():
    import fakeredis

    from swarm.database.repository import Repository
    return Repository(redis_client=fakeredis.FakeStrictRedis(decode_responses=True))


def test_a_location_round_trips_through_the_registry():
    repo = _repo()
    loc = {"agent_id": "3", "host": "10.0.0.3", "port": 21003, "produced_at": 1.0}
    repo.save({"id": "j1", "state": 8}, produced_data=["a.txt"], produced_locations={"a.txt": loc})

    assert repo.data_available(["a.txt"]) is True
    assert repo.data_locations(["a.txt"]) == {"a.txt": loc}


def test_readiness_and_location_land_in_the_same_write():
    """The invariant: a name can never be observable as ready without its location. As two
    writes, a descendant released between them refuses to stage and it reads as a staging bug."""
    repo = _repo()
    repo.save({"id": "j1", "state": 8},
              produced_data=["x", "y"],
              produced_locations={"x": {"host": "h", "port": 1}, "y": {"host": "h", "port": 1}})
    ready = repo.data_available(["x", "y"])
    located = repo.data_locations(["x", "y"])
    assert ready and set(located) == {"x", "y"}


def test_a_name_with_no_location_is_absent_not_guessed():
    """Staging off: the name is ready, the shared mount is the location. The caller must be able
    to tell that from "produced somewhere unreachable"."""
    repo = _repo()
    repo.save({"id": "j1", "state": 8}, produced_data=["a.txt"])
    assert repo.data_available(["a.txt"]) is True
    assert repo.data_locations(["a.txt"]) == {}


def test_a_corrupt_location_reads_as_absent():
    repo = _repo()
    repo.redis.hset(repo._data_loc_key(), mapping={"bad": "{not json"})
    assert repo.data_locations(["bad"]) == {}


def test_the_location_key_is_run_scoped_and_cleanable():
    """Both halves, for the same reasons as the readiness key: `delete_all("*")` must reach it
    between runs, and a cell that skipped cleanup must not inherit locations pointing at an
    agent that is no longer serving that file."""
    repo = _repo()
    key = repo._data_loc_key()
    assert key.count(":") >= 2, "a bare key survives delete_all('*')"
    assert str(repo.run_id) in key


def test_locations_for_unknown_names_are_simply_missing():
    repo = _repo()
    repo.save({"id": "j1", "state": 8}, produced_data=["a"],
              produced_locations={"a": {"host": "h", "port": 1}})
    got = repo.data_locations(["a", "never-produced"])
    assert set(got) == {"a"}
    assert repo.data_locations([]) == {}
