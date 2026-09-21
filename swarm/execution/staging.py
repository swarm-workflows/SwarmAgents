# MIT License
#
# Copyright (c) 2024 swarm-workflows
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# Author: Komal Thareja(kthare10@renci.org)
"""Moving a workflow's files between agents.

Design and the decisions behind it: `docs/STAGING_DESIGN.md`. In short — a job's outputs live on
the agent that produced them, the location registry says which agent that is, and a consumer
elsewhere fetches them over a **second gRPC port on the same host consensus uses**, so transfers
traverse the measured data-plane paths without sharing a channel with consensus traffic.

Off by default. With `runtime.execution.staging.enabled: false` (the shipped value) nothing here
runs and `stage_inputs` behaves exactly as it did: local work dir, then the inputs root.
"""
from __future__ import annotations

import hashlib
import logging
import os
import threading
import time
from concurrent import futures
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple

import grpc

from swarm.comm import consensus_pb2, consensus_pb2_grpc

logger = logging.getLogger(__name__)

#: Big enough that a WAN round trip is amortised, small enough that a chunk is not a memory
#: event on an agent already running jobs.
DEFAULT_CHUNK_BYTES = 1 << 20


@dataclass
class StagingPolicy:
    """Process-wide staging policy, set once at agent startup from
    `runtime.execution.staging`. Class-level for the same reason `ExecutionPolicy` is: a `Job`
    is rebuilt from Redis on every read and has no route back to the agent's config."""

    enabled: bool = False
    #: Data port = the agent's consensus port + this. One offset rather than a second per-agent
    #: port assignment, so a fleet that already allocates one port per agent gets the second for
    #: free and cannot allocate the two inconsistently.
    port_offset: int = 1000
    chunk_bytes: int = DEFAULT_CHUNK_BYTES
    timeout_s: float = 300.0
    #: The staging site an output is pushed to as it is produced, so it outlives the agent that
    #: made it. Empty disables stage-out and leaves staging peer-only — fast, and **not durable**:
    #: a dead producer's outputs are gone, its job is COMPLETE so nothing re-runs it, and every
    #: descendant is released and then refuses (`docs/STAGING_DESIGN.md` §6).
    store_host: str = ""
    store_port: int = 21000
    #: How long a stage-out may take before the producer gives up and keeps going. A failed push
    #: costs durability for that file, not the job: the peer location still works until the
    #: producer dies, which is exactly the window the push was insuring against.
    store_timeout_s: float = 120.0
    #: Digest the stream at the sender and check it at the receiver. On by default: it costs
    #: nothing when nothing is transferred, and it catches the one failure that is otherwise
    #: invisible — a truncated transfer, which yields a short file that a job will happily read.
    verify: bool = True


_POLICY = StagingPolicy()


def configure(**kwargs) -> StagingPolicy:
    global _POLICY
    known = {f for f in StagingPolicy.__dataclass_fields__}
    unknown = set(kwargs) - known
    if unknown:
        # Refused rather than ignored: a misspelled staging key that silently did nothing would
        # produce a run that looks staged and is not, which is the failure `resolve_mode` exists
        # to prevent for `mode` itself.
        raise ValueError(
            f"unknown runtime.execution.staging keys: {sorted(unknown)}; known: {sorted(known)}")
    _POLICY = StagingPolicy(**kwargs)
    return _POLICY


def policy() -> StagingPolicy:
    return _POLICY


def data_port(grpc_port: int, pol: Optional[StagingPolicy] = None) -> int:
    return int(grpc_port) + int((pol or _POLICY).port_offset)


# --------------------------------------------------------------------------------------------
# Serving
# --------------------------------------------------------------------------------------------

class PublishedFiles:
    """The names this agent will serve, and where each one is on its disk.

    **A map, never a directory join.** The server resolves a requested name by lookup here and
    nowhere else, so a name it has not published has no path at all and there is no traversal
    surface to defend — the same reasoning that makes `resolve_under_root` refuse a relative
    path that climbs out of its root, arrived at by removing the join instead of checking it.
    """

    def __init__(self):
        self._lock = threading.RLock()
        self._paths: Dict[str, str] = {}

    def publish(self, name: str, path: str) -> None:
        with self._lock:
            self._paths[str(name)] = os.path.abspath(path)

    def publish_all(self, mapping: Dict[str, str]) -> None:
        with self._lock:
            for name, path in (mapping or {}).items():
                self._paths[str(name)] = os.path.abspath(path)

    def path_for(self, name: str) -> Optional[str]:
        with self._lock:
            return self._paths.get(str(name))

    def names(self) -> Tuple[str, ...]:
        with self._lock:
            return tuple(self._paths)

    def __len__(self) -> int:
        with self._lock:
            return len(self._paths)


@dataclass
class StagingContext:
    """Who this agent is and how it looks a name up — process-wide, for the same reason the
    policy is: `Job.execute()` reaches the runner with no route back to the agent, because a
    `Job` is rebuilt from Redis on every read. Installed once at startup by the agent.

    `locator` is `Repository.data_locations`. Holding the bound method rather than importing
    the repository keeps the execution package free of a dependency on the data layer, and
    makes both halves trivially substitutable in a test.
    """

    locator: Optional[object] = None
    run_id: str = ""
    agent_id: str = ""
    published: PublishedFiles = field(default_factory=lambda: PublishedFiles())


_CONTEXT = StagingContext()


def set_context(locator=None, run_id: str = "", agent_id: str = "",
                published: Optional["PublishedFiles"] = None) -> StagingContext:
    global _CONTEXT
    _CONTEXT = StagingContext(locator=locator, run_id=str(run_id or ""),
                              agent_id=str(agent_id or ""),
                              published=published or PublishedFiles())
    return _CONTEXT


def context() -> StagingContext:
    return _CONTEXT


def _safe_run_dir(run_id: str) -> str:
    """A run id as one directory component. Run ids are minted by `run_test.py`, but they reach
    the store over the wire, so the same containment rule the file names get applies: this is
    joined onto the store directory and must not climb out of it."""
    safe = os.path.basename(str(run_id))
    return safe if safe and safe not in (".", "..") else "_unnamed"


class _Servicer(consensus_pb2_grpc.DataTransferServiceServicer):
    def __init__(self, published: PublishedFiles, run_id: str,
                 pol: Optional[StagingPolicy] = None, store_dir: Optional[str] = None):
        self.published = published
        self.run_id = str(run_id or "")
        self.pol = pol or _POLICY
        #: Set only on a staging site. An ordinary agent serves what it produced and takes no
        #: uploads, so `Put` has nowhere to write and says so — the capability is granted by
        #: configuration rather than assumed from the request.
        self.store_dir = store_dir
        self.served = 0
        self.bytes_served = 0
        self.refused = 0
        self.stored = 0
        self.bytes_stored = 0
        self.put_refused = 0

    def _refuse(self, reason: str):
        self.refused += 1
        logger.warning("[STAGE_SERVE] refused: %s", reason)
        yield consensus_pb2.DataChunk(error=reason, last=True)

    def Fetch(self, request, context):  # noqa: N802 (gRPC naming)
        name = str(request.name or "")
        if request.run_id and self.run_id and request.run_id != self.run_id:
            # The same guard the run-scoped registry key gives: a stale location record from a
            # previous run must not be answered with this run's file of the same name.
            yield from self._refuse(
                f"{name!r} requested for run {request.run_id!r} but this agent serves "
                f"{self.run_id!r}")
            return
        # At a staging site the namespace is (run, name), not name: one store serves many
        # runs and workflow file names are a flat namespace — 62 colliding names measured in
        # the shipped profile — so `x.txt` from another run is a different file that happens
        # to share a spelling. An agent serves one run's outputs and needs no such key.
        path = self.published.path_for(
            f"{request.run_id}/{name}" if self.store_dir else name)
        if not path:
            yield from self._refuse(
                f"{name!r} was not produced by this agent in this run "
                f"({len(self.published)} names published)")
            return
        if not os.path.isfile(path):
            # Published but gone: the work dir was cleaned, or the file was never written
            # despite a successful exit. Say which name, so the refusal names a workflow fault
            # rather than a network one.
            yield from self._refuse(f"{name!r} is published but no longer on disk at {path}")
            return
        try:
            size = os.path.getsize(path)
            digest = hashlib.sha256() if self.pol.verify else None
            sent = 0
            with open(path, "rb") as fh:
                while True:
                    block = fh.read(self.pol.chunk_bytes)
                    if not block:
                        break
                    if digest is not None:
                        digest.update(block)
                    sent += len(block)
                    yield consensus_pb2.DataChunk(
                        content=block, size=size, last=False)
            # A final, empty chunk carries the digest. Separating it from the last data chunk
            # keeps the sender from having to know which read was the last one.
            yield consensus_pb2.DataChunk(
                size=size, last=True, sha256=digest.hexdigest() if digest else "")
            self.served += 1
            self.bytes_served += sent
            logger.info("[STAGE_SERVE] %s -> %s (%d bytes)", name, request.requester or "?", sent)
        except OSError as exc:
            yield from self._refuse(f"could not read {name!r}: {exc}")


    def Put(self, request_iterator, context):  # noqa: N802 (gRPC naming)
        """Accept one uploaded file into the store.

        Placement follows the same two rules a fetch does, for the same reasons: a unique
        temporary file first, then **`os.link`, not `os.replace`** — a second upload of the
        same name (a re-run after reassignment, say) must not truncate a copy something is
        already reading. A re-upload is therefore idempotent and harmless.
        """
        if not self.store_dir:
            self.put_refused += 1
            return consensus_pb2.PutAck(
                ok=False, error="this server is not a staging site; it takes no uploads")

        name = local = None
        digest = hashlib.sha256() if self.pol.verify else None
        received = 0
        tmp = None
        fh = None
        try:
            for chunk in request_iterator:
                if name is None:
                    name = str(chunk.name or "")
                    # Containment where the path is built, exactly as in `fetch`: the name is
                    # workflow-supplied, and joined onto the store a traversing name writes
                    # outside it.
                    local = os.path.basename(name)
                    if not local or local in (".", "..") or local != name:
                        self.put_refused += 1
                        return consensus_pb2.PutAck(
                            ok=False, error=f"{name!r} is not a plain file name")
                    if chunk.run_id and self.run_id and chunk.run_id != self.run_id:
                        self.put_refused += 1
                        return consensus_pb2.PutAck(
                            ok=False,
                            error=f"upload for run {chunk.run_id!r} but this store serves "
                                  f"{self.run_id!r}")
                    run = str(chunk.run_id or "")
                    if not run:
                        # Without a run there is no namespace to put this in, and a flat store
                        # would let the next run's file of the same name collide with it — and
                        # since the placement rule is never-overwrite, the collision resolves
                        # *silently in favour of the older file*, which the store then serves.
                        self.put_refused += 1
                        return consensus_pb2.PutAck(
                            ok=False, error="upload carries no run_id; a store is keyed by "
                                            "(run, name) and cannot file it")
                    run_dir = os.path.join(self.store_dir, _safe_run_dir(run))
                    os.makedirs(run_dir, exist_ok=True)
                    tmp = os.path.join(
                        run_dir,
                        f".{local}.put.{os.getpid()}.{threading.get_ident()}")
                    fh = open(tmp, "wb")
                if chunk.content:
                    fh.write(chunk.content)
                    received += len(chunk.content)
                    if digest is not None:
                        digest.update(chunk.content)
                if chunk.last:
                    if digest is not None and chunk.sha256:
                        got = digest.hexdigest()
                        if got != chunk.sha256:
                            self.put_refused += 1
                            return consensus_pb2.PutAck(
                                ok=False,
                                error=(f"{name!r} failed verification: sender "
                                       f"{chunk.sha256[:12]}… store {got[:12]}…"))
                    break
            if fh is None:
                self.put_refused += 1
                return consensus_pb2.PutAck(ok=False, error="empty upload")
            fh.close()
            fh = None
            dest = os.path.join(run_dir, local)
            try:
                os.link(tmp, dest)
            except FileExistsError:
                # Already stored *for this run*. Keeping the first copy is the never-overwrite
                # rule — something may be reading it — but within one run two different bodies
                # under one name is a real collision, so say so rather than report a clean
                # store of bytes that were discarded.
                if digest is not None:
                    try:
                        with open(dest, "rb") as held:
                            existing = hashlib.sha256(held.read()).hexdigest()
                        if existing != digest.hexdigest():
                            logger.error(
                                "[STAGE_STORE] %s/%s already held with DIFFERENT content "
                                "(held %s… vs offered %s…); the held copy stands and the "
                                "upload was discarded", run, local,
                                existing[:12], digest.hexdigest()[:12])
                    except OSError:
                        pass
            # Stored files are servable: the store is a peer like any other from a consumer's
            # point of view, which is what makes it a fallback rather than a special case.
            self.published.publish(f"{run}/{local}", dest)
            self.stored += 1
            self.bytes_stored += received
            logger.info("[STAGE_STORE] %s/%s from %s (%d bytes)", run, local,
                        chunk.sender or "?", received)
            return consensus_pb2.PutAck(ok=True, bytes=received)
        except OSError as exc:
            self.put_refused += 1
            return consensus_pb2.PutAck(ok=False, error=f"could not store {name!r}: {exc}")
        finally:
            if fh is not None:
                try:
                    fh.close()
                except OSError:
                    pass
            if tmp:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass


class TransferServer:
    """The agent's data-transfer endpoint. Started beside the consensus server, on its own port."""

    def __init__(self, published: PublishedFiles, bind_host: str, port: int, run_id: str,
                 max_workers: int = 8, pol: Optional[StagingPolicy] = None,
                 store_dir: Optional[str] = None):
        self.published = published
        self.bind_host = bind_host
        self.port = int(port)
        self.pol = pol or _POLICY
        if store_dir:
            os.makedirs(store_dir, exist_ok=True)
        self.store_dir = store_dir
        self.servicer = _Servicer(published, run_id, self.pol, store_dir=store_dir)
        # A modest pool on purpose: this shares a host with an agent that is running jobs, and
        # an unbounded pool would let a fan-in of fetches compete with the work being measured.
        self._server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=max_workers),
            options=[("grpc.max_send_message_length", self.pol.chunk_bytes * 4),
                     ("grpc.max_receive_message_length", self.pol.chunk_bytes * 4)])
        consensus_pb2_grpc.add_DataTransferServiceServicer_to_server(self.servicer, self._server)
        self._started = False

    def start(self) -> None:
        if self._started:
            return
        self._server.add_insecure_port(f"{self.bind_host}:{self.port}")
        self._server.start()
        self._started = True
        logger.info("[STAGE_SERVE] data transfer listening on %s:%d", self.bind_host, self.port)

    def stop(self, grace: float = 1.0) -> None:
        if self._started:
            self._server.stop(grace)
            self._started = False

    def stats(self) -> Dict[str, int]:
        return {"served": self.servicer.served,
                "bytes_served": self.servicer.bytes_served,
                "refused": self.servicer.refused,
                "stored": self.servicer.stored,
                "bytes_stored": self.servicer.bytes_stored,
                "put_refused": self.servicer.put_refused,
                "published": len(self.published)}


# --------------------------------------------------------------------------------------------
# Fetching
# --------------------------------------------------------------------------------------------

@dataclass
class FetchResult:
    ok: bool
    reason: str = ""
    size: int = 0
    bytes_received: int = 0


def fetch(name: str, location: dict, dest_dir: str, run_id: str, requester: str = "",
          pol: Optional[StagingPolicy] = None) -> FetchResult:
    """Fetch one produced file from the agent that has it, into *dest_dir*.

    Placement follows the same two rules the local staging path uses, for the same reasons:
    write to a unique temporary name first, then **`os.link`, not `os.replace`** — a parent job
    can finish and write this very file while the transfer is in flight, and a replace would
    clobber a fresh result with the copy that started earlier.

    The digest is checked before the file is put in place, so a truncated transfer never becomes
    a short file that a job reads happily.
    """
    pol = pol or _POLICY

    # Containment, at the point the local path is built rather than anywhere downstream. The
    # name comes from a job record, which is workflow-supplied data: joined onto `dest_dir` a
    # name like `../../x` writes outside the working directory, and it does so *before* the
    # server's refusal is ever read, so the server being careful is not enough. A staged file
    # is always a plain name in the working directory — `stage_inputs` already basenames for
    # the same reason, and this makes the rule hold for every caller.
    local = os.path.basename(str(name))
    if not local or local in (".", "..") or local != str(name):
        return FetchResult(
            False, f"{name!r} is not a plain file name; refusing to stage it")

    host, port = location.get("host"), location.get("port")
    if not host or not port:
        return FetchResult(False, f"location for {name!r} has no host/port: {location!r}")

    target = f"{host}:{int(port)}"
    tmp = os.path.join(dest_dir, f".{local}.fetch.{os.getpid()}.{threading.get_ident()}")
    digest = hashlib.sha256() if pol.verify else None
    received = 0
    size = 0
    try:
        with grpc.insecure_channel(
                target,
                options=[("grpc.max_send_message_length", pol.chunk_bytes * 4),
                         ("grpc.max_receive_message_length", pol.chunk_bytes * 4)]) as channel:
            stub = consensus_pb2_grpc.DataTransferServiceStub(channel)
            request = consensus_pb2.FetchRequest(
                name=str(name), run_id=str(run_id or ""), requester=str(requester or ""))
            with open(tmp, "wb") as out:
                for chunk in stub.Fetch(request, timeout=pol.timeout_s):
                    if chunk.error:
                        return FetchResult(False, f"{target} refused {name!r}: {chunk.error}")
                    if chunk.size:
                        size = int(chunk.size)
                    if chunk.content:
                        out.write(chunk.content)
                        received += len(chunk.content)
                        if digest is not None:
                            digest.update(chunk.content)
                    if chunk.last:
                        if digest is not None and chunk.sha256:
                            got = digest.hexdigest()
                            if got != chunk.sha256:
                                return FetchResult(
                                    False,
                                    f"{name!r} failed verification from {target}: sender "
                                    f"{chunk.sha256[:12]}… receiver {got[:12]}…",
                                    size, received)
                        break
        if size and received != size:
            # Belt and braces for the unverified path: a stream that ends early without an
            # error chunk is otherwise indistinguishable from a complete one.
            return FetchResult(False,
                               f"{name!r} truncated from {target}: {received} of {size} bytes",
                               size, received)
        dest = os.path.join(dest_dir, local)
        try:
            os.link(tmp, dest)
        except FileExistsError:
            pass                            # someone won the race; their copy stands
        return FetchResult(True, "", size, received)
    except grpc.RpcError as exc:
        # The producing agent is unreachable. Worth naming precisely, because the most likely
        # cause is that it died — and a dead producer's outputs are simply gone under per-agent
        # storage. See `docs/STAGING_DESIGN.md` §6: this is the case that needs a durability
        # decision before a failure-injection cell runs with staging on.
        code = exc.code() if hasattr(exc, "code") else "?"
        return FetchResult(False, f"could not reach {target} for {name!r}: {code}")
    except OSError as exc:
        return FetchResult(False, f"could not write {name!r} while staging: {exc}")
    finally:
        try:
            os.unlink(tmp)
        except OSError:
            pass


@dataclass
class PutResult:
    ok: bool
    reason: str = ""
    bytes_sent: int = 0
    elapsed_s: float = 0.0


def put(name: str, path: str, run_id: str, sender: str = "",
        pol: Optional[StagingPolicy] = None) -> PutResult:
    """Push one produced file to the staging site, so it outlives the agent that made it.

    This is the durability half of staging (`docs/STAGING_DESIGN.md` §6). Peer-to-peer fetch
    alone is fast and **not durable**: a dead producer's outputs are gone, its job is COMPLETE
    so nothing re-runs it, and every descendant is released and then refuses. Pushing each
    output as it is produced removes that without the cascading re-runs that re-running the
    producer would cause — its own inputs may be gone too.

    **It costs an upload on the critical path of every DAG edge**, which lands on makespan.
    `elapsed_s` is returned so that cost is a measured number rather than an assumption.
    """
    pol = pol or _POLICY
    if not pol.store_host:
        return PutResult(False, "no staging site configured (staging.store_host is empty)")
    local = os.path.basename(str(name))
    if not local or local in (".", "..") or local != str(name):
        return PutResult(False, f"{name!r} is not a plain file name; refusing to stage it out")
    if not os.path.isfile(path):
        return PutResult(False, f"{name!r} is not on disk at {path}")

    target = f"{pol.store_host}:{int(pol.store_port)}"
    started = time.time()
    try:
        size = os.path.getsize(path)
        digest = hashlib.sha256() if pol.verify else None

        def _chunks():
            sent = 0
            with open(path, "rb") as fh:
                while True:
                    block = fh.read(pol.chunk_bytes)
                    if not block:
                        break
                    if digest is not None:
                        digest.update(block)
                    sent += len(block)
                    yield consensus_pb2.PutChunk(
                        name=local, run_id=str(run_id or ""), content=block,
                        last=False, sender=str(sender or ""))
            # A final, empty chunk carries the digest, so the sender never has to know which
            # read was the last one.
            yield consensus_pb2.PutChunk(
                name=local, run_id=str(run_id or ""), last=True,
                sha256=digest.hexdigest() if digest else "", sender=str(sender or ""))

        with grpc.insecure_channel(
                target,
                options=[("grpc.max_send_message_length", pol.chunk_bytes * 4),
                         ("grpc.max_receive_message_length", pol.chunk_bytes * 4)]) as channel:
            stub = consensus_pb2_grpc.DataTransferServiceStub(channel)
            ack = stub.Put(_chunks(), timeout=pol.store_timeout_s)
        elapsed = time.time() - started
        if not ack.ok:
            return PutResult(False, f"{target} refused {name!r}: {ack.error}", 0, elapsed)
        return PutResult(True, "", int(ack.bytes or size), elapsed)
    except grpc.RpcError as exc:
        code = exc.code() if hasattr(exc, "code") else "?"
        return PutResult(False, f"could not reach staging site {target} for {name!r}: {code}",
                         0, time.time() - started)
    except OSError as exc:
        return PutResult(False, f"could not read {name!r} to stage it out: {exc}",
                         0, time.time() - started)


def fetch_any(name: str, locations, dest_dir: str, run_id: str, requester: str = "",
              pol: Optional[StagingPolicy] = None) -> FetchResult:
    """Fetch *name* from the first location that answers, **in the order given**.

    The order is the preference and it is set by the producer: the peer that made the file
    first, the staging site last. That gives the fast path normally — one hop, straight from
    the producer — and durability exactly when it is needed, which is when the producer has
    gone. Falling straight to the store instead would make every edge pay two WAN hops.

    Every location's reason is kept and reported together on total failure. With one location
    that reads exactly as `fetch` did; with several, "could not reach agent-4" alone would hide
    that the store refused it too, which is the difference between a dead agent and a file that
    was never stored.
    """
    if isinstance(locations, dict):          # a single location, as an earlier revision wrote
        locations = [locations]
    reasons = []
    for loc in locations or []:
        result = fetch(name, loc, dest_dir, run_id=run_id, requester=requester, pol=pol)
        if result.ok:
            return result
        reasons.append(f"{loc.get('agent_id', loc.get('host', '?'))}: {result.reason}")
    if not reasons:
        return FetchResult(False, f"no location is recorded for {name!r}")
    return FetchResult(False, f"{name!r} could not be fetched from any of "
                              f"{len(reasons)} location(s) — " + "; ".join(reasons))
