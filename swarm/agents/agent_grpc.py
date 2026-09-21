# MIT License
#
# Copyright (c) 2024 swarm-workflows

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# Author: Komal Thareja(kthare10@renci.org)

import json
import queue
import logging
import os
import threading
import time
import traceback
from abc import abstractmethod
from typing import Tuple
from logging.handlers import RotatingFileHandler

import redis
import yaml

from swarm.comm.grpc_transport import GrpcTransport
from swarm.utils.queues import AgentQueues
from swarm.consensus.messages.message import MessageType, Message
from swarm.comm.observer import Observer
from swarm.database.repository import Repository
from swarm.models.agent_info import AgentInfo
from swarm.utils.thread_safe_dict import ThreadSafeDict
from swarm.topology.topology import Topology, TopologyType
from swarm.utils.iterable_queue import IterableQueue
from swarm.utils.yaml_strict import safe_load as yaml_safe_load_strict


class Agent(Observer):
    def __init__(self, agent_id: int, config_file: str, debug: bool = False):
        self.debug = debug
        self.agent_id = agent_id
        self.messages_dropped = 0  # inbound messages shed by the bounded queue
        self.neighbor_map = ThreadSafeDict[int, AgentInfo]()
        self.children = ThreadSafeDict[int, AgentInfo]()
        self.parents = ThreadSafeDict[int, AgentInfo]()
        self.peer_by_endpoint = ThreadSafeDict[
            Tuple[str, int],
            Tuple[int, bool]
        ]()


        with open(config_file, 'r') as f:
            # Strict: a duplicate key raises rather than silently keeping the last value.
            # `runtime.peer_expiry_seconds` was defined twice in the shipped config for the whole
            # campaign, so the documented 300 s was really 45 s and nothing said so.
            self.config = yaml_safe_load_strict(f)

        self.queues = AgentQueues()
        self._init_stop_state()
        self.grpc_config = self.config.get("grpc", {})
        self.log_config = self.config.get("logging", {})
        self.runtime_config = self.config.get("runtime", {})
        self.redis_config = self.config.get("redis", {"host": "127.0.0.1", "port": 6379})
        self.topology = Topology(topo=self.config.get("topology", {}))
        self.logger = self._setup_logger()

        self.redis_client = redis.StrictRedis(host=self.redis_config["host"],
                                              port=self.redis_config["port"],
                                              decode_responses=True)
        self.repository = Repository(redis_client=self.redis_client)

        self._configure_job_execution_simulation()
        self._configure_real_execution()

        self.condition = threading.Condition()
        self.shutdown = False
        self.shutdown_path = "./shutdown"

        self.transport = GrpcTransport(host=self.grpc_host, port=self.grpc_port,
                                       logger=self.logger, on_peer_status=self.on_peer_status)

        self.threads = {
            "periodic": threading.Thread(target=self._do_periodic, daemon=True),
            "inbound": threading.Thread(target=self._do_inbound, daemon=True),
        }
        self.last_non_empty_time = time.time()

    def _configure_job_execution_simulation(self) -> None:
        """Apply `runtime.wall_time_*` to the process-wide job execution simulation.

        Warns when the legacy flat-1 s policy is selected, because a run under it cannot report
        makespan, throughput or utilisation honestly — every job takes the same time regardless
        of its real duration.
        """
        from swarm.models.job import Job

        scale = float(self.runtime_config.get("wall_time_scale", 1.0))
        Job.configure_execution_simulation(
            scale=scale,
            min_s=float(self.runtime_config.get("wall_time_min_s", 0.0)),
            max_s=float(self.runtime_config.get("wall_time_max_s", 120.0)),
        )
        if scale <= 0:
            self.logger.warning(
                "[EXEC_SIM] runtime.wall_time_scale=%s — every job will simulate a flat 1s "
                "regardless of its wall_time. Makespan, throughput and utilisation from this "
                "run are NOT meaningful.", scale)
        else:
            self.logger.info(
                "[EXEC_SIM] wall_time_scale=%s min=%ss max=%ss", scale,
                Job._WALL_TIME_MIN_S, Job._WALL_TIME_MAX_S)

    def _configure_real_execution(self) -> None:
        """Apply `runtime.execution` to the process-wide real-execution policy.

        Default is `simulate`, so an agent that says nothing about execution behaves exactly
        as it always has. `real` is logged at WARNING because it changes what the run *is* —
        exit statuses become the processes' own rather than the replayed ones, and job
        durations become real work rather than a scaled wall_time — and because a run that
        was meant to be simulated and quietly executed instead would be hard to spot
        afterwards from the metrics alone.
        """
        from swarm.execution import runner

        cfg = (self.runtime_config.get("execution") or {}) if self.runtime_config else {}
        # Resolved in runner.resolve_mode, which owns the default and the refusal — anything
        # else that needs to know what this run will do (run_test.py's bundle validation) asks
        # the same function rather than re-deriving it.
        mode = runner.resolve_mode(cfg)

        work_dir = str(cfg.get("work_dir", "") or "")
        # A shared scratch per run, so logical file names resolve between jobs of one DAG
        # while two concurrent runs cannot overwrite each other's outputs.
        run_id = os.environ.get("SWARM_RUN_ID", "")
        if work_dir and run_id:
            work_dir = os.path.join(work_dir, run_id)

        # A bundle expands to the three roots. `pegasus_to_swarm_converter.py` writes
        # `code/`, `inputs/` and (optionally) `images/` into its output directory, so
        # pointing at that directory is all a converted workflow needs — which is the point
        # of bundling: copy the directory anywhere and run it.
        #
        # Explicit `roots` entries still win. Images are normally NOT bundled (they are
        # gigabytes), so `roots.images` pointing at each host's local image store alongside
        # `bundle` is the expected combination rather than an exception.
        roots = dict(cfg.get("roots", {}) or {})
        bundle = str(cfg.get("bundle", "") or "")
        if bundle:
            for kind, sub in (("code", "code"), ("inputs", "inputs"), ("images", "images")):
                roots.setdefault(kind, os.path.join(bundle, sub))

        runner.configure(
            mode=mode,
            work_dir=work_dir,
            timeout_s=float(cfg.get("timeout_s", 3600.0)),
            container_runtime=str(cfg.get("container_runtime", "auto")),
            path_rewrites=cfg.get("path_rewrites", ()) or (),
            image_overrides=dict(cfg.get("image_overrides", {}) or {}),
            capture_output=bool(cfg.get("capture_output", True)),
            roots=roots,
        )
        if mode == "real":
            self.logger.warning(
                "[EXEC] runtime.execution.mode=real — jobs will RUN, not simulate. "
                "work_dir=%s timeout=%ss runtime=%s", work_dir,
                cfg.get("timeout_s", 3600.0), cfg.get("container_runtime", "auto"))

        self._configure_staging(cfg, work_dir, mode)

    def _configure_staging(self, execution_cfg: dict, work_dir: str, mode: str) -> None:
        """Install the staging policy and, when it is on, start this agent's data endpoint.

        Off by default, so an existing run is untouched: `stage_inputs` then resolves an input
        in the working directory or under `roots.inputs` exactly as before, and nothing here
        listens. See `docs/STAGING_DESIGN.md`.
        """
        from swarm.execution import staging

        cfg = dict((execution_cfg or {}).get("staging", {}) or {})
        try:
            pol = staging.configure(**cfg)
        except ValueError as exc:
            # A misspelled staging key is refused rather than ignored, for the same reason an
            # unknown `mode` is: a run that looks staged and is not cannot be told apart
            # afterwards from one that never asked.
            self.logger.error("[STAGE] %s", exc)
            raise

        if not pol.enabled:
            return
        if mode != "real":
            self.logger.warning(
                "[STAGE] staging is enabled but runtime.execution.mode=%s, so no job produces "
                "a file to stage; nothing will be served.", mode)

        if str(self.grpc_host) in ("0.0.0.0", "::", ""):
            # The location record advertises this address to peers, and it is the same field
            # consensus already dials, so a wildcard here is broken for both. Worth saying out
            # loud at startup: as a location it fails at the far end, one fetch at a time.
            self.logger.error(
                "[STAGE] grpc.host is %r — a wildcard cannot be dialled by a peer, so every "
                "location this agent publishes will be unusable. Set it to the address peers "
                "reach this agent on.", self.grpc_host)

        if pol.store_host and not os.environ.get("SWARM_RUN_ID", ""):
            # A store is keyed by (run, name) and refuses an upload carrying no run, so without
            # this every stage-out would fail one file at a time and the run would finish with
            # no durable copy of anything. Refused at startup instead: it is a configuration
            # fault that repeats for every job, which is the same reason an unknown execution
            # mode raises rather than defaulting.
            raise ValueError(
                "runtime.execution.staging.store_host is set but SWARM_RUN_ID is empty. A "
                "staging site files uploads under the run, and would refuse every one of them. "
                "run_test.py exports SWARM_RUN_ID; an agent started by hand must too.")

        self.staged_files = staging.PublishedFiles()
        staging.set_context(locator=self.repository.data_locations,
                            run_id=os.environ.get("SWARM_RUN_ID", ""),
                            agent_id=str(self.agent_id),
                            published=self.staged_files)
        port = staging.data_port(self.grpc_port, pol)
        self.transfer_server = staging.TransferServer(
            self.staged_files, self.grpc_host, port,
            run_id=os.environ.get("SWARM_RUN_ID", ""), pol=pol)
        try:
            self.transfer_server.start()
        except Exception as exc:                # noqa: BLE001
            # Refuse loudly: with staging on and no endpoint, this agent's outputs are
            # unreachable and every descendant of every job it runs would refuse to stage —
            # which reads as a workflow fault a long way from its cause.
            self.logger.error(
                "[STAGE] could not start the data endpoint on %s:%d (%s). With staging on, "
                "this agent's outputs would be unreachable to every peer.",
                self.grpc_host, port, exc)
            raise
        self.logger.warning(
            "[STAGE] staging ON — outputs are served from this agent on %s:%d and fetched from "
            "peers on demand. The work dir must be LOCAL (%s); a shared one makes every fetch a "
            "no-op and measures nothing.", self.grpc_host, port, work_dir)

    @property
    def live_agent_count(self) -> int:
        """Returns the count of all known agents including self."""
        return len(self.neighbor_map)

    @property
    def configured_agent_count(self) -> int:
        """Returns the expected total number of agents from the runtime configuration."""
        #return self.topology.group_size
        if self.topology.type == TopologyType.Ring:
            return self.runtime_config.get("total_agents", 0)
        else:
            return self.topology.group_size

    @property
    def results_dir(self) -> str:
        return self.runtime_config.get("results_dir", "results_dir")

    @property
    def grpc_port(self) -> int:
        return self.grpc_config.get("port", 50051)

    @property
    def grpc_host(self):
        return self.grpc_config.get("host", "localhost")

    def _setup_logger(self):
        log_path = f"{self.log_config['log-directory']}/{self.log_config['log-file']}-{self.agent_id}.log"
        logger = logging.getLogger(f"{self.log_config['logger']}-{self.agent_id}")
        logger.setLevel(self.log_config.get("log-level", logging.INFO))

        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        handler = RotatingFileHandler(log_path,
                                      backupCount=int(self.log_config.get("log-retain", 5)),
                                      maxBytes=int(self.log_config.get("log-size", 10**6)))
        formatter = logging.Formatter(
            self.log_config.get("log-format",
                                '%(asctime)s - %(name)s - {%(filename)s:%(lineno)d} - [%(threadName)s]- %(levelname)s - %(message)s'))
        formatter = logging.Formatter(
            self.log_config.get("log-format",
                                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.CRITICAL)
        logger.addHandler(stream_handler)

        return logger

    def start(self):
        try:
            self.transport.register_observers(observer=self)
            self.transport.start()

            for thread in self.threads.values():
                thread.start()

            for thread in self.threads.values():
                thread.join()
        except Exception as e:
            self.logger.info(f"Exception occurred in startup: {e}")
            self.logger.error(traceback.format_exc())
            self.stop()

    def _init_stop_state(self):
        """Set up the teardown guard. Kept as one method so a test double cannot drift from
        it: a stub that built its own `threading.Lock()` here tested the stub's lock and
        reported the shipped self-deadlock as fixed.

        stop() is reached from two directions — the SIGTERM handler in main.py and the
        periodic thread's own shutdown condition — and the stop script touches the shutdown
        flag and signals in the same breath, so both commonly fire. on_shutdown persists
        metrics, so running it twice concurrently let an earlier snapshot land on top of a
        later one.
        """
        # RLock, not Lock: a signal handler runs on whichever thread the signal is delivered
        # to, so SIGTERM landing while that same thread is inside stop()'s critical section
        # would block on a lock it already holds — a self-deadlock reached BEFORE the
        # re-entry check could catch it, ending in SIGKILL with no metrics saved.
        self._stop_lock = threading.RLock()
        self._stopped = False
        # Set once teardown has actually finished. A second caller waits on this rather than
        # returning straight away: the SIGTERM handler os._exit()s the process the moment
        # stop() returns, and the stop script touches the shutdown flag and signals in the
        # same breath — so the periodic thread is usually already inside save_results when the
        # signal lands, and returning early killed the process mid-write.
        self._stop_complete = threading.Event()
        self._stop_thread_ident = None

    def stop(self, wait_timeout: float = 60.0) -> bool:
        """Run teardown once, and do not return until it has finished.

        `wait_timeout` bounds how long a second caller waits for the first caller's teardown;
        it exists for the SIGTERM handler, which exits the process as soon as this returns.

        Returns False only when called re-entrantly **on the thread already running
        teardown** — a signal delivered to that very thread, say. The caller must not exit the
        process in that case: the teardown it interrupted has not finished, and unwinding to it
        is the only way it ever will. True otherwise, including when the wait timed out (the
        stop script's SIGKILL is close behind at that point, so exiting is no worse).
        """
        with self._stop_lock:
            first = not self._stopped
            if first:
                # Ident BEFORE the flag, and the order is load-bearing: a signal arriving on
                # this thread between the two would otherwise see `_stopped` set with no
                # matching ident, and wait out the full timeout on an event only the thread it
                # just interrupted can set.
                self._stop_thread_ident = threading.get_ident()
                self._stopped = True
        if not first:
            if self._stop_thread_ident == threading.get_ident():
                # Re-entered from inside teardown itself; waiting here would deadlock on an
                # event only this thread can set.
                self.logger.debug("stop() re-entered on the teardown thread; ignoring")
                return False
            self.logger.debug("stop() already running elsewhere; waiting for it to finish")
            if not self._stop_complete.wait(timeout=max(0.0, wait_timeout)):
                self.logger.warning(
                    f"[SHUTDOWN] teardown still running after {wait_timeout:.0f}s; "
                    f"proceeding without it — metrics for this agent may be incomplete")
            return True
        try:
            self.shutdown = True
            self.queues.message_event.set()
            self.queues.pending_event.set()
            self.queues.selected_event.set()
            with self.condition:
                self.condition.notify_all()
            self.on_shutdown()
            self.transport.stop()
        except Exception as e:
            self.logger.error(f"Exception occurred in shutdown: {e}")
            self.logger.error(traceback.format_exc())
        finally:
            # Released even on the exception path: a waiter must never be left blocked by a
            # teardown that failed.
            self._stop_complete.set()
        return True

    def on_shutdown(self):
        pass

    def _do_periodic(self):
        while not self.shutdown:
            try:
                self.on_periodic()

                time.sleep(0.5)
                if self.should_shutdown():
                    self.logger.info("[SHUTDOWN] Queue has been empty for too long. Triggering shutdown.")
                    break
            except Exception as e:
                self.logger.error(f"Periodic update error: {e}\n{traceback.format_exc()}")

        self.stop()

    def on_periodic(self):
        pass

    def _do_inbound(self):
        self.logger.info("Inbound Message Handler - Start")
        while not self.shutdown:
            try:
                self.queues.message_event.wait(timeout=0.5)
                self.queues.message_event.clear()
                messages = list(IterableQueue(self.queues.message_queue))
                if messages:
                    self._process(messages=messages)
            except Exception as e:
                self.logger.error(f"Inbound processing error: {e}\n{traceback.format_exc()}")
        self.logger.info("Inbound Message Handler - Stopped")

    @abstractmethod
    def _process(self, messages: list[dict]):
        pass

    def on_message(self, message: str):
        try:
            # Parse message
            payload = json.loads(message) if isinstance(message, str) else message

            source_agent_id = payload.get("source") or payload.get("agent_id")
            if source_agent_id == self.agent_id:
                return  # Skip self-messages

            message_type = payload.get("message_type")
            msg_name = MessageType(message_type)
            fwd = payload.get("forwarded_by")

            # Log message. Guarded: the json.dumps of every payload otherwise runs even
            # with DEBUG off — a large per-message CPU tax on the inbound path exactly
            # when the consumer is the bottleneck (measured: queue pinned at 20k).
            if self.logger.isEnabledFor(logging.DEBUG):
                log_msg = f"[IN] [{msg_name}] [SRC: {source_agent_id}]"
                if fwd:
                    log_msg += f" [FWD: {fwd}]"
                log_msg += f", Payload: {json.dumps(payload)}"
                self.logger.debug(log_msg)

            # Queue message; the queue is bounded — under overload drop-and-count instead
            # of growing without limit (consensus re-proposal recovers dropped votes).
            try:
                self.queues.message_queue.put_nowait(payload)
            except queue.Full:
                self.messages_dropped += 1
                if self.messages_dropped % 1000 == 1:
                    self.logger.warning(
                        f"Inbound queue full ({self.queues.message_queue.maxsize}); "
                        f"dropped {self.messages_dropped} messages so far")
                return
            self.queues.message_event.set()

            with self.condition:
                self.condition.notify_all()

        except Exception as e:
            self.logger.debug(f"Failed to enqueue message: {message}, error: {e}")

    def consensus_skip_set(self) -> set:
        """Peers a consensus phase must NOT send to.

        Exactly the peers heartbeat detection has declared failed — the same authority
        `calculate_quorum` counts through, since `_remove_failed_agents` takes them out of
        `neighbor_map` and `_refresh_agent_map` refuses to re-add them while they are in here.
        The two sets are therefore the same set by construction, which is the point: **an agent
        must never silence a peer it still counts towards quorum.**

        **SWIM is deliberately not consulted** (code review 2026-09-18, §6). It used to be, and
        a SWIM false-FAILED then removed a live peer from every consensus phase while quorum
        went on being computed over `neighbor_map`, which still contained it. Under PBFT that
        peer could not vote and the job waited out a reselection timeout; under Snow it merely
        abstained. SWIM false-fails precisely under consensus bursts — acks queue behind the
        single inbound consumer and blow the probe window, which is why `live_peer_ids` already
        treats an empty SWIM live set as a false reading — so the regime where the skip cost
        the most was the regime it fired most in, and the collapse cell was biased against
        PBFT for a reason that is not PBFT. The docs say heartbeat is authoritative; this is
        what makes that true for `broadcast` as well as for reassignment. SWIM keeps its real
        jobs: Snow's live-peer sample and gossip fan-out, neither of which is a quorum.

        The efficiency this gives up is small. Skipping was introduced in the same commit as
        the fire-and-forget broadcast pool (`85f26208`), and it is the pool that removed the
        ~8.7 s serial block per dead peer per phase; what remains is pool slots held for a
        dead peer's send timeout until heartbeat evicts it, which the bounded semaphore already
        sheds and counts rather than letting it block.
        """
        failed_map = getattr(self, "failed_agents", None)
        if failed_map is None:
            return set()
        try:
            return set(failed_map.keys())
        except Exception:
            return set()

    def broadcast(self, message: Message):
        peers = self.topology.peers
        skip = self.consensus_skip_set()
        if skip:
            peers = [p for p in peers if p not in skip]
        self.transport.broadcast(payload=message,
                                 peers=peers,
                                 neighbor_map=self.neighbor_map,
                                 sender=self.agent_id)

    def send(self, dest: int, payload: object, timeout: float = 2.0, retries: int = 4) -> None:
        peer_info = self.neighbor_map.get(dest)
        if peer_info is None:
            return
        self.transport.send(host=peer_info.host, port=peer_info.port, payload=payload,
                      dest=peer_info.agent_id, src=self.agent_id,
                      timeout=timeout, retries=retries)

    def calculate_quorum(self) -> int:
        # Simple majority over the live set. `live_agent_count` is `len(neighbor_map)`, and
        # `consensus_skip_set` is a subset of what that map already excludes — see the note
        # there: the peers we refuse to talk to and the peers we count must be one set, or a
        # quorum can be made unreachable by construction.
        return (self.live_agent_count // 2) + 1


    @abstractmethod
    def should_shutdown(self):
        """
        Returns True if shutdown has been requested.
        """
        return True

    def should_process(self, msg: Message) -> bool:
        path = msg.path if msg.path else []
        if self.agent_id in path:
            # already seen me — don’t forward again
            return False
        path.append(self.agent_id)
        return True


    def on_peer_status(self, target: str, up: bool, reason: str):
        """Called by GrpcClient/ChannelPool when a channel moves UP/DOWN."""
        if not up:
            self.logger.info(f"Peer {target} health {up} reason {reason}")

    def _get_peer_state_for_endpoint(self, host: str, port: int):
        """
        Returns (agent_id, is_up) for a given endpoint, or (None, None) if unknown.
        Expect self.peer_by_endpoint to be keyed by (host, port) -> (agent_id, is_up_bool)
        """
        return self.peer_by_endpoint.get((host, port), (None, None))