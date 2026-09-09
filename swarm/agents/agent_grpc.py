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
        # stop() is reached from two directions — the SIGTERM handler in main.py and the
        # periodic thread's own shutdown condition — and the stop script touches the shutdown
        # flag and signals in the same breath, so both commonly fire. on_shutdown persists
        # metrics, so running it twice concurrently let an earlier snapshot land on top of a
        # later one; this makes the teardown path run once.
        self._stop_lock = threading.Lock()
        self._stopped = False
        # Set once teardown has actually finished. A second caller waits on this rather than
        # returning straight away: the SIGTERM handler os._exit()s the process the moment
        # stop() returns, and the stop script touches the shutdown flag and signals in the same
        # breath — so the periodic thread is usually already inside save_results when the
        # signal lands, and returning early killed the process mid-write.
        self._stop_complete = threading.Event()
        self._stop_thread_ident = None
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
                self._stopped = True
                self._stop_thread_ident = threading.get_ident()
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

    def broadcast(self, message: Message):
        # Skip peers known to be FAILED (SWIM failed set and/or heartbeat detection) —
        # otherwise every consensus phase pays full send timeouts for dead peers.
        # SWIM SUSPECT peers still receive traffic so they can refute the suspicion.
        peers = self.topology.peers
        skip = set()
        swim = getattr(self, "swim", None)
        if swim is not None:
            try:
                skip.update(swim.failed_agents())
            except Exception:
                pass
        failed_map = getattr(self, "failed_agents", None)
        if failed_map is not None:
            try:
                skip.update(failed_map.keys())
            except Exception:
                pass
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
        # Simple majority quorum calculation
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