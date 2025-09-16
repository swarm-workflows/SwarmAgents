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
import logging
import os
import threading
import time
import traceback
from abc import abstractmethod
from logging.handlers import RotatingFileHandler

import redis
import yaml

from swarm.comm.message_service_grpcv2 import MessageServiceGrpcV2
from swarm.utils.queues import AgentQueues
from swarm.comm.messages.message import MessageType, Message
from swarm.comm.observer import Observer
from swarm.database.repository import Repository
from swarm.models.agent_info import AgentInfo
from swarm.utils.thread_safe_dict import ThreadSafeDict
from swarm.utils.topology import Topology, TopologyType
from swarm.utils.iterable_queue import IterableQueue


class Agent(Observer):
    def __init__(self, agent_id: int, config_file: str, debug: bool = False):
        self.debug = debug
        self.agent_id = agent_id
        self.neighbor_map = ThreadSafeDict[int, AgentInfo]()
        self.children = ThreadSafeDict[int, AgentInfo]()
        self.parents = ThreadSafeDict[int, AgentInfo]()

        with open(config_file, 'r') as f:
            self.config = yaml.safe_load(f)

        self.queues = AgentQueues()
        self.grpc_config = self.config.get("grpc", {})
        self.log_config = self.config.get("logging", {})
        self.runtime_config = self.config.get("runtime", {})
        self.redis_config = self.config.get("redis", {"host": "127.0.0.1", "port": 6379})
        self.topology = Topology(topo=self.config.get("topology", {}))
        self.logger = self._setup_logger()

        self.redis_client = redis.StrictRedis(host=self.redis_config["host"],
                                              port=self.redis_config["port"],
                                              decode_responses=True)
        self.repo = Repository(redis_client=self.redis_client)

        self.condition = threading.Condition()
        self.shutdown = False
        self.shutdown_path = "./shutdown"

        self.grpc_thread = MessageServiceGrpcV2(host=self.grpc_host, port=self.grpc_port, logger=self.logger)

        self.threads = {
            "periodic": threading.Thread(target=self._do_periodic, daemon=True),
            "inbound": threading.Thread(target=self._process_inbound_messages, daemon=True),
        }
        self.last_non_empty_time = time.time()

    @property
    def max_pending_elections(self):
        return self.runtime_config.get("max_pending_elections", 50)

    @property
    def live_agent_count(self) -> int:
        """Returns the count of all known agents including self."""
        #return 1 + len(self.neighbor_map)
        return len(self.neighbor_map)

    @property
    def configured_agent_count(self) -> int:
        """Returns the expected total number of agents from the runtime configuration."""
        if self.topology.type == TopologyType.Ring:
            return int(self.runtime_config.get("total_agents", 0))

        return 1 + len(self.topology.peers)

    @property
    def results_dir(self) -> str:
        return self.runtime_config.get("results_dir", "results_dir")

    @property
    def grpc_port(self) -> int:
        return (self.grpc_config.get("port", 50051) + self.agent_id)

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
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.CRITICAL)
        logger.addHandler(stream_handler)

        return logger

    def start(self):
        try:
            self.grpc_thread.register_observers(agent=self)
            self.grpc_thread.start()

            for thread in self.threads.values():
                thread.start()

            for thread in self.threads.values():
                thread.join()
        except Exception as e:
            self.logger.error(f"Exception occurred in startup: {e}")
            self.logger.error(traceback.format_exc())
            self.stop()

    def stop(self):
        try:
            self.shutdown = True
            self.grpc_thread.stop()
            with self.condition:
                self.condition.notify_all()
            self.save_results()
        except Exception as e:
            self.logger.error(f"Exception occurred in shutdown: {e}")
            self.logger.error(traceback.format_exc())

    @abstractmethod
    def save_results(self):
        pass

    @abstractmethod
    def _do_periodic(self):
        pass

    def _process_inbound_messages(self):
        self.logger.info("Inbound Message Handler - Start")
        while not self.shutdown:
            try:
                messages = list(IterableQueue(self.queues.message_queue))
                if messages:
                    self._process(messages=messages)
                time.sleep(0.5)
            except Exception as e:
                self.logger.error(f"Inbound processing error: {e}\n{traceback.format_exc()}")
        self.logger.info("Inbound Message Handler - Stopped")

    @abstractmethod
    def _process(self, messages: list[dict]):
        pass

    def dispatch_message(self, message: str):
        try:
            # Parse message
            payload = json.loads(message) if isinstance(message, str) else message

            source_agent_id = payload.get("source") or payload.get("agent_id")
            if source_agent_id == self.agent_id:
                return  # Skip self-messages

            message_type = payload.get("message_type")
            msg_name = MessageType(message_type)
            fwd = payload.get("forwarded_by")

            # Log message
            log_msg = f"[IN] [{msg_name}] [SRC: {source_agent_id}]"
            if fwd:
                log_msg += f" [FWD: {fwd}]"
            log_msg += f", Payload: {json.dumps(payload)}"
            self.logger.debug(log_msg)

            # Queue message
            self.queues.message_queue.put_nowait(payload)

            with self.condition:
                self.condition.notify_all()

        except Exception as e:
            self.logger.debug(f"Failed to enqueue message: {message}, error: {e}")

    def send_message(self, message: Message, src: int = None, fwd: int = None):
        message.path.append(self.agent_id)
        json_message = message.to_dict()
        src = src or self.agent_id

        for peer_id in self.topology.peers:
            if peer_id in message.path:
                continue

            peer_info = self.neighbor_map.get(peer_id)
            if not peer_info:
                continue

            #self.logger.debug(f"SENDING: {self.agent_id} -> {peer_info.host}:{peer_info.port} Msg: {json_message.get('message_type')}")
            if not self.grpc_thread.send(
                msg=json_message,
                host=peer_info.host,
                port=peer_info.port,
                dest=peer_id,
                src=src,
                fwd=fwd
            ):
                self.logger.error(f"Failed to send message to {peer_id}")

    def calculate_quorum(self) -> int:
        # Simple majority quorum calculation
        return (self.live_agent_count // 2) + 1


    @abstractmethod
    def should_shutdown(self):
        """
        Returns True if shutdown has been requested.
        """

    def should_process(self, msg: Message) -> bool:
        path = msg.path if msg.path else []
        if self.agent_id in path:
            # already seen me — don’t forward again
            return False
        path.append(self.agent_id)
        return True
