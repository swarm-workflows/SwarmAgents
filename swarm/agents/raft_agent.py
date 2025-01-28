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

import threading
import time
import traceback
from typing import Dict, List

import redis
from pyraft.raft import RaftNode

from swarm.agents.agent import Agent
from swarm.comm.messages.heart_beat import HeartBeat
from swarm.comm.messages.message import MessageType
from swarm.models.capacities import Capacities
from swarm.models.agent_info import AgentInfo
from swarm.models.job import JobState, Job, JobRepository


class ExtendedRaftNode(RaftNode):
    def __init__(self, node_id, address, peers):
        super().__init__(node_id, address, peers)
        self._is_leader = False

    def on_leader(self):
        super().on_leader()
        self._is_leader = True

    def on_follower(self):
        super().on_follower()
        self._is_leader = False

    def is_leader(self):
        return self._is_leader


class RaftAgent(Agent):
    def __init__(self, agent_id: str, config_file: str, cycles: int, address: str = "127.0.0.1", port: int = 5010,
                 peers: Dict[str, str] = {}, redis_host: str = "127.0.0.1", redis_port: int = 6379):
        super(RaftAgent, self).__init__(agent_id=agent_id, config_file=config_file, cycles=cycles)
        self.agent_id = agent_id
        self.peers = peers
        self.raft = ExtendedRaftNode(self.agent_id, f"{address}:{port}", peers)
        self.redis_client = redis.StrictRedis(host=redis_host, port=redis_port, decode_responses=True)
        self.shutdown_flag = threading.Event()
        self.job_list = 'jobs'
        self.job_repo = JobRepository(redis_client=self.redis_client)

    def start(self, clean: bool = False):
        self.raft.start()

        if not self.agent_id and clean:
            self.job_repo.delete_all()
        super().start()

    def stop(self):
        #super(RaftAgent, self).stop()
        self.shutdown = True
        self.ctrl_msg_srv.stop()
        self.hrt_msg_srv.stop()
        with self.condition:
            self.condition.notify_all()
        jobs = self.job_repo.get_all_jobs(key_prefix="allocated")
        self.plot_results(jobs=jobs)
        self.shutdown_flag.set()
        self.raft.shutdown()

    def is_leader(self):
        return self.raft.is_leader()

    def can_peer_accommodate_job(self, job: Job, peer_agent: AgentInfo):
        allocated_caps = peer_agent.capacity_allocations
        if not allocated_caps:
            allocated_caps = Capacities()
        capacities = peer_agent.capacities

        if capacities and allocated_caps:
            available = capacities - allocated_caps
            #self.logger.debug(f"Agent Total Capacities: {self.capacities}")
            #self.logger.debug(f"Agent Allocated Capacities: {allocated_caps}")
            #self.logger.debug(f"Agent Available Capacities: {available}")
            #self.logger.debug(f"Job: {job.get_job_id()} Requested capacities: {job.get_capacities()}")

            # Check if the agent can accommodate the given job based on its capacities
            # Compare the requested against available
            available = available - job.get_capacities()
            negative_fields = available.negative_fields()
            if len(negative_fields) > 0:
                return False

        return True

    def __find_neighbor_with_lowest_load(self) -> AgentInfo:
        # Initialize variables to track the neighbor with the lowest load
        lowest_load = float('inf')  # Initialize with positive infinity to ensure any load is lower
        lowest_load_neighbor = None

        # Iterate through each neighbor in neighbor_map
        for peer_agent_id, neighbor_info in self.neighbor_map.items():
            # Check if the current load is lower than the lowest load found so far
            if neighbor_info.load < lowest_load:
                # Update lowest load and lowest load neighbor
                lowest_load = neighbor_info.load
                lowest_load_neighbor = neighbor_info

        return lowest_load_neighbor

    def run_as_leader(self):
        #self.logger.debug("Running as leader")
        try:
            processed = 0
            jobs = self.job_repo.get_all_jobs()
            if len(jobs):
                self.logger.debug(f"Fetched jobs: {len(jobs)} {processed} {self.neighbor_map.values()}")
            for job in jobs:
                if self.shutdown_flag.is_set():
                    break
                if job.is_pending():
                    processed += 1
                    my_load = self.compute_overall_load()
                    peer = self.__find_neighbor_with_lowest_load()
                    #self.logger.debug(f"Peer found: {peer}")
                    job.change_state(new_state=JobState.PREPARE)

                    if peer and peer.load < 70.00 and self.can_peer_accommodate_job(peer_agent=peer,
                                                                                    job=job):
                        job.set_leader(leader_agent_id=peer.agent_id)
                        self.job_repo.delete_job(job_id=job.get_job_id())
                        self.job_repo.save_job(job=job, key_prefix="allocated")
                        payload = {"dest": peer.agent_id}
                        self.send_message(message_type=MessageType.Commit, job_id=job.job_id, payload=payload)

                    elif self.is_job_feasible(job=job, total=self.capacities, projected_load=my_load):
                        job.set_leader(leader_agent_id=self.agent_id)
                        #job.change_state(new_state=JobState.RUNNING)
                        job.change_state(new_state=JobState.READY)
                        self.job_repo.delete_job(job_id=job.get_job_id())
                        self.job_repo.save_job(job=job, key_prefix="allocated")
                        self.select_job(job=job)

                if processed >= 40:
                    time.sleep(1)
                    processed = 0
            if not len(jobs):
                time.sleep(5)
        except Exception as e:
            self.logger.info(f"RUN Leader -- error: {e}")
            self.logger.info("Running as leader -- stop")
            traceback.print_exc()

    def __receive_commit(self, incoming: dict):
        dest = incoming.get("dest")
        if not dest or dest != self.agent_id:
            self.logger.debug(f"Discarding incoming message: {incoming}")
            return
        peer_agent_id = incoming.get("agent_id")
        job_id = incoming.get("job_id")

        self.logger.info(f"Received commit from Agent: {peer_agent_id} for Job: {job_id}")
        job = self.job_repo.get_job(job_id=job_id, key_prefix="allocated")
        if job:
            job.change_state(new_state=JobState.READY)
            self.job_repo.save_job(job=job, key_prefix="allocated")
            self.select_job(job=job)
            '''
            if self.is_job_feasible(job=job, total=self.capacities, projected_load=self.compute_overall_load()):
                self.select_job(job=job)
            else:
                self.logger.info(f"Agent: {self.agent_id} cannot execute Job: {job_id}")
                self.fail_job(job=job)
            '''
        else:
            self.logger.error(f"Unable to fetch job from queue: {job_id}")

    def send_message(self, message_type: MessageType, job_id: str = None, payload: dict = None):
        message = {
            "message_type": message_type.value,
            "agent_id": self.agent_id,
            "load": self.compute_overall_load()
        }
        if job_id:
            message["job_id"] = job_id

        if payload:
            for key, value in payload.items():
                message[key] = value

        # Produce the message to the Kafka topic
        self._send_message(json_message=message)

    def _process_messages(self, *, messages: List[dict]):
        for message in messages:
            try:
                begin = time.time()
                self.logger.debug(f"Consumer received message: {message}")

                message_type = MessageType(message.get('message_type'))
                if message_type == MessageType.HeartBeat:
                    incoming = HeartBeat.from_dict(message)
                    self._receive_heartbeat(incoming=incoming)
                elif message_type == MessageType.Commit:
                    self.__receive_commit(incoming=message)

                diff = int(time.time() - begin)
                if diff > 0:
                    self.logger.info(f"Event {message.get('message_type')} TIME: {diff}")
            except Exception as e:
                self.logger.error(f"Error while processing message {type(message)}, {e}")
                self.logger.error(traceback.format_exc())

    def run_as_follower(self):
        self.logger.info("Running as follower")

    def job_selection_main(self):
        self.logger.info(f"Starting agent: {self}")
        self.ctrl_msg_srv.register_observers(agent=self)
        if self.is_leader():
            self.logger.info("Running as leader!")

        while not self.shutdown_flag.is_set():
            try:
                if self.is_leader():
                    # Job Allocation
                    self.run_as_leader()
                else:
                    # Wait for job execution
                    self.run_as_follower()
                    time.sleep(1)  # Adjust the sleep duration as needed

            except Exception as e:
                self.logger.error(f"Error occurred e: {e}")
                self.logger.error(traceback.format_exc())

        self.logger.info(f"Stopped agent: {self}")

    def fail_job(self, job: Job):
        job.change_state(new_state=JobState.FAILED)
        #self.job_repo.save_job(job=job, key_prefix="allocated")
        # TODO move it back to pending
