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
import enum
import json
import logging
import os
import threading
import time
import traceback
import uuid
from typing import Tuple, Any, List

import paramiko
import redis

from swarm.models.capacities import Capacities
from swarm.models.data_node import DataNode


class JobState(enum.Enum):
    PENDING = enum.auto()
    PRE_PREPARE = enum.auto()
    PREPARE = enum.auto()
    COMMIT = enum.auto()
    READY = enum.auto()
    RUNNING = enum.auto()
    IDLE = enum.auto()
    COMPLETE = enum.auto()
    FAILED = enum.auto()


job = {
    "id": "job_id",
    "no_op": 10,
    "capacities": {
        "core": 1,
        "ram": 100,
        "disk": 10,
    },
    "data_in": [{
        "remote_ip": "1.2.3.4",
        "remote_user": "rocky",
        "remote_file": "/tmp/input1.txt"
    },
    {
            "remote_ip": "1.2.3.4",
            "remote_user": "rocky",
            "remote_file": "/tmp/input1.txt"
    }],
    "data_out": [{
        "remote_ip": "1.2.3.4",
        "remote_user": "rocky",
        "remote_file": "/tmp/output1.txt"
    },
        {
            "remote_ip": "1.2.3.4",
            "remote_user": "rocky",
            "remote_file": "/tmp/output2.txt"
        }]
}


class Job:
    OP_GET = "GET"
    OP_PUT = "PUT"

    def __init__(self, logger: logging.Logger = None):
        self.job_id = None
        self.capacities = None
        self.capacity_allocations = None
        self.no_op = 0
        self.data_in = []
        self.data_out = []
        self.state = JobState.PENDING
        self.data_in_time = 0
        self.data_out_time = 0
        self.prepares = 0
        self.commits = 0
        self.logger = logger if logger else logging.getLogger(self.__class__.__name__)
        self.lock = threading.Lock()  # Lock for synchronization
        self.created_at = time.time()
        self.selection_started_at = None
        self.selected_by_agent_at = None
        self.scheduled_at = None
        self.completed_at = None
        self.leader_agent_id = None
        self.time_last_state_change = 0

    def get_age(self) -> float:
        """
        Returns the age of the job in seconds since it was created.
        :return: Age of the job in seconds
        """
        return time.time() - self.created_at

    def set_selection_start_time(self):
        with self.lock:
            if self.selected_by_agent_at is None:
                self.selection_started_at = time.time()

    def set_selection_end_time(self):
        with self.lock:
            self.selected_by_agent_at = time.time()

    def set_scheduled_time(self):
        with self.lock:
            self.scheduled_at = time.time()

    def set_completed_time(self):
        with self.lock:
            self.completed_at = time.time()

    def get_leader_agent_id(self) -> str:
        with self.lock:
            return self.leader_agent_id

    def set_leader(self, leader_agent_id: str):
        with self.lock:
            self.leader_agent_id = leader_agent_id

    def set_job_id(self, job_id: str):
        with self.lock:
            self.job_id = job_id

    def get_job_id(self) -> str:
        with self.lock:
            return self.job_id

    def set_capacities(self, cap: Capacities) -> None:
        with self.lock:
            assert (cap is None or isinstance(cap, Capacities))
            assert (cap is None or isinstance(cap, Capacities))
            self.capacities = cap

    def get_capacities(self) -> Capacities:
        with self.lock:
            return self.capacities

    def set_capacity_allocations(self, cap: Capacities) -> None:
        with self.lock:
            assert (cap is None or isinstance(cap, Capacities))
            self.capacity_allocations = cap

    def get_capacity_allocations(self) -> Capacities:
        with self.lock:
            return self.capacity_allocations

    def add_incoming_data_dep(self, data_node: DataNode):
        with self.lock:
            self.data_in.append(data_node)

    def add_outgoing_data_dep(self, data_node: DataNode):
        with self.lock:
            self.data_out.append(data_node)

    def get_data_in(self) -> List[DataNode]:
        with self.lock:
            return self.data_in.copy()

    def get_data_out(self) -> List[DataNode]:
        with self.lock:
            return self.data_out.copy()

    def set_state(self, state: JobState):
        with self.lock:
            self.state = state
            self.time_last_state_change = time.time()

    def get_state(self) -> JobState:
        return self.state

    def set_properties(self, **kwargs):
        """
        Lets you set multiple properties exposed via setter methods
        :param kwargs:
        :return:
        """
        # set any property on a sliver that has a setter
        for k, v in kwargs.items():
            # we can set anything the sliver model has a setter for
            self.__getattribute__('set_' + k)(v)

    @classmethod
    def list_properties(cls) -> Tuple[str]:
        """
        List properties available for setting/getting on a sliver (those exposing
        setters)
        :return:
        """
        ret = list()
        exclude_set = {"set_property", "set_properties"}
        for k in dir(cls):
            if k.startswith('set_') and k not in exclude_set:
                ret.append(k[4:])
        return tuple(ret)

    def set_property(self, prop_name: str, prop_val: Any):
        """
        Lets you set any property exposed via a setter
        :param prop_name:
        :param prop_val:
        :return:
        """
        return self.__getattribute__('set_' + prop_name)(prop_val)

    def get_property(self, prop_name: str):
        """
        Lets you get a property that is exposed via getter method
        :param prop_name:
        :return:
        """
        return self.__getattribute__('get_' + prop_name)()

    def property_exists(self, prop_name: str):
        """
        Does this property have a getter?
        """
        try:
            self.__getattribute__('get_' + prop_name)
            exists = True
        except AttributeError:
            exists = False
        return exists

    def __repr__(self):
        exclude_set = {"get_property"}
        print_set = list()
        for k in dir(self):
            if k.startswith('get_') and k not in exclude_set:
                print_set.append(k[4:])
        print_set.sort()
        print_vals = {'job_id': self.job_id if self.job_id else "NONE"}
        for p in print_set:
            try:
                pval = self.get_property(p)
                if pval is not None and len(str(pval)) != 0:
                    print_vals[p] = str(pval)
            except AttributeError:
                # sometimes a property is not available due to e.g. unpicking
                # an older version of the object, and that's ok.
                pass
        return str(print_vals)

    def __str__(self):
        return self.__repr__()

    def get_no_op(self) -> float:
        with self.lock:
            return self.no_op

    def set_data_in_time(self, data_in_time: int):
        with self.lock:
            self.data_in_time = data_in_time

    def set_data_out_time(self, data_out_time: int):
        with self.lock:
            self.data_out_time = data_out_time

    def execute(self, data_transfer: bool = True):
        try:
            self.logger.info(f"Starting execution for job: {self.job_id}")
            self.change_state(new_state=JobState.RUNNING)
            if data_transfer:
                begin = time.time()
                # Transfer files from data_in nodes
                for data_node in self.get_data_in():
                    remote_ip = data_node.get_remote_ip()
                    remote_user = data_node.get_remote_user()
                    remote_file = data_node.get_remote_file()
                    # Perform file transfer from remote node
                    self.logger.info(f"Transferring file {remote_file} from {remote_user}@{remote_ip}")
                    # Implement file transfer logic here, e.g., using SSH, SCP, or other file transfer protocols
                    self.file_transfer(operation=Job.OP_GET, remote_ip=data_node.get_remote_ip(),
                                       remote_user=data_node.get_remote_user(), remote_file=data_node.get_remote_file())
                self.set_data_in_time(data_in_time=int(time.time() - begin))

            # Simulate job execution by sleeping for no_op seconds
            no_op = self.get_no_op()
            self.logger.info(f"Sleeping for {no_op} seconds to simulate job execution")
            time.sleep(no_op)

            if data_transfer:
                begin = time.time()
                # Transfer files to data_out nodes
                for data_node in self.get_data_out():
                    remote_ip = data_node.get_remote_ip()
                    remote_user = data_node.get_remote_user()
                    remote_file = data_node.get_remote_file()
                    # Perform file transfer to remote node
                    self.logger.info(f"Transferring file {remote_file} to {remote_user}@{remote_ip}")
                    self.file_transfer(operation=Job.OP_PUT, remote_ip=data_node.get_remote_ip(),
                                       remote_user=data_node.get_remote_user(), remote_file=data_node.get_remote_file())
                self.set_data_out_time(data_out_time=int(time.time() - begin))

            self.change_state(new_state=JobState.COMPLETE)
            self.logger.info(f"Completed execution for job: {self.job_id}")
        except Exception as e:
            self.logger.error(f"Error occurred while executing Task: {self.get_job_id()} e: {e}")
            self.logger.error(traceback.format_exc())

    @staticmethod
    def file_transfer(operation: str, remote_ip: str, remote_user: str, remote_file: str):
        ssh_client = paramiko.SSHClient()
        try:
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname=remote_ip, username=remote_user, key_filename="~/.ssh/id_rsa")

            file_name_with_ext = os.path.basename(remote_file)
            remote_dir = os.path.dirname(remote_file)
            file_name, file_ext = os.path.splitext(file_name_with_ext)
            random_uuid = str(uuid.uuid4())

            random_file = f"{file_name}_{random_uuid}{file_ext}"

            if operation == Job.OP_GET:
                # Open an SFTP session
                with ssh_client.open_sftp() as sftp:
                    # Transfer the file from the remote node
                    sftp.get(remote_file, random_file)
            elif operation == Job.OP_PUT:
                # Open an SFTP session
                with ssh_client.open_sftp() as sftp:
                    # Transfer the file to the remote node
                    sftp.put(file_name_with_ext, f"{remote_dir}/{random_file}")
        except Exception as e:
            print(f"Failed to get the file: {e}")
        finally:
            ssh_client.close()

    def is_pending(self):
        return self.get_state() == JobState.PENDING

    def is_running(self):
        return self.get_state() == JobState.RUNNING

    def is_ready(self):
        return self.get_state() == JobState.READY

    def is_commit(self):
        return self.get_state() == JobState.COMMIT

    def is_complete(self):
        return self.get_state() == JobState.COMPLETE

    def change_state(self, new_state: JobState):
        self.logger.debug(f"Transitioning job {self.job_id} from {self.state} to {new_state}")
        self.set_state(state=new_state)
        if new_state in [JobState.PRE_PREPARE, JobState.PREPARE]:
            self.set_selection_start_time()
        elif new_state in [JobState.READY, JobState.COMMIT]:
            self.set_selection_end_time()
        elif new_state == JobState.RUNNING:
            self.set_scheduled_time()
        elif new_state == JobState.COMPLETE:
            self.set_completed_time()

    def to_dict(self):
        return {
            'id': self.job_id,
            'capacities': self.capacities.to_dict() if self.capacities else None,
            'capacity_allocations': self.capacity_allocations,
            'no_op': self.no_op,
            'data_in': [data_node.to_dict() for data_node in self.data_in],
            'data_out': [data_node.to_dict() for data_node in self.data_out],
            'state': self.state.value,
            'data_in_time': self.data_in_time,
            'data_out_time': self.data_out_time,
            'prepares': self.prepares,
            'commits': self.commits,
            'created_at': self.created_at,
            'selection_started_at': self.selection_started_at,
            'selected_by_agent_at': self.selected_by_agent_at,
            'scheduled_at': self.scheduled_at,
            'completed_at': self.completed_at,
            'leader_agent_id': self.leader_agent_id,
            'time_last_state_change': self.time_last_state_change
        }

    def from_dict(self, job_data: dict):
        self.job_id = job_data['id']
        self.capacities = Capacities.from_dict(job_data['capacities']) if job_data.get('capacities') else None
        self.capacity_allocations = Capacities.from_dict(job_data['capacity_allocations']) if job_data.get('capacity_allocations') else None
        self.no_op = job_data['no_op']
        self.data_in = [DataNode.from_dict(data_in) for data_in in job_data['data_in']]
        self.data_out = [DataNode.from_dict(data_out) for data_out in job_data['data_out']]
        self.state = JobState(job_data['state']) if job_data.get('state') else JobState.PENDING
        self.data_in_time = job_data['data_in_time'] if job_data.get('data_in_time') is not None else None
        self.data_out_time = job_data['data_out_time'] if job_data.get('data_out_time') is not None else None
        self.prepares = job_data['prepares'] if job_data.get('prepares') else 0
        self.commits = job_data['commits'] if job_data.get('commits') else 0
        self.created_at = job_data['created_at'] if job_data.get('created_at') is not None else time.time()
        self.selection_started_at = job_data['selection_started_at'] if job_data.get('selection_started_at') is not None else time.time()
        self.selected_by_agent_at = job_data['selected_by_agent_at'] if job_data.get('selected_by_agent_at') is not None else None
        self.scheduled_at = job_data['scheduled_at'] if job_data.get('scheduled_at') is not None else None
        self.completed_at = job_data['completed_at'] if job_data.get('completed_at') is not None else None
        self.leader_agent_id = job_data['leader_agent_id'] if job_data.get('leader_agent_id') is not None else None
        self.time_last_state_change = job_data['time_last_state_change'] if job_data.get('time_last_state_change') is not None else None


class JobRepository:
    def __init__(self, redis_client=None, etcd_client=None):
        self.redis = redis_client
        self.etcd_client = etcd_client
        self.lock = threading.Lock()

    def save_job(self, job: Job, key_prefix: str = "job"):
        if job.job_id is None:
            raise ValueError("job_id must be set to save a job")
        key = f"{key_prefix}:{job.job_id}"
        with self.lock:
            pipeline = self.redis.pipeline()
            while True:
                try:
                    pipeline.watch(key)
                    pipeline.multi()
                    pipeline.set(key, json.dumps(job.to_dict()))
                    pipeline.execute()
                    break
                except redis.WatchError:
                    continue

    def get_job(self, job_id: str, key_prefix: str = "job") -> Job:
        key = f"{key_prefix}:{job_id}"
        with self.lock:
            data = self.redis.get(key)
            if data is not None:
                job = Job()
                job.from_dict(json.loads(data))
                return job

    def delete_job(self, job_id: str, key_prefix: str = "job"):
        key = f"{key_prefix}:{job_id}"
        with self.lock:
            self.redis.delete(key)

    def get_all_job_ids(self, key_prefix: str = "job") -> list:
        """
        Retrieves all job IDs from Redis, stripping the key prefix.
        """
        with self.lock:
            job_keys = self.redis.keys(f'{key_prefix}:*')  # Fetch all keys with prefix
            job_ids = [key.split(f"{key_prefix}:")[1] for key in job_keys]  # Extract job_id
        return job_ids

    def get_all_jobs(self, key_prefix: str = "job") -> list:
        with self.lock:
            job_keys = self.redis.keys(f'{key_prefix}:*')  # Assuming job keys are prefixed with 'job:'
            jobs = []
            for key in job_keys:
                data = self.redis.get(key)
                if data:
                    job = Job()
                    job.from_dict(json.loads(data))  # Redis stores data as bytes, so using eval to convert back to dict
                    jobs.append(job)
            return jobs

    def delete_all(self, key_prefix="job", client_type="redis"):
        """
        Deletes all entries matching a key_prefix from the specified client.
        Supports Redis and etcd.
        """
        if client_type == "redis":
            if not self.redis:
                print("Redis client not initialized for JobRepository.")
                return
            with self.lock:
                keys_to_delete = self.redis.keys(f"{key_prefix}:*")
                if keys_to_delete:
                    deleted_count = self.redis.delete(*keys_to_delete)
                    print(f"Deleted {deleted_count} tasks from Redis matching prefix '{key_prefix}'.")
                else:
                    print(f"No Redis tasks found matching prefix '{key_prefix}' to delete.")
        elif client_type == "etcd":
            if not self.etcd_client:
                print("Etcd client not initialized for JobRepository.")
                return
            # etcd prefixes are handled differently, '/' means all for top-level
            # For specific prefixes, it's already encoded as bytes by get_prefix

            # pyetcd's delete takes the exact key, so we need to get all keys
            # and delete them one by one.
            # Convert prefix to bytes for etcd operations
            etcd_prefix_bytes = key_prefix.encode() if key_prefix != "*" else b"/"

            print(f"Fetching entries for etcd prefix '{key_prefix}' for deletion...")
            try:
                # pyetcd's get_prefix returns a generator of (value, metadata) tuples
                # We need the key from metadata
                if key_prefix == "*":
                    entries = list(self.etcd_client.get_all())
                else:
                    entries = list(self.etcd_client.get_prefix(etcd_prefix_bytes))
            except Exception as e:
                print(f"Error fetching etcd entries for deletion: {e}")
                return

            if not entries:
                print(f"No etcd entries found matching prefix '{key_prefix}' to delete.")
                return

            deleted_count = 0
            for _, meta in entries:
                key_to_delete = meta.key
                try:
                    self.etcd_client.delete(key_to_delete)
                    # print(f"  Deleted etcd key: {key_to_delete.decode()}") # Uncomment for verbose output
                    deleted_count += 1
                except Exception as e:
                    print(f"Error deleting etcd key {key_to_delete.decode()}: {e}")
            print(f"Deleted {deleted_count} etcd entries matching prefix '{key_prefix}'.")
        else:
            print(f"Unknown client type: {client_type}")