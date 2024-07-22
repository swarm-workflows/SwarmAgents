#!/usr/bin/env python3
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


class TaskState(enum.Enum):
    PENDING = enum.auto()
    PRE_PREPARE = enum.auto()
    PREPARE = enum.auto()
    COMMIT = enum.auto()
    READY = enum.auto()
    RUNNING = enum.auto()
    IDLE = enum.auto()
    COMPLETE = enum.auto()
    FAILED = enum.auto()


task = {
    "id": "task_id",
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


class Task:
    OP_GET = "GET"
    OP_PUT = "PUT"

    def __init__(self, logger: logging.Logger = None):
        self.task_id = None
        self.capacities = None
        self.capacity_allocations = None
        self.no_op = 0
        self.data_in = []
        self.data_out = []
        self.state = TaskState.PENDING
        self.data_in_time = 0
        self.data_out_time = 0
        self.prepares = 0
        self.commits = 0
        self.logger = logger if logger else logging.getLogger(self.__class__.__name__)
        self.lock = threading.Lock()  # Lock for synchronization
        self.creation_time = time.time()
        self.time_to_elect_leader = None
        self.time_on_queue = None
        self.time_to_execute = None
        self.time_to_completion = None
        self.leader_agent_id = None
        self.time_last_state_change = None

    def get_leader_agent_id(self) -> str:
        with self.lock:
            return self.leader_agent_id

    def set_leader(self, leader_agent_id: str):
        with self.lock:
            self.leader_agent_id = leader_agent_id

    def set_time_to_elect_leader(self):
        with self.lock:
            if self.time_on_queue and self.creation_time:
                self.time_to_elect_leader = int(time.time() - self.time_on_queue - self.creation_time)
            else:
                self.time_to_elect_leader = int(time.time() - self.creation_time)

    def reset_time_on_queue(self):
        with self.lock:
            self.time_on_queue = None

    def set_time_on_queue(self):
        with self.lock:
            if not self.time_on_queue:
                self.time_on_queue = int(time.time() - self.creation_time)

    def set_task_id(self, task_id):
        with self.lock:
            self.task_id = task_id

    def get_task_id(self):
        with self.lock:
            return self.task_id

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

    def set_state(self, state: TaskState):
        with self.lock:
            self.state = state
            self.time_last_state_change = time.time()

    def get_state(self) -> TaskState:
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
        print_vals = {'task_id': self.task_id if self.task_id else "NONE"}
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

    def set_time_to_completion(self):
        with self.lock:
            self.time_to_completion = int(time.time() - self.creation_time)

    def set_time_to_execute(self, time_to_execute: int):
        with self.lock:
            self.time_to_execute = time_to_execute

    def execute(self, data_transfer: bool = True):
        try:
            self.logger.info(f"Starting execution for task: {self.task_id}")
            start = time.time()
            self.change_state(new_state=TaskState.RUNNING)
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
                    self.file_transfer(operation=Task.OP_GET, remote_ip=data_node.get_remote_ip(),
                                       remote_user=data_node.get_remote_user(), remote_file=data_node.get_remote_file())
                self.set_data_in_time(data_in_time=int(time.time() - begin))

            # Simulate task execution by sleeping for no_op seconds
            no_op = self.get_no_op()
            self.logger.info(f"Sleeping for {no_op} seconds to simulate task execution")
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
                    self.file_transfer(operation=Task.OP_PUT, remote_ip=data_node.get_remote_ip(),
                                       remote_user=data_node.get_remote_user(), remote_file=data_node.get_remote_file())
                self.set_data_out_time(data_out_time=int(time.time() - begin))

            self.change_state(new_state=TaskState.COMPLETE)
            self.set_time_to_execute(time_to_execute=int(time.time() - start))
            self.set_time_to_completion()
            self.logger.info(f"Completed execution for task: {self.task_id}")
        except Exception as e:
            self.logger.error(f"Error occurred while executing Task: {self.get_task_id()} e: {e}")
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

            if operation == Task.OP_GET:
                # Open an SFTP session
                with ssh_client.open_sftp() as sftp:
                    # Transfer the file from the remote node
                    sftp.get(remote_file, random_file)
            elif operation == Task.OP_PUT:
                # Open an SFTP session
                with ssh_client.open_sftp() as sftp:
                    # Transfer the file to the remote node
                    sftp.put(file_name_with_ext, f"{remote_dir}/{random_file}")
        except Exception as e:
            print(f"Failed to get the file: {e}")
        finally:
            ssh_client.close()

    def is_pending(self):
        return self.get_state() == TaskState.PENDING

    def is_running(self):
        return self.get_state() == TaskState.RUNNING

    def is_ready(self):
        return self.get_state() == TaskState.READY

    def is_commit(self):
        return self.get_state() == TaskState.COMMIT

    def is_complete(self):
        return self.get_state() == TaskState.COMPLETE

    def change_state(self, new_state: TaskState):
        self.logger.debug(f"Transitioning task {self.task_id} from {self.state} to {new_state}")
        self.set_state(state=new_state)

    def to_dict(self):
        return {
            'id': self.task_id,
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
            'creation_time': self.creation_time,
            'time_to_elect_leader': self.time_to_elect_leader,
            'time_on_queue': self.time_on_queue,
            'time_to_execute': self.time_to_execute,
            'time_to_completion': self.time_to_completion,
            'leader_agent_id': self.leader_agent_id,
            'time_last_state_change': self.time_last_state_change
        }

    def from_dict(self, task_data: dict):
        self.task_id = task_data['id']
        self.capacities = Capacities.from_dict(task_data['capacities']) if task_data.get('capacities') else None
        self.capacity_allocations = Capacities.from_dict(task_data['capacity_allocations']) if task_data.get('capacity_allocations') else None
        self.no_op = task_data['no_op']
        self.data_in = [DataNode.from_dict(data_in) for data_in in task_data['data_in']]
        self.data_out = [DataNode.from_dict(data_out) for data_out in task_data['data_out']]
        self.state = TaskState(task_data['state']) if task_data.get('state') else TaskState.PENDING
        self.data_in_time = task_data['data_in_time'] if task_data.get('data_in_time') is not None else None
        self.data_out_time = task_data['data_out_time'] if task_data.get('data_out_time') is not None else None
        self.prepares = task_data['prepares'] if task_data.get('prepares') else 0
        self.commits = task_data['commits'] if task_data.get('commits') else 0
        self.creation_time = task_data['creation_time'] if task_data.get('creation_time') is not None else time.time()
        self.time_to_elect_leader = task_data['time_to_elect_leader'] if task_data.get('time_to_elect_leader') is not None else None
        self.time_on_queue = task_data['time_on_queue'] if task_data.get('time_on_queue') is not None else None
        self.time_to_execute = task_data['time_to_execute'] if task_data.get('time_to_execute') is not None else None
        self.time_to_completion = task_data['time_to_completion'] if task_data.get('time_to_completion') is not None else None
        self.leader_agent_id = task_data['leader_agent_id'] if task_data.get('leader_agent_id') is not None else None
        self.time_last_state_change = task_data['time_last_state_change'] if task_data.get('time_last_state_change') is not None else None


class TaskQueue:
    def __init__(self):
        self.tasks = {}
        self.lock = threading.Lock()

    def has_pending_tasks(self):
        with self.lock:
            for t in self.tasks.values():
                if t.is_pending():
                    return True
        return False

    def add_task(self, task: Task):
        with self.lock:
            self.tasks[task.get_task_id()] = task

    def remove_task(self, task_id: str):
        with self.lock:
            if task_id in self.tasks:
                self.tasks.pop(task_id)

    def capacities(self):
        allocated_caps = Capacities()
        with self.lock:
            for t in self.tasks.values():
                allocated_caps += t.get_capacities()
        return allocated_caps

    def get_task(self, task_id: str) -> Task:
        try:
            self.lock.acquire()
            return self.tasks.get(task_id)
        finally:
            self.lock.release()

    def build_tasks_from_json(self, json_file):
        try:
            self.lock.acquire()
            self.tasks = {}
            with open(json_file, 'r') as f:
                data = json.load(f)
                for task_data in data:
                    task = Task()
                    task.task_id = task_data['id']
                    task.set_capacities(Capacities.from_dict(task_data['capacities']))
                    task.no_op = task_data['no_op']
                    for data_in in task_data['data_in']:
                        data_node = DataNode.from_dict(data_in)
                        task.add_incoming_data_dep(data_node)
                    for data_out in task_data['data_out']:
                        data_node = DataNode.from_dict(data_out)
                        task.add_outgoing_data_dep(data_node)
                    self.tasks[task.task_id] = task
        finally:
            self.lock.release()

    def size(self):
        try:
            self.lock.acquire()
            return len(self.tasks)
        finally:
            self.lock.release()


class TaskRepository:
    def __init__(self, redis_client):
        self.redis = redis_client
        self.lock = threading.Lock()

    def save_task(self, task: Task, key_prefix: str = "task"):
        if task.task_id is None:
            raise ValueError("task_id must be set to save a task")
        key = f"{key_prefix}:{task.task_id}"
        with self.lock:
            pipeline = self.redis.pipeline()
            while True:
                try:
                    pipeline.watch(key)
                    pipeline.multi()
                    pipeline.set(key, json.dumps(task.to_dict()))
                    pipeline.execute()
                    break
                except redis.WatchError:
                    continue

    def get_task(self, task_id: str, key_prefix: str = "task") -> Task:
        key = f"{key_prefix}:{task_id}"
        with self.lock:
            data = self.redis.get(key)
            if data is not None:
                task = Task()
                task.from_dict(json.loads(data))
                return task

    def delete_task(self, task_id: str, key_prefix: str = "task"):
        key = f"{key_prefix}:{task_id}"
        with self.lock:
            self.redis.delete(key)

    def get_all_tasks(self, key_prefix: str = "task") -> list:
        with self.lock:
            task_keys = self.redis.keys(f'{key_prefix}:*')  # Assuming task keys are prefixed with 'task:'
            tasks = []
            for key in task_keys:
                data = self.redis.get(key)
                if data:
                    task = Task()
                    task.from_dict(json.loads(data) )  # Redis stores data as bytes, so using eval to convert back to dict
                    tasks.append(task)
            return tasks

    def delete_all(self, key_prefix: str = "task"):
        with self.lock:
            task_keys = self.redis.keys(f'{key_prefix}:*')  # Assuming task keys are prefixed with 'task:'
            for key in task_keys:
                self.redis.delete(key)