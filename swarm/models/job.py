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
    "execution_time": 10,
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
        self.execution_time = 0
        self.data_in = []
        self.data_out = []
        self.state = JobState.PENDING
        self.status = 0
        self.data_in_time = 0
        self.data_out_time = 0
        self.prepares = 0
        self.commits = 0
        self.logger = logger if logger else logging.getLogger(self.__class__.__name__)
        self.lock = threading.RLock()  # Lock for synchronization
        self.created_at = time.time()
        self.selection_started_at = None
        self.selected_by_agent_at = None
        self.scheduled_at = None
        self.completed_at = None
        self.leader_agent_id = None
        self.time_last_state_change = 0
        self.no_op_count = 0
        self.job_type = None  # default until classified

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

    def get_leader_agent_id(self) -> int:
        with self.lock:
            return self.leader_agent_id

    def set_leader(self, leader_agent_id: int):
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

    def get_execution_time(self) -> float:
        with self.lock:
            return self.execution_time

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
            # TODO: Transfer files from data_in nodes

            # Simulate job execution by sleeping for execution_time seconds
            execution_time = self.get_execution_time()
            self.logger.info(f"Sleeping for {execution_time} seconds to simulate job execution")
            time.sleep(execution_time)

            # TODO: Transfer files from data_out nodes

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
        elif new_state in [JobState.READY]:
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
            'execution_time': self.execution_time,
            'data_in': [data_node.to_dict() for data_node in self.data_in],
            'data_out': [data_node.to_dict() for data_node in self.data_out],
            'state': self.state.value,
            'status': self.status,
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
            'time_last_state_change': self.time_last_state_change,
            'job_type': self.job_type
        }

    def from_dict(self, job_data: dict):
        self.job_id = job_data['id']
        self.capacities = Capacities.from_dict(job_data['capacities']) if job_data.get('capacities') else None
        self.capacity_allocations = Capacities.from_dict(job_data['capacity_allocations']) if job_data.get('capacity_allocations') else None
        self.execution_time = job_data['execution_time']
        self.data_in = [DataNode.from_dict(data_in) for data_in in job_data['data_in']]
        self.data_out = [DataNode.from_dict(data_out) for data_out in job_data['data_out']]
        self.state = JobState(job_data['state']) if job_data.get('state') else JobState.PENDING
        self.status = job_data['status'] if job_data.get('status') else 0
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
        self.classify_job_type()

    def classify_job_type(self):
        """
        Classify the job into a job_type string based on its resource demand,
        execution time, and data transfer requirements.
        """
        # ---- Resource dominance classification ----
        resource_ratios = {
            "cpu_bound": self.capacities.core,
            "ram_bound": self.capacities.ram,
            "disk_bound": self.capacities.disk,
            "gpu_bound": getattr(self.capacities, "gpu", 0),
        }
        resource_class = max(resource_ratios, key=resource_ratios.get)

        # ---- Execution time classification ----
        if self.execution_time <= 5:
            time_class = "short"
        elif self.execution_time <= 20:
            time_class = "medium"
        else:
            time_class = "long"

        # ---- DTN / data I/O classification ----
        required_dtns = (self.data_in or []) + (self.data_out or [])
        io_class = "dtn_heavy" if len(required_dtns) > 1 else "dtn_light"

        # ---- Combine ----
        self.job_type = f"{resource_class}_{time_class}_{io_class}"
        return self.job_type