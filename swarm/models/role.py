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
import logging
import time
import traceback
from typing import Tuple, Any, List

from swarm.models.object import Object, ObjectState
from swarm.models.capacities import Capacities
from swarm.models.data_node import DataNode

class Role(Object):
    def __init__(self, logger: logging.Logger = None):
        super().__init__()
        self.capacities = None
        self.service_id = None
        self.startOrStop = None

        self.logger = logger if logger else logging.getLogger(self.__class__.__name__)
        self.created_at = time.time()
        self.selection_started_at = None
        self.selected_by_agent_at = None

    def get_age(self) -> float:
        """
        Returns the age of the role in seconds since it was created.
        :return: Age of the role in seconds
        """
        return time.time() - self.created_at

    def set_selection_start_time(self):
        with self.lock:
            if self.selected_by_agent_at is None:
                self.selection_started_at = time.time()

    def set_selection_end_time(self):
        with self.lock:
            self.selected_by_agent_at = self.time_last_state_change

    @property
    def role_id(self) -> str:
        with self.lock:
            return self.object_id

    @role_id.setter
    def role_id(self, value):
        with self.lock:
            self.object_id = value

    def set_capacities(self, cap: Capacities) -> None:
        with self.lock:
            assert (cap is None or isinstance(cap, Capacities))
            assert (cap is None or isinstance(cap, Capacities))
            self.capacities = cap

    def get_capacities(self) -> Capacities:
        with self.lock:
            return self.capacities

    def get_state(self) -> ObjectState:
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
        print_vals = {'role_id': self.role_id if self.role_id else "NONE"}
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

    def on_state_changed(self, old_state, new_state):
        self.logger.debug(
            f"Transitioning role {self.role_id} from {old_state} to {new_state}"
        )
        if new_state in (ObjectState.PRE_PREPARE, ObjectState.PREPARE):
            self.set_selection_start_time()
        elif new_state is ObjectState.READY:
            self.set_selection_end_time()

    def to_dict(self):
        return {
            'id': self.role_id,
            'capacities': self.capacities.to_dict() if self.capacities else None,
            'state': self.state.value,
            'created_at': self.created_at,
            'selection_started_at': self.selection_started_at,
            'selected_by_agent_at': self.selected_by_agent_at,
            'leader_id': self.leader_id,
            'time_last_state_change': self.time_last_state_change,
        }

    def from_dict(self, role_data: dict):
        self.role_id = role_data['id']
        self.capacities = Capacities.from_dict(role_data['capacities']) if role_data.get('capacities') else None
        self.state = ObjectState(role_data['state']) if role_data.get('state') else ObjectState.PENDING
        self.created_at = role_data['created_at'] if role_data.get('created_at') is not None else time.time()
        self.selection_started_at = role_data['selection_started_at'] if role_data.get('selection_started_at') is not None else time.time()
        self.selected_by_agent_at = role_data['selected_by_agent_at'] if role_data.get('selected_by_agent_at') is not None else None
        self.leader_id = role_data['leader_id'] if role_data.get('leader_id') is not None else None
        self.time_last_state_change = role_data['time_last_state_change'] if role_data.get('time_last_state_change') is not None else None

