#!/usr/bin/env python3
# Inherited from FABRIC InformationModel
from typing import List, Tuple

from swarm.models.capacities import Capacities
from swarm.models.json_field import JSONField
from swarm.models.profile import ProfileType, PROFILE_MAP, Profile


class Peer(JSONField):
    """
    Implements basic capacity field handling - encoding and decoding
    from JSON dictionaries of properties.
    """
    def __init__(self, **kwargs):
        self.agent_id = None
        self.capacities = Capacities()
        self.capacity_allocations = Capacities()
        self.load = 0.0
        self._set_fields(**kwargs)

    def set_agent_id(self, agent_id: str):
        self.agent_id = agent_id

    def set_load(self, load: float):
        self.load = load

    def get_agent_id(self) -> str:
        return self.agent_id

    def get_load(self) -> float:
        return self.load

    def set_capacities(self, capacities: dict):
        self.capacities = Capacities.from_dict(capacities)

    def get_capacities(self) -> Capacities:
        return self.capacities

    def set_capacity_allocations(self, capacity_allocations: dict):
        self.capacity_allocations = Capacities.from_dict(capacity_allocations)

    def get_capacity_allocations(self) -> Capacities:
        return self.capacity_allocations

    def _set_fields(self, forgiving=False, **kwargs):
        """
        Set fields
        :param kwargs:
        :return: self to support call chaining
        """
        for k, v in kwargs.items():
            try:
                # will toss an exception if field is not defined
                self.__getattribute__(k)
                if k == "capacities":
                    self.set_capacities(v)
                elif k == "capacity_allocations":
                    self.set_capacity_allocations(v)
                else:
                    self.__setattr__(k, v)
            except AttributeError:
                report = f"Unable to set field {k} of peer, no such field available "\
                       f"{[k for k in self.__dict__.keys()]}"
                if forgiving:
                    print(report)
                else:
                    raise PeerException(report)
        return self

    def __str__(self):
        result = f"agent_id: {self.agent_id}, load: {self.load}, capacities: {self.capacities}"
        if self.capacity_allocations:
            result += f" capacity_allocations: {self.capacity_allocations}"
        return result


class PeerException(Exception):
    """
    Exception with a capacity
    """
    def __init__(self, msg: str):
        assert msg is not None
        super().__init__(f"Peer exception: {msg}")
