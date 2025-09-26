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

from swarm.models.capacities import Capacities
from swarm.models.data_node import DataNode
from swarm.models.json_field import JSONField


class AgentInfo(JSONField):
    """
    Implements basic Peer field handling - encoding and decoding
    from JSON dictionaries of properties.
    """
    def __init__(self, **kwargs):
        self.agent_id = 0
        self._host = None
        self._port = None
        self.load = 0.0
        self.proposed_load = 0.0
        self._capacities = Capacities()
        self._capacity_allocations = Capacities()
        self.last_updated = 0.0
        self._dtns = {}
        self._set_fields(**kwargs)

    @property
    def port(self) -> int:
        return self._port

    @port.setter
    def port(self, port: int) -> None:
        self._port = port

    @property
    def host(self) -> str:
        return self._host

    @host.setter
    def host(self, value: str):
        self._host = value

    @property
    def dtns(self) -> dict[str, DataNode]:
        return self._dtns

    @dtns.setter
    def dtns(self, value: dict):
        if isinstance(value, list):
            for dtn_info in value:
                self.dtns[dtn_info.get('name')] = dtn_info
        elif isinstance(value, dict):
            for key, dtn_info in value.items():
                self.dtns[key] = DataNode.from_dict(dtn_info)
        else:
            raise ValueError("Unsupported value type for dtns")

    @property
    def capacities(self) -> Capacities:
        return self._capacities

    @capacities.setter
    def capacities(self, value: Capacities):
        if isinstance(value, Capacities):
            self._capacities = value
        elif isinstance(value, dict):
            self._capacities = Capacities.from_dict(value)
        else:
            raise ValueError("Unsupported value type for capacities")

    @property
    def capacity_allocations(self) -> Capacities:
        return self._capacity_allocations

    @capacity_allocations.setter
    def capacity_allocations(self, value: Capacities):
        if isinstance(value, Capacities):
            self._capacity_allocations = value
        elif isinstance(value, dict):
            self._capacity_allocations = Capacities.from_dict(value)
        else:
            raise ValueError("Unsupported value type for capacity_allocations")

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
        result = f"agent_id: {self.agent_id}, load: {self.load}, proposed_load: {self.proposed_load}," \
                 f" capacities: {self.capacities}"
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


if __name__ == '__main__':
    json_object = {"agent_id": 5, "host": "localhost", "port": 20005, "capacities": {"cpu": 2, "core": 4, "ram": 16, "disk": 250}, "last_updated": 1758223197.978923, "dtns": {"dtn7": {"name": "dtn7", "ip": "192.168.100.7", "user": "dtn_user7", "connectivity_score": 0.64}, "dtn6": {"name": "dtn6", "ip": "192.168.100.6", "user": "dtn_user6", "connectivity_score": 0.84}, "dtn3": {"name": "dtn3", "ip": "192.168.100.3", "user": "dtn_user3", "connectivity_score": 0.82}, "dtn10": {"name": "dtn10", "ip": "192.168.100.10", "user": "dtn_user10", "connectivity_score": 0.67}}}
    agent = AgentInfo.from_dict(json_object)
    print(agent.to_dict())
    print(agent.to_json())
