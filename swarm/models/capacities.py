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
from typing import List

from swarm.models.json_field import JSONField


class Capacities(JSONField):
    """
    Implements basic capacity field handling - encoding and decoding
    from JSON dictionaries of properties.
    """
    UNITS = {'cpu': '', 'unit': '',
             'core': '', 'ram': 'G',
             'disk': 'G', 'bw': 'Gbps',
             'burst_size': 'Mbits',
             'mtu': 'B'}

    def __init__(self, **kwargs):
        self.cpu = 0
        self.core = 0
        self.ram = 0
        self.disk = 0
        self.bw = 0
        self.burst_size = 0
        self.unit = 0
        self.mtu = 0
        self._set_fields(**kwargs)

    def _set_fields(self, forgiving=False, **kwargs):
        """
        Universal integer setter for all fields.
        Values should be non-negative integers. Throws a CapacityException
        if you try to set a non-existent field.
        :param kwargs:
        :return: self to support call chaining
        """
        for k, v in kwargs.items():
            if v is not None:
                assert v >= 0
                assert isinstance(v, float)
            try:
                # will toss an exception if field is not defined
                self.__getattribute__(k)
                self.__setattr__(k, v)
            except AttributeError:
                report = f"Unable to set field {k} of capacity, no such field available "\
                       f"{[k for k in self.__dict__.keys()]}"
                if forgiving:
                    print(report)
                else:
                    raise CapacityException(report)
        return self

    def __add__(self, other):
        assert isinstance(other, Capacities)
        ret = Capacities()
        for f, v in self.__dict__.items():
            ret.__dict__[f] = round((self.__dict__[f] + other.__dict__[f]), 2)

        return ret

    def __sub__(self, other):
        assert isinstance(other, Capacities)
        ret = Capacities()

        for f, v in self.__dict__.items():
            ret.__dict__[f] = round((self.__dict__[f] - other.__dict__[f]), 2)

        return ret

    def __gt__(self, other):
        assert isinstance(other, Capacities)
        for f, v in self.__dict__.items():
            if v < other.__dict__[f]:
                return False
        return True

    def __lt__(self, other):
        assert isinstance(other, Capacities)
        for f, v in self.__dict__.items():
            if v > other.__dict__[f]:
                return False
        return True

    def __eq__(self, other):
        if not other:
            return False
        assert isinstance(other, Capacities)
        for f, v in self.__dict__.items():
            # since we may be dealing with pickled versions
            # of Capcities created with older version of FIM
            # need to be careful not to assume all the same
            # fields are present
            if v != other.__dict__.get(f, 0):
                return False
        return True

    def negative_fields(self) -> List[str]:
        """
        returns list of fields that are negative
        :return:
        """
        ret = list()
        for f, v in self.__dict__.items():
            if v < 0:
                ret.append(f)

        return ret

    def positive_fields(self, fields: List[str] or str) -> bool:
        """
        Return true if indicated fields are positive >0
        :param fields: string or list of strings
        :return:
        """
        assert fields is not None
        if isinstance(fields, str):
            fields = [fields]
        for f in fields:
            if self.__dict__[f] <= 0:
                return False
        return True

    def __str__(self):
        d = self.__dict__.copy()
        for k in self.__dict__:
            if d[k] is None or d[k] == 0:
                d.pop(k)
        if len(d) == 0:
            return ''
        ret = "{ "
        for i, v in d.items():
            ret = ret + i + ": " + f'{v:,} ' + self.UNITS[i] + ", "
        return ret[:-2] + "}"


class CapacityException(Exception):
    """
    Exception with a capacity
    """
    def __init__(self, msg: str):
        assert msg is not None
        super().__init__(f"Capacity exception: {msg}")
