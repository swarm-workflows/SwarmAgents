# MIT License
#
# Copyright (c) 2024 swarm-workflows
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
from fontTools.misc.timeTools import DAYNAMES

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# Author: Komal Thareja(kthare10@renci.org)
from swarm.models.json_field import JSONField


class DataNode(JSONField):
    """
    Implements basic data node field handling - encoding and decoding
    from JSON dictionaries of properties. Only ints are allowed.
    """
    def __init__(self, **kwargs):
        self._name = None
        self._file = None
        self._ip = None
        self._user = None
        self._connectivity_score = 0.0
        self._set_fields(**kwargs)

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, name: str):
        self._name = name

    @property
    def file(self):
        return self._file

    @file.setter
    def file(self, file: str):
        self._file = file

    @property
    def ip(self):
        return self._ip

    @ip.setter
    def ip(self, ip: str):
        self._ip = ip

    @property
    def user(self):
        return self._user

    @user.setter
    def user(self, user: str):
        self._user = user

    @property
    def connectivity_score(self):
        return self._connectivity_score

    @connectivity_score.setter
    def connectivity_score(self, connectivity_score: float):
        self._connectivity_score = connectivity_score

    def _set_fields(self, forgiving=False, **kwargs):
        """
        Universal integer setter for all fields.
        Values should be non-negative integers. Throws a CapacityException
        if you try to set a non-existent field.
        :param kwargs:
        :return: self to support call chaining
        """
        for k, v in kwargs.items():
            try:
                # will toss an exception if field is not defined
                self.__getattribute__(k)
                self.__setattr__(k, v)
            except AttributeError:
                report = f"Unable to set field {k} of data_node, no such field available "\
                       f"{[k for k in self.__dict__.keys()]}"
                if forgiving:
                    print(report)
                else:
                    raise DataNodeException(report)
        return self


class DataNodeException(Exception):
    """
    Exception with a DataNodeException
    """
    def __init__(self, msg: str):
        assert msg is not None
        super().__init__(f"DataNode exception: {msg}")


if __name__ == "__main__":
    json_dtn = {"name": "dtn7", "ip": "192.168.100.7", "user": "dtn_user7", "connectivity_score": 0.64}
    #dtn = DataNode(**json_dtn)
    dtn = DataNode().from_dict(json_dtn)
    print(dtn)
    print(dtn.to_dict())
    print(dtn.to_json())