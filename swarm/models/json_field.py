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
from typing import Dict

from abc import ABC, abstractmethod


class JSONField(ABC):

    @classmethod
    def update(cls, lab, **kwargs):
        """
        quasi-copy constructor if kwargs are ommitted,
        otherwise also sets additional fields. Creates a new
        instance of appropriate subclass and returns it.
        DOES NOT UPDATE IN PLACE!
        :param lab:
        :param kwargs:
        :return:
        """
        assert isinstance(lab, JSONField)
        inst = lab.__class__()
        for k, v in lab.__dict__.items():
            inst.__setattr__(k, v)
        inst._set_fields(**kwargs)
        return inst

    @abstractmethod
    def _set_fields(self, forgiving=False, **kwargs):
        """
        Abstract private set_fields method
        :param kwargs:
        :return:
        """

    def to_json(self) -> str:
        """
        Dumps to JSON the __dict__ of the instance. Be careful as the fields in this
        class should only be those that can be present in JSON output.
        If there are no values in the object, returns empty string.
        :return:
        """
        d = self.to_dict()
        if len(d) == 0:
            return ''
        return json.dumps(d, skipkeys=True, sort_keys=True)

    @classmethod
    def from_json(cls, json_string: str):
        """
        Set fields from json string and returns a new object
        :param json_string:
        :return: object
        """
        if json_string is None or len(json_string) == 0:
            return None
        d = json.loads(json_string)
        ret = cls()
        # we make constructing from JSON more forgiving to allow some limited
        # forward compatibility, in case the fields change
        ret._set_fields(forgiving=True, **d)
        return ret

    @classmethod
    def from_dict(cls, json_dict: dict):
        """
        Set fields from dict and returns a new object
        :param json_dict:
        :return: object
        """
        if json_dict is None or len(json_dict) == 0:
            return None
        ret = cls()
        # we make constructing from JSON more forgiving to allow some limited
        # forward compatibility, in case the fields change
        ret._set_fields(forgiving=True, **json_dict)
        return ret

    def to_dict(self) -> Dict[str, str] or None:
        """
        Convert to a dictionary, skipping empty fields and removing underscores
        from the beginning of the keys. Returns None if all fields are empty.
        """
        new_dict = {}

        for k, v in self.__dict__.items():
            # Remove the underscore from the beginning of the key
            new_key = k.lstrip('_')

            if v is None:
                continue

            if isinstance(v, JSONField):
                new_value = v.to_dict()
                if new_value:
                    new_dict[new_key] = new_value
            elif isinstance(v, enum.Enum):
                new_dict[new_key] = v.value
            elif isinstance(v, list):
                new_value = []
                for x in v:
                    if isinstance(x, JSONField):
                        new_x = x.to_dict()
                        if new_x:
                            new_value.append(new_x)
                    else:
                        new_value.append(x)
                if len(new_value):
                    new_dict[new_key] = new_value
            else:
                new_dict[new_key] = v

        if len(new_dict) == 0:
            return None

        return new_dict

    def __repr__(self):
        return self.to_json()

    def __str__(self):
        return self.to_json()

    def list_fields(self):
        l = list(self.__dict__.keys())
        l.sort()
        return l