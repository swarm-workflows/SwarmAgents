#!/usr/bin/env python3
# import json
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
        d = self.__dict__.copy()
        for k in self.__dict__:
            if d[k] is None or d[k] == 0:
                d.pop(k)
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
        Convert to a dictionary skipping empty fields. Returns None
        if all fields are empty
        :return:
        """
        d = self.__dict__.copy()
        for k in self.__dict__:
            if d[k] is None or d[k] == 0:
                d.pop(k)
        if len(d) == 0:
            return None
        return d

    def __repr__(self):
        return self.to_json()

    def __str__(self):
        return self.to_json()

    def list_fields(self):
        l = list(self.__dict__.keys())
        l.sort()
        return l