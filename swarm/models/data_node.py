#!/usr/bin/env python3
from swarm.models.json_field import JSONField


class DataNode(JSONField):
    """
    Implements basic data node field handling - encoding and decoding
    from JSON dictionaries of properties. Only ints are allowed.
    """
    def __init__(self, **kwargs):
        self.remote_ip = None
        self.remote_user = None
        self.remote_file = None
        self._set_fields(**kwargs)

    def get_remote_ip(self) -> str:
        return self.remote_ip

    def set_remote_ip(self, remote_ip):
        self.remote_ip = remote_ip

    def get_remote_user(self) -> str:
        return self.remote_user

    def set_remote_user(self, remote_user):
        self.remote_user = remote_user

    def get_remote_file(self) -> str:
        return self.remote_file

    def set_remote_file(self, remote_file):
        self.remote_file = remote_file

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
                assert isinstance(v, str)
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
                    raise DataNodeException(report)
        return self


class DataNodeException(Exception):
    """
    Exception with a capacity
    """
    def __init__(self, msg: str):
        assert msg is not None
        super().__init__(f"DataNode exception: {msg}")
