# MIT License
#
# Copyright (c) 2024 swarm-workflows
#
# Author: Komal Thareja(kthare10@renci.org)
from swarm.models.json_field import JSONField


class AgentStateEntry(JSONField):
    """
    Versioned snapshot of one agent's load/utilization state.

    Used by the gossip dissemination layer; entries are merged by ``version``
    (higher wins) and expire after the disseminator's configured TTL. Numeric
    fields default to 0 so a partial update (e.g. an agent that only reports
    cpu/ram) doesn't clobber the rest with None.
    """

    def __init__(self, **kwargs):
        self._agent_id = None
        self._cpu_util = 0.0
        self._ram_util = 0.0
        self._disk_util = 0.0
        self._gpu_util = 0.0
        self._load = 0.0
        self._version = 0
        self._set_fields(**kwargs)

    @property
    def agent_id(self):
        return self._agent_id

    @agent_id.setter
    def agent_id(self, v):
        self._agent_id = int(v) if v is not None else None

    @property
    def cpu_util(self):
        return self._cpu_util

    @cpu_util.setter
    def cpu_util(self, v):
        self._cpu_util = float(v or 0.0)

    @property
    def ram_util(self):
        return self._ram_util

    @ram_util.setter
    def ram_util(self, v):
        self._ram_util = float(v or 0.0)

    @property
    def disk_util(self):
        return self._disk_util

    @disk_util.setter
    def disk_util(self, v):
        self._disk_util = float(v or 0.0)

    @property
    def gpu_util(self):
        return self._gpu_util

    @gpu_util.setter
    def gpu_util(self, v):
        self._gpu_util = float(v or 0.0)

    @property
    def load(self):
        return self._load

    @load.setter
    def load(self, v):
        self._load = float(v or 0.0)

    @property
    def version(self):
        return self._version

    @version.setter
    def version(self, v):
        self._version = int(v or 0)

    def _set_fields(self, forgiving=False, **kwargs):
        for k, v in kwargs.items():
            try:
                self.__getattribute__(k)
                self.__setattr__(k, v)
            except AttributeError:
                if not forgiving:
                    raise
        return self
