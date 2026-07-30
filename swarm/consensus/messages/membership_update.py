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
#
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


class MembershipUpdate(JSONField):
    """
    A single piggybackable membership change.

    Carries an agent's status (alive | suspect | failed | joined) together with an
    incarnation counter. Higher (status_rank, incarnation) wins on merge, where the
    status rank is alive=0 < joined=0 < suspect=1 < failed=2. Failed is monotonic.

    Used by the SWIM membership layer to disseminate state changes by piggybacking
    on ping / ack / ping-req messages.
    """

    STATUS_ALIVE = "alive"
    STATUS_JOINED = "joined"
    STATUS_SUSPECT = "suspect"
    STATUS_FAILED = "failed"

    def __init__(self, **kwargs):
        self._agent_id = None
        self._status = None
        self._incarnation = 0
        self._set_fields(**kwargs)

    @property
    def agent_id(self):
        return self._agent_id

    @agent_id.setter
    def agent_id(self, v):
        self._agent_id = int(v) if v is not None else None

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, v):
        self._status = v

    @property
    def incarnation(self):
        return self._incarnation

    @incarnation.setter
    def incarnation(self, v):
        self._incarnation = int(v or 0)

    def _set_fields(self, forgiving=False, **kwargs):
        for k, v in kwargs.items():
            try:
                self.__getattribute__(k)
                self.__setattr__(k, v)
            except AttributeError:
                if not forgiving:
                    raise
        return self
