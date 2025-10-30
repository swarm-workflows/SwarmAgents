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
import threading
import time
from abc import ABC

class ObjectState(enum.Enum):
    PENDING = enum.auto()
    PRE_PREPARE = enum.auto()
    PREPARE = enum.auto()
    COMMIT = enum.auto()
    READY = enum.auto()
    RUNNING = enum.auto()
    IDLE = enum.auto()
    COMPLETE = enum.auto()
    FAILED = enum.auto()


class Object(ABC):
    def __init__(self):
        self._object_id = None
        self._state = ObjectState.PENDING
        self._time_last_state_change = time.time()
        self._leader_id = None
        self.lock = threading.RLock()  # Lock for synchronization

    @property
    def time_last_state_change(self):
        with self.lock:
            return self._time_last_state_change

    @time_last_state_change.setter
    def time_last_state_change(self, time_last_state_change):
        with self.lock:
            self._time_last_state_change = time_last_state_change

    @property
    def state(self):
        with self.lock:
            return self._state

    @state.setter
    def state(self, value):
        with self.lock:
            old = self._state
            self._state = value
            self.time_last_state_change = time.time()
        # Call a hook after unlock to avoid lock reentrancy surprises
        self.on_state_changed(old, value)

    def on_state_changed(self, old_state, new_state):
        pass

    @property
    def object_id(self):
        with self.lock:
            return self._object_id

    @object_id.setter
    def object_id(self, value):
        with self.lock:
            self._object_id = value

    @property
    def is_pending(self):
        with self.lock:
            return self.state == ObjectState.PENDING

    @property
    def is_running(self):
        with self.lock:
            return self.state == ObjectState.RUNNING

    @property
    def is_ready(self):
        with self.lock:
            return self.state == ObjectState.READY

    @property
    def is_commit(self):
        with self.lock:
            return self.state == ObjectState.COMMIT

    @property
    def is_complete(self):
        with self.lock:
            return self.state == ObjectState.COMPLETE

    @property
    def leader_id(self):
        with self.lock:
            return self._leader_id

    @leader_id.setter
    def leader_id(self, value):
        with self.lock:
            self._leader_id = value