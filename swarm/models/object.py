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
        self._time_last_state_change = 0
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
        return self.state == ObjectState.PENDING

    @property
    def is_running(self):
        return self.state == ObjectState.RUNNING

    @property
    def is_ready(self):
        return self.state == ObjectState.READY

    @property
    def is_commit(self):
        return self.state == ObjectState.COMMIT

    @property
    def is_complete(self):
        return self.state == ObjectState.COMPLETE