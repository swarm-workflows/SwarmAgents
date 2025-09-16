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
import threading
from itertools import islice

from swarm.models.job import Job, ObjectState
from swarm.models.object import Object
from swarm.queue.object_queue import ObjectQueue


class SimpleQueue(ObjectQueue):
    def __init__(self):
        self.objects = {}
        self.lock = threading.RLock()

    def size(self):
        with self.lock:
            return len(self.objects)

    def gets(self, states: list[ObjectState] = None, count: int = None) -> list[Job]:
        # Copy references while holding the lock, so iteration happens outside
        with self.lock:
            all_objects = list(self.objects.values())

        if states:
            states_set = set(states)
            all_objects = (j for j in all_objects if j.state in states_set)

        if count:
            all_objects = islice(all_objects, count)

        return list(all_objects)

    def add(self, object: Object):
        with self.lock:
            self.objects[object.object_id] = object

    def update(self, object: Object):
        with self.lock:
            self.objects[object.object_id] = object

    def remove(self, object_id: str):
        with self.lock:
            if object_id in self.objects:
                self.objects.pop(object_id)

    def get(self, object_id: str) -> Job:
        with self.lock:
            return self.objects.get(object_id)

    def __contains__(self, object_id):
        with self.lock:
            return object_id in self.objects
