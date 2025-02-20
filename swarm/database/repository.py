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
import json
import threading

import redis


class Repository:
    def __init__(self, redis_client):
        self.redis = redis_client
        self.lock = threading.Lock()

    def save(self, obj: dict, key_prefix: str = "job"):
        if obj.get("id") is None:
            raise ValueError("id must be set to save an object")
        key = f"{key_prefix}:{obj.get('id')}"
        with self.lock:
            pipeline = self.redis.pipeline()
            while True:
                try:
                    pipeline.watch(key)
                    pipeline.multi()
                    pipeline.set(key, json.dumps(obj))
                    pipeline.execute()
                    break
                except redis.WatchError:
                    continue

    def get(self, id: str, key_prefix: str = "job") -> dict:
        key = f"{key_prefix}:{id}"
        with self.lock:
            data = self.redis.get(key)
            if data:
                return json.loads(data)

    def delete(self, id: str, key_prefix: str = "job"):
        key = f"{key_prefix}:{id}"
        with self.lock:
            self.redis.delete(key)

    def get_all_ids(self, key_prefix: str = "job") -> list:
        """
        Retrieves all IDs from Redis, stripping the key prefix.
        """
        with self.lock:
            job_keys = self.redis.keys(f'{key_prefix}:*')  # Fetch all keys with prefix
            ids = [key.split(f"{key_prefix}:")[1] for key in job_keys]  # Extract id
        return ids

    def get_all_objects(self, key_prefix: str = "job") -> list[dict]:
        with self.lock:
            keys = self.redis.keys(f'{key_prefix}:*')  # Assuming job keys are prefixed with 'job:'
            result = []
            for key in keys:
                data = self.redis.get(key)
                if data:
                    result.append(json.loads(data))
            return result

    def delete_all(self, key_prefix: str = "job"):
        with self.lock:
            keys = self.redis.keys(f'{key_prefix}:*')  # Assuming job keys are prefixed with 'job:'
            for key in keys:
                self.redis.delete(key)
