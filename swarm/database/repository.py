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
# Author: Komal Thareja (kthare10@renci.org)

import json
import threading
import redis
from typing import Optional, Dict, Tuple, List, Union


class Repository:
    """
    Repository class to handle Redis-based storage for jobs, agents, and multi-phase consensus state
    (Pre-Prepare, Prepare, Commit) for decentralized job scheduling.
    """

    KEY_STATE = "state"
    KEY_JOB = "job"
    KEY_AGENT = "agent"
    KEY_PRE_PREPARE = "pre_prepare"
    KEY_PREPARE = "prepare"
    KEY_COMMIT = "commit"
    KEY_METRICS = "metrics"

    def __init__(self, redis_client: redis.Redis):
        """
        Initialize Repository instance.

        Args:
            redis_client (redis.Redis): Redis client connection object.
        """
        self.redis = redis_client

    ##########################
    # GENERIC JOB OPERATIONS #
    ##########################

    def save(self, obj: dict, key_prefix: str = KEY_JOB, key: Optional[str] = None, level: int = 0, group: int = 0):
        """
        Save a generic object into Redis under the given key.

        Args:
            obj (dict): Object to save.
            key_prefix (str): Prefix to use (job, agent, etc.).
            key (Optional[str]): Specific Redis key. If None, will derive key from object ID.
            level (int): Agent level in hierarchy.
            group (int): Agent group in hierarchy at a level.
        """
        if not key:
            obj_id = obj.get("id") or obj.get(f"{key_prefix}_id")
            if obj_id is None:
                raise ValueError("obj_id must be set to save an object")
            key = f"{key_prefix}:{level}:{group}:{obj_id}"

        pipeline = self.redis.pipeline()
        while True:
            try:
                pipeline.watch(key)
                old_data = pipeline.get(key)
                pipeline.multi()
                pipeline.set(key, json.dumps(obj))

                # Maintain secondary index by state
                new_state = obj.get(self.KEY_STATE)
                if new_state is not None:
                    state_key = f"{self.KEY_STATE}:{level}:{group}:{new_state}"
                    pipeline.sadd(state_key, key)

                    if old_data:
                        old_obj = json.loads(old_data)
                        old_state = old_obj.get(self.KEY_STATE)
                        if old_state is not None and old_state != new_state:
                            old_state_key = f"{self.KEY_STATE}:{level}:{group}:{old_state}"
                            pipeline.srem(old_state_key, key)
                pipeline.execute()
                break
            except redis.WatchError:
                continue

    def get(self, obj_id: str, key_prefix: str = KEY_JOB, level: int = 0, group: int = 0) -> dict:
        """
        Retrieve a generic object from Redis.

        Args:
            obj_id (str): Object ID.
            key_prefix (str): Prefix of key to search under.
            level (int): Agent level in hierarchy.
            group (int): Agent group in hierarchy at a level.

        Returns:
            dict: Retrieved object, or empty dict if not found.
        """
        key = f"{key_prefix}:{level}:{group}:{obj_id}"
        data = self.redis.get(key)
        return json.loads(data) if data else {}

    def delete(self, obj_id: str, key_prefix: str = KEY_JOB, level: int = 0, group: int = 0):
        """
        Delete object from Redis.

        Args:
            obj_id (str): Object ID.
            key_prefix (str): Prefix of key to delete under.
            level (int): Agent level in hierarchy.
            group (int): Agent group in hierarchy at a level.
        """
        key = f"{key_prefix}:{level}:{group}:{obj_id}"
        data = self.redis.get(key)
        if data:
            job = json.loads(data)
            state = job.get("state")
            if state is not None:
                state_key = f"state:{level}:{group}:{state}"
                self.redis.srem(state_key, key)
        self.redis.delete(key)

    def get_all_ids(self, key_prefix: str = KEY_JOB, level: int = 0, group: int = 0, state: int = None) -> List[str]:
        """
        Get list of all IDs under given key prefix.

        Args:
            key_prefix (str): Prefix to search.
            level (int): Agent level in hierarchy.
            group (int): Agent group in hierarchy at a level.
            state (int): Job State

        Returns:
            List[str]: List of object IDs.
        """
        if state:
            state_key = f"state:{level}:{group}:{state}"
            all_keys = self.redis.smembers(state_key)
        else:
            all_keys = self.redis.scan_iter(f'{key_prefix}:{level}:{group}:*')
        return [key.split(":", 3)[-1] for key in all_keys]

    def get_all_objects(self, key_prefix: str = KEY_JOB, level: int = 0, group: int = None, state: int = None) -> List[dict]:
        """
        Retrieve all objects under given key prefix.

        Args:
            key_prefix (str): Prefix to search.
            level (int): Agent level in hierarchy.
            group (int): Agent group in hierarchy at a level.
            state (int): Job State

        Returns:
            List[dict]: List of retrieved objects.
        """
        if state:
            if group is not None:
                state_key = f"state:{level}:{group}:{state}"
            else:
                state_key = f"state:{level}:*:{state}"
            keys = self.redis.smembers(state_key)
        else:
            if level is None:
                keys = self.redis.scan_iter(f'{key_prefix}:*')
            else:
                if group is not None:
                    keys = self.redis.scan_iter(f'{key_prefix}:{level}:{group}:*')
                else:
                    keys = self.redis.scan_iter(f'{key_prefix}:{level}:*')
        results = []
        for key in keys:
            val = self.redis.get(key)
            if val:
                results.append(json.loads(val))
        return results

    def delete_all(self, key_prefix: str = KEY_JOB):
        """
        Delete all objects under given key prefix.

        Args:
            key_prefix (str): Prefix to delete.
            group (int): Agent group in hierarchy at a level.
            level (int): Agent level in hierarchy.
        """
        keys = self.redis.scan_iter(f'{key_prefix}:*')
        for key in keys:
            self.redis.delete(key)
