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

    KEY_JOB = "job"
    KEY_AGENT = "agent"
    KEY_PRE_PREPARE = "pre_prepare"
    KEY_PREPARE = "prepare"
    KEY_COMMIT = "commit"

    def __init__(self, redis_client: redis.Redis):
        """
        Initialize Repository instance.

        Args:
            redis_client (redis.Redis): Redis client connection object.
        """
        self.redis = redis_client
        self.lock = threading.Lock()

    ##########################
    # GENERIC JOB OPERATIONS #
    ##########################

    def save(self, obj: dict, key_prefix: str = KEY_JOB, key: Optional[str] = None, level: int = 0):
        """
        Save a generic object into Redis under the given key.

        Args:
            obj (dict): Object to save.
            key_prefix (str): Prefix to use (job, agent, etc.).
            key (Optional[str]): Specific Redis key. If None, will derive key from object ID.
            level (int): Agent level in hierarchy.
        """
        if not key:
            obj_id = obj.get("id") or obj.get(f"{key_prefix}_id")
            if obj_id is None:
                raise ValueError("obj_id must be set to save an object")
            key = f"{key_prefix}:{level}:{obj_id}"

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

    def get(self, obj_id: str, key_prefix: str = KEY_JOB, level: int = 0) -> dict:
        """
        Retrieve a generic object from Redis.

        Args:
            obj_id (str): Object ID.
            key_prefix (str): Prefix of key to search under.
            level (int): Agent level in hierarchy.

        Returns:
            dict: Retrieved object, or empty dict if not found.
        """
        key = f"{key_prefix}:{level}:{obj_id}"
        data = self.redis.get(key)
        return json.loads(data.decode()) if data else {}

    def delete(self, obj_id: str, key_prefix: str = KEY_JOB, level: int = 0):
        """
        Delete object from Redis.

        Args:
            obj_id (str): Object ID.
            key_prefix (str): Prefix of key to delete under.
            level (int): Agent level in hierarchy.
        """
        key = f"{key_prefix}:{level}:{obj_id}"
        self.redis.delete(key)

    def get_all_ids(self, key_prefix: str = KEY_JOB, level: int = 0) -> List[str]:
        """
        Get list of all IDs under given key prefix.

        Args:
            key_prefix (str): Prefix to search.
            level (int): Agent level in hierarchy.

        Returns:
            List[str]: List of object IDs.
        """
        all_keys = self.redis.keys(f'{key_prefix}:{level}:*')
        return list(set(key.decode().split(":")[2] for key in all_keys))

    def get_all_objects(self, key_prefix: str = KEY_JOB, level: int = 0) -> List[dict]:
        """
        Retrieve all objects under given key prefix.

        Args:
            key_prefix (str): Prefix to search.
            level (int): Agent level in hierarchy.

        Returns:
            List[dict]: List of retrieved objects.
        """
        keys = self.redis.keys(f'{key_prefix}:{level}:*')
        results = []
        for key in keys:
            val = self.redis.get(key)
            if val:
                results.append(json.loads(val.decode()))
        return results

    def delete_all(self, key_prefix: str = KEY_JOB, level: int = 0):
        """
        Delete all objects under given key prefix.

        Args:
            key_prefix (str): Prefix to delete.
            level (int): Agent level in hierarchy.
        """
        keys = self.redis.keys(f'{key_prefix}:{level}:*')
        for key in keys:
            self.redis.delete(key)

    ################################
    # PRE-PREPARE PHASE OPERATIONS #
    ################################

    def push_pre_prepare(self, job_id: str, cost: float, agent_id: int, level: int = 0):
        redis_key = f"{self.KEY_PRE_PREPARE}:{level}:{job_id}:{agent_id}"
        self.redis.set(redis_key, round(float(cost), 2))

    def get_pre_prepare(self, job_id: str, level: int = 0) -> Dict[str, float]:
        return self._get_votes(self.KEY_PRE_PREPARE, job_id, level)

    def get_min_cost_agent_for_job(self, job_id: str, level: int = 0) -> Tuple[Optional[int], float]:
        job_costs = self.get_pre_prepare(job_id, level)
        if not job_costs:
            return None, float('inf')
        min_agent = min(job_costs, key=job_costs.get)
        return int(min_agent), job_costs[min_agent]

    ##############################
    # PREPARE PHASE OPERATIONS   #
    ##############################

    def push_prepare_vote(self, job_id: str, leader_agent_id: int, agent_id: int, level: int = 0):
        redis_key = f"{self.KEY_PREPARE}:{level}:{job_id}:{agent_id}"
        self.redis.set(redis_key, leader_agent_id)

    def get_prepare(self, job_id: str, level: int = 0) -> Dict[str, int]:
        return self._get_votes(self.KEY_PREPARE, job_id, level)

    ##############################
    # COMMIT PHASE OPERATIONS    #
    ##############################

    def push_commit_vote(self, job_id: str, leader_agent_id: int, agent_id: int, level: int = 0):
        redis_key = f"{self.KEY_COMMIT}:{level}:{job_id}:{agent_id}"
        self.redis.set(redis_key, leader_agent_id)

    def get_commit(self, job_id: str, level: int = 0) -> Dict[str, int]:
        return self._get_votes(self.KEY_COMMIT, job_id, level)

    #####################
    # INTERNAL UTILITIES #
    #####################

    def _get_votes(self, phase_prefix: str, job_id: str, level: int = 0) -> Dict[str, Union[float, int]]:
        pattern = f"{phase_prefix}:{level}:{job_id}:*"
        keys = self.redis.keys(pattern)
        result = {}
        for key in keys:
            key_str = key.decode()
            parts = key_str.split(":")
            if len(parts) < 4:
                continue
            agent_id = parts[3]
            raw_value = self.redis.get(key)
            if raw_value is None:
                continue
            try:
                decoded = raw_value.decode()
                value = float(decoded) if phase_prefix == self.KEY_PRE_PREPARE else int(decoded)
                result[agent_id] = value
            except (ValueError, UnicodeDecodeError):
                print(f"Invalid value for key {key_str}: {raw_value}")
        return result
