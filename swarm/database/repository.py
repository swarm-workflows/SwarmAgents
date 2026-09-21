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
import logging
import os
import random
import threading
import time
import redis
from typing import Optional, Dict, Tuple, List, Union

LOG = logging.getLogger(__name__)


class Repository:
    """
    Repository class to handle Redis-based storage for jobs, agents, and multi-phase consensus state
    (Pre-Prepare, Prepare, Commit) for decentralized job scheduling.
    """

    KEY_STATE = "state"
    KEY_JOB = "job"
    KEY_ROLE = "role"
    KEY_AGENT = "agent"
    KEY_PRE_PREPARE = "pre_prepare"
    KEY_PREPARE = "prepare"
    KEY_COMMIT = "commit"
    KEY_METRICS = "metrics"
    KEY_ASSIGNEE = "assignee"

    def __init__(self, redis_client: redis.Redis, run_id: Optional[str] = None):
        """
        Initialize Repository instance.

        Args:
            redis_client (redis.Redis): Redis client connection object.
            run_id (Optional[str]): Identity of the run, for keys that must not be inherited
                by the next one. Defaults to ``SWARM_RUN_ID``, which ``run_test.py`` exports
                to every agent (inherited locally, re-exported over ssh), so every agent in a
                run agrees on it without being told. Resolved here and only here.
        """
        self.redis = redis_client
        self.run_id = run_id or os.environ.get("SWARM_RUN_ID") or "norun"
        # Contention instrumentation: total WatchError retries across all save() calls.
        # A high rate at scale means hot keys are serializing writers (see SCALABILITY_REVIEW).
        self.watch_retries = 0
        self.saves = 0

    ##########################
    # GENERIC JOB OPERATIONS #
    ##########################

    def save(self, obj: dict, key_prefix: str = KEY_JOB, key: Optional[str] = None,
             level: int = 0, group: int = 0, max_retries: int = 10,
             produced_data: Optional[List[str]] = None,
             produced_locations: Optional[Dict[str, dict]] = None):
        """
        Save a generic object into Redis under the given key.

        Uses optimistic locking (WATCH/MULTI/EXEC) with exponential backoff
        and jitter on contention. Raises RuntimeError after max_retries.

        Args:
            obj (dict): Object to save.
            key_prefix (str): Prefix to use (job, agent, etc.).
            key (Optional[str]): Specific Redis key. If None, will derive key from object ID.
            level (int): Agent level in hierarchy.
            group (int): Agent group in hierarchy at a level.
            max_retries (int): Maximum retry attempts on WatchError (default: 10).
            produced_data (Optional[List[str]]): Output names this write makes available,
                added to the readiness registry **inside the same MULTI**. This is what closes
                the window between "the job is complete" and "its outputs exist": as two
                writes, an agent dying in between left every descendant of that job gated for
                the rest of the run, and no retry queue can cover it because the queue dies
                with the agent. As one transaction there is no in-between — either both landed
                or neither, and a job that is not COMPLETE is reselected by the existing
                machinery. Pass it only on a *successful* terminal write; a failed job's
                outputs do not exist.
            produced_locations (Optional[Dict[str, dict]]): Where each of those names can be
                fetched from — `{name: {agent_id, host, port, produced_at}}` — written into the
                location registry **inside the same MULTI** as the names themselves. It rides
                the same transaction for the same reason `produced_data` does, one step further
                on: a descendant that can see a name as ready but cannot find out where it is
                would be released and then refuse to stage, which looks like a staging bug and
                is really a torn write. Either both landed or neither. Ignored when staging is
                off, where the shared mount is the location.
        """
        if not key:
            obj_id = obj.get("id") or obj.get(f"{key_prefix}_id")
            if obj_id is None:
                raise ValueError("obj_id must be set to save an object")
            key = f"{key_prefix}:{level}:{group}:{obj_id}"

        pipeline = self.redis.pipeline()
        for attempt in range(1, max_retries + 1):
            try:
                pipeline.watch(key)
                old_data = pipeline.get(key)
                pipeline.multi()
                pipeline.set(key, json.dumps(obj))
                if key_prefix == self.KEY_AGENT:
                    pipeline.sadd(self._members_key(level, group), key)
                if produced_data:
                    pipeline.sadd(self._data_ready_key(),
                                  *[str(n) for n in produced_data if n])
                if produced_locations:
                    pipeline.hset(
                        self._data_loc_key(),
                        mapping={str(n): json.dumps(loc)
                                 for n, loc in produced_locations.items() if n})

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
                self.saves += 1
                return  # success
            except redis.WatchError:
                self.watch_retries += 1
                if attempt == max_retries:
                    raise RuntimeError(
                        f"Redis optimistic lock failed after {max_retries} attempts for key={key}"
                    )
                backoff = min(0.01 * (2 ** attempt), 0.5) + random.uniform(0, 0.01)
                LOG.debug("WatchError on key=%s (attempt %d/%d), retrying in %.3fs",
                          key, attempt, max_retries, backoff)
                time.sleep(backoff)

    @staticmethod
    def _members_key(level: int, group: int) -> str:
        return f"members:{level}:{group}"

    def save_fast(self, obj: dict, key_prefix: str = KEY_AGENT, key: Optional[str] = None,
                  level: int = 0, group: int = 0, ttl_s: Optional[int] = None):
        """Save for SINGLE-WRITER keys (each agent writes only its own agent-info):
        plain pipelined SET — no WATCH/read-back (that optimistic-lock round-trip is
        pure overhead when there is exactly one writer). Also maintains the
        members:{level}:{group} registry SET so discovery is one SMEMBERS+MGET instead
        of a keyspace SCAN every refresh tick, and applies a TTL so keys of dead
        agents age out instead of accumulating (they previously lived forever).
        """
        if not key:
            obj_id = obj.get("id") or obj.get(f"{key_prefix}_id") or obj.get("agent_id")
            if obj_id is None:
                raise ValueError("obj_id must be set to save an object")
            key = f"{key_prefix}:{level}:{group}:{obj_id}"
        pipe = self.redis.pipeline(transaction=False)
        pipe.set(key, json.dumps(obj), ex=ttl_s)
        if key_prefix == self.KEY_AGENT:
            pipe.sadd(self._members_key(level, group), key)
        pipe.execute()
        self.saves += 1

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

    def get_many(self, obj_ids: List[str], key_prefix: str = KEY_JOB,
                 level: int = 0, group: int = 0) -> Dict[str, dict]:
        """Fetch many objects in ONE MGET round-trip (the per-id get() loop it replaces
        paid one WAN RTT per object every periodic tick). Missing ids are omitted."""
        if not obj_ids:
            return {}
        keys = [f"{key_prefix}:{level}:{group}:{oid}" for oid in obj_ids]
        values = self.redis.mget(keys)
        out: Dict[str, dict] = {}
        for oid, v in zip(obj_ids, values):
            if v:
                out[oid] = json.loads(v)
        return out

    def get_many_grouped(self, pairs: List[tuple], key_prefix: str = KEY_JOB,
                         level: int = 0) -> Dict[tuple, dict]:
        """Fetch (obj_id, group) pairs across groups in ONE MGET round-trip.
        Returns {(obj_id, group): obj}; missing pairs are omitted."""
        if not pairs:
            return {}
        keys = [f"{key_prefix}:{level}:{group}:{oid}" for oid, group in pairs]
        values = self.redis.mget(keys)
        return {pair: json.loads(v) for pair, v in zip(pairs, values) if v}

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
        if key_prefix == self.KEY_AGENT:
            self.redis.srem(self._members_key(level, group), key)
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

    def get_all_ids_multi(self, key_prefix: str = KEY_JOB, level: int = 0,
                          group: int = 0, states: list = None) -> Dict[int, List[str]]:
        """
        Get IDs for multiple states in a single Redis pipeline round-trip.

        Args:
            key_prefix (str): Prefix to search.
            level (int): Agent level in hierarchy.
            group (int): Agent group in hierarchy at a level.
            states (list): List of state values to query.

        Returns:
            Dict[int, List[str]]: Mapping of state -> list of object IDs.
        """
        if not states:
            return {}
        pipe = self.redis.pipeline()
        for state in states:
            state_key = f"state:{level}:{group}:{state}"
            pipe.smembers(state_key)
        results = pipe.execute()
        return {state: [k.split(":", 3)[-1] for k in keys]
                for state, keys in zip(states, results)}

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
                keys = self.redis.smembers(f"state:{level}:{group}:{state}")
            else:
                # SMEMBERS does not support glob patterns — a wildcard key here silently
                # returned an empty set. Find the per-group state-index keys and union
                # their members instead.
                keys = set()
                for state_key in self.redis.scan_iter(f"state:{level}:*:{state}"):
                    keys |= self.redis.smembers(state_key)
        elif key_prefix == self.KEY_AGENT and level is not None and group is not None:
            # Hot path (every agent, every periodic tick): use the members registry SET
            # instead of a keyspace SCAN — SCAN restarts its cursor on every call and
            # its cost grows with total keyspace, not group size. Prune registry entries
            # whose agent key has expired (TTL) or been deleted; fall back to SCAN only
            # when the registry is empty (first boot / mixed-version migration).
            mkey = self._members_key(level, group)
            keys = list(self.redis.smembers(mkey))
            if keys:
                values = self.redis.mget(keys)
                stale = [k for k, v in zip(keys, values) if not v]
                if stale:
                    self.redis.srem(mkey, *stale)
                return [json.loads(v) for v in values if v]
            keys = self.redis.scan_iter(f'{key_prefix}:{level}:{group}:*')
        else:
            if level is None:
                keys = self.redis.scan_iter(f'{key_prefix}:*')
            else:
                if group is not None:
                    keys = self.redis.scan_iter(f'{key_prefix}:{level}:{group}:*')
                else:
                    keys = self.redis.scan_iter(f'{key_prefix}:{level}:*')
        keys = list(keys)
        if not keys:
            return []
        values = self.redis.mget(keys)
        return [json.loads(v) for v in values if v]

    def try_claim_assignment(self, job_id: str, agent_id: int,
                             level: int = 0, group: int = 0) -> int:
        """
        Atomically claim ``job_id`` for ``agent_id`` using Redis SET NX.

        Returns the winning agent_id — ``agent_id`` if this caller won the
        claim, otherwise the agent_id of whoever got there first. Used by the
        Snow/Avalanche engine to finalize a probabilistic decision into an
        exactly-once assignment.
        """
        key = f"{self.KEY_ASSIGNEE}:{level}:{group}:{job_id}"
        # `nx=True` returns True iff we set the key; otherwise read the existing value.
        if self.redis.set(key, str(int(agent_id)), nx=True):
            return int(agent_id)
        existing = self.redis.get(key)
        return int(existing) if existing is not None else int(agent_id)

    # ---- Produced-data registry (workflow DAG gating) -------------------------------- #
    #
    # One Redis SET per run holding the logical file names that have actually been produced.
    # A job whose inputs name files in this set is runnable; one whose inputs do not is a
    # descendant whose parent has not finished. Deliberately a *set of names* rather than a
    # graph of job ids: a Pegasus DAG's edges are file dependencies, the converter recovers
    # them by matching one job's outputs against another's inputs, and a name is the only
    # thing both halves agree on. It is also flat across levels and groups, because the
    # producer and the consumer of a file are on different agents by construction.

    KEY_DATA_READY = "data_ready"

    def _data_ready_key(self) -> str:
        """Run-scoped, and shaped like every other key so cleanup finds it.

        Both halves are load-bearing. The shape (`prefix:a:b:c`) matters because
        `delete_all("*")` — which `cleanup.py` runs between runs — scans `*:*`, so a *bare*
        key survives every cleanup this project has: the next run of the same workflow would
        find its predecessor's file names already present and release the whole DAG at once,
        on the first tick, with nothing saying so. The run id matters because cleanup is not
        the only path — an operator flushing by hand, or a cell that skipped it, must still
        not inherit readiness. It is the same reason metrics payloads are run-stamped.
        """
        return f"{self.KEY_DATA_READY}:{self.run_id}:0:names"

    #: Where each produced name can be fetched from. A HASH beside the readiness SET rather
    #: than fields inside it, because the two are read on completely different paths: readiness
    #: is `smismember` for every gated job on every scheduling pass and must stay one round
    #: trip, while a location is read only when a file is actually about to be staged.
    KEY_DATA_LOC = "data_loc"

    def _data_loc_key(self) -> str:
        """Run-scoped and `prefix:a:b:c`-shaped for the same two reasons as the readiness key:
        `delete_all("*")` must reach it between runs, and a cell that skipped cleanup must not
        inherit a previous run's locations — which would point a fetch at an agent that is not
        serving that file any more, or worse, at one that is serving a stale copy."""
        return f"{self.KEY_DATA_LOC}:{self.run_id}:0:names"

    def data_locations(self, names: List[str]) -> Dict[str, dict]:
        """`{name: location}` for the names that have one. One round trip, like readiness.

        A name present in the readiness registry but absent here means the run is staging-less
        (the shared mount is the location) — the caller distinguishes those two cases, because
        "nowhere to fetch from" and "no fetching needed" call for opposite responses.
        """
        names = [str(n) for n in names if n]
        if not names:
            return {}
        raw = self.redis.hmget(self._data_loc_key(), names)
        out: Dict[str, dict] = {}
        for name, value in zip(names, raw):
            if not value:
                continue
            try:
                out[name] = json.loads(value)
            except (ValueError, TypeError):
                continue                    # a corrupt entry is treated as absent, never guessed
        return out

    def mark_data_available(self, names: List[str]) -> None:
        """Record that *names* now exist. Called once a job has actually completed."""
        names = [str(n) for n in names if n]
        if not names:
            return
        self.redis.sadd(self._data_ready_key(), *names)

    def data_available(self, names: List[str]) -> bool:
        """True when every name in *names* has been produced (or the list is empty).

        One round trip regardless of how many names are asked about: this is called for every
        gated job on every scheduling pass, and a per-name GET loop would put a WAN round trip
        per edge into the scheduling loop of every agent."""
        names = [str(n) for n in names if n]
        if not names:
            return True
        return all(self.redis.smismember(self._data_ready_key(), names))

    def available_data(self) -> set:
        """Every name produced so far — for diagnostics, not for the scheduling path."""
        return {v.decode() if isinstance(v, bytes) else v
                for v in self.redis.smembers(self._data_ready_key())}

    def get_assignment(self, job_id: str, level: int = 0, group: int = 0):
        """Return the committed assignee for ``job_id``, or None if unclaimed."""
        key = f"{self.KEY_ASSIGNEE}:{level}:{group}:{job_id}"
        v = self.redis.get(key)
        return int(v) if v is not None else None

    def release_assignment(self, job_id: str, level: int = 0, group: int = 0) -> bool:
        """Drop the exactly-once claim on ``job_id`` so it can be assigned again.

        The claim is what makes a Snow finalization exactly-once: `try_claim_assignment` is a
        `SET NX`, and every later finalizer is told the first winner. Nothing released it, so a
        job whose assignee *died* could never be re-finalized to anyone else — the CAS kept
        returning the dead agent for the rest of the run, and reassignment was silently
        impossible under the shipped protocol. Releasing is safe only once the job is no
        longer running anywhere, which is exactly the failed-agent case; it must never be
        called on a live assignment, or two agents could execute the same job.
        """
        return bool(self.redis.delete(f"{self.KEY_ASSIGNEE}:{level}:{group}:{job_id}"))

    KEY_REASSIGN = "reassign"

    def try_claim_reassignment(self, job_id: str, failed_agent_id: int,
                               level: int = 0, group: int = 0, ttl_s: int = 300) -> bool:
        """Win the right to reassign ``job_id`` away from ``failed_agent_id``. `SET NX`.

        Every live agent detects the same failure at roughly the same moment and would
        otherwise each reset the job. Resetting twice is not harmless: the second reset can
        land *after* the first reassignment has already won a fresh election, clobbering a
        live assignment back to PENDING and running the job twice. One winner per (job,
        failure) removes that, using the same primitive as the assignment claim itself.

        The TTL matters: if the winner dies mid-reassignment, the right has to become
        available again, or the job is stranded by the very mechanism meant to rescue it.
        """
        key = f"{self.KEY_REASSIGN}:{level}:{group}:{job_id}:{int(failed_agent_id)}"
        return bool(self.redis.set(key, "1", nx=True, ex=int(ttl_s)))

    def delete_all(self, key_prefix: str = KEY_JOB):
        """
        Delete all objects under given key prefix.

        Args:
            key_prefix (str): Prefix to delete.
            group (int): Agent group in hierarchy at a level.
            level (int): Agent level in hierarchy.
        """
        keys = list(self.redis.scan_iter(f'{key_prefix}:*'))
        if keys:
            self.redis.delete(*keys)
