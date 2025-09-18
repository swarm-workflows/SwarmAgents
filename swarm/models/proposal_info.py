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

import threading
from typing import Dict, List, Optional, Iterable, TypeVar, Callable

from swarm.models.json_field import JSONField


T = TypeVar("T")


def _dedupe_preserve_order(items: Iterable[T]) -> List[T]:
    """Return a list with duplicates removed while preserving first-seen order."""
    seen = set()
    out = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


class ProposalException(Exception):
    """
    Exception for proposal container/operations
    """
    def __init__(self, msg: str):
        assert msg is not None
        super().__init__(f"ProposalException exception: {msg}")


class ProposalInfo(JSONField):
    """
    Proposal payload persisted by the container.

    Fields:
      - p_id: unique proposal id (string)
      - object_id: the job/object this proposal is for (string)
      - prepares: list of agent ids (or messages) that prepared
      - commits: list of agent ids (or messages) that committed
      - agent_id: the proposer / owner of this proposal
      - seed: tie-breaker / priority metric (float)
    """
    def __init__(self, **kwargs):
        self.p_id: Optional[str] = None
        self.object_id: Optional[str] = None
        self.prepares: List = []
        self.commits: List = []
        self.agent_id: Optional[str] = None
        self.seed: float = 0.0
        self._set_fields(**kwargs)

    def _set_fields(self, forgiving: bool = False, **kwargs):
        """
        Universal setter for defined fields only.
        """
        for k, v in kwargs.items():
            try:
                getattr(self, k)  # raises if unknown
                setattr(self, k, v)
            except AttributeError:
                report = (
                    f"Unable to set field {k} of ProposalInfo, no such field available "
                    f"{[k for k in self.__dict__.keys()]}"
                )
                if forgiving:
                    print(report)
                else:
                    raise ProposalException(report)
        return self

    # ----------------------------
    # Merge rules
    # ----------------------------
    def merge(
        self,
        other: "ProposalInfo",
        *,
        list_union: Optional[Dict[str, Callable[[List, List], List]]] = None,
        update_scalars: bool = True,
        update_seed_policy: str = "latest",
    ) -> "ProposalInfo":
        """
        Merge another ProposalInfo into this one.

        - Lists ('prepares', 'commits') are merged by union while preserving order.
          You can override per-field behavior via 'list_union'.
        - Scalar fields:
            - If update_scalars=True:
                - agent_id: take latest non-empty (from 'other') if provided
                - seed: policy controlled by 'update_seed_policy':
                    * 'latest': take other.seed if provided
                    * 'min':    take min(self.seed, other.seed)
                    * 'max':    take max(self.seed, other.seed)
              If update_scalars=False, keep current values.

        Returns self.
        """
        if not other:
            return self

        # Merge lists
        union_strategies = {
            "prepares": lambda a, b: _dedupe_preserve_order([*a, *b]),
            "commits":  lambda a, b: _dedupe_preserve_order([*a, *b]),
        }
        if list_union:
            union_strategies.update(list_union)

        if hasattr(self, "prepares") and hasattr(other, "prepares"):
            self.prepares = union_strategies["prepares"](self.prepares, other.prepares)

        if hasattr(self, "commits") and hasattr(other, "commits"):
            self.commits = union_strategies["commits"](self.commits, other.commits)

        # Scalars
        if update_scalars:
            if other.agent_id:
                self.agent_id = other.agent_id

            if update_seed_policy == "latest":
                # accept latest value if other provided one (0.0 considered provided; change if not desired)
                self.seed = other.seed
            elif update_seed_policy == "min":
                self.seed = min(self.seed, other.seed)
            elif update_seed_policy == "max":
                self.seed = max(self.seed, other.seed)
            else:
                raise ProposalException(f"Unknown update_seed_policy: {update_seed_policy}")

        return self


class ProposalContainer:
    """
    Thread-safe container for proposals.

    Internal structure:
      - proposals_by_pid: Dict[p_id, ProposalInfo]
      - proposals_by_object_id: Dict[object_id, Dict[p_id, ProposalInfo]]

    Guarantees:
      - Single ProposalInfo per (object_id, p_id)
      - O(1) add/get/remove by p_id
      - O(1) get bucket by object_id
      - On duplicate arrivals, entries are merged (union of lists, configurable scalar policy)
    """
    def __init__(self):
        self.lock = threading.RLock()
        self.proposals_by_pid: Dict[str, ProposalInfo] = {}
        self.proposals_by_object_id: Dict[str, Dict[str, ProposalInfo]] = {}

        # Merge behavior defaults (can be made configurable if needed)
        self._update_scalars_default = True
        self._update_seed_policy_default = "latest"  # choose: 'latest' | 'min' | 'max'
        self._list_union_default: Dict[str, Callable[[List, List], List]] = {
            "prepares": lambda a, b: _dedupe_preserve_order([*a, *b]),
            "commits":  lambda a, b: _dedupe_preserve_order([*a, *b]),
        }

    # ----------------------------
    # Internal helpers
    # ----------------------------
    def _ensure_bucket(self, object_id: str) -> Dict[str, ProposalInfo]:
        bucket = self.proposals_by_object_id.get(object_id)
        if bucket is None:
            bucket = {}
            self.proposals_by_object_id[object_id] = bucket
        return bucket

    def _remove_empty_bucket(self, object_id: str):
        bucket = self.proposals_by_object_id.get(object_id)
        if bucket is not None and not bucket:
            self.proposals_by_object_id.pop(object_id, None)

    # ----------------------------
    # Public API
    # ----------------------------
    def add_proposal(
        self,
        proposal: ProposalInfo,
        *,
        update_scalars: Optional[bool] = None,
        update_seed_policy: Optional[str] = None,
        list_union: Optional[Dict[str, Callable[[List, List], List]]] = None,
    ):
        """
        Idempotent upsert with MERGE:
          - If (p_id) exists, merge into existing ProposalInfo using union for lists and selected policy for scalars.
          - Otherwise, insert new.

        Args:
          update_scalars: override default scalar update behavior for this call
          update_seed_policy: 'latest' (default), 'min', or 'max'
          list_union: per-list merge strategy overrides
        """
        if not proposal or not proposal.object_id or not proposal.p_id:
            raise ProposalException("Proposal must have object_id and p_id")

        with self.lock:
            bucket = self._ensure_bucket(proposal.object_id)

            existing = self.proposals_by_pid.get(proposal.p_id)
            if existing:
                # If existing was stored under a different object_id, move it to correct bucket
                if existing.object_id != proposal.object_id:
                    old_bucket = self.proposals_by_object_id.get(existing.object_id)
                    if old_bucket and existing.p_id in old_bucket:
                        del old_bucket[existing.p_id]
                        self._remove_empty_bucket(existing.object_id)
                    bucket[existing.p_id] = existing
                    existing.object_id = proposal.object_id  # keep consistent

                # Merge behavior
                existing.merge(
                    proposal,
                    list_union=(list_union or self._list_union_default),
                    update_scalars=(
                        self._update_scalars_default if update_scalars is None else update_scalars
                    ),
                    update_seed_policy=(update_seed_policy or self._update_seed_policy_default),
                )

                # Maps already point to 'existing'
                self.proposals_by_pid[existing.p_id] = existing
                bucket[existing.p_id] = existing
            else:
                # Insert new
                self.proposals_by_pid[proposal.p_id] = proposal
                bucket[proposal.p_id] = proposal

    def contains(self, object_id: str = None, p_id: str = None) -> bool:
        if object_id is None and p_id is None:
            return False
        with self.lock:
            if p_id is not None and p_id not in self.proposals_by_pid:
                return False
            if object_id is not None and object_id not in self.proposals_by_object_id:
                return False
            if object_id is not None and p_id is not None:
                return p_id in self.proposals_by_object_id.get(object_id, {})
            return True

    def get_proposal(self, object_id: str = None, p_id: str = None) -> Optional[ProposalInfo]:
        with self.lock:
            if p_id is not None and object_id is not None:
                return self.proposals_by_object_id.get(object_id, {}).get(p_id)
            if p_id is not None:
                return self.proposals_by_pid.get(p_id)
            if object_id is not None:
                bucket = self.proposals_by_object_id.get(object_id)
                if bucket:
                    return next(iter(bucket.values()))
            return None

    def get_proposals_by_object_id(self, job_id: str) -> List[ProposalInfo]:
        with self.lock:
            bucket = self.proposals_by_object_id.get(job_id, {})
            return list(bucket.values())

    def size(self) -> int:
        with self.lock:
            return len(self.proposals_by_pid)

    def remove_proposal(self, p_id: str, object_id: Optional[str] = None):
        """
        Remove a single proposal by p_id. If object_id is provided and correct, it's O(1),
        otherwise we resolve via p_id's authoritative mapping.
        """
        with self.lock:
            existing = self.proposals_by_pid.pop(p_id, None)
            if existing is not None:
                bucket = self.proposals_by_object_id.get(existing.object_id)
                if bucket and p_id in bucket:
                    del bucket[p_id]
                    self._remove_empty_bucket(existing.object_id)
                return

            # Fallback if only object_id was correct (should be rare with authoritative p_id path)
            if object_id and object_id in self.proposals_by_object_id:
                bucket = self.proposals_by_object_id[object_id]
                if p_id in bucket:
                    del bucket[p_id]
                    self._remove_empty_bucket(object_id)

    def remove_object(self, object_id: str):
        """
        Remove all proposals for a given object_id.
        """
        with self.lock:
            bucket = self.proposals_by_object_id.pop(object_id, None)
            if bucket:
                for p in bucket.values():
                    self.proposals_by_pid.pop(p.p_id, None)

    def objects(self) -> List[str]:
        with self.lock:
            return list(self.proposals_by_object_id.keys())

    def has_better_proposal(self, proposal: ProposalInfo) -> Optional[ProposalInfo]:
        """
        Return a better proposal for the same object_id (if any), based on:
          - Lower seed wins
          - If equal seed, lexicographically smaller agent_id wins
        """
        if not proposal or not proposal.object_id:
            return None

        with self.lock:
            bucket = self.proposals_by_object_id.get(proposal.object_id, {})
            best = None
            for p in bucket.values():
                if p.p_id == proposal.p_id:
                    continue
                if (p.seed < proposal.seed) or (
                    p.seed == proposal.seed and (p.agent_id or "") < (proposal.agent_id or "")
                ):
                    best = p
                    break
            return best
