# consensus/interfaces.py
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
from typing import Protocol, Iterable, Optional, Any
from swarm.models.object import Object

class TopologyRouter(Protocol):
    """Pure routing: knows neighbors/next hops for a given topology."""
    def should_forward(self) -> bool: ...

class ConsensusTransport(Protocol):
    def send(self, dest: int, payload: object) -> None: ...
    def broadcast(self, payload: object) -> None: ...

class ConsensusHost(Protocol):
    # domain introspection
    def get_object(self, object_id: str) -> Optional[Object]: ...
    def is_agreement_achieved(self, object_id: str) -> bool: ...
    def calculate_quorum(self) -> int: ...

    # side effects
    def on_leader_elected(self, obj: Object, proposal_id: str) -> None: ...
    def on_participant_commit(self, obj: Object, leader_id: int, proposal_id: str) -> None: ...

    # utility
    def now(self) -> float: ...
    def log_debug(self, msg: str) -> None: ...
    def log_info(self, msg: str) -> None: ...
    def log_warn(self, msg: str) -> None: ...
