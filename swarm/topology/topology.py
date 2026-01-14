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


class TopologyType(enum.Enum):
    Ring = enum.auto(),
    Star = enum.auto(),
    Mesh = enum.auto(),
    Hierarchical = enum.auto()

    def __repr__(self):
        return self.name

    def __str__(self):
        return self.name

    @staticmethod
    def from_str(topo: str):
        if topo.lower() == str(TopologyType.Ring).lower():
            return TopologyType.Ring
        if topo.lower() == str(TopologyType.Star).lower():
            return TopologyType.Star
        if topo.lower() == str(TopologyType.Mesh).lower():
            return TopologyType.Mesh
        if topo.lower() == str(TopologyType.Hierarchical).lower():
            return TopologyType.Hierarchical
        return None


class Topology:
    def __init__(self, topo: dict):
        self.type = TopologyType.from_str(topo.get("type"))
        self.parent = topo.get("parent", None)
        self.children = topo.get("children", None)
        self.level = topo.get("level", 0)
        self.group = topo.get("group", 0)
        self.group_size = topo.get("group_size", 0)
        self.group_count = topo.get("group_count", 0)
        self.peers = topo.get("peer_agents", [])
