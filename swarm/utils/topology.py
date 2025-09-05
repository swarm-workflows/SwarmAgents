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
        self.peers = topo.get("peer_agents", [])
