"""
SimulatedAgent — thread-safe capacity tracker for baseline schedulers.

Each SimulatedAgent represents one compute node with fixed total capacity.
It tracks currently allocated resources and provides thread-safe
allocate/release operations for concurrent job execution.
"""
import threading

from swarm.models.capacities import Capacities
from swarm.models.data_node import DataNode


class SimulatedAgent:
    """Thread-safe simulated agent that tracks resource allocation."""

    def __init__(self, agent_id: int, capacities: Capacities, dtns: dict[str, DataNode],
                 host: str = "localhost"):
        self.agent_id = agent_id
        self.capacities = capacities          # total capacity
        self.current_alloc = Capacities()     # currently consumed by running jobs
        self.dtns = dtns                      # {dtn_name: DataNode}
        self.host = host                      # remote hostname for distributed mode
        self.lock = threading.Lock()
        self.jobs_completed = 0

    def available(self) -> Capacities:
        """Return currently available capacity (total - allocated)."""
        with self.lock:
            return self.capacities - self.current_alloc

    def can_fit(self, job_capacities: Capacities) -> bool:
        """Check if job fits within available capacity."""
        with self.lock:
            residual = (self.capacities - self.current_alloc) - job_capacities
            return len(residual.negative_fields()) == 0

    def allocate(self, job_capacities: Capacities):
        """Reserve capacity for a job."""
        with self.lock:
            self.current_alloc = self.current_alloc + job_capacities

    def release(self, job_capacities: Capacities):
        """Free capacity after job completion."""
        with self.lock:
            self.current_alloc = self.current_alloc - job_capacities
            self.jobs_completed += 1

    @classmethod
    def from_profile(cls, agent_id: int, profile: dict) -> "SimulatedAgent":
        """Create a SimulatedAgent from an agent_profiles.json entry.

        Args:
            agent_id: integer agent ID
            profile: dict with keys core, ram, disk, gpu, dtns
        """
        cap = Capacities()
        cap._set_fields(
            core=profile.get("core", 0),
            ram=profile.get("ram", 0),
            disk=profile.get("disk", 0),
            gpu=profile.get("gpu", 0),
        )

        dtns = {}
        for dtn_data in profile.get("dtns", []):
            dn = DataNode.from_dict(dtn_data)
            dtns[dn.name] = dn

        # Host field: check grpc.host (from generate_configs), then top-level host
        host = "localhost"
        grpc_cfg = profile.get("grpc", {})
        if isinstance(grpc_cfg, dict) and grpc_cfg.get("host"):
            host = grpc_cfg["host"]
        elif profile.get("host"):
            host = profile["host"]

        return cls(agent_id=agent_id, capacities=cap, dtns=dtns, host=host)
