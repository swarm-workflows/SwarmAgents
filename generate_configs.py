import argparse
import copy
import json
import math
import os
import random
import sys
from typing import Dict, List, Optional, Tuple

import yaml

from job_generator import JobGenerator
from swarm.utils.yaml_strict import safe_load as yaml_safe_load_strict


class TopologyError(Exception):
    """A topology that cannot be built as requested.

    Raised rather than printed-and-returned: `run_test.py` and `batch_tests_v2.py` invoke this
    script with `check=True`, so exiting 0 after refusing to write anything told the campaign
    driver the configs were ready. It then launched agents against an empty config directory —
    or, worse, against a previous cell's leftovers.
    """

INSTANCE_FLAVORS = [
    {"name": "small",      "core": 2,  "ram": 8,   "disk": 100,  "gpu": 0},
    {"name": "medium",     "core": 4,  "ram": 16,  "disk": 250,  "gpu": 0},
    {"name": "large",      "core": 8,  "ram": 32,  "disk": 500,  "gpu": 4},
    {"name": "xtralarge",  "core": 16, "ram": 64,  "disk": 1000, "gpu": 4},
    {"name": "xxtralarge", "core": 32, "ram": 128, "disk": 1000, "gpu": 4},
]

DEFAULT_FLAVOR_PERCENTAGES = [0.4, 0.25, 0.15, 0.15, 0.05]

# Quantum backends assignable to agents via --quantum-agents-pct.
# Mix of noisy simulators and hardware-like profiles across architectures
# (CLOPS/fidelity values are representative, not vendor measurements).
QUANTUM_BACKEND_CATALOG = [
    {"name": "aer-sim-64", "arch": "superconducting", "qubits": 64, "clops": 100000,
     "gate_fidelity": 0.999, "error_rate": 0.001, "calibration_downtime_pct": 0.0,
     "simulator": True},
    {"name": "heron-133", "arch": "superconducting", "qubits": 133, "clops": 150000,
     "gate_fidelity": 0.998, "error_rate": 0.002, "calibration_downtime_pct": 0.05,
     "simulator": False},
    {"name": "aqt-ion-32", "arch": "ion-trap", "qubits": 32, "clops": 2000,
     "gate_fidelity": 0.9995, "error_rate": 0.0005, "calibration_downtime_pct": 0.08,
     "simulator": False},
    {"name": "neutral-atom-100", "arch": "neutral-atom", "qubits": 100, "clops": 5000,
     "gate_fidelity": 0.995, "error_rate": 0.005, "calibration_downtime_pct": 0.1,
     "simulator": False},
]


class SwarmConfigGenerator:
    """
    Generate per-agent configs given a base YAML, topology, DB host, and options.
    Supports:
      - mesh and ring grouping via --groups / --group-size
      - legacy defaults when grouping is not specified
      - mapping N agents onto M hosts via agents_per_host
    """
    AGENT_DTNS = "agent_dtns.json"

    def __init__(
        self,
        num_agents: int,
        jobs_per_proposal: int,
        base_config_path: str,
        output_dir: str,
        topology: str,
        db_host: str,
        enable_dtns: bool,
        agents_per_host: int = 1,
        groups: Optional[int] = None,
        group_size: Optional[int] = None,
        hierarchical_level1_agent_type: str = "llm",
        agent_type: str = "resource",
        initial_group_size: Optional[int] = None,
        co_parent_count: int = 1,
        groups_per_coordinator: int = 1,
        quantum_agents_pct: float = 0.0,
        master_fleet_size: Optional[int] = None,
        delegation_policy: Optional[str] = None,
    ):
        self.num_agents = num_agents
        # Fleet size the per-agent draws are made FOR, which may exceed the fleet being written.
        # Flavours are allocated as percentages of fleet size, so with master=None agent 3 gets a
        # 16-core flavour in a 10-agent fleet and a 2-core one in a 30-agent fleet even under the
        # same seed — the scale ladder would then compare different fleets, not one fleet at
        # different sizes. Setting master to the largest rung makes every smaller fleet a strict
        # prefix of it: agent i is identical at every size.
        self.master_fleet_size = int(master_fleet_size or num_agents)
        if self.master_fleet_size < num_agents:
            raise ValueError(
                f"master_fleet_size ({self.master_fleet_size}) cannot be smaller than "
                f"num_agents ({num_agents})")
        self.jobs_per_proposal = jobs_per_proposal
        self.base_config_path = base_config_path
        self.output_dir = output_dir
        self.base_config = self.load_base_config()
        self.topology = topology
        self.db_host = db_host
        self.enable_dtns = enable_dtns
        self.agents_per_host = agents_per_host

        # grouping controls (only used for mesh/ring)
        self.req_groups = groups
        self.req_group_size = group_size

        # hierarchical level 1 agent type (llm or resource)
        self.hierarchical_level1_agent_type = hierarchical_level1_agent_type

        # default agent type for all non-hierarchical level 1 agents
        self.agent_type = agent_type

        # initial group size for dynamic agent addition (if different from num_agents)
        self.initial_group_size = initial_group_size

        # How many child groups one Level-1 coordinator exclusively parents. 1 is the shipped
        # 1:1 mapping, under which no coordinator has a routing decision to make.
        self.groups_per_coordinator = max(1, int(groups_per_coordinator or 1))

        # co-parent count for hierarchical topology shared parenting
        self.co_parent_count = co_parent_count

        # fraction of agents that own a quantum backend (0.0 = classical only)
        self.quantum_agents_pct = quantum_agents_pct

        # Which plane picks the child group a coordinator delegates to (E4's arms). Applied to
        # the base config every per-agent file is copied from. Before this flag the arm was
        # switched by hand-editing config_swarm_multi.yml on the controller, so nothing in a
        # run directory recorded which arm produced it, and the two arms of a comparison could
        # differ by an edit nobody wrote down.
        if delegation_policy:
            self.base_config.setdefault("delegation", {})["policy"] = delegation_policy
            print(f"delegation.policy = {delegation_policy} (from --delegation-policy)")

        # legacy ring helper (used when no grouping flags provided for ring)
        self.rings_default = self._create_default_rings()
        self.agent_dtns_map: Dict[str, List[dict]] = self._load_agent_dtns(path=self.AGENT_DTNS)

    def assign_quantum_backends(self) -> Dict[int, dict]:
        """
        Assign a quantum backend (round-robin from the catalog) to a random
        subset of agents sized by quantum_agents_pct. Hierarchical level-1
        coordinators are never assigned backends (leaf agents execute jobs).
        """
        if self.quantum_agents_pct <= 0:
            return {}
        # Drawn over the master fleet and filtered, for the same prefix-stability reason as
        # flavours: agent i either has a backend at every fleet size or at none.
        master = self.master_fleet_size
        count = max(1, round(self.quantum_agents_pct * master))
        chosen = random.sample(range(1, master + 1), min(count, master))
        return {
            agent_id: copy.deepcopy(QUANTUM_BACKEND_CATALOG[i % len(QUANTUM_BACKEND_CATALOG)])
            for i, agent_id in enumerate(sorted(chosen))
            if agent_id <= self.num_agents
        }

    # -----------------------------
    # Flavor assignment
    # -----------------------------
    def assign_flavors(self, percentages):
        if len(percentages) != len(INSTANCE_FLAVORS):
            raise ValueError(
                f"Expected {len(INSTANCE_FLAVORS)} flavor percentages, got {len(percentages)}"
            )
        total = sum(percentages)
        if abs(total - 1.0) > 1e-6:
            raise ValueError("Flavor percentages must sum to 1.0")

        # Allocate over the MASTER fleet, then keep this fleet's prefix — see master_fleet_size.
        total_agents = self.master_fleet_size
        raw_counts = [p * total_agents for p in percentages]
        counts = [int(x) for x in raw_counts]
        assigned = sum(counts)

        remainder = total_agents - assigned
        if remainder > 0:
            fracs = [(raw_counts[i] - counts[i], i) for i in range(len(counts))]
            fracs.sort(reverse=True)
            for _, idx in fracs[:remainder]:
                counts[idx] += 1

        agent_flavors = []
        for idx, cnt in enumerate(counts):
            agent_flavors.extend([INSTANCE_FLAVORS[idx]] * cnt)

        random.shuffle(agent_flavors)
        return agent_flavors[:self.num_agents]

    # -----------------------------
    # Base config
    # -----------------------------
    def load_base_config(self):
        with open(self.base_config_path, "r") as file:
            return yaml_safe_load_strict(file)

    # -----------------------------
    # Defaults for ring (legacy)
    # -----------------------------
    def _create_default_rings(self) -> List[List[int]]:
        """
        Legacy default: Create rings of up to 5 agents; always include agent 1 in the first ring.
        Works for num_agents < 5 as well.
        """
        agents = list(range(1, self.num_agents + 1))
        if not agents:
            return []

        first_ring = agents[:min(5, len(agents))]
        rings = [first_ring]

        i = len(first_ring)
        while i < self.num_agents:
            ring = agents[i:i + 5]
            if len(ring) < 5:
                rings.append(ring)
                break
            rings.append(ring)
            i += 5
        return rings

    def _print_rings(self, rings: List[List[int]], label: str = "Ring Topology"):
        print(f"\n{label}:")
        for i, ring in enumerate(rings):
            if not ring:
                ring_display = "(empty)"
            else:
                ring_display = " ⟶ ".join(map(str, ring)) + f" ⟶ {ring[0]}"
            print(f"Ring {i + 1}: {ring_display}")

    # -----------------------------
    # Grouping helpers
    # -----------------------------
    def _partition_agents(self, groups: Optional[int], group_size: Optional[int]) -> List[List[int]]:
        """
        Partition agents [1..N] into groups according to requested groups or group_size.
        If both are None: return a single group containing all agents.
        If both are provided: validate that groups * group_size == num_agents (or last group smaller).
        We keep the last group smaller if needed, but ensure every agent is placed.
        """
        agents = list(range(1, self.num_agents + 1))
        if not agents:
            return []

        # No grouping requested => single group with all agents
        if groups is None and group_size is None:
            return [agents]

        # If only group_size provided
        if groups is None and group_size is not None:
            if group_size <= 0:
                raise ValueError("--group-size must be > 0")
            out = []
            for i in range(0, self.num_agents, group_size):
                out.append(agents[i:i + group_size])
            return out

        # If only groups provided
        if groups is not None and group_size is None:
            if groups <= 0:
                raise ValueError("--groups must be > 0")
            base_size = self.num_agents // groups
            rem = self.num_agents % groups
            out = []
            idx = 0
            for g in range(groups):
                sz = base_size + (1 if g < rem else 0)
                out.append(agents[idx:idx + sz])
                idx += sz
            return out

        # Both provided
        if groups is not None and group_size is not None:
            if groups <= 0 or group_size <= 0:
                raise ValueError("--groups and --group-size must be > 0")
            out = []
            idx = 0
            for _ in range(groups):
                out.append(agents[idx:idx + group_size])
                idx += group_size
                if idx >= self.num_agents:
                    break
            # If there are stragglers (due to mismatch), put them in a final small group
            if idx < self.num_agents:
                out.append(agents[idx:])
            # sanity: must cover all agents exactly once
            flat = [a for grp in out for a in grp]
            if len(set(flat)) != self.num_agents:
                raise ValueError("Grouping produced duplicate or missing agents; check --groups/--group-size.")
            return out

        # Should not reach here
        return [agents]

    # -----------------------------
    # Topology generators (mesh/ring with grouping)
    # -----------------------------
    def _build_mesh_topology(self, groups: List[List[int]]) -> Dict[int, dict]:
        agent_topo: Dict[int, dict] = {}
        for gid, group in enumerate(groups):
            for a in group:
                peers = [x for x in group if x != a]
                agent_topo[a] = {
                    "peers": peers,
                    "parent": None,
                    "children": None,
                    "group": gid,
                    "level": 0,
                    "group_size": len(group),
                    "group_count": len(groups),
                }
        return agent_topo

    from typing import Dict, List, Optional

    def _build_ring_topology(
            self,
            rings: List[List[int]],
            cross_rings: bool = True,
            cross_ring_indices: Optional[List[int]] = [0],
    ) -> Dict[int, dict]:
        """
        Build ring topologies.

        Args:
            rings: List of rings, each ring is a list of agent ids in order.
            cross_rings: If True, connect chosen same-index nodes across adjacent rings (with wrap).
            cross_ring_indices: Which within-ring indices to cross-link. If None and cross_rings=True,
                                link *all* valid indices across rings. Example: [0] links only 1-6-11-1
                                in your 3-ring, 5-per-ring case.

        Behavior:
          - Always builds intra-ring neighbors (prev/next).
          - If cross_rings=True, adds cross-ring peers at the specified indices.
          - When cross_rings=True, sets group=0 for all nodes, group_size=total agents, group_count=1.
            Otherwise, each ring has its own group id and size; group_count=len(rings).
        """
        agent_topo: Dict[int, dict] = {}

        def _meta(group_id: int, group_size: int, group_count: int) -> dict:
            return {
                "parent": None,
                "children": None,
                "group": group_id,
                "level": 0,
                "group_size": group_size,
                "group_count": group_count,
            }

        total_agents = sum(len(r) for r in rings)
        num_groups = 1 if cross_rings else len(rings)

        # 1) Intra-ring links
        for gid, ring in enumerate(rings):
            n = len(ring)
            if n == 0:
                continue
            if n == 1:
                node = ring[0]
                agent_topo[node] = {
                    "peers": [],
                    **_meta(0 if cross_rings else gid,
                            total_agents if cross_rings else 1,
                            num_groups),
                }
                continue

            for k in range(n):
                cur = ring[k]
                nxt = ring[(k + 1) % n]
                prv = ring[(k - 1) % n]
                existing = agent_topo.get(cur, {"peers": []})
                peers = set(existing.get("peers", []))
                peers.update((nxt, prv))
                agent_topo[cur] = {
                    "peers": sorted(peers),
                    **_meta(0 if cross_rings else gid,
                            total_agents if cross_rings else n,
                            num_groups),
                }

        # 2) Cross-ring links (same-index across adjacent rings, with wrap)
        if cross_rings and len(rings) > 1:
            m = len(rings)

            # Determine which indices we will cross-link
            if cross_ring_indices is None:
                # Link all indices that are valid across each adjacent ring pair
                # We'll compute per-pair max and intersect on the fly
                per_pair_indices = [None] * m  # None means "0..min_len-1"
            else:
                # Use the given indices for every adjacent pair (will skip if index out of range)
                per_pair_indices = [set(cross_ring_indices)] * m

            for i in range(m):
                j = (i + 1) % m  # adjacent ring (wrap)
                ring_i, ring_j = rings[i], rings[j]
                if per_pair_indices[i] is None:
                    max_idx = min(len(ring_i), len(ring_j))
                    indices = range(max_idx)
                else:
                    indices = (idx for idx in per_pair_indices[i])

                for idx in indices:
                    if idx < len(ring_i) and idx < len(ring_j):
                        a, b = ring_i[idx], ring_j[idx]
                        # add bidirectional cross peers
                        ai = agent_topo.get(a)
                        bi = agent_topo.get(b)
                        if ai is not None:
                            ai_peers = set(ai["peers"])
                            ai_peers.add(b)
                            ai["peers"] = sorted(ai_peers)
                        if bi is not None:
                            bi_peers = set(bi["peers"])
                            bi_peers.add(a)
                            bi["peers"] = sorted(bi_peers)

            # Normalize group metadata to a single group when cross-ring is enabled
            for node in agent_topo:
                agent_topo[node]["group"] = 0
                agent_topo[node]["group_size"] = total_agents
                agent_topo[node]["group_count"] = 1

        return agent_topo

    # -----------------------------
    # DTN helpers
    # -----------------------------
    def _load_agent_dtns(self, path: str) -> Dict[str, List[dict]]:
        """Reuse a previous run's DTN assignment if one is lying around.

        This is a REPRODUCIBILITY TRAP and the warning is the point: reusing the file takes a
        different code path (`adjust_scores` rather than `assign_agent_dtns`) which consumes the
        RNG differently, so `--seed N` from a dirty directory does not reproduce `--seed N` from
        a clean one. A reproducibility check run without deleting this file disagrees with
        itself. `run_test.py` deletes it before every generation.
        """
        if path and os.path.exists(path):
            with open(path, "r") as f:
                data = json.load(f)
            print(f"WARNING: reusing existing {path} ({len(data)} agents). DTN assignments are "
                  f"NOT being drawn fresh, and --seed will not reproduce a clean-state run. "
                  f"Delete {path} (and agent_profiles.json) first if that is not intended.")
            return {str(k): v for k, v in data.items()}
        return {}

    @staticmethod
    def random_capacity(min_val, max_val):
        return random.randint(min_val, max_val)

    def generate_global_dtn_pool(self, total_count=10):
        pool = []
        for i in range(1, total_count + 1):
            pool.append({
                "name": f"dtn{i}",
                "ip": f"192.168.100.{i}",
                "user": f"dtn_user{i}",
                "base_connectivity_score": round(random.uniform(0.6, 0.95), 2)
            })
        return pool

    def adjust_scores(self, dtns: List[dict]):
        for d in dtns:
            adjusted_score = min(1.0, max(0.0, d.get("connectivity_score", 0.8) + random.uniform(-0.05, 0.05)))
            d["connectivity_score"] = round(adjusted_score, 2)
        return dtns

    def assign_agent_dtns(self, pool, min_dtns=1, max_dtns=4):
        count = random.randint(min_dtns, max_dtns)
        selected = random.sample(pool, min(count, len(pool)))
        agent_dtns = []
        for d in selected:
            adjusted_score = min(1.0, max(0.0, d["base_connectivity_score"] + random.uniform(-0.05, 0.05)))
            agent_dtns.append({
                "name": d["name"],
                "ip": d["ip"],
                "user": d["user"],
                "connectivity_score": round(adjusted_score, 2)
            })
        return agent_dtns

    # -----------------------------
    # Main generation
    # -----------------------------
    def get_config_prefix(self):
        filename = os.path.basename(self.base_config_path)
        prefix, _ = os.path.splitext(filename)
        return prefix

    def generate_configs(
        self,
        flavor_percentages,
        agent_hosts: Optional[List[str]],
        agent_sites: Optional[List[str]] = None,
        save_agent_profiles_path: str = "agent_profiles.json",
    ):
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        # Build peer map by topology (respect grouping for mesh & ring)
        agent_topo: Dict[int, dict] = {}

        if self.topology == "ring":
            if self.req_groups is None and self.req_group_size is None:
                # Legacy default behavior
                self._print_rings(self.rings_default, "Ring Topology (default groups of up to 5)")
                agent_topo = self._build_ring_topology(self.rings_default)
            else:
                rings = self._partition_agents(self.req_groups, self.req_group_size)
                self._print_rings(rings, "Ring Topology (grouped)")
                agent_topo = self._build_ring_topology(rings)

        elif self.topology == "mesh":
            if self.req_groups is None and self.req_group_size is None:
                # one big mesh
                groups = [list(range(1, self.num_agents + 1))]
            else:
                groups = self._partition_agents(self.req_groups, self.req_group_size)
            print("\nMesh groups:", groups)
            agent_topo = self._build_mesh_topology(groups)

        elif self.topology == "star":
            # unchanged from your original
            core_agents = [a for a in range(1, min(6, self.num_agents + 1))]
            agent_peers = {}
            for i, cur in enumerate(core_agents):
                nxt = core_agents[(i + 1) % len(core_agents)] if core_agents else None
                prv = core_agents[(i - 1) % len(core_agents)] if core_agents else None
                agent_peers[cur] = [p for p in (prv, nxt) if p and p != cur]
            for leaf_id in range(len(core_agents) + 1, self.num_agents + 1):
                primary = core_agents[(leaf_id - 1) % len(core_agents)]
                secondary = core_agents[(leaf_id) % len(core_agents)]
                agent_peers.setdefault(leaf_id, [])
                agent_peers[leaf_id].extend([primary, secondary])
                agent_peers[primary].append(leaf_id)
                agent_peers[secondary].append(leaf_id)
            for aid, peers in agent_peers.items():
                agent_topo[aid] = {
                    "peers": sorted(set(peers)),
                    "parent": None,
                    "children": None,
                    "group": 0,
                    "level": 0
                }

        elif self.topology == "hierarchical":
            if self.num_agents < 30:
                raise TopologyError(
                    f"Minimum number of agents for hierarchical topology is 30 "
                    f"(got {self.num_agents})")
            agent_topo = {}

            # Determine hierarchy structure based on agent count
            if self.num_agents == 30:
                # Two-level: 25 Level-0 + 5 Level-1 = 30
                num_groups = 5
                group_size = 5
                num_super_groups = 0  # No Level 2
                level_1_base = 26

            elif self.num_agents == 60:
                # Two-level: 50 Level-0 (10 groups of 5) + 10 Level-1 = 60
                num_groups = 10
                group_size = 5
                num_super_groups = 0  # No Level 2
                level_1_base = 51

            elif self.num_agents == 90:
                # Two-level: 81 Level-0 (9 groups of 9) + 9 Level-1 = 90.
                # The evaluation plan has named Hier-90 since it was written (E2's "9 groups"),
                # but there was no preset for it: 90 fell into the `<= 110` branch below, which
                # builds a 110-agent hierarchy and then had ids 91-110 dropped when configs were
                # written — a fleet of 90 leaves with no coordinators at all.
                num_groups = 9
                group_size = 9
                num_super_groups = 0  # No Level 2
                level_1_base = 82

            elif self.num_agents == 100:
                # Three-level: Level-0 80 - 16 groups of 5
                # Level-1: 16 (81-96) - 4 groups of 4
                # Level-2: 4 (97-100) - 1 group of 4
                num_super_groups = 4
                groups_per_super_group = 4
                num_groups = num_super_groups * groups_per_super_group  # 16 groups
                group_size = 5
                super_group_size = 4
                level_1_base = 81
                level_2_base = 97

            elif self.num_agents <= 110:
                # Two-level: 100 Level-0 + 10 Level-1 = 110
                num_groups = 10
                group_size = 10
                num_super_groups = 0  # No Level 2
                level_1_base = 101

            elif self.num_agents == 120:
                # Two-level: 100 Level-0 (20 groups of 5) + 20 Level-1 = 120
                num_groups = 20
                group_size = 5
                num_super_groups = 0  # No Level 2
                level_1_base = 101

            elif self.num_agents == 250:
                # Two-level: 225 Level-0 + 25 Level-1 = 250
                num_groups = 25
                group_size = 9
                num_super_groups = 0  # No Level 2
                level_1_base = 226

            elif self.num_agents == 270:
                # Two-level: 243 Level-0 (27 groups of 9) + 27 Level-1 = 270.
                # The top rung of the plan's 30 / 90 / 270 ladder: same group size as Hier-90
                # with 3x the groups, so scale moves the group COUNT and not the group shape.
                num_groups = 27
                group_size = 9
                num_super_groups = 0  # No Level 2
                level_1_base = 244

            elif self.num_agents == 990:
                # Three-level: Level-0 880 - 88 groups of 10
                # Level-1: 88 (881-968) - 22 super-groups of 4
                # Level-2: 22 (969-990) - 1 group of 22
                num_super_groups = 22
                groups_per_super_group = 4
                super_group_size = 22
                num_groups = num_super_groups * groups_per_super_group  # 88 groups
                group_size = 10
                level_1_base = 881
                level_2_base = 969

            elif self.num_agents == 1000:
                # Three-level: Level-0 900 - 90 groups of 10
                # Level-1: 90 (901-990) - 10 groups of 9
                # Level-2: 10 (991-1000) - 1 group of 10
                num_super_groups = 10
                groups_per_super_group = 9
                super_group_size = 10
                num_groups = num_super_groups * groups_per_super_group  # 90 groups
                group_size = 10
                level_1_base = 901
                level_2_base = 991

            else:
                raise TopologyError(
                    f"Hierarchical topology currently supports 30, 60, 90, 100, 110, 120, 250, "
                    f"270, 990, or 1000 agents (got {self.num_agents})")

            # How many child groups one coordinator exclusively parents. G=1 is the shipped
            # 1:1 mapping and takes the original code path untouched. G>1 is what gives a
            # coordinator a delegation decision to make at all: with one group there is no
            # routing choice, so the bandit returns its only arm and LLM delegation
            # short-circuits, and --co-parents does not help (leadership goes to the lowest-ID
            # live co-parent, concentrating groups on one coordinator rather than spreading a
            # choice to each). See docs/FGCS_EVAL_PLAN.md section 0.6.
            # Clamped to the groups that exist: asking for more per coordinator than there are
            # simply means one coordinator parents them all, and reporting the requested number
            # instead of the real one made the log line say "1 coordinator x 99 groups" for a
            # fleet with 5.
            G = max(1, min(int(self.groups_per_coordinator), num_groups))
            if G > 1 and num_super_groups > 0:
                raise TopologyError(
                    f"--groups-per-coordinator {G} is only supported for two-level hierarchies "
                    f"(30, 60, 90, 110, 120, 250, 270 agents); {self.num_agents} agents builds "
                    f"a three-level hierarchy, whose super-groups are sized in Level-1 agents "
                    f"and would need restructuring too.")
            num_coords = math.ceil(num_groups / G)

            # Coordinator slots freed by the larger fan-out go back to Level 0, so the fleet
            # still has exactly num_agents agents and run_test's 1..N id range stays valid.
            # Groups then differ in size by at most one; each agent reports its own group's
            # size. At G=1 this is skipped entirely, so the presets are reproduced byte for
            # byte rather than recomputed.
            if G > 1:
                level_0_total = self.num_agents - num_coords
                if level_0_total < num_groups:
                    raise TopologyError(
                        f"--groups-per-coordinator {G} leaves {level_0_total} Level-0 agents "
                        f"for {num_groups} groups; not enough to fill them.")
                base, remainder = divmod(level_0_total, num_groups)
                group_sizes = [base + (1 if i < remainder else 0) for i in range(num_groups)]
                level_1_base = level_0_total + 1
                print(f"Hierarchical fan-out: {num_coords} coordinator(s) x {G} group(s) each, "
                      f"{level_0_total} Level-0 agents in groups of {sorted(set(group_sizes))}")
            else:
                group_sizes = [group_size] * num_groups

            group_starts = []
            _next = 1
            for size in group_sizes:
                group_starts.append(_next)
                _next += size

            # Build co-parent assignment map (circular round-robin)
            K = self.co_parent_count
            level_1_agents = [level_1_base + c for c in range(num_coords)]

            if num_super_groups > 0:
                # Three-level: apply circular assignment within each super-group
                co_parent_map = {}
                for sg in range(num_super_groups):
                    sg_start = sg * groups_per_super_group
                    sg_end = sg_start + groups_per_super_group
                    sg_agents = level_1_agents[sg_start:sg_end]
                    for i, group in enumerate(range(sg_start, sg_end)):
                        parents = []
                        for k in range(K):
                            parent_idx = (i + k) % len(sg_agents)
                            parents.append(sg_agents[parent_idx])
                        co_parent_map[group] = sorted(parents)
            else:
                # Two-level: circular assignment across all Level-1 agents, starting from the
                # coordinator that owns the group. At G=1 `group // G == group`, so this is the
                # original assignment; at G>1 a coordinator's own G groups list it first and
                # the extra co-parents walk outward from there.
                co_parent_map = {}
                for group in range(num_groups):
                    owner_idx = group // G
                    parents = []
                    for k in range(min(K, len(level_1_agents))):
                        parent_idx = (owner_idx + k) % len(level_1_agents)
                        parents.append(level_1_agents[parent_idx])
                    co_parent_map[group] = sorted(set(parents))

            # Level 0 (leaf/worker agents)
            for group in range(num_groups):
                start = group_starts[group]
                end = start + group_sizes[group]

                # Primary parent is lowest-ID co-parent (backward compat)
                parent_id = co_parent_map[group][0]

                # For 3-level hierarchy, determine super-group
                super_group = group // groups_per_super_group if num_super_groups > 0 else 0

                for agent_id in range(start, end):
                    peers = [a for a in range(start, end) if a != agent_id]
                    topo_entry = {
                        "peers": peers,
                        "parent": parent_id,
                        "children": None,
                        "group": group,
                        "level": 0,
                        "group_size": group_sizes[group],
                        "group_count": num_groups,
                        "super_group": super_group
                    }
                    if K > 1:
                        topo_entry["co_parents"] = co_parent_map[group]
                    agent_topo[agent_id] = topo_entry

            # Level 1 (group coordinators). Iterates COORDINATORS, not groups: with
            # --groups-per-coordinator G there are ceil(num_groups / G) of them, and each one
            # exclusively parents groups [c*G, (c+1)*G). At G=1 the two are the same thing and
            # `group` below is the coordinator's own single group, as before.
            for coord_idx in range(num_coords):
                group = coord_idx * G          # this coordinator's first (primary) group
                parent_id = level_1_base + coord_idx

                if num_super_groups > 0:
                    # Three-level hierarchy: Level-1 coordinators form groups within super-groups
                    super_group = group // groups_per_super_group
                    super_group_start = super_group * groups_per_super_group
                    super_group_end = super_group_start + groups_per_super_group

                    # Peers are other Level-1 coordinators in same super-group
                    peers = [level_1_base + i for i in range(super_group_start, super_group_end) if i != group]
                    level_1_parent = level_2_base + super_group
                else:
                    # Two-level hierarchy: Level-1 coordinators form flat mesh
                    peers = [level_1_base + i for i in range(num_coords) if i != coord_idx]
                    level_1_parent = None

                # Level-1 group metadata should reflect the super-group (not the child group id)
                # For 2-level hierarchies, all Level-1 agents are in the same group (0)
                l1_group = super_group if num_super_groups > 0 else 0
                l1_group_count = num_super_groups if num_super_groups > 0 else 1
                # Level 1 group_size should be the number of Level 1 coordinators in the peer group
                l1_group_size = groups_per_super_group if num_super_groups > 0 else num_coords

                # Determine which groups this agent co-parents
                my_groups = {g: co_parent_map[g] for g in range(num_groups) if parent_id in co_parent_map[g]}

                topo_entry = {
                    "peers": peers,
                    "parent": level_1_parent,
                    "children": sorted(my_groups.keys()),
                    "group": l1_group,
                    "level": 1,
                    "group_size": l1_group_size,
                    "group_count": l1_group_count,
                    "super_group": super_group if num_super_groups > 0 else 0,
                    "primary_group": group,
                }
                if K > 1:
                    topo_entry["co_parent_groups"] = {g: my_groups[g] for g in sorted(my_groups.keys())}
                agent_topo[parent_id] = topo_entry

            # Level 2 (super-coordinators) - only for three-level hierarchy
            if num_super_groups > 0:
                for super_group in range(num_super_groups):
                    super_coord_id = level_2_base + super_group

                    # Peers are other Level-2 super-coordinators
                    peers = [level_2_base + i for i in range(num_super_groups) if i != super_group]

                    # Children is the Level 1 group (super_group) managed by this Level 2 agent
                    children = [super_group]

                    agent_topo[super_coord_id] = {
                        "peers": peers,
                        "parent": None,
                        "children": children,
                        "group": 0,
                        "level": 2,
                        "group_size": super_group_size,
                        "group_count": 1,
                        "super_group": super_group
                    }

            # The presets above hardcode a fleet size each, and the `<= 110` branch accepts a
            # RANGE while building a 110-agent hierarchy. Ask for 90 and the topology places
            # coordinators at ids 101-110, but only ids 1..N get config files written — so a
            # Hier-90 run came out as 90 leaf agents, ZERO coordinators, and every `parent`
            # pointing at an agent that does not exist. No delegation happens at all, and
            # nothing says so. Refuse instead.
            if len(agent_topo) != self.num_agents:
                supported = "30, 60, 90, 100, 110, 120, 250, 270, 990, 1000"
                raise TopologyError(
                    f"Hierarchical topology for {self.num_agents} agents would need "
                    f"{len(agent_topo)} agents ({num_groups} groups of {group_sizes[0]}"
                    f"{'-' + str(group_sizes[-1]) if len(set(group_sizes)) > 1 else ''} plus "
                    f"{num_coords} coordinator(s)"
                    f"{' plus ' + str(num_super_groups) + ' super-coordinators' if num_super_groups > 0 else ''}"
                    f"). Only ids 1..{self.num_agents} would be written, silently dropping the "
                    f"rest — a fleet with no coordinators if the drop reaches Level 1. Use one "
                    f"of the supported sizes: {supported}.")

        else:
            # default to full mesh (backward compatible)
            for i in range(1, self.num_agents + 1):
                peers = [j for j in range(1, self.num_agents + 1) if j != i]
                agent_topo[i] = {
                    "peers": peers,
                    "parent": None,
                    "children": None,
                    "group": 0,
                    "level": 0,
                    "group_size": self.num_agents,
                    "group_count": 1
                }

        config_prefix = self.get_config_prefix()

        # DTN pool (once), or use saved map
        if self.enable_dtns:
            dtn_pool = None if self.agent_dtns_map else self.generate_global_dtn_pool(total_count=10)
        else:
            dtn_pool = None

        # Flavors
        if flavor_percentages is None:
            flavor_percentages = DEFAULT_FLAVOR_PERCENTAGES
        agent_flavors = self.assign_flavors(flavor_percentages)

        # Quantum backends (subset of agents when --quantum-agents-pct > 0)
        quantum_backends = self.assign_quantum_backends()

        agent_profiles = {}

        if agent_hosts:
            host_count = len(agent_hosts)
            if host_count * self.agents_per_host < self.num_agents:
                raise ValueError(
                    f"Not enough hosts ({host_count}) for {self.num_agents} agents "
                    f"with {self.agents_per_host} per host"
                )

        for agent_id in range(1, self.num_agents + 1):
            config = copy.deepcopy(self.base_config)
            if agent_hosts:
                host_idx = (agent_id - 1) // self.agents_per_host
                host = agent_hosts[host_idx]
                config['grpc']['host'] = host
                # Site label parallels the hosts file (one site per host line), so all
                # agents on the same host share a site. Enables topology-aware Snow sampling.
                if agent_sites and host_idx < len(agent_sites):
                    config['site'] = agent_sites[host_idx]
                if self.agents_per_host > 1:
                    config['grpc']['port'] += agent_id
            else:
                config['grpc']['port'] += agent_id

            # DTNs
            if self.enable_dtns:
                if dtn_pool is not None:
                    config["dtns"] = self.assign_agent_dtns(dtn_pool, min_dtns=1, max_dtns=4)
                    self.agent_dtns_map[str(agent_id)] = config["dtns"]
                else:
                    existing = self.agent_dtns_map.get(str(agent_id), [])
                    config["dtns"] = self.adjust_scores(existing)

            # Capacities from flavor
            flavor = agent_flavors[agent_id - 1]
            caps = config.setdefault('capacities', {})
            caps['core'] = flavor['core']
            caps['gpu'] = flavor['gpu']
            caps['ram'] = flavor['ram']
            caps['disk'] = flavor['disk']

            # DB + topology + runtime
            config.setdefault("redis", {})
            config["redis"]["host"] = self.db_host
            topo = agent_topo.get(agent_id, {"peers": [], "parent": None, "children": None, "group": 0, "level": 0})

            # Quantum backend (execution-capable agents only, never coordinators);
            # mirror qubit count into capacities for allocation arithmetic
            backend = quantum_backends.get(agent_id)
            if backend and not topo.get("children"):
                config["quantum_backend"] = backend
                caps['qubits'] = backend["qubits"]

            # Use initial_group_size for initial agents if specified (for dynamic agent addition)
            group_size_to_use = topo["group_size"]
            if self.initial_group_size is not None and agent_id <= self.initial_group_size:
                group_size_to_use = self.initial_group_size

            config["topology"] = {
                "peer_agents": topo["peers"],
                "type": self.topology,
                "parent": topo["parent"],
                "children": topo["children"],
                "level": topo["level"],
                "group": topo["group"],
                "group_size": group_size_to_use,
                "group_count": topo["group_count"],
                "co_parents": topo.get("co_parents", None),
                "co_parent_groups": topo.get("co_parent_groups", None),
                "primary_group": topo.get("primary_group", None),
            }
            config.setdefault("runtime", {})
            config["runtime"]["total_agents"] = self.num_agents
            config["runtime"]["jobs_per_proposal"] = self.jobs_per_proposal

            # Set agent type based on topology level (for hierarchical)
            if self.topology == "hierarchical" and topo["level"] == 1:
                config["agent_type"] = self.hierarchical_level1_agent_type
                #print(f"Settting config type: {config['agent_type']}")
            else:
                config["agent_type"] = self.agent_type

            # Write file
            config_file_path = os.path.join(self.output_dir, f"{config_prefix}_{agent_id}.yml")
            with open(config_file_path, "w") as f:
                yaml.safe_dump(config, f, default_flow_style=False, sort_keys=False)

            # Profile for jobs
            dtns = config.get("dtns", [])
            agent_profiles[str(agent_id)] = {
                "core": caps['core'],
                "ram": caps['ram'],
                "disk": caps['disk'],
                "gpu": caps['gpu'],
                "qubits": caps.get('qubits', 0),
                "quantum_backend": config.get("quantum_backend"),
                "dtns": dtns,
                "grpc": {
                    "host": config.get('grpc', {}).get('host', 'localhost'),
                    "port": config.get('grpc', {}).get('port', 50051),
                },
            }

        # Persist DTN assignments (if any)
        if self.enable_dtns:
            with open(self.AGENT_DTNS, 'w') as f:
                json.dump(self.agent_dtns_map, f, indent=2)

        # Save agent profiles
        if save_agent_profiles_path:
            with open(save_agent_profiles_path, "w") as f:
                json.dump(agent_profiles, f, indent=2)

        print(f"\nGenerated {self.num_agents} config files in {self.output_dir}")


def load_agent_hosts(path: str) -> List[str]:
    """
    Read HOSTS ONLY (one per line). May be fewer than num_agents.
    Agents will be mapped to hosts round-robin (agents_per_host controls grouping per host).
    """
    with open(path, "r") as f:
        hosts = [line.strip() for line in f if line.strip()]
    if not hosts:
        raise ValueError("No hosts found in agent hosts file")
    return hosts


def load_agent_sites(path: str) -> List[str]:
    """
    Read SITE labels (one per line), parallel to the agent hosts file: line i is the
    site of host i. Used for topology-aware (locality-weighted) Snow sampling.
    """
    with open(path, "r") as f:
        sites = [line.strip() for line in f if line.strip()]
    if not sites:
        raise ValueError("No sites found in agent sites file")
    return sites


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate agent configuration files.")
    parser.add_argument("num_agents", type=int, help="Number of agents to generate configurations for.")
    parser.add_argument("jobs_per_proposal", type=int, help="Number of Jobs per proposal.")
    parser.add_argument("base_config_file", type=str, help="Path to the base configuration YAML file.")
    parser.add_argument("output_dir", type=str, help="Directory where generated configs should be saved.")
    parser.add_argument("topology", type=str, default="mesh",
                        help="Topology: mesh | ring | star | hierarchical")
    parser.add_argument("database", type=str, help="Database host")
    parser.add_argument("job_cnt", type=int, help="Job Count")
    parser.add_argument("--dtns", action="store_true", help="Enable DTNs")
    parser.add_argument("--flavor-percentages", nargs='*', type=float, metavar='PERCENT',
                        help="Percentages for small, medium, large, xtralarge, xxtralarge (e.g. 0.4 0.25 0.15 0.15 0.05)")
    parser.add_argument("--agent-hosts-file", type=str, help="Path to file with agent hosts (one per line)")
    parser.add_argument("--agent-sites-file", type=str,
                        help="Path to file with site labels (one per line), parallel to the hosts file. "
                             "Enables topology-aware Snow sampling (each host's agents get that site).")
    parser.add_argument("--agents-per-host", type=int, default=1,
                        help="Number of agents per host (for grpc.host assignment)")

    # NEW: grouping controls for mesh/ring
    parser.add_argument("--groups", type=int, default=None,
                        help="Number of groups for mesh/ring (independent sub-topologies)")
    parser.add_argument("--group-size", type=int, default=None,
                        help="Group size for mesh/ring (independent sub-topologies)")

    # Hierarchical topology control
    parser.add_argument("--hierarchical-level1-agent-type", type=str,
                        choices=["llm", "resource"], default="llm",
                        help="Agent type for level 1 (parent) agents in hierarchical topology (default: llm)")

    # Agent type control (for all non-hierarchical-level-1 agents)
    parser.add_argument("--agent-type", type=str,
                        choices=["llm", "resource"], default="resource",
                        help="Default agent type for all agents (default: resource)")

    # Dynamic agent addition support
    parser.add_argument("--initial-group-size", type=int, default=None,
                        help="Initial group size for dynamic agent addition (if different from total agents)")

    parser.add_argument("--co-parents", type=int, default=1,
                        help="Number of co-parents per child group in hierarchical topology (default: 1)")
    parser.add_argument("--groups-per-coordinator", type=int, default=1,
                        help="Child groups each Level-1 coordinator exclusively parents "
                             "(default: 1). Above 1 is what gives a coordinator a delegation "
                             "decision: at 1 there is a single candidate, so both the MAB and "
                             "delegation.policy=llm are inert. Freed coordinator slots become "
                             "Level-0 agents, so the fleet size is unchanged. Two-level "
                             "hierarchies only.")

    parser.add_argument("--delegation-policy", choices=["bandit", "llm"], default=None,
                        help="Override delegation.policy in the generated configs (default: "
                             "whatever the base config says). Requires "
                             "--groups-per-coordinator > 1 to have any effect.")

    parser.add_argument("--fit-all", action="store_true",
                        help="Size every job to fit ALL agents (min capacities). "
                             "Enables any agent to take over jobs from failed agents.")

    parser.add_argument("--quantum-agents-pct", type=float, default=0.0,
                        help="Fraction (0.0-1.0) of agents that own a quantum backend "
                             "from the built-in catalog (default: 0.0, classical only)")

    parser.add_argument("--quantum-fraction", type=float, default=0.0,
                        help="Fraction (0.0-1.0) of generated jobs with a one-shot quantum component")

    parser.add_argument("--hybrid-fraction", type=float, default=0.0,
                        help="Fraction (0.0-1.0) of generated jobs with a hybrid classical<->quantum loop")

    parser.add_argument("--master-fleet-size", type=int, default=None,
                        help="Draw per-agent flavours and quantum backends for a fleet of this "
                             "size, then write only the first <num_agents> of them. Set it to the "
                             "largest rung of a scale ladder (e.g. 270) so every smaller fleet is "
                             "a strict prefix and agent i is identical at every size. Without it, "
                             "flavours are percentages of the fleet being generated, so the same "
                             "seed gives agent i a different machine at each size.")

    parser.add_argument("--skip-jobs", action="store_true",
                        help="Generate agent configs only; do not synthesize jobs/. Used when the "
                             "job pool comes from elsewhere (e.g. Pegasus profiles converted after "
                             "the fleet's DTN assignments are known).")

    parser.add_argument("--seed", type=int, default=None,
                        help="Seed the RNG so agent capacities, flavors and DTN assignments are "
                             "reproducible. Required to compare runs against each other.")

    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    if args.agent_hosts_file:
        agent_hosts = load_agent_hosts(args.agent_hosts_file)
    else:
        agent_hosts = None

    if args.agent_sites_file:
        agent_sites = load_agent_sites(args.agent_sites_file)
    else:
        agent_sites = None

    # Normalize flavor percentages: fill missing with defaults
    if args.flavor_percentages:
        fp = list(args.flavor_percentages)
        if len(fp) < len(INSTANCE_FLAVORS):
            fp += DEFAULT_FLAVOR_PERCENTAGES[len(fp):]
        flavor_percentages = fp
    else:
        flavor_percentages = DEFAULT_FLAVOR_PERCENTAGES

    generator = SwarmConfigGenerator(
        num_agents=args.num_agents,
        jobs_per_proposal=args.jobs_per_proposal,
        base_config_path=args.base_config_file,
        output_dir=args.output_dir,
        topology=args.topology,
        db_host=args.database,
        enable_dtns=args.dtns,
        agents_per_host=args.agents_per_host,
        groups=args.groups,
        group_size=args.group_size,
        hierarchical_level1_agent_type=args.hierarchical_level1_agent_type,
        agent_type=args.agent_type,
        initial_group_size=args.initial_group_size,
        co_parent_count=args.co_parents,
        groups_per_coordinator=args.groups_per_coordinator,
        delegation_policy=args.delegation_policy,
        quantum_agents_pct=args.quantum_agents_pct,
        master_fleet_size=args.master_fleet_size,
    )
    try:
        generator.generate_configs(flavor_percentages=flavor_percentages, agent_hosts=agent_hosts,
                                   agent_sites=agent_sites)
    except TopologyError as e:
        # Non-zero, so a driver running with check=True stops here instead of launching agents
        # against an empty (or stale) config directory.
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    # Create jobs if not present (and not deferred to another producer)
    if not args.skip_jobs and not os.path.exists("jobs"):
        jg = JobGenerator(job_count=args.job_cnt, agent_profile_path='agent_profiles.json',
                          quantum_fraction=args.quantum_fraction,
                          hybrid_fraction=args.hybrid_fraction)
        jg.generate_job_files(output_dir="jobs", enable_dtns=args.dtns, fit_all=args.fit_all)
