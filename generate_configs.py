import argparse
import copy
import json
import os
import random
from typing import Dict, List, Optional, Tuple

import yaml

from job_generator import JobGenerator

INSTANCE_FLAVORS = [
    {"name": "small",      "core": 2,  "ram": 8,   "disk": 100,  "gpu": 0},
    {"name": "medium",     "core": 4,  "ram": 16,  "disk": 250,  "gpu": 0},
    {"name": "large",      "core": 8,  "ram": 32,  "disk": 500,  "gpu": 4},
    {"name": "xtralarge",  "core": 16, "ram": 64,  "disk": 1000, "gpu": 4},
    {"name": "xxtralarge", "core": 32, "ram": 128, "disk": 1000, "gpu": 4},
]

DEFAULT_FLAVOR_PERCENTAGES = [0.4, 0.25, 0.15, 0.15, 0.05]


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
    ):
        self.num_agents = num_agents
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

        # legacy ring helper (used when no grouping flags provided for ring)
        self.rings_default = self._create_default_rings()
        self.agent_dtns_map: Dict[str, List[dict]] = self._load_agent_dtns(path=self.AGENT_DTNS)

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

        total_agents = self.num_agents
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
        return agent_flavors

    # -----------------------------
    # Base config
    # -----------------------------
    def load_base_config(self):
        with open(self.base_config_path, "r") as file:
            return yaml.safe_load(file)

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
        if path and os.path.exists(path):
            with open(path, "r") as f:
                data = json.load(f)
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
                print("Minimum number of agents for hierarchical topology is 30")
                return
            agent_topo = {}

            # Determine hierarchy structure based on agent count
            if self.num_agents == 30:
                # Two-level: 25 Level-0 + 5 Level-1 = 30
                num_groups = 5
                group_size = 5
                num_super_groups = 0  # No Level 2
                level_1_base = 26

            elif self.num_agents == 100:
                # Three-level: 80 Level-0 + 16 Level-1 + 4 Level-2 = 100
                num_super_groups = 4
                groups_per_super_group = 4
                num_groups = num_super_groups * groups_per_super_group  # 16 groups
                group_size = 5
                level_1_base = 81
                level_2_base = 97

            elif self.num_agents <= 110:
                # Two-level: 100 Level-0 + 10 Level-1 = 110
                num_groups = 10
                group_size = 10
                num_super_groups = 0  # No Level 2
                level_1_base = 101

            elif self.num_agents == 1000:
                # Three-level: 900 Level-0 + 90 Level-1 + 10 Level-2 = 1000
                num_super_groups = 10
                groups_per_super_group = 9
                num_groups = num_super_groups * groups_per_super_group  # 90 groups
                group_size = 10
                level_1_base = 901
                level_2_base = 991

            else:
                print(f"Hierarchical topology currently supports 30, 100, 110, or 1000 agents (got {self.num_agents})")
                return

            # Level 0 (leaf/worker agents)
            for group in range(num_groups):
                start = group * group_size + 1
                end = start + group_size
                parent_id = level_1_base + group

                # For 3-level hierarchy, determine super-group
                super_group = group // groups_per_super_group if num_super_groups > 0 else 0

                for agent_id in range(start, end):
                    peers = [a for a in range(start, end) if a != agent_id]
                    agent_topo[agent_id] = {
                        "peers": peers,
                        "parent": parent_id,
                        "children": None,
                        "group": group,
                        "level": 0,
                        "group_size": group_size,
                        "group_count": num_groups,
                        "super_group": super_group
                    }

            # Level 1 (group coordinators)
            for group in range(num_groups):
                parent_id = level_1_base + group

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
                    peers = [level_1_base + i for i in range(num_groups) if i != group]
                    level_1_parent = None

                # Level-1 group metadata should reflect the super-group (not the child group id)
                l1_group = super_group if num_super_groups > 0 else group
                l1_group_count = num_super_groups if num_super_groups > 0 else num_groups
                # Level 1 group_size should be the number of Level 1 coordinators in the peer group
                l1_group_size = groups_per_super_group if num_super_groups > 0 else num_groups

                agent_topo[parent_id] = {
                    "peers": peers,
                    "parent": level_1_parent,
                    "children": [group],
                    "group": l1_group,
                    "level": 1,
                    "group_size": l1_group_size,
                    "group_count": l1_group_count,
                    "super_group": super_group if num_super_groups > 0 else 0
                }

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
                        "group_size": groups_per_super_group,
                        "group_count": num_super_groups,
                        "super_group": super_group
                    }

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
            config["topology"] = {
                "peer_agents": topo["peers"],
                "type": self.topology,
                "parent": topo["parent"],
                "children": topo["children"],
                "level": topo["level"],
                "group": topo["group"],
                "group_size": topo["group_size"],
                "group_count": topo["group_count"]
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
                "dtns": dtns
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

    args = parser.parse_args()

    if args.agent_hosts_file:
        agent_hosts = load_agent_hosts(args.agent_hosts_file)
    else:
        agent_hosts = None

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
    )
    generator.generate_configs(flavor_percentages=flavor_percentages, agent_hosts=agent_hosts)

    # Create jobs if not present
    if not os.path.exists("jobs"):
        jg = JobGenerator(job_count=args.job_cnt, agent_profile_path='agent_profiles.json')
        jg.generate_job_files(output_dir="jobs", enable_dtns=args.dtns)
