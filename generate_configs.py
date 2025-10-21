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
                }
        return agent_topo

    def _build_ring_topology(self, rings: List[List[int]]) -> Dict[int, dict]:
        """
        Build independent rings (no cross-links). Each entry in `rings` is a group.
        """
        agent_topo: Dict[int, dict] = {}
        for gid, ring in enumerate(rings):
            n = len(ring)
            if n == 0:
                continue
            if n == 1:
                # single node "ring": no peers
                agent_topo[ring[0]] = {
                    "peers": [],
                    "parent": None,
                    "children": None,
                    "group": gid,
                    "level": 0,
                }
                continue
            for k in range(n):
                cur = ring[k]
                nxt = ring[(k + 1) % n]
                prv = ring[(k - 1) % n]
                agent_topo[cur] = {
                    "peers": sorted(set([nxt, prv])),
                    "parent": None,
                    "children": None,
                    "group": gid,
                    "level": 0,
                }
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
            num_groups = 10 if self.num_agents > 30 else 5
            group_size = 10 if self.num_agents > 30 else 5

            # Level 0 (leaf agents)
            for group in range(num_groups):
                start = group * group_size + 1
                end = min(start + group_size, self.num_agents + 1)
                base = 101 if self.num_agents > 30 else 26
                parent_id = base + group
                for agent_id in range(start, end):
                    peers = [a for a in range(start, end) if a != agent_id]
                    agent_topo[agent_id] = {
                        "peers": peers,
                        "parent": parent_id,
                        "children": None,
                        "group": group,
                        "level": 0
                    }

            # Level 1 (parent agents)
            for group in range(num_groups):
                base = 101 if self.num_agents > 30 else 26
                parent_id = base + group
                agent_topo[parent_id] = {
                    "peers": [base + i for i in range(num_groups) if i != group],
                    "parent": None,
                    "children": [group],
                    "group": 0,
                    "level": 1
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
                    "level": 0
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
                "group": topo["group"]
            }
            config.setdefault("runtime", {})
            config["runtime"]["total_agents"] = self.num_agents
            config["runtime"]["jobs_per_proposal"] = self.jobs_per_proposal

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
    )
    generator.generate_configs(flavor_percentages=flavor_percentages, agent_hosts=agent_hosts)

    # Create jobs if not present
    if not os.path.exists("jobs"):
        jg = JobGenerator(job_count=args.job_cnt, agent_profile_path='agent_profiles.json')
        jg.generate_job_files(output_dir="jobs", enable_dtns=args.dtns)
