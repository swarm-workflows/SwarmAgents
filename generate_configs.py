import json
import random

import yaml
import os
import argparse

from job_generator import JobGenerator

INSTANCE_FLAVORS = [
    {"name": "small",      "core": 2,  "ram": 8,   "disk": 100,  "gpu": 0},
    {"name": "medium",     "core": 4,  "ram": 16,  "disk": 250,  "gpu": 0},
    {"name": "large",      "core": 8,  "ram": 32,  "disk": 500,  "gpu": 4},
    {"name": "xtralarge",  "core": 16, "ram": 64,  "disk": 1000, "gpu": 4},
    {"name": "xxtralarge", "core": 32, "ram": 128, "disk": 1000, "gpu": 4},
]

class SwarmConfigGenerator:
    """
    A class to generate configuration files for agents in a structured ring topology.
    """
    AGENT_DTNS = "agent_dtns.json"

    def __init__(self, num_agents, jobs_per_proposal, base_config_path, output_dir, topology,
                 db_host, enable_dtns):
        """
        Initializes the generator with the number of agents, base config path, and output directory.

        :param num_agents: Number of agents to generate configurations for.
        :param jobs_per_proposal: Jobs per proposal
        :param base_config_path: Path to the base configuration YAML file.
        :param output_dir: Directory where generated configs should be saved.
        """
        self.num_agents = num_agents
        self.jobs_per_proposal = jobs_per_proposal
        self.base_config_path = base_config_path
        self.output_dir = output_dir
        self.base_config = self.load_base_config()
        self.rings = self.create_ring_topology()
        self.topology = topology
        self.db_host = db_host
        self.agent_dtns_map = self._load_agent_dtns(path=self.AGENT_DTNS)
        self.enable_dtns = enable_dtns
    
    def assign_flavors(self, percentages):
        """
        Assign flavors to agents based on CLI percentages.
        Returns a list of flavor dicts for each agent.
        """
        agent_flavors = []
        total_agents = self.num_agents
        assigned = 0
        for idx, flavor in enumerate(INSTANCE_FLAVORS):
            pct = percentages[idx]
            count = int(pct * total_agents)
            agent_flavors.extend([flavor] * count)
            assigned += count
        # Fill any remainder with the first flavor
        while len(agent_flavors) < total_agents:
            agent_flavors.append(INSTANCE_FLAVORS[0])
        random.shuffle(agent_flavors)
        return agent_flavors

    def load_base_config(self):
        """
        Loads the base configuration from a YAML file.

        :return: Dictionary containing the base configuration.
        """
        with open(self.base_config_path, "r") as file:
            return yaml.safe_load(file)

    def create_ring_topology(self):
        """
        Creates a ring topology with interconnections between rings.

        :return: A list of lists, where each sublist represents a ring of agents.
        """
        rings = []
        agents = list(range(1, self.num_agents + 1))  # Start IDs from 1

        # Always include agent 1 in the first ring
        first_ring = agents[:5]
        rings.append(first_ring)

        # Create additional rings of 5 agents
        i = 5
        while i < self.num_agents:
            ring = agents[i:i+5]
            if len(ring) < 5:
                break  # Ignore if less than 5 agents left (not enough for a full ring)
            rings.append(ring)
            i += 5

        return rings

    def print_ring_topology(self):
        """
        Prints the ring topology in a clear format.
        """
        print("\nRing TopologyType:")
        for i, ring in enumerate(self.rings):
            ring_display = " ⟶ ".join(map(str, ring)) + f" ⟶ {ring[0]}"  # Make it a closed loop
            print(f"Ring {i + 1}: {ring_display}")

    def get_config_prefix(self):
        """
        Extracts the prefix from the base configuration file name.

        :return: Prefix string to use for generated config files.
        """
        filename = os.path.basename(self.base_config_path)
        prefix, _ = os.path.splitext(filename)  # Extract name without extension
        return prefix

    def generate_configs(self, flavor_percentages, agent_hosts, save_agent_profiles_path="agent_profiles.json"):
        """
        Generates YAML configuration files for agents based on the ring topology.
        """
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        if self.topology == "ring":
            agent_topo = {}
            agent_peers = {i: [] for i in range(1, self.num_agents + 1)}  # IDs start from 1
            self.print_ring_topology()  # Print rings before generating configs

            # Assign peer connections based on ring topology
            for ring in self.rings:
                for i in range(len(ring)):
                    next_agent = ring[(i + 1) % len(ring)]  # Circular connection
                    prev_agent = ring[(i - 1) % len(ring)]
                    agent_peers[ring[i]].extend([next_agent, prev_agent])

            # Connect last agent of each ring to the first agent of the previous ring
            for i in range(len(self.rings)):  # Start from first ring
                first_agent = self.rings[i][0]
                if i == 0:
                    first_prev_ring = self.rings[-1][0]  # Last ring connects to first ring
                else:
                    first_prev_ring = self.rings[i - 1][0]
                if i != len(self.rings) - 1:
                    first_next_ring = self.rings[i + 1][0]
                    agent_peers[first_agent].append(first_next_ring)
                else:
                    if first_agent != self.rings[0][0]:
                        agent_peers[first_agent].append(self.rings[0][0])  # Last ring connects to first

                if first_agent != first_prev_ring:
                    agent_peers[first_agent].append(first_prev_ring)

            # Remove duplicate entries
            for agent_id in agent_peers:
                agent_topo[agent_id] = {
                    "peers": sorted(set(agent_peers[agent_id])),
                    "parent": None,
                    "children": None,
                    "group": 0,
                    "level": 0
                }
        elif self.topology == "star":
            core_agents = [1, 2, 3, 4, 5]
            agent_peers = {}
            agent_topo = {}

            # Step 1: Connect core agents in a ring
            for i in range(len(core_agents)):
                current = core_agents[i]
                next_core = core_agents[(i + 1) % len(core_agents)]
                prev_core = core_agents[(i - 1) % len(core_agents)]
                agent_peers[current] = [prev_core, next_core]

            # Step 2: Distribute leaf agents among core agents
            leaf_agents = list(range(6, self.num_agents + 1))
            for idx, leaf_id in enumerate(leaf_agents):
                # Round-robin assignment to 2 core agents
                primary = core_agents[idx % len(core_agents)]
                secondary = core_agents[(idx + 1) % len(core_agents)]
                agent_peers[leaf_id] = [primary, secondary]

                # Add leaf to the assigned core agents' peer lists
                agent_peers[primary].append(leaf_id)
                agent_peers[secondary].append(leaf_id)

            # Step 3: Deduplicate and sort peer lists
            for agent_id in agent_peers:
                agent_topo[agent_id] = {
                    "peers": sorted(set(agent_peers[agent_id])),
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
                end = start + group_size
                base = 101 if self.num_agents > 30 else 26
                parent_id = base + group
                for agent_id in range(start, end):
                    peers = list(range(start, end))
                    peers.remove(agent_id)
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
                    "peers": [base + i for i in range(num_groups) if i != group],  # all other parents
                    "parent": None,
                    "children": [group],  # Just the group number this agent manages
                    "group": 0,
                    "level": 1
                }
        else:
            agent_topo = {}
            agent_peers = {
                i: [j for j in range(1, self.num_agents + 1) if j != i]
                for i in range(1, self.num_agents + 1)
            }
            for agent_id in agent_peers:
                agent_topo[agent_id] = {
                    "peers": sorted(set(agent_peers[agent_id])),
                    "parent": None,
                    "children": None,
                    "group": 0,
                    "level": 0
                }

        config_prefix = self.get_config_prefix()

                # Step 1: Create a global DTN pool once
        if len(self.agent_dtns_map) == 0:
            dtn_pool = self.generate_global_dtn_pool(total_count=10)
        else:
            dtn_pool = None

        # Assign flavors
        if flavor_percentages is None:
            # Default percentages: [small, medium, large, xtralarge, xxtralarge]
            flavor_percentages = [0.4, 0.25, 0.15, 0.15, 0.05]
        agent_flavors = self.assign_flavors(flavor_percentages)

        agent_profiles = {}
        # Generate YAML files for each agent
        for agent_id in range(1, self.num_agents + 1):
            config = self.base_config.copy()
            config['grpc']['host'] = agent_hosts[agent_id - 1]

            # Step 3: Assign random DTNs from pool to this agent
            if self.enable_dtns:
                if dtn_pool is not None:
                    config["dtns"] = self.assign_agent_dtns(dtn_pool, min_dtns=1, max_dtns=4)
                    self.agent_dtns_map[str(agent_id)] = config["dtns"]
                else:
                    config["dtns"] = self.adjust_scores(self.agent_dtns_map[str(agent_id)])

            # Assign capacities and gpus based on flavor
            flavor = agent_flavors[agent_id - 1]
            config['capacities']['core'] = flavor['core']
            config['capacities']['gpu'] = flavor['gpu']
            config['capacities']['ram'] = flavor['ram']
            config['capacities']['disk'] = flavor['disk']

            config["redis"]["host"] = self.db_host
            config["topology"] = {"peer_agents": agent_topo[agent_id]["peers"], "type": self.topology,
                                  "parent": agent_topo[agent_id]["parent"], "children": agent_topo[agent_id]["children"],
                                  "level": agent_topo[agent_id]["level"], "group": agent_topo[agent_id]["group"]}
            config["runtime"]["total_agents"] = self.num_agents
            config["runtime"]["jobs_per_proposal"] = self.jobs_per_proposal

            config_file_path = os.path.join(f"{self.output_dir}/{config_prefix}_{agent_id}.yml")

            # Assign DTNs if present
            dtns = config.get("dtns", [])
            # Save agent profile for this agent
            agent_profiles[str(agent_id)] = {
                "core": config['capacities']['core'],
                "ram": config['capacities']['ram'],
                "disk": config['capacities']['disk'],
                "gpu": config['capacities']['gpu'],
                "dtns": dtns
            }
            with open(config_file_path, "w") as file:
                yaml.dump(config, file, default_flow_style=False)

        # Dump all agent DTNs to a JSON file
        dtn_json_path = os.path.join(self.AGENT_DTNS)
        with open(dtn_json_path, 'w') as f:
            json.dump(self.agent_dtns_map, f, indent=2)

        # Save agent profiles JSON if requested
        if save_agent_profiles_path:
            with open(save_agent_profiles_path, "w") as f:
                json.dump(agent_profiles, f, indent=2)

        print(f"\nGenerated {self.num_agents} config files in {self.output_dir}")

    def _load_agent_dtns(self, path: str) -> dict[int, list[str]]:
        if path and os.path.exists(path):
            with open(path, "r") as f:
                return json.load(f)
        return {}

    @staticmethod
    def random_capacity(min_val, max_val):
        return random.randint(min_val, max_val)

    def generate_global_dtn_pool(self, total_count=10):
        """
        Create a global pool of DTNs with base connectivity scores.
        These are later subsetted for each agent.
        """
        pool = []
        for i in range(1, total_count + 1):
            pool.append({
                "name": f"dtn{i}",
                "ip": f"192.168.100.{i}",
                "user": f"dtn_user{i}",
                "base_connectivity_score": round(random.uniform(0.6, 0.95), 2)
            })
        return pool

    def adjust_scores(self, dtns: list):
        """
        adjusting connectivity scores slightly to reflect per-agent network differences.
        :param dtns: list of dtns for the agent
        """
        for d in dtns:
            # Add per-agent variation to score
            adjusted_score = min(1.0, max(0.0, d["connectivity_score"] + random.uniform(-0.05, 0.05)))
            d["connectivity_score"] = adjusted_score
        return dtns

    def assign_agent_dtns(self, pool, min_dtns=1, max_dtns=4):
        """
        Assign a random subset of DTNs to an agent, adjusting connectivity scores
        slightly to reflect per-agent network differences.
        """
        count = random.randint(min_dtns, max_dtns)
        selected = random.sample(pool, count)
        agent_dtns = []
        for d in selected:
            # Add per-agent variation to score
            adjusted_score = min(1.0, max(0.0, d["base_connectivity_score"] + random.uniform(-0.05, 0.05)))
            agent_dtns.append({
                "name": d["name"],
                "ip": d["ip"],
                "user": d["user"],
                "connectivity_score": round(adjusted_score, 2)
            })
        return agent_dtns

DEFAULT_FLAVOR_PERCENTAGES = [0.4, 0.25, 0.15, 0.15, 0.05]

def load_agent_hosts(path, num_agents):
    with open(path, "r") as f:
        hosts = [line.strip() for line in f if line.strip()]
    if len(hosts) < num_agents:
        raise ValueError("Not enough hosts specified for agents")
    return hosts[:num_agents]

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate agent configuration files with a structured ring topology.")
    parser.add_argument("num_agents", type=int, help="Number of agents to generate configurations for.")
    parser.add_argument("jobs_per_proposal", type=int, help="Number of Jobs per proposal.")
    parser.add_argument("base_config_file", type=str, help="Path to the base configuration YAML file.")
    parser.add_argument("output_dir", type=str, help="Directory where generated configs should be saved.")
    parser.add_argument("topology", type=str, default="all", help="Agent TopologyType: "
                                                                  "Possible values - mesh, ring, star, hierarchical")
    parser.add_argument("database", type=str, default="all", help="Database Host")
    parser.add_argument("job_cnt", type=int, help="Job Count")
    parser.add_argument("--dtns", action="store_true", required=False, help="Enable DTNs")
    parser.add_argument("--flavor-percentages", nargs='*', type=float, metavar='PERCENT',
                    help="Percentages for small, medium, large, xtralarge, xxtralarge flavors (e.g. 0.4 0.25 0.15 0.15 0.05)")
    parser.add_argument("--agent-hosts-file", type=str, help="Path to file with agent hosts (one per line)")
    
    args = parser.parse_args()

    if args.agent_hosts_file:
        agent_hosts = load_agent_hosts(args.agent_hosts_file, args.num_agents)
    else:
        agent_hosts = ["localhost"] * args.num_agents

    # Fill missing percentages with defaults
    if args.flavor_percentages:
        flavor_percentages = list(args.flavor_percentages) + DEFAULT_FLAVOR_PERCENTAGES[len(args.flavor_percentages):]
        total = sum(flavor_percentages)
        if abs(total - 1.0) > 0.01:
            raise ValueError("Flavor percentages must sum to 1.0")
        generator = SwarmConfigGenerator(args.num_agents, args.jobs_per_proposal, args.base_config_file,
                                         args.output_dir, args.topology, args.database, args.dtns)
        generator.generate_configs(flavor_percentages=flavor_percentages, agent_hosts=agent_hosts)
    else:
        generator = SwarmConfigGenerator(args.num_agents, args.jobs_per_proposal, args.base_config_file,
                                         args.output_dir, args.topology, args.database, args.dtns)
        generator.generate_configs(flavor_percentages=DEFAULT_FLAVOR_PERCENTAGES, agent_hosts=agent_hosts)

    if not os.path.exists("jobs"):
        generator = JobGenerator(job_count=args.job_cnt, agent_profile_path='agent_profiles.json')
        generator.generate_job_files(output_dir="jobs", enable_dtns=args.dtns)