import random
import yaml
import os
import argparse


class SwarmConfigGenerator:
    """
    A class to generate configuration files for agents in various topologies,
    including multi-level hierarchies.
    """

    def __init__(self, num_agents, jobs_per_proposal, base_config_path, output_dir, topology, db_host, levels=3):
        """
        Initializes the generator.

        :param num_agents: Total number of agents.
        :param jobs_per_proposal: Number of jobs per proposal.
        :param base_config_path: Path to the base configuration YAML file.
        :param output_dir: Directory for generated configs.
        :param topology: The network topology (e.g., 'hierarchical', 'mesh').
        :param db_host: The database host.
        :param levels: The number of levels for the hierarchical topology.
        """
        self.num_agents = num_agents
        self.jobs_per_proposal = jobs_per_proposal
        self.base_config_path = base_config_path
        self.output_dir = output_dir
        self.base_config = self.load_base_config()
        self.topology = topology
        self.db_host = db_host
        self.levels = max(1, levels)  # Ensure at least one level

    def load_base_config(self):
        """Loads the base configuration from a YAML file."""
        with open(self.base_config_path, "r") as file:
            return yaml.safe_load(file)

    def get_config_prefix(self):
        """Extracts the prefix from the base configuration file name."""
        filename = os.path.basename(self.base_config_path)
        prefix, _ = os.path.splitext(filename)
        return prefix

    # HIERARCHICAL CHANGE: Added a helper method to print the tree structure.
    def _print_topology_tree(self, agent_id, agent_metadata, prefix=""):
        """Recursively prints a visual representation of the hierarchy."""
        is_last = prefix.endswith("└── ")
        connector = "    " if is_last else "│   "

        node_info = agent_metadata.get(agent_id)
        if not node_info:
            return

        print(f"{prefix}Agent {agent_id} (L{node_info['level']})")

        children = node_info.get('children_ids', [])
        for i, child_id in enumerate(children):
            is_last_child = i == (len(children) - 1)
            child_prefix = "└── " if is_last_child else "├── "
            self._print_topology_tree(child_id, agent_metadata, prefix=connector + child_prefix)

    def generate_configs(self):
        """Generates YAML configuration files for each agent based on the specified topology."""
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        agent_peers = {}
        agent_metadata = {}

        if self.topology == "hierarchical":
            print(f"\nGenerating {self.levels}-Level Hierarchical TopologyType...")
            agents_by_level = [[] for _ in range(self.levels)]
            agent_nodes = {i: {'level': -1, 'parent_id': None, 'children_ids': []} for i in
                           range(1, self.num_agents + 1)}

            # --- Tree Construction Algorithm ---
            if self.num_agents > 0:
                # 1. Assign agents to levels
                agents_by_level[0].append(1)
                agent_nodes[1]['level'] = 0

                remaining_agents = list(range(2, self.num_agents + 1))

                if self.levels > 1 and remaining_agents:
                    level_proportions = [0.1] * (self.levels - 2) + [0.9]  # Simple proportion for non-root levels

                    start_idx = 0
                    for i in range(self.levels - 1):
                        level = i + 1
                        if i == self.levels - 2:  # Last assignable level gets all the rest
                            num_to_assign = len(remaining_agents) - start_idx
                        else:
                            num_to_assign = max(1, int(len(remaining_agents) * level_proportions[i]))

                        assigned = remaining_agents[start_idx: start_idx + num_to_assign]
                        if not assigned and level < self.levels - 1:
                            continue

                        agents_by_level[level].extend(assigned)
                        start_idx += len(assigned)

                        for agent_id in assigned:
                            agent_nodes[agent_id]['level'] = level

                # 2. Link parents and children
                for level in range(self.levels - 1):
                    parents = agents_by_level[level]
                    children = agents_by_level[level + 1]
                    if not parents or not children:
                        continue
                    for i, child_id in enumerate(children):
                        parent_id = parents[i % len(parents)]
                        agent_nodes[child_id]['parent_id'] = parent_id
                        agent_nodes[parent_id]['children_ids'].append(child_id)

                # 3. Define peers: parent, siblings, and children
                for agent_id, data in agent_nodes.items():
                    peers = set()
                    if data['parent_id']:
                        peers.add(data['parent_id'])
                    peers.update(data['children_ids'])
                    if data['parent_id']:
                        parent_data = agent_nodes[data['parent_id']]
                        peers.update(s_id for s_id in parent_data['children_ids'] if s_id != agent_id)
                    agent_peers[agent_id] = sorted(list(peers))

            agent_metadata = agent_nodes
            # HIERARCHICAL CHANGE: Call the print function for the generated topology.
            print("-" * 30)
            self._print_topology_tree(1, agent_metadata)  # Start printing from the root (agent 1)
            print("-" * 30)

        else:
            print("\nGenerating Mesh TopologyType...")
            agent_peers = {
                i: [j for j in range(1, self.num_agents + 1) if j != i]
                for i in range(1, self.num_agents + 1)
            }

        config_prefix = self.get_config_prefix()

        for agent_id in range(1, self.num_agents + 1):
            config = self.base_config.copy()
            config['capacities']['cpu'] = random.randint(2, 8)
            config['capacities']['ram'] = random.randint(16, 64)
            config['capacities']['disk'] = random.randint(100, 500)
            config["redis"]["host"] = self.db_host
            config["topology"] = {"peer_agents": agent_peers.get(agent_id, []), "type": self.topology}
            config["runtime"]["total_agents"] = self.num_agents
            config["runtime"]["jobs_per_proposal"] = self.jobs_per_proposal

            if self.topology == "hierarchical":
                config['hierarchical'] = agent_metadata.get(agent_id, {})

            config_file_path = os.path.join(self.output_dir, f"{config_prefix}_{agent_id}.yml")
            with open(config_file_path, "w") as file:
                yaml.dump(config, file, default_flow_style=False)

        print(f"\nGenerated {self.num_agents} config files in '{self.output_dir}'")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate agent configuration files for various topologies.")
    parser.add_argument("num_agents", type=int, help="Total number of agents to generate.")
    parser.add_argument("jobs_per_proposal", type=int, help="Number of Jobs per proposal.")
    parser.add_argument("base_config_file", type=str, help="Path to the base configuration YAML file.")
    parser.add_argument("output_dir", type=str, help="Directory to save generated configs.")
    parser.add_argument("topology", type=str, default="mesh", choices=['mesh', 'hierarchical'],
                        help="Agent network topology.")
    parser.add_argument("database", type=str, default="redis", help="Database Host name.")
    parser.add_argument("--levels", type=int, default=3, help="Number of levels for hierarchical topology.")
    args = parser.parse_args()

    generator = SwarmConfigGenerator(
        args.num_agents,
        args.jobs_per_proposal,
        args.base_config_file,
        args.output_dir,
        args.topology,
        args.database,
        args.levels
    )
    generator.generate_configs()