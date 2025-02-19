import yaml
import os
import argparse

class SwarmConfigGenerator:
    """
    A class to generate configuration files for agents in a structured ring topology.
    """

    def __init__(self, num_agents, base_config_path, output_dir):
        """
        Initializes the generator with the number of agents, base config path, and output directory.

        :param num_agents: Number of agents to generate configurations for.
        :param base_config_path: Path to the base configuration YAML file.
        :param output_dir: Directory where generated configs should be saved.
        """
        self.num_agents = num_agents
        self.base_config_path = base_config_path
        self.output_dir = output_dir
        self.base_config = self.load_base_config()
        self.rings = self.create_ring_topology()

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
        print("\nRing Topology:")
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

    def generate_configs(self):
        """
        Generates YAML configuration files for agents based on the ring topology.
        """
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        self.print_ring_topology()  # Print rings before generating configs

        agent_peers = {i: [] for i in range(1, self.num_agents + 1)}  # IDs start from 1

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
            agent_peers[agent_id] = sorted(set(agent_peers[agent_id]))

        config_prefix = self.get_config_prefix()

        # Generate YAML files for each agent
        for agent_id in range(1, self.num_agents + 1):
            config = self.base_config.copy()
            config["topology"] = {"peer_agents": agent_peers[agent_id]}

            config_file_path = os.path.join(self.output_dir, f"{config_prefix}_{agent_id}.yml")
            with open(config_file_path, "w") as file:
                yaml.dump(config, file, default_flow_style=False)

        print(f"\nGenerated {self.num_agents} config files in {self.output_dir}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate agent configuration files with a structured ring topology.")
    parser.add_argument("num_agents", type=int, help="Number of agents to generate configurations for.")
    parser.add_argument("base_config_file", type=str, help="Path to the base configuration YAML file.")
    parser.add_argument("output_dir", type=str, help="Directory where generated configs should be saved.")

    args = parser.parse_args()

    generator = SwarmConfigGenerator(args.num_agents, args.base_config_file, args.output_dir)
    generator.generate_configs()
