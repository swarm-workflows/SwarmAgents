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
import argparse

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="swarm-agent",
        description="Start a swarm agent (resource or LLM) with CLI-configurable options."
    )
    parser.add_argument("agent_id", type=int, help="Numeric id for this agent")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    # Agent selection
    parser.add_argument(
        "--agent-type",
        choices=["resource", "llm", "colmena"],
        default="resource",
        help="Type of agent to start (default: resource)"
    )

    # Config
    parser.add_argument(
        "--config",
        help="Path to config YAML. If omitted, a sensible default is chosen based on agent type."
    )

    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args()
    agent_id = args.agent_id
    debug = args.debug
    agent_type = args.agent_type

    config_file = args.config
    if not config_file:
        config_file = f"./configs/config_swarm_multi_{agent_id}.yml"

    if agent_type == "resource":
        from swarm.agents.resource_agent import ResourceAgent
        agent = ResourceAgent(agent_id=agent_id, config_file=config_file, debug=debug)
    elif agent_type == "llm":
        from swarm.agents.llm.llm_agent import LlmAgent
        agent = LlmAgent(agent_id=agent_id, config_file=config_file)
    elif agent_type == "colmena":
        from swarm.agents.colmena_agent import ColmenaAgent
        agent = ColmenaAgent(agent_id=agent_id, config_file=config_file, debug=debug)
    else:
        raise ValueError(f"Unknown agent type: {agent_type}")

    agent.start()
