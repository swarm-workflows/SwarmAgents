import logging
import sys
import time
from typing import Dict

from swarm.agents.raft_agent import RaftAgent
from task_generator import TaskGenerator


def generate_peers(agent_id: int, total_agents: int, port: int = 5010) -> Dict[str, str]:
    peers = {}
    for i in range(total_agents):
        if i != agent_id:
            peers[str(i)] = f"127.0.0.1:{port + i}"
    return peers


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python script.py <agent_id> <total_agents> <task_count>")
        sys.exit(1)

    start_port = 5010
    agent_id = int(sys.argv[1])
    total_agents = int(sys.argv[2])
    task_count = int(sys.argv[3])

    if agent_id >= total_agents:
        print("agent_id must be less than total_agents")
        sys.exit(1)

    logger = logging.getLogger('pyraft')
    logger.setLevel(logging.INFO)

    # Generate peers based on the agent_id and total_agents
    peers = generate_peers(agent_id, total_agents, start_port) if agent_id > 0 else {}

    # Create and start the Raft agent
    print(f"Starting at: {start_port + agent_id} with peers: {peers}")
    agent = RaftAgent(str(agent_id), port=start_port + agent_id, peers=peers,
                      config_file="./config.yml", cycles=1000)
    agent.start(clean=True)
    task_generator = TaskGenerator(task_count=task_count)
    if agent_id == 0:
        task_generator.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        agent.stop()
        if agent_id == 0:
            task_generator.stop()

