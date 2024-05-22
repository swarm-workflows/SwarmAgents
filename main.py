import argparse
import hashlib
import json

# Assuming the Task class is defined as mentioned

# Define a function to build Task objects from JSON data
import string
import random
from swarm.agents.pbft_agent import PBFTAgent
from swarm.models.capacities import Capacities
from swarm.models.data_node import DataNode
from swarm.models.task import Task


def build_tasks_from_json(json_file):
    tasks = {}
    with open(json_file, 'r') as f:
        data = json.load(f)
        for task_data in data:
            task = Task()
            task.set_task_id(task_data['id'])
            task.set_capacities(Capacities.from_dict(task_data['capacities']))
            task.no_op = task_data['no_op']
            for data_in in task_data['data_in']:
                data_node = DataNode.from_dict(data_in)
                task.add_incoming_data_dep(data_node)
            for data_out in task_data['data_out']:
                data_node = DataNode.from_dict(data_out)
                task.add_outgoing_data_dep(data_node)
            tasks[task.task_id] = task
    return tasks


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="PBFT Agent")
    parser.add_argument("--agent_id", default=None, help="Optional agent ID")

    args = parser.parse_args()

    if args.agent_id is None:
        agent_id = hashlib.sha256(''.join(random.choices(string.ascii_lowercase, k=8)).encode()).hexdigest()[:8]
    else:
        agent_id = str(args.agent_id)

    agent = PBFTAgent(agent_id=agent_id, config_file="./config.yml", cycles=100)
    agent.task_queue.build_tasks_from_json('tasks.json')
    agent.start()
    agent.run()
    agent.stop()
