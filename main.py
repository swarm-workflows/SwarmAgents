import argparse
import hashlib
import json
import random
import string
import threading
import time
from queue import Queue

from swarm.agents.pbft_agent import PBFTAgent
from swarm.models.capacities import Capacities
from swarm.models.data_node import DataNode
from swarm.models.task import Task


class TaskDistributor(threading.Thread):
    def __init__(self, agents, task_pool, tasks_per_interval, interval):
        super().__init__()
        self.agents = agents
        self.task_pool = task_pool
        self.tasks_per_interval = tasks_per_interval
        self.interval = interval
        self.shutdown_flag = threading.Event()

    def run(self):
        while not self.shutdown_flag.is_set() and self.task_pool:
            tasks_to_add = [self.task_pool.pop() for _ in range(min(self.tasks_per_interval, len(self.task_pool)))]
            for agent in self.agents:
                for task in tasks_to_add:
                    agent.task_queue.add_task(task)
            #time.sleep(self.interval)
            time.sleep(random.uniform(0.1, 1.0))

    def stop(self):
        self.shutdown_flag.set()


def build_tasks_from_json(json_file):
    tasks = []
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
            tasks.append(task)
    return tasks


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="PBFT Agent")
    parser.add_argument("--agent_id", default=None, help="Optional agent ID")

    args = parser.parse_args()

    if args.agent_id is None:
        agent_id = hashlib.sha256(''.join(random.choices(string.ascii_lowercase, k=8)).encode()).hexdigest()[:8]
    else:
        agent_id = str(args.agent_id)

    agent = PBFTAgent(agent_id=agent_id, config_file="./config.yml", cycles=1000)
    agents = [agent]

    task_pool = build_tasks_from_json('tasks.json')
    tasks_per_interval = 1  # Number of tasks to add each interval
    interval = 5  # Interval in seconds

    distributor = TaskDistributor(agents=agents, task_pool=task_pool, tasks_per_interval=tasks_per_interval,
                                  interval=interval)

    agent.start()
    distributor.start()
    agent.run()
    distributor.stop()
    agent.stop()
