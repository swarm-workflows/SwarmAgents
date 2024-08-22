import json
import random
import sys
import threading
import time
from typing import List

from swarm.agents.swarm_agentv2 import SwarmAgent
from swarm.models.capacities import Capacities
from swarm.models.data_node import DataNode
from swarm.models.job import Job


class TaskDistributor(threading.Thread):
    def __init__(self, agents: List[SwarmAgent], task_pool: List[Job], tasks_per_interval: int, interval: int):
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
                    agent.job_queue.add_job(task)
            #time.sleep(random.uniform(0.1, 3.0))
            time.sleep(0.5)

    def stop(self):
        self.shutdown_flag.set()


def build_tasks_from_json(json_file):
    tasks = []
    with open(json_file, 'r') as f:
        data = json.load(f)
        for task_data in data:
            task = Job()
            task.set_job_id(task_data['id'])
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
    if len(sys.argv) != 3:
        print("Usage: python script.py <agent_id> <task_count>")
        sys.exit(1)

    agent_id = int(sys.argv[1])
    task_count = int(sys.argv[2])

    agent = SwarmAgent(agent_id=str(agent_id), config_file="./config.yml", cycles=1000)

    task_pool = build_tasks_from_json('tasks.json')
    tasks_per_interval = 1  # Number of tasks to add each interval
    interval = 5  # Interval in seconds

    distributor = TaskDistributor(agents=[agent], task_pool=task_pool, tasks_per_interval=tasks_per_interval,
                                  interval=interval)

    agent.start()
    distributor.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        distributor.stop()
        agent.stop()

    agent.plot_results()
