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
import json
import sys
import threading
import time
from typing import List

from swarm.agents.agent import Agent
from swarm.models.capacities import Capacities
from swarm.models.data_node import DataNode
from swarm.models.job import Job
from swarm.queue.simple_job_queue import SimpleJobQueue


class TaskDistributor(threading.Thread):
    def __init__(self, agent: Agent, task_pool: List[Job], tasks_per_interval: int, interval: int):
        super().__init__()
        self.agent = agent
        self.task_pool = task_pool
        self.tasks_per_interval = tasks_per_interval
        self.interval = interval
        self.shutdown = False
        self.total_tasks = len(self.task_pool)

    def run(self):
        print("Started Task Distributor")
        total_tasks_added = 0
        while not self.shutdown and self.task_pool:
            tasks_to_add = [self.task_pool.pop() for _ in range(min(self.tasks_per_interval, len(self.task_pool)))]
            for task in tasks_to_add:
                self.agent.queues.job_queue.add_job(task)
                total_tasks_added += 1
            time.sleep(1)
            if total_tasks_added == self.total_tasks:
                break
        print("Stopped Task Distributor")

    def stop(self):
        self.shutdown = True


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
    if len(sys.argv) < 5:
        print("Usage: python main.py <agent_type> <agent_id> <task_count> <agent_count> [<topo>]")
        sys.exit(1)

    agent_type = sys.argv[1].lower()
    agent_id = int(sys.argv[2])
    task_count = int(sys.argv[3])
    total_agents = int(sys.argv[4])

    local_topo = True if len(sys.argv) > 5 else False

    # Load configuration based on agent type
    if agent_type == "pbft":
        config_file = "./config_pbft.yml"
        from swarm.agents.pbft_agentv2 import PBFTAgent
        agent = PBFTAgent(agent_id=agent_id, config_file=config_file)
    elif agent_type == "swarm-single":
        config_file = "./config_swarm_single.yml"
        if local_topo:
            config_file = f"./config_swarm_single_{agent_id}.yml"
        # Initialize your swarm-single agent here using the config_file
        from swarm.agents.swarm_agent import SwarmAgent
        agent = SwarmAgent(agent_id=agent_id, config_file=config_file)
    elif agent_type == "swarm-multi":
        config_file = "./config_swarm_multi.yml"
        if local_topo:
            config_file = f"./config_swarm_multi_{agent_id}.yml"
        # Initialize your swarm-multi agent here using the config_file
        from swarm.agents.swarm_agent_multi_grpc import SwarmAgent
        agent = SwarmAgent(agent_id=agent_id, config_file=config_file)
    elif agent_type == "redis-swarm-multi":
        config_file = "./config_swarm_multi.yml"
        if local_topo:
            config_file = f"./config_swarm_multi_{agent_id}.yml"
        # Initialize your swarm-multi agent here using the config_file
        from swarm.agents.swarm_agent_multi_db import SwarmAgent
        agent = SwarmAgent(agent_id=agent_id, config_file=config_file)
    else:
        print(f"Unknown agent type: {agent_type}")
        sys.exit(1)

    task_pool = build_tasks_from_json('tasks.json')
    tasks_per_interval = 1  # Number of tasks to add each interval
    interval = 5  # Interval in seconds

    if isinstance(agent.queues.job_queue, SimpleJobQueue):
        distributor = TaskDistributor(agent=agent, task_pool=task_pool, tasks_per_interval=tasks_per_interval,
                                      interval=interval)

        distributor.start()

    agent.start()
