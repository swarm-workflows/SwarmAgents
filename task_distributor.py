import threading
import time
import random
import redis
import json
import sys

from swarm.models.capacities import Capacities
from swarm.models.data_node import DataNode
from swarm.models.task import Task, TaskRepository


class TaskDistributor(threading.Thread):
    def __init__(self, redis_host, redis_port, task_pool, tasks_per_interval, interval):
        super().__init__()
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.task_pool = task_pool
        self.tasks_per_interval = tasks_per_interval
        self.interval = interval
        self.shutdown_flag = threading.Event()
        self.redis_client = redis.StrictRedis(host=self.redis_host, port=self.redis_port, decode_responses=True)
        self.task_repo = TaskRepository(self.redis_client)

    def run(self):
        self.task_repo.delete_all()
        while not self.shutdown_flag.is_set() and self.task_pool:
            tasks_to_add = [self.task_pool.pop() for _ in range(min(self.tasks_per_interval, len(self.task_pool)))]
            for task in tasks_to_add:
                self.task_repo.save_task(task)
            time.sleep(random.uniform(0.1, 1.0))

    def stop(self):
        self.shutdown_flag.set()


def load_tasks_from_json(file_path):
    tasks = []
    with open(file_path, 'r') as f:
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


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: python script.py <redis_host> <redis_port> <tasks_json_file> <tasks_per_interval>")
        sys.exit(1)

    redis_host = sys.argv[1]
    redis_port = int(sys.argv[2])
    tasks_json_file = sys.argv[3]
    tasks_per_interval = int(sys.argv[4])

    task_pool = load_tasks_from_json(tasks_json_file)
    interval = 1  # interval in seconds; adjust as needed

    task_distributor = TaskDistributor(redis_host, redis_port, task_pool, tasks_per_interval, interval)
    task_distributor.start()

    try:
        while task_distributor.is_alive():
            task_distributor.join(1)
    except KeyboardInterrupt:
        task_distributor.stop()
        task_distributor.join()
