import os

import matplotlib.pyplot as plt
from collections import defaultdict
import redis
from swarm.models.task import TaskRepository, Task

class Plotter:
    def __init__(self, host: str = "localhost", port: int = 6379):
        """
        Task generator
        :param host: Redis host
        :param port: Redis port
        """
        self.redis_client = redis.StrictRedis(host=host, port=port, decode_responses=True)
        self.task_repository = TaskRepository(self.redis_client)

    def plot_tasks_per_agent(self):
        tasks = self.task_repository.get_all_tasks(key_prefix="*")
        tasks_per_agent = {}
        for t in tasks:
            if t.leader_agent_id:
                if t.leader_agent_id not in tasks_per_agent:
                    tasks_per_agent[t.leader_agent_id] = 0
                tasks_per_agent[t.leader_agent_id] += 1

        plt.bar(list(tasks_per_agent.keys()), list(tasks_per_agent.values()), color='blue')
        plt.xlabel('Agent ID')
        plt.ylabel('Number of Tasks Executed')
        plt.title('Number of Tasks Executed by Each Agent')
        plt.grid(axis='y', linestyle='--', linewidth=0.5)
        #plt.show()
        plot_path = os.path.join("", 'raft-by-agent.png')
        plt.savefig(plot_path)
        plt.close()

    def plot_wait_time(self):
        tasks = self.task_repository.get_all_tasks(key_prefix="*")
        waiting_times = [t.time_on_queue for t in tasks if t.time_on_queue is not None]
        plt.plot(waiting_times, 'ro-', label='Scheduling Latency')

        plt.legend()
        plt.title(f'Scheduling Latency')
        plt.xlabel('Task Index')
        plt.ylabel('Time Units (seconds)')
        plt.grid(True)
        plot_path = os.path.join("", 'raft-by-time.png')
        plt.savefig(plot_path)
        plt.close()


def main():
    plotter = Plotter()
    plotter.plot_tasks_per_agent()
    plotter.plot_wait_time()


if __name__ == '__main__':
    main()
