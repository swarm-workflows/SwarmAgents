import csv
import os

import matplotlib.pyplot as plt
from collections import defaultdict
import redis
from swarm.models.job import JobRepository, Job


class Plotter:
    def __init__(self, host: str = "localhost", port: int = 6379):
        """
        Task generator
        :param host: Redis host
        :param port: Redis port
        """
        self.redis_client = redis.StrictRedis(host=host, port=port, decode_responses=True)
        self.task_repository = JobRepository(self.redis_client)

    def plot_tasks_per_agent(self):
        tasks = self.task_repository.get_all_jobs(key_prefix="*")
        tasks_per_agent = {}
        for t in tasks:
            if t.leader_agent_id:
                if t.leader_agent_id not in tasks_per_agent:
                    tasks_per_agent[t.leader_agent_id] = 0
                tasks_per_agent[t.leader_agent_id] += 1

        # Save tasks_per_agent to CSV
        with open('tasks_per_agent.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['Agent ID', 'Number of Tasks Executed'])
            for agent_id, task_count in tasks_per_agent.items():
                writer.writerow([agent_id, task_count])

        # Plotting the tasks per agent as a bar chart
        plt.bar(list(tasks_per_agent.keys()), list(tasks_per_agent.values()), color='blue')
        plt.xlabel('Agent ID')
        plt.ylabel('Number of Tasks Executed')

        # Title with RAFT and number of agents
        num_agents = len(tasks_per_agent)
        plt.title(f'RAFT: Number of Tasks Executed by Each Agent (Total Agents: {num_agents})')

        plt.grid(axis='y', linestyle='--', linewidth=0.5)

        # Save the plot
        plot_path = os.path.join("", 'tasks-per-agent.png')
        plt.savefig(plot_path, bbox_inches='tight')
        plt.close()

    def plot_wait_time(self):
        tasks = self.task_repository.get_all_jobs(key_prefix="*")
        waiting_times = [t.waited_on_queue for t in tasks if t.waited_on_queue is not None]

        # Save scheduling latency to CSV
        with open('scheduling_latency.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['Task Index', 'Scheduling Latency'])
            for index, wait_time in enumerate(waiting_times):
                writer.writerow([index, wait_time])

        # Plotting scheduling latency
        plt.plot(waiting_times, 'ro-', label='Scheduling Latency')

        plt.legend()

        # Title with RAFT and number of agents
        num_agents = len(set(t.leader_agent_id for t in tasks if t.leader_agent_id is not None))
        plt.title(f'RAFT: Scheduling Latency (Total Agents: {num_agents})')

        plt.xlabel('Task Index')
        plt.ylabel('Time Units (seconds)')
        plt.grid(True)

        # Save the plot
        plot_path = os.path.join("", 'raft-by-time.png')
        plt.savefig(plot_path, bbox_inches='tight')
        plt.close()


def main():
    plotter = Plotter()
    plotter.plot_tasks_per_agent()
    plotter.plot_wait_time()


if __name__ == '__main__':
    main()
