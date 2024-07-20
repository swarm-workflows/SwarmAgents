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

    def __count_tasks_by_state(self):
        state_counts = defaultdict(int)
        tasks = self.task_repository.get_all_tasks(key_prefix="allocated")
        for task in tasks:
            state = task.state.value
            if state is not None:
                state_counts[state] += 1
        return state_counts

    def plot_state_counts(self):
        state_counts = self.__count_tasks_by_state()
        states = list(state_counts.keys())
        counts = list(state_counts.values())

        plt.bar(states, counts, color='blue')
        plt.xlabel('Task State')
        plt.ylabel('Number of Tasks')
        plt.title('Number of Tasks in Different States')
        plt.xticks(states)
        #plt.show()
        plot_path = os.path.join("", 'raft-by-states.png')
        plt.savefig(plot_path)
        plt.close()

    def __count_tasks_by_agent(self):
        agent_counts = defaultdict(int)
        tasks = self.task_repository.get_all_tasks()
        for task in tasks:
            agent_id = task.leader_agent_id
            if agent_id is not None:
                agent_counts[agent_id] += 1
        return agent_counts

    def plot_agent_counts(self):
        agent_counts = self.__count_tasks_by_agent()
        agents = list(agent_counts.keys())
        counts = list(agent_counts.values())

        plt.bar(agents, counts, color='blue')
        plt.xlabel('Agent')
        plt.ylabel('Number of Tasks')
        plt.title('Number of Tasks on Different Agents')
        plt.xticks(agents)
        #plt.show()
        plot_path = os.path.join("", 'raft-by-agent.png')
        plt.savefig(plot_path)
        plt.close()


    def __get_tasks_wait_times(self):
        wait_times = {}
        tasks = self.task_repository.get_all_tasks()
        for task in tasks:
            if task.time_on_queue is not None:
                wait_times[task.get_task_id()] = task.time_on_queue
        return wait_times

    def plot_wait_time(self):
        wait_times = self.__get_tasks_wait_times()
        tasks = list(wait_times.keys())
        times = list(wait_times.values())

        plt.bar(tasks, times, color='blue')
        plt.xlabel('Task ID')
        plt.ylabel('Time on Queue (seconds)')
        plt.title('Wait Time for Different Tasks')
        plt.xticks(rotation=90)  # Rotate x labels for better readability if there are many tasks
        #plt.show()
        plot_path = os.path.join("", 'raft-by-time.png')
        plt.savefig(plot_path)
        plt.close()


def main():
    plotter = Plotter()
    plotter.plot_state_counts()
    plotter.plot_agent_counts()
    plotter.plot_wait_time()


if __name__ == '__main__':
    main()
