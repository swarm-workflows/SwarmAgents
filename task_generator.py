import random
import threading
import time

import redis

from swarm.models.task import TaskRepository, Task


class TaskGenerator:
    def __init__(self, host: str = "localhost", port: int = 6379, task_count: int = 0):
        """
        Task generator
        :param host: Redis host
        :param port: Redis port
        :param task_count: Max Task Count
        """
        self.redis_client = redis.StrictRedis(host=host, port=port, decode_responses=True)
        self.task_repository = TaskRepository(self.redis_client)
        self.task_count = task_count
        self.shutdown_flag = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True, name="TaskGenerator")

    def generate_task(self, x):
        #task_id = hashlib.sha256(''.join(random.choices(string.ascii_lowercase, k=8)).encode()).hexdigest()[:8]
        task_id = str(x)

        no_op = round(random.uniform(0.1, 70.0), 2)  # Random sleep time between 0.1 and 70 seconds

        core = round(random.uniform(0.1, 16.0), 2)  # Simulate CPU request in floating point
        ram = round(random.uniform(0.1, 4.0), 2)  # Simulate RAM request in floating point
        disk = round(random.uniform(0.1, 4.0), 2)  # Simulate disk request in floating point

        remote_ips = ['192.158.2.1', '192.158.1.2', '192.158.4.2']
        input_files = ['/var/tmp/outgoing/file100M.txt', '/var/tmp/outgoing/file500M.txt', '/var/tmp/outgoing/file1G.txt']
        output_files = ['/var/tmp/outgoing/file100M.txt', '/var/tmp/outgoing/file500M.txt', '/var/tmp/outgoing/file1G.txt']

        data_in = []
        data_out = []

        # Randomly select remote IPs, input files, and output files from the provided lists
        for _ in range(random.randint(0, 3)):  # Random number of input files
            remote_ip = random.choice(remote_ips)
            remote_file = random.choice(input_files)
            data_in.append({'remote_ip': remote_ip, 'remote_file': remote_file, 'remote_user': 'root'})

        for _ in range(random.randint(0, 3)):  # Random number of output files
            remote_ip = random.choice(remote_ips)
            remote_file = random.choice(output_files)
            data_out.append({'remote_ip': remote_ip, 'remote_file': remote_file, 'remote_user': 'root'})

        task = {
            'id': task_id,
            'no_op': no_op,
            'capacities': {'core': core, 'ram': ram, 'disk': disk},
            'data_in': data_in,
            'data_out': data_out
        }

        return task

    def generate_and_store_task(self, x):
        task_data = self.generate_task(x)
        task = Task()
        task.from_dict(task_data)
        self.task_repository.save_task(task)

    def run(self):
        x = 0
        print(f"Starting with Task ID: {0}")
        while not self.shutdown_flag.is_set():
            self.generate_and_store_task(x)
            x += 1
            if self.task_count and x == self.task_count:
                break
            time.sleep(random.uniform(0.1, 1.0))

    def start(self):
        self.thread.start()

    def stop(self):
        self.shutdown_flag.set()
        if self.thread.is_alive():
            self.thread.join()

        self.redis_client.close()


def main():
    task_generator = TaskGenerator(task_count=100)

    task_generator.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        task_generator.stop()


if __name__ == '__main__':
    main()
