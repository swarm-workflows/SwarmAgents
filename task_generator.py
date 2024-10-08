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
import random
import threading
import time
import argparse

import redis

from swarm.models.job import JobRepository, Job


class TaskGenerator:
    def __init__(self, host: str = "localhost", port: int = 6379, task_count: int = 0):
        """
        Task generator
        :param host: Redis host
        :param port: Redis port
        :param task_count: Max Task Count
        """
        self.redis_client = redis.StrictRedis(host=host, port=port, decode_responses=True)
        self.task_repository = JobRepository(self.redis_client)
        self.task_count = task_count
        self.shutdown_flag = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True, name="TaskGenerator")

    def generate_task(self, x):
        # task_id = hashlib.sha256(''.join(random.choices(string.ascii_lowercase, k=8)).encode()).hexdigest()[:8]
        task_id = str(x)

        no_op = round(random.uniform(0.1, 30.0), 2)  # Random sleep time between 0.1 and 70 seconds

        core = round(random.uniform(0.1, 4.0), 2)  # Simulate CPU request in floating point
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
        task = Job()
        task.from_dict(task_data)
        self.task_repository.save_job(task)

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

    def generate_task_file(self, file_name: str):
        tasks = []
        for x in range(self.task_count):
            task_data = self.generate_task(x)
            tasks.append(task_data)

        with open(file_name, 'w') as file:
            json.dump(tasks, file, indent=4)
        print(f"Tasks saved to {file_name}")


def main(task_count: int):
    task_generator = TaskGenerator(task_count=task_count)
    task_generator.generate_task_file(file_name="tasks.json")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Generate tasks for the swarm.")
    parser.add_argument('task_count', type=int, help="Number of tasks to generate")
    args = parser.parse_args()
    main(task_count=args.task_count)
