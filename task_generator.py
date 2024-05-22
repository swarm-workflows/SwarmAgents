import json
import random
import string
import hashlib
from typing import List


class TaskGenerator:
    def __init__(self, *, remote_ips: List[str], input_files: List[str], output_files: List[str],
                 max_core: float, max_ram: float, max_disk: float, max_no_op: float):
        """
        Task generator
        :param remote_ips:
        :param input_files:
        :param output_files:
        :param max_core:
        :param max_ram:
        :param max_disk:
        :param max_no_op:
        """
        self.remote_ips = remote_ips
        self.input_files = input_files
        self.output_files = output_files
        self.max_core = max_core
        self.max_disk = max_disk
        self.max_ram = max_ram
        self.max_no_op = max_no_op

    def generate_task(self, x):
        task_id = hashlib.sha256(''.join(random.choices(string.ascii_lowercase, k=8)).encode()).hexdigest()[:8]
        task_id = str(x)

        no_op = round(random.uniform(0.1, self.max_no_op), 2)  # Random sleep time between 0.1 and 10 seconds

        core = round(random.uniform(0.1, self.max_core), 2)  # Simulate CPU request in floating point
        ram = round(random.uniform(0.1, self.max_ram), 2)  # Simulate RAM request in floating point
        disk = round(random.uniform(0.1, self.max_disk), 2)  # Simulate disk request in floating point

        data_in = []
        data_out = []

        # Randomly select remote IPs, input files, and output files from the provided lists
        for _ in range(random.randint(0, 3)):  # Random number of input files
            remote_ip = random.choice(self.remote_ips)
            remote_file = random.choice(self.input_files)
            data_in.append({'remote_ip': remote_ip, 'remote_file': remote_file, 'remote_user': 'root'})

        for _ in range(random.randint(0, 3)):  # Random number of output files
            remote_ip = random.choice(self.remote_ips)
            remote_file = random.choice(self.output_files)
            data_out.append({'remote_ip': remote_ip, 'remote_file': remote_file, 'remote_user': 'root'})

        task = {
            'id': task_id,
            'no_op': no_op,
            'capacities': {'core': core, 'ram': ram, 'disk': disk},
            'data_in': data_in,
            'data_out': data_out
        }

        return task


if __name__ == '__main__':
    # Example usage:
    remote_ips = ['192.158.2.1', '192.158.1.2', '192.158.4.2']
    input_files = ['/var/tmp/outgoing/file100M.txt', '/var/tmp/outgoing/file500M.txt', '/var/tmp/outgoing/file1G.txt']
    output_files = ['/var/tmp/outgoing/file100M.txt', '/var/tmp/outgoing/file500M.txt', '/var/tmp/outgoing/file1G.txt']
    max_core = 6.0  # Maximum CPU capacity in floating point
    max_ram = 10.0  # Maximum RAM capacity in floating point
    max_disk = 10.0  # Maximum disk capacity in floating point

    task_generator = TaskGenerator(remote_ips=remote_ips, input_files=input_files, output_files=output_files,
                                   max_core=max_core, max_ram=max_ram, max_disk=max_disk, max_no_op=70)

    # Generate 1000 tasks
    tasks = [task_generator.generate_task(x) for x in range(100)]

    # Serialize tasks to JSON
    tasks_json = json.dumps(tasks, indent=4)

    # Write JSON to a file
    with open('tasks.json', 'w') as f:
        f.write(tasks_json)

    print("Tasks dumped to tasks.json file.")