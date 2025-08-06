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
# MIT License
# Author: Komal Thareja (kthare10@renci.org)

import json
import random
import argparse
import os
from typing import Dict, Any


class JobGenerator:
    """
    Generates random jobs and stores them in Redis or as JSON files.
    """

    def __init__(self, job_count: int = 0, dtn_json_path: str = None) -> None:
        """
        Initialize the JobGenerator.

        :param job_count: Number of jobs to generate
        :param dtn_json_path: Optional path to JSON file mapping agent_id -> list of DTNs
        """
        self.job_count = job_count
        self.agent_dtns_map = self._load_agent_dtns(dtn_json_path)

    def _load_agent_dtns(self, path: str) -> dict[int, list[str]]:
        if path and os.path.exists(path):
            with open(path, "r") as f:
                return json.load(f)
        return {}

    def generate_job(self, x: int) -> Dict[str, Any]:
        """
        Generate a single job dictionary with randomized fields.

        :param x: Job index (used as job ID)
        :return: Dictionary representing a job
        """
        job_id = str(x)
        execution_time = round(random.uniform(0.1, 30.0), 2)
        core = round(random.uniform(0.1, 4.0), 2)
        gpu = round(random.uniform(0.1, 4.0), 2)
        ram = round(random.uniform(0.1, 4.0), 2)
        disk = round(random.uniform(0.1, 4.0), 2)
        status = random.choice([0, -1])
        dtn_count = 10
        dtns = []
        if self.agent_dtns_map:
            # Choose a random agent to derive valid DTNs
            agent_id = random.choice(list(self.agent_dtns_map.keys()))
            candidate_dtns = self.agent_dtns_map[agent_id]
            dtn_count = random.randint(1, len(candidate_dtns))
            dtns = random.sample(candidate_dtns, dtn_count)
        else:
            # Fallback: random dtns
            dtns = [f"dtn{random.randint(1, dtn_count)}"]

        input_files = ['/var/tmp/outgoing/file100M.txt',
                       '/var/tmp/outgoing/file500M.txt',
                       '/var/tmp/outgoing/file1G.txt']

        output_files = ['/var/tmp/outgoing/file100M.txt',
                        '/var/tmp/outgoing/file500M.txt',
                        '/var/tmp/outgoing/file1G.txt']

        data_in = [
            {'name': random.choice(dtns),
             'file': random.choice(input_files)}
            for _ in range(random.randint(0, 3))
        ]
        data_out = [
            {'name': random.choice(dtns),
             'file': random.choice(output_files)}
            for _ in range(random.randint(0, 3))
        ]

        return {
            'id': job_id,
            'execution_time': execution_time,
            'capacities': {'core': core, 'ram': ram, 'disk': disk, 'gpu': gpu},
            'data_in': data_in,
            'data_out': data_out,
            'status': status
        }

    def generate_job_files(self, output_dir: str) -> None:
        """
        Generate individual job JSON files and save to the specified directory.

        :param output_dir: Path to directory where job files will be written
        """
        os.makedirs(output_dir, exist_ok=True)
        for x in range(self.job_count):
            job_data = self.generate_job(x)
            file_path = os.path.join(output_dir, f"job_{x}.json")
            with open(file_path, 'w') as f:
                json.dump(job_data, f, indent=4)
        print(f"{self.job_count} jobs written to {output_dir}")


def main(job_count: int, output_dir: str) -> None:
    """
    Entry point for generating jobs to a directory.

    :param job_count: Number of jobs to generate
    :param output_dir: Directory to save individual job JSON files
    """
    generator = JobGenerator(job_count=job_count)
    generator.generate_job_files(output_dir)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Generate job JSON files.")
    parser.add_argument('job_count', type=int, help="Number of jobs to generate")
    parser.add_argument('output_dir', type=str, help="Directory to save generated job files")
    args = parser.parse_args()
    main(job_count=args.job_count, output_dir=args.output_dir)
