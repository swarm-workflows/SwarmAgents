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

import os
import json
import csv
import random
from typing import Dict, Any, List, Optional

class JobGenerator:
    """
    Generates random jobs and stores them as JSON files.
    """

    def __init__(self, job_count: int = 0, agent_profile_path: str = None) -> None:
        """
        Initialize the JobGenerator.

        :param job_count: Number of jobs to generate
        :param agent_profile_path: Path to JSON file mapping agent_id -> profile dict (capacities, dtns)
        """
        self.job_count = job_count
        self.agent_profiles = self._load_agent_profiles(agent_profile_path)

    def _load_agent_profiles(self, path: Optional[str]) -> Dict[str, Dict[str, Any]]:
        """
        Load agent profiles from a JSON file.
        Each profile should include capacities and optionally DTNs.
        """
        if path and os.path.exists(path):
            with open(path, "r") as f:
                return json.load(f)
        return {}

    @staticmethod
    def biased_uniform(min_val, max_val, bias_factor=3):
        """Generate values skewed toward the lower range."""
        return round(min_val + (max_val - min_val) * (random.random() ** bias_factor), 2)

    def generate_job(self, x: int, enable_dtns: bool) -> Dict[str, Any]:
        """
        Generate a single job dictionary with randomized fields,
        ensuring requirements fit within a randomly selected agent's profile.
        """
        job_id = str(x)
        exit_status = random.choice([0, -1])

        # Choose a random agent profile
        agent_id = random.choice(list(self.agent_profiles.keys()))
        agent_profile = self.agent_profiles[agent_id]

        # Ensure job requirements do not exceed agent's capacities
        core = self.biased_uniform(0.1, agent_profile.get("core", 2))
        gpu = self.biased_uniform(0.1, agent_profile.get("gpu", 0)) if agent_profile.get("gpu", 0) > 0 else 0
        ram = self.biased_uniform(0.1, agent_profile.get("ram", 8))
        disk = self.biased_uniform(1, agent_profile.get("disk", 100))
        wall_time = self.biased_uniform(0.1, 30.0)

        input_files = ['/var/tmp/outgoing/file100M.txt',
                       '/var/tmp/outgoing/file500M.txt',
                       '/var/tmp/outgoing/file1G.txt']

        output_files = ['/var/tmp/outgoing/file100M.txt',
                        '/var/tmp/outgoing/file500M.txt',
                        '/var/tmp/outgoing/file1G.txt']

        data_in, data_out = None, None
        if enable_dtns and "dtns" in agent_profile and agent_profile["dtns"]:
            candidate_dtns = agent_profile["dtns"]
            # Limit to max 2 DTNs to ensure jobs can be satisfied by multiple agents
            max_dtns = min(2, len(candidate_dtns))
            dtn_count = random.randint(1, max_dtns)
            dtns = random.sample(candidate_dtns, dtn_count)
            data_in = [
                {'name': dtn.get("name") if isinstance(dtn, dict) else dtn,
                 'file': random.choice(input_files)}
                for dtn in dtns
            ]
            data_out = [
                {'name': dtn.get("name") if isinstance(dtn, dict) else dtn,
                 'file': random.choice(output_files)}
                for dtn in dtns
            ]

        return {
            'id': job_id,
            'wall_time': wall_time,
            'capacities': {'core': core, 'ram': ram, 'disk': disk, 'gpu': gpu},
            'data_in': data_in if enable_dtns else None,
            'data_out': data_out if enable_dtns else None,
            'exit_status': exit_status
        }

    def is_job_feasible(self, job: Dict[str, Any], agent_profile: Dict[str, Any]) -> bool:
        """
        Check if a job is feasible for a given agent profile.

        :param job: Job dictionary with capacities requirements and optional DTN requirements
        :param agent_profile: Agent profile with available capacities and DTNs
        :return: True if agent can run the job, False otherwise
        """
        job_caps = job.get('capacities', {})
        agent_caps = agent_profile

        # Check each resource dimension
        if job_caps.get('core', 0) > agent_caps.get('core', 0):
            return False
        if job_caps.get('ram', 0) > agent_caps.get('ram', 0):
            return False
        if job_caps.get('disk', 0) > agent_caps.get('disk', 0):
            return False
        if job_caps.get('gpu', 0) > agent_caps.get('gpu', 0):
            return False

        # Check DTN requirements
        job_data_in = job.get('data_in', []) or []
        job_data_out = job.get('data_out', []) or []

        # Collect all required DTN names from data_in and data_out
        required_dtns = set()
        for data_node in job_data_in:
            if isinstance(data_node, dict):
                required_dtns.add(data_node.get('name'))
        for data_node in job_data_out:
            if isinstance(data_node, dict):
                required_dtns.add(data_node.get('name'))

        # If job requires DTNs, check if agent has them
        if required_dtns:
            agent_dtns = agent_profile.get('dtns', [])
            # Extract DTN names from agent profile
            agent_dtn_names = set()
            for dtn in agent_dtns:
                if isinstance(dtn, dict):
                    agent_dtn_names.add(dtn.get('name'))
                elif isinstance(dtn, str):
                    agent_dtn_names.add(dtn)

            # Check if agent has ALL required DTNs
            if not required_dtns.issubset(agent_dtn_names):
                return False

        return True

    def generate_feasibility_csv(self, jobs: List[Dict[str, Any]], output_path: str) -> None:
        """
        Generate a CSV file showing job-to-agent feasibility mapping.

        CSV format:
        job_id, core, ram, disk, gpu, feasible_agents, infeasible_agents, total_feasible, total_agents

        :param jobs: List of generated job dictionaries
        :param output_path: Path to save the CSV file
        """
        with open(output_path, 'w', newline='') as csvfile:
            fieldnames = [
                'job_id', 'core', 'ram', 'disk', 'gpu', 'wall_time',
                'feasible_agents', 'infeasible_agents',
                'total_feasible', 'total_agents', 'feasibility_pct'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for job in jobs:
                job_id = job['id']
                caps = job['capacities']

                # Check feasibility against all agents
                feasible_agents = []
                infeasible_agents = []

                for agent_id, agent_profile in self.agent_profiles.items():
                    if self.is_job_feasible(job, agent_profile):
                        feasible_agents.append(agent_id)
                    else:
                        infeasible_agents.append(agent_id)

                total_agents = len(self.agent_profiles)
                total_feasible = len(feasible_agents)
                feasibility_pct = (total_feasible / total_agents * 100) if total_agents > 0 else 0

                writer.writerow({
                    'job_id': job_id,
                    'core': caps.get('core', 0),
                    'ram': caps.get('ram', 0),
                    'disk': caps.get('disk', 0),
                    'gpu': caps.get('gpu', 0),
                    'wall_time': job.get('wall_time', 0),
                    'feasible_agents': ','.join(map(str, feasible_agents)),
                    'infeasible_agents': ','.join(map(str, infeasible_agents)),
                    'total_feasible': total_feasible,
                    'total_agents': total_agents,
                    'feasibility_pct': round(feasibility_pct, 2)
                })

    def generate_job_files(self, output_dir: str = "jobs", enable_dtns: bool = False) -> None:
        """
        Generate job files in the specified output directory and a feasibility CSV.
        """
        os.makedirs(output_dir, exist_ok=True)

        # Generate jobs and store them for feasibility analysis
        jobs = []
        for i in range(1, self.job_count + 1):
            job = self.generate_job(i, enable_dtns)
            jobs.append(job)

            # Save individual job JSON file
            job_path = os.path.join(output_dir, f"job_{i}.json")
            with open(job_path, "w") as f:
                json.dump(job, f, indent=2)

        # Generate feasibility CSV
        csv_path = os.path.join(output_dir, "job_feasibility_mapping.csv")
        self.generate_feasibility_csv(jobs, csv_path)
        print(f"Generated {len(jobs)} jobs in {output_dir}")
        print(f"Feasibility mapping saved to: {csv_path}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Generate jobs matching agent profiles.")
    parser.add_argument("--job-count", type=int, required=True, help="Number of jobs to generate")
    parser.add_argument("--agent-profile-path", type=str, required=True, help="Path to agent profiles JSON")
    parser.add_argument("--output-dir", type=str, default="jobs", help="Directory to save job files")
    parser.add_argument("--enable-dtns", action="store_true", help="Assign DTNs to jobs based on agent profiles")
    args = parser.parse_args()

    generator = JobGenerator(job_count=args.job_count, agent_profile_path=args.agent_profile_path)
    generator.generate_job_files(output_dir=args.output_dir, enable_dtns=args.enable_dtns)