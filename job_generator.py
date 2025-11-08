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
            dtn_count = min(random.randint(1, len(candidate_dtns)), len(candidate_dtns))
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

    def generate_job_files(self, output_dir: str = "jobs", enable_dtns: bool = False) -> None:
        """
        Generate job files in the specified output directory.
        """
        os.makedirs(output_dir, exist_ok=True)
        for i in range(1, self.job_count + 1):
            job = self.generate_job(i, enable_dtns)
            job_path = os.path.join(output_dir, f"job_{i}.json")
            with open(job_path, "w") as f:
                json.dump(job, f, indent=2)

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