import re
import glob
import argparse
from collections import defaultdict

LOG_PATTERN = re.compile(r'\[EXECUTE\] Starting job (\d+) on agent (\d+)')

def parse_logs(log_dir: str):
    """
    Parse agent logs and extract executed job IDs per agent.

    Args:
        log_dir (str): Directory containing agent logs.

    Returns:
        Dict[int, set]: Mapping of agent_id -> executed job IDs.
    """
    job_map = defaultdict(set)

    for logfile in glob.glob(f"{log_dir}/agent-*.log"):
        with open(logfile, 'r') as f:
            for line in f:
                match = LOG_PATTERN.search(line)
                if match:
                    job_id = int(match.group(1))
                    agent_id = int(match.group(2))
                    job_map[agent_id].add(job_id)

    return job_map

def check_consistency(job_map: dict, expected_total_jobs: int):
    """
    Perform duplicate and missing job checks.

    Args:
        job_map (dict): Mapping of agent_id -> executed job IDs.
        expected_total_jobs (int): Total number of jobs expected.
    """
    all_jobs = set()
    duplicates = set()

    for agent, jobs in job_map.items():
        for job in jobs:
            if job in all_jobs:
                duplicates.add(job)
            else:
                all_jobs.add(job)

    missing_jobs = set(range(expected_total_jobs)) - all_jobs

    print(f"\n========= SWARM EXECUTION VERIFICATION =========")
    print(f"Total agents: {len(job_map)}")
    print(f"Total jobs expected: {expected_total_jobs}")
    print(f"Total unique jobs executed: {len(all_jobs)}")

    if duplicates:
        print(f"\nDUPLICATE JOBS FOUND ({len(duplicates)}): {sorted(duplicates)}")
    else:
        print("\nNo duplicate jobs found ✅")

    if missing_jobs:
        print(f"\nMISSING JOBS ({len(missing_jobs)}): {sorted(missing_jobs)}")
    else:
        print("\nNo missing jobs ✅")

    print("\nJobs executed per agent:")
    for agent in sorted(job_map):
        print(f"  Agent {agent}: {len(job_map[agent])} jobs")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SWARM job execution verifier")
    parser.add_argument("--log_dir", required=True, help="Directory containing agent logs")
    parser.add_argument("--expected_jobs", type=int, required=True, help="Total number of expected jobs")

    args = parser.parse_args()

    job_map = parse_logs(args.log_dir)
    check_consistency(job_map, args.expected_jobs)
