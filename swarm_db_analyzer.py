import argparse
import redis
from collections import defaultdict

def parse_db(redis_client):
    """Parse Redis database keys directly."""
    data_by_phase = defaultdict(dict)

    keys = redis_client.keys("*")
    for key in keys:
        key_str = key if isinstance(key, str) else key.decode()
        value = redis_client.get(key)
        value_str = value if isinstance(value, str) else value.decode()

        try:
            if key_str.startswith("pre_prepare:"):
                _, job_id, agent_id = key_str.split(":")
                data_by_phase['pre_prepare'].setdefault(job_id, {})[agent_id] = float(value_str)
            elif key_str.startswith("prepare:"):
                _, job_id, agent_id = key_str.split(":")
                data_by_phase['prepare'].setdefault(job_id, {})[agent_id] = int(value_str)
            elif key_str.startswith("commit:"):
                _, job_id, agent_id = key_str.split(":")
                data_by_phase['commit'].setdefault(job_id, {})[agent_id] = int(value_str)
        except Exception:
            continue

    return data_by_phase

def analyze_db(data_by_phase, total_jobs):
    """Analyze missing commits and print phase-wise status."""
    missing_jobs = []
    expected_jobs = set(range(total_jobs))

    for job_id in expected_jobs:
        job_id_str = str(job_id)
        commit_votes = data_by_phase['commit'].get(job_id_str, {})
        if not commit_votes:
            missing_jobs.append(job_id)

    print("\n========= LIVE REDIS DB ANALYSIS =========")
    if missing_jobs:
        print(f"MISSING JOBS IN COMMIT PHASE ({len(missing_jobs)}): {sorted(missing_jobs)}")
        for job_id in missing_jobs:
            job_id_str = str(job_id)
            print(f"\nJob {job_id}:")
            print("  pre_prepare:", data_by_phase['pre_prepare'].get(job_id_str, {}))
            print("  prepare:", data_by_phase['prepare'].get(job_id_str, {}))
            print("  commit:", data_by_phase['commit'].get(job_id_str, {}))
    else:
        print("All jobs have commit votes ✅")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SWARM live Redis DB analyzer")
    parser.add_argument("--db_host", required=True, help="Redis host")
    parser.add_argument("--db_port", type=int, default=6379, help="Redis port (default 6379)")
    parser.add_argument("--expected_jobs", type=int, required=True, help="Total expected jobs")

    args = parser.parse_args()

    redis_client = redis.Redis(host=args.db_host, port=args.db_port, decode_responses=True)

    data_by_phase = parse_db(redis_client)
    analyze_db(data_by_phase, args.expected_jobs)
