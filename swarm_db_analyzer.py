import argparse
import redis
import pyetcd
from collections import defaultdict
import json

def parse_redis_db(redis_client):
    """Parse Redis database keys."""
    data_by_phase = defaultdict(dict)
    all_known_job_ids = set()

    keys = redis_client.keys("*")
    for key_bytes in keys:
        key_str = key_bytes.decode()

        try:
            value = redis_client.get(key_bytes)
            value_str = value.decode() if value else ""

            if key_str.startswith("pre_prepare:"):
                parts = key_str.split(":")
                if len(parts) == 3:
                    _, job_id, agent_id = parts
                    data_by_phase['pre_prepare'].setdefault(job_id, {})[agent_id] = float(value_str)
                    all_known_job_ids.add(job_id)
            elif key_str.startswith("prepare:"):
                parts = key_str.split(":")
                if len(parts) == 3:
                    _, job_id, agent_id = parts
                    data_by_phase['prepare'].setdefault(job_id, {})[agent_id] = int(value_str)
                    all_known_job_ids.add(job_id)
            elif key_str.startswith("commit:"):
                parts = key_str.split(":")
                if len(parts) == 3:
                    _, job_id, agent_id = parts
                    data_by_phase['commit'].setdefault(job_id, {})[agent_id] = int(value_str)
                    all_known_job_ids.add(job_id)
            # Add other key prefixes if you store other data in Redis (e.g., "job:")
            # elif key_str.startswith("job:"):
            #     parts = key_str.split(":")
            #     if len(parts) == 2:
            #         _, job_id = parts
            #         all_known_job_ids.add(job_id)

        except (ValueError, IndexError, UnicodeDecodeError) as e:
            print(f"Warning (Redis): Skipping malformed key or value '{key_str}' / '{value_str}' due to error: {e}")
            continue

    return data_by_phase, all_known_job_ids


def parse_etcd_db(etcd_client):
    """Parse etcd database keys."""
    data_by_phase = defaultdict(dict)
    all_known_job_ids = set()

    try:
        all_entries = etcd_client.get_all()
    except Exception as e:
        print(f"Error: Failed to fetch entries from etcd: {e}")
        return data_by_phase, all_known_job_ids

    for value_bytes, meta in all_entries:
        key_str = meta.key.decode()

        try:
            value_str = value_bytes.decode()

            parts = key_str.split("/")
            if len(parts) >= 3:
                prefix = parts[0]
                job_id = parts[1]
                agent_id = parts[2]

                if prefix == "pre_prepare":
                    data_by_phase['pre_prepare'].setdefault(job_id, {})[agent_id] = float(value_str)
                    all_known_job_ids.add(job_id)
                elif prefix == "prepare":
                    data_by_phase['prepare'].setdefault(job_id, {})[agent_id] = int(value_str)
                    all_known_job_ids.add(job_id)
                elif prefix == "commit":
                    data_by_phase['commit'].setdefault(job_id, {})[agent_id] = int(value_str)
                    all_known_job_ids.add(job_id)
            elif len(parts) == 2:
                prefix = parts[0]
                obj_id = parts[1]
                if prefix == "job" or prefix == "agent":
                    all_known_job_ids.add(obj_id)

        except (ValueError, IndexError, UnicodeDecodeError, json.JSONDecodeError) as e:
            print(f"Warning (etcd): Skipping malformed key or value '{key_str}' / '{value_str}' due to error: {e}")
            continue

    return data_by_phase, all_known_job_ids


def analyze_db(data_by_phase, all_known_job_ids, db_type, expected_jobs=None):
    """Analyze missing commits and print phase-wise status."""
    jobs_to_check = set()
    analysis_scope = ""

    if expected_jobs is not None:
        # If expected_jobs is provided, check against that specific range
        jobs_to_check = set(str(i) for i in range(expected_jobs))
        analysis_scope = f" (out of {expected_jobs} expected jobs)"
    else:
        # Otherwise, check all jobs found in the database
        jobs_to_check = all_known_job_ids
        analysis_scope = f" (out of {len(all_known_job_ids)} discovered jobs)"


    missing_jobs_in_commit = []
    for job_id in sorted(list(jobs_to_check)):
        commit_votes = data_by_phase['commit'].get(job_id, {})
        if not commit_votes:
            missing_jobs_in_commit.append(job_id)

    print(f"\n========= LIVE {db_type.upper()} DB ANALYSIS =========")
    if missing_jobs_in_commit:
        print(f"MISSING JOBS IN COMMIT PHASE ({len(missing_jobs_in_commit)}){analysis_scope}: {sorted(missing_jobs_in_commit)}")
        for job_id in missing_jobs_in_commit:
            print(f"\n--- Job {job_id} Details ---")
            pre_prepare_data = data_by_phase['pre_prepare'].get(job_id, {})
            prepare_data = data_by_phase['prepare'].get(job_id, {})
            commit_data = data_by_phase['commit'].get(job_id, {})

            print(f"  Pre-prepare votes ({len(pre_prepare_data)}): {pre_prepare_data}")
            print(f"  Prepare votes ({len(prepare_data)}): {prepare_data}")
            print(f"  Commit votes ({len(commit_data)}): {commit_data}")
        print("\n--- End of Missing Jobs Report ---")
    else:
        print(f"All jobs in commit phase are present {analysis_scope} ✅")

    '''
    # Overall Job Status Summary (always based on all jobs found in the DB, not just expected)
    if all_known_job_ids:
        print(f"\n--- Overall Job Status Summary ({len(all_known_job_ids)} unique jobs discovered in DB) ---")
        for job_id in sorted(list(all_known_job_ids)):
            pre_p_count = len(data_by_phase['pre_prepare'].get(job_id, {}))
            prep_count = len(data_by_phase['prepare'].get(job_id, {}))
            comm_count = len(data_by_phase['commit'].get(job_id, {}))
            status = "COMPLETE" if comm_count > 0 else "INCOMPLETE"
            print(f"  Job {job_id}: Pre-P: {pre_p_count}, Prep: {prep_count}, Commit: {comm_count} [{status}]")
        print("--- End of Overall Job Status Summary ---")
    '''


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SWARM live Redis/etcd DB analyzer")
    parser.add_argument("--db-type", choices=['redis', 'etcd'], required=True,
                        help="Type of database to connect to (redis or etcd)")
    parser.add_argument("--db-host", required=True, help="Database host")
    parser.add_argument("--db-port", type=int, help="Database port (default: 6379 for Redis, 2379 for etcd)")
    parser.add_argument("--expected-jobs", type=int, default=None,
                        help="Total expected jobs (integer IDs from 0 to N-1). If not provided, all discovered jobs are analyzed.")


    args = parser.parse_args()

    # Set default port based on type if not provided
    if args.db_port is None:
        if args.db_type == 'redis':
            args.db_port = 6379
        elif args.db_type == 'etcd':
            args.db_port = 2379

    client = None
    data_by_phase = defaultdict(dict)
    all_known_job_ids = set() # This will hold all job IDs found in the database

    if args.db_type == 'redis':
        try:
            client = redis.Redis(host=args.db_host, port=args.db_port, decode_responses=True)
            client.ping()
            print(f"Connected to Redis at {args.db_host}:{args.db_port}")
            data_by_phase, all_known_job_ids = parse_redis_db(client)
        except redis.exceptions.ConnectionError as e:
            print(f"Error: Could not connect to Redis at {args.db_host}:{args.db_port} - {e}")
            exit(1)
    elif args.db_type == 'etcd':
        try:
            client = pyetcd.client(host=args.db_host, port=args.db_port)
            client.exit_status()
            print(f"Connected to etcd at {args.db_host}:{args.db_port}")
            data_by_phase, all_known_job_ids = parse_etcd_db(client)
        except Exception as e:
            print(f"Error: Could not connect to etcd at {args.db_host}:{args.db_port} - {e}")
            exit(1)

    if client:
        analyze_db(data_by_phase, all_known_job_ids, args.db_type, expected_jobs=args.expected_jobs)