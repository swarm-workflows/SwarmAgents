import json
import pyetcd
import redis
import argparse
# You may need to import specific exceptions for pyetcd if they are custom
# from pyetcd import EtcdException # Example, check pyetcd docs for actual exception classes

def display_tasks(redis_host='localhost', redis_port=6379, task_list='*', count=False):
    """Connects to Redis and displays tasks or task count from the specified task list."""
    try:
        redis_client = redis.StrictRedis(host=redis_host, port=redis_port, decode_responses=True)
        # A quick ping to verify connection
        redis_client.ping()
    except redis.exceptions.ConnectionError as e:
        print(f"Error connecting to Redis at {redis_host}:{redis_port}: {e}")
        return

    task_keys = redis_client.keys(f"{task_list}:*")  # Filter keys based on task list prefix

    if not task_keys:
        print(f"No tasks found in queue '{task_list}'.")
        return

    if count:
        print(f"Total tasks in '{task_list}': {len(task_keys)}")
    else:
        for key in task_keys:
            data = redis_client.get(key)
            print(f"{key}: {data}")


def display_etcd_tasks(etcd_host='localhost', etcd_port=2379, task_list='*', count=False):
    try:
        etcd_client = pyetcd.client(host=etcd_host, port=etcd_port)
        # You might need a more robust way to test connection with pyetcd,
        # as its initial client creation might not immediately raise an error.
        # A simple way is to perform a quick, non-disruptive operation:
        etcd_client.status() # This is a common method to check cluster status
    except Exception as e: # Catch a broader exception if pyetcd doesn't have a specific ConnectionError
        print(f"Error connecting to etcd at {etcd_host}:{etcd_port}: {e}")
        return

    if task_list == "*":
        prefix = "*"
    else:
        prefix = f"{task_list}/"

    # Encode prefix to bytes
    # Consider removing or commenting out this line unless you specifically want
    # to see ALL etcd entries at the root before your filtered results.
    # print(etcd_client.get_prefix("/"))
    if prefix == "*":
        entries = list(etcd_client.get_all())
    else:
        entries = list(etcd_client.get_prefix(prefix.encode()))

    if not entries:
        print(f"No entries found for prefix '{task_list}'.")
        return

    if count:
        print(f"Total entries for prefix '{task_list}': {len(entries)}")
    else:
        for value, meta in entries:
            key = meta.key.decode()
            try:
                decoded_value = json.loads(value.decode())
            except:
                decoded_value = value.decode()
            print(f"{key}: {decoded_value}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Display Redis task queue or count.")
    parser.add_argument("--host", default="localhost", help="Host (default: localhost)")
    parser.add_argument("--key", default="*", help="Task key prefix to match (default: *)")
    parser.add_argument("--count", action="store_true", help="Only display the count of entries")
    parser.add_argument("--type", choices=['redis', 'etcd'], default='etcd',
                        help="Type of data store to query (default: etcd)")

    args = parser.parse_args()

    if args.type == 'redis':
        display_tasks(redis_host=args.host, task_list=args.key, count=args.count)
    elif args.type == 'etcd':
        display_etcd_tasks(etcd_host=args.host, task_list=args.key, count=args.count)