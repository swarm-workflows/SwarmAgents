import json
import redis
import argparse
# You may need to import specific exceptions for pyetcd if they are custom
# from pyetcd import EtcdException # Example, check pyetcd docs for actual exception classes


def display(redis_host='localhost', redis_port=6379, obj_list='*', count=False):
    """Connects to Redis and displays objects or object count from the specified object list."""
    try:
        redis_client = redis.StrictRedis(host=redis_host, port=redis_port, decode_responses=True)
        # A quick ping to verify connection
        redis_client.ping()
    except redis.exceptions.ConnectionError as e:
        print(f"Error connecting to Redis at {redis_host}:{redis_port}: {e}")
        return

    obj_keys = redis_client.keys(f"{obj_list}:*")  # Filter keys based on object list prefix

    if not obj_keys:
        print(f"No objects found in queue '{obj_list}'.")
        return

    if count:
        print(f"Total objects in '{obj_list}': {len(obj_keys)}")
    else:
        for key in obj_keys:
            key_type = redis_client.type(key)
            if key_type == "set":
                data = redis_client.smembers(key)
            elif key_type == "string":
                data = redis_client.get(key)
            else:
                data = f"[{key_type} not supported for display]"
            print(f"{key}: {data}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Display Redis queue or count.")
    parser.add_argument("--host", default="localhost", help="Host (default: localhost)")
    parser.add_argument("--key", default="*", help="Key prefix to match (default: *)")
    parser.add_argument("--count", action="store_true", help="Only display the count of entries")
    parser.add_argument("--type", choices=['redis', 'etcd'], default='redis',
                        help="Type of data store to query (default: etcd)")

    args = parser.parse_args()

    if args.type == 'redis':
        display(redis_host=args.host, obj_list=args.key, count=args.count)
