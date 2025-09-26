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
import argparse
import time
import json  # Import json for decoding values if needed

import redis

from swarm.database.repository import Repository

def display_tasks(redis_host='localhost', redis_port=6379, task_list='*', count=False):
    """Connects to Redis and displays tasks or task count from the specified task list."""
    try:
        redis_client = redis.StrictRedis(host=redis_host, port=redis_port, decode_responses=True)
        redis_client.ping()  # Test connection
    except redis.exceptions.ConnectionError as e:
        print(f"Error connecting to Redis at {redis_host}:{redis_port}: {e}")
        return

    task_keys = redis_client.keys(f"{task_list}:*")  # Filter keys based on task list prefix

    if not task_keys:
        print(f"No tasks found in Redis queue '{task_list}'.")
        return

    if count:
        print(f"Total tasks in Redis queue '{task_list}': {len(task_keys)}")
    else:
        print(f"\n--- Redis Tasks for prefix '{task_list}' ---")
        for key in task_keys:
            data = redis_client.get(key)
            print(f"{key}: {data}")
        print(f"--- End Redis Tasks ---")

if __name__ == '__main__':
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Kafka topic, Redis, and etcd management and cleanup.")
    parser.add_argument('--topic', type=str, required=False, help='Kafka topic name (for create/delete)')
    parser.add_argument('--agents', type=int, required=False,
                        help='Number of agents (for Kafka topic naming convention)')
    parser.add_argument('--broker', type=str, default="localhost:19092",
                        help='Kafka Broker address (default: localhost:19092)')
    parser.add_argument('--redis-host', type=str, required=False, help='Redis host for cleanup/display')
    parser.add_argument('--redis-port', type=int, default=6379, help='Redis port (default: 6379)')
    parser.add_argument('--display-key', type=str, default="*",
                        help="Key prefix to display (for Redis or etcd, default: *)")
    parser.add_argument('--display-count', action="store_true",
                        help="Only display the count of entries for display commands")
    parser.add_argument('--display-type', choices=['redis', 'etcd'], default='redis',
                        help="Type of data store to display from (redis or etcd)")
    parser.add_argument('--cleanup-redis', action='store_true', help='Clean up all Redis tasks (deletes all keys)')

    # Parse command-line arguments
    args = parser.parse_args()

    # Redis Cleanup
    if args.cleanup_redis:
        if not args.redis_host:
            print("--cleanup-redis requires --redis-host to be specified.")
        else:
            print(f"\n--- Cleaning up Redis at {args.redis_host}:{args.redis_port} ---")
            try:
                redis_client = redis.StrictRedis(host=args.redis_host, port=args.redis_port, decode_responses=True)
                redis_client.ping()  # Test connection
                task_repo = Repository(redis_client=redis_client)
                # Redis delete_all in Repository uses key_prefix="*" to delete all
                task_repo.delete_all(key_prefix="*")
            except redis.exceptions.ConnectionError as e:
                print(f"Error connecting to Redis for cleanup: {e}")
            print(f"--- End Redis Cleanup ---")


    # Display operations (run after cleanup, if cleanup was requested)
    if args.display_type:
        if args.display_type == 'redis':
            if not args.redis_host:
                print("--display-type redis requires --redis-host to be specified.")
            else:
                display_tasks(redis_host=args.redis_host, redis_port=args.redis_port,
                              task_list=args.display_key, count=args.display_count)