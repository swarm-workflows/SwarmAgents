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
import pyetcd  # Keep pyetcd as per your current script
from confluent_kafka.admin import AdminClient, NewTopic

from swarm.database.etcd_repository import EtcdRepository
from swarm.database.repository import Repository


def delete_topic(admin_client, topic_name):
    """Delete the specified Kafka topic."""
    futures = admin_client.delete_topics([topic_name], operation_timeout=30)
    for topic, future in futures.items():
        try:
            future.result()  # The result itself is None
            print(f"Topic {topic} deleted successfully.")
        except Exception as e:
            print(f"Failed to delete topic {topic}: {e}")


def create_topic(admin_client, topic_name, num_partitions=1, replication_factor=1):
    """Create a new Kafka topic."""
    new_topic = NewTopic(topic_name, num_partitions, replication_factor)
    futures = admin_client.create_topics([new_topic])
    for topic, future in futures.items():
        try:
            future.result()  # The result itself is None
            print(f"Topic {topic} created successfully.")
        except Exception as e:
            print(f"Failed to create topic {topic}: {e}")


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


def display_etcd_tasks(etcd_host='localhost', etcd_port=2379, task_list='*', count=False):
    """Connects to etcd and displays tasks or task count from the specified prefix."""
    try:
        etcd_client = pyetcd.client(host=etcd_host, port=etcd_port)
        # Attempt a simple operation to verify connection, status() is a good choice if available
        # Note: pyetcd's client creation doesn't always fail immediately if host is unreachable.
        # A simple get or status call is more reliable.
        etcd_client.status()  # This is a common etcd v3 operation, check pyetcd's specific methods.
    except Exception as e:  # Catch broader exceptions for pyetcd connection issues
        print(f"Error connecting to etcd at {etcd_host}:{etcd_port}: {e}")
        return

    if task_list == "*":
        # In etcd, a prefix of b'/' means get all keys
        prefix_bytes = b"/"
        display_prefix = "ALL_KEYS (/)"
    else:
        # Etcd prefixes are exact; they don't use '*' wildcard in the same way Redis keys do.
        # If the user provides "my_tasks", we interpret it as "/my_tasks"
        prefix_bytes = f"/{task_list}".encode() if not task_list.startswith('/') else task_list.encode()
        display_prefix = task_list

    # print(etcd_client.get_prefix("/")) # Removed: This was printing the generator object for all keys
    # and might cause confusion.

    print(f"Fetching entries for etcd prefix '{display_prefix}'...")
    entries = list(etcd_client.get_prefix(prefix_bytes))

    if not entries:
        print(f"No entries found for prefix '{display_prefix}'.")
        return

    if count:
        print(f"Total entries for prefix '{display_prefix}': {len(entries)}")
    else:
        print(f"\n--- Etcd Entries for prefix '{display_prefix}' ---")
        for value, meta in entries:
            key = meta.key.decode()
            try:
                decoded_value = json.loads(value.decode())
            except (json.JSONDecodeError, UnicodeDecodeError):  # Catch specific decoding errors
                decoded_value = value.decode(errors='ignore')  # Decode, ignoring unreadable chars
            print(f"{key}: {decoded_value}")
        print(f"--- End Etcd Entries ---")


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
    parser.add_argument('--etcd-host', type=str, required=False, help='Etcd host for cleanup/display')
    parser.add_argument('--etcd-port', type=int, default=2379, help='Etcd port (default: 2379)')
    parser.add_argument('--display-key', type=str, default="*",
                        help="Key prefix to display (for Redis or etcd, default: *)")
    parser.add_argument('--display-count', action="store_true",
                        help="Only display the count of entries for display commands")
    parser.add_argument('--display-type', choices=['redis', 'etcd'],
                        help="Type of data store to display from (redis or etcd)")
    parser.add_argument('--cleanup-redis', action='store_true', help='Clean up all Redis tasks (deletes all keys)')
    parser.add_argument('--cleanup-etcd', action='store_true', help='Clean up all etcd entries (deletes all keys)')
    parser.add_argument('--cleanup-etcd-prefix', type=str, required=False,
                        help='Specify an etcd prefix to cleanup (e.g., "/my_tasks"). If not provided with --cleanup-etcd, all keys are deleted.')

    # Parse command-line arguments
    args = parser.parse_args()

    # Kafka Topic Management
    if args.topic:
        bootstrap_servers = args.broker

        try:
            admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
        except Exception as e:
            print(f"Error connecting to Kafka broker at {bootstrap_servers}: {e}")
            admin_client = None  # Ensure admin_client is None if connection fails

        if admin_client:
            print(f"\n--- Kafka Topic Operations for topic '{args.topic}' ---")
            if args.agents:
                for x in range(1, args.agents + 1):
                    delete_topic(admin_client, f"{args.topic}-{x}")
                    delete_topic(admin_client, f"{args.topic}-hb-{x}")
            else:
                delete_topic(admin_client, args.topic)
                delete_topic(admin_client, f"{args.topic}-hb")

            time.sleep(1)  # Give Kafka a moment

            if args.agents:
                for x in range(1, args.agents + 1):
                    create_topic(admin_client, f"{args.topic}-{x}")
                    time.sleep(0.5)  # Shorter sleep between creates
                    create_topic(admin_client, f"{args.topic}-hb-{x}")
                    time.sleep(0.5)
            else:
                create_topic(admin_client, args.topic)
                time.sleep(0.5)
                create_topic(admin_client, f"{args.topic}-hb")
                time.sleep(0.5)
            print(f"--- End Kafka Topic Operations ---")
        else:
            print("Kafka operations skipped due to connection error.")

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

    # Etcd Cleanup
    if args.cleanup_etcd:
        if not args.etcd_host:
            print("--cleanup-etcd requires --etcd-host to be specified.")
        else:
            print(f"\n--- Cleaning up etcd at {args.etcd_host}:{args.etcd_port} ---")
            try:
                task_repo = EtcdRepository(host=args.etcd_host, port=args.etcd_port)

                # Determine the prefix for etcd cleanup
                cleanup_prefix = args.cleanup_etcd_prefix if args.cleanup_etcd_prefix else "*"
                task_repo.delete_all(key_prefix=cleanup_prefix)

            except Exception as e:
                print(f"Error connecting to etcd for cleanup: {e}")
            print(f"--- End Etcd Cleanup ---")

    # Display operations (run after cleanup, if cleanup was requested)
    if args.display_type:
        if args.display_type == 'redis':
            if not args.redis_host:
                print("--display-type redis requires --redis-host to be specified.")
            else:
                display_tasks(redis_host=args.redis_host, redis_port=args.redis_port,
                              task_list=args.display_key, count=args.display_count)
        elif args.display_type == 'etcd':
            if not args.etcd_host:
                print("--display-type etcd requires --etcd-host to be specified.")
            else:
                display_etcd_tasks(etcd_host=args.etcd_host, etcd_port=args.etcd_port,
                                   task_list=args.display_key, count=args.display_count)