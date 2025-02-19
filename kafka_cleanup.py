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

import redis
from confluent_kafka.admin import AdminClient, NewTopic

from swarm.models.job import JobRepository


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


if __name__ == '__main__':
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Kafka topic management")
    parser.add_argument('--topic', type=str, required=True, help='Kafka topic name')
    parser.add_argument('--agents', type=int, required=False, help='Kafka topic name')
    parser.add_argument('--broker', type=str, required=False, help='Kafka Broker')

    # Parse command-line arguments
    args = parser.parse_args()
    topic_name = args.topic

    bootstrap_servers = "localhost:19092"
    if args.broker:
        bootstrap_servers = args.broker

    admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})

    if args.agents:
        for x in range(1, args.agents + 1):
            delete_topic(admin_client, f"{topic_name}-{x}")
            delete_topic(admin_client, f"{topic_name}-hb-{x}")
    else:
        # Delete the topic
        delete_topic(admin_client, topic_name)
        delete_topic(admin_client, f"{topic_name}-hb")

    time.sleep(10)

    if args.agents:
        for x in range(1, args.agents + 1):
            # Create the topic
            create_topic(admin_client, f"{topic_name}-{x}")
            time.sleep(5)

            create_topic(admin_client, f"{topic_name}-hb-{x}")
            time.sleep(5)
    else:
        # Create the topic
        create_topic(admin_client, topic_name)
        time.sleep(5)

        create_topic(admin_client, f"{topic_name}-hb")
        time.sleep(5)

    redis_client = redis.StrictRedis(host="127.0.0.1", port=6379, decode_responses=True)
    task_repo = JobRepository(redis_client=redis_client)
    task_repo.delete_all(key_prefix="*")
