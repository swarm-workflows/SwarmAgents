import time

import redis
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Consumer, TopicPartition

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
    bootstrap_servers = "localhost:19092"
    topic_name = "agent_load"

    admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
    # Delete the topic
    delete_topic(admin_client, topic_name)
    time.sleep(10)

    # Create the topic
    create_topic(admin_client, topic_name)
    time.sleep(5)

    redis_client = redis.StrictRedis(host="127.0.0.1", port=6379, decode_responses=True)
    task_repo = JobRepository(redis_client=redis_client)
    task_repo.delete_all(key_prefix="*")
