from confluent_kafka.admin import AdminClient, NewTopic


class AdminClientHelper:
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        # Create a Kafka AdminClient instance
        self.admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})

    def create_topic(self, topic_name, partitions=1, replication_factor=1):
        # Create a NewTopic object with the specified settings
        new_topic = NewTopic(topic_name, num_partitions=partitions, replication_factor=replication_factor)
        # Call the create_topics method of the AdminClient to create the topic
        self.admin_client.create_topics([new_topic])
        print(f"Topic '{topic_name}' created successfully.")

    def delete_topic(self, topic_name):
        # Call the delete_topics method of the AdminClient to delete the topic
        self.admin_client.delete_topics([topic_name])
        print(f"Topic '{topic_name}' deleted successfully.")

