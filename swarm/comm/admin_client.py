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

