#!/bin/bash

# Add Confluent repository key
wget -qO - https://packages.confluent.io/deb/7.7/archive.key | sudo apt-key add - > /dev/null

# Add Confluent repositories non-interactively
sudo DEBIAN_FRONTEND=noninteractive add-apt-repository -y "deb [arch=amd64] https://packages.confluent.io/deb/7.7 stable main" > /dev/null
sudo DEBIAN_FRONTEND=noninteractive add-apt-repository -y "deb https://packages.confluent.io/clients/deb $(lsb_release -cs) main" > /dev/null

# Update package list and install Confluent community edition
sudo DEBIAN_FRONTEND=noninteractive apt-get update -q > /dev/null && \
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -q confluent-community-2.13

sudo DEBIAN_FRONTEND=noninteractive apt install -y openjdk-11-jdk

# Configure Zookeeper properties
sudo tee /etc/kafka/zookeeper.properties > /dev/null <<EOL
tickTime=2000
dataDir=/var/lib/zookeeper/
clientPort=2181
initLimit=5
syncLimit=2
server.1=zoo-0:2888:3888
server.2=zoo-1:2888:3888
server.3=zoo-2:2888:3888
autopurge.snapRetainCount=3
autopurge.purgeInterval=24
EOL

# Write the first argument to /var/lib/zookeeper/myid
echo "$((i + 1))" | sudo tee /var/lib/zookeeper/myid > /dev/null
sudo sed -i "s/^broker.id=0/broker.id=$1/" /etc/kafka/server.properties
sudo sed -i "s/^zookeeper.connect=localhost:2181/zookeeper.connect=zoo-0:2181,zoo-1:2181,zoo-2:2181/" /etc/kafka/server.properties
sed -i 's/^#listeners=PLAINTEXT:\/\/:9092/listeners=PLAINTEXT:\/\/:9092/' /etc/kafka/server.properties

sudo systemctl enable confluent-zookeeper
sudo systemctl enable confluent-kafka
sudo systemctl start confluent-zookeeper
sudo systemctl start confluent-kafka

#if [ "$1" -eq 0 ]; then
#  sudo sed -i "s|^kafkastore.bootstrap.servers=PLAINTEXT://localhost:9092|kafkastore.bootstrap.servers=PLAINTEXT://zoo-0:9092,PLAINTEXT://zoo-1:9092,PLAINTEXT://zoo-2:9092|" /etc/schema-registry/schema-registry.properties
#  sudo systemctl enable confluent-schema-registry
#  sudo systemctl start confluent-schema-registry
#fi