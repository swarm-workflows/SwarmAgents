#!/bin/bash

# Update package list
sudo DEBIAN_FRONTEND=noninteractive apt update

# Install Git
sudo DEBIAN_FRONTEND=noninteractive apt install -y git

# Install Python 3.11 and dependencies
sudo DEBIAN_FRONTEND=noninteractive apt install -y python3.11 python3.11-venv python3.11-dev

# Download and install pip for Python 3.11
curl -O https://bootstrap.pypa.io/get-pip.py
sudo python3.11 get-pip.py

# Install Development Tools (build-essential on Ubuntu)
sudo DEBIAN_FRONTEND=noninteractive apt install -y build-essential

# Install dependencies from requirements.txt
pip3.11 install -r requirements.txt

# Add Docker's official GPG key
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg

# Set up the Docker stable repository
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Update package list again to include Docker packages
sudo DEBIAN_FRONTEND=noninteractive apt update

# Install Docker components
sudo DEBIAN_FRONTEND=noninteractive apt install -y docker-ce docker-ce-cli containerd.io

# Start and enable Docker service
sudo systemctl start docker
sudo systemctl enable docker

# Verify Docker installation
sudo docker --version
