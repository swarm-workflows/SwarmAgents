#!/bin/bash

# Update package index
sudo apt-get update -q

# Install required packages
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -q apt-transport-https ca-certificates curl software-properties-common

# Add Docker GPG key
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg

# Add Docker repository
echo "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | \
sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Update package list again with Docker repository
sudo apt-get update -q

# Create Docker configuration directory
sudo mkdir -p /etc/docker

# Copy daemon.json file for Docker configuration if it exists
script_dir="$(dirname "$0")"
if [ -f "${script_dir}/docker/daemon.json" ]; then
  sudo cp "${script_dir}/docker/daemon.json" /etc/docker/daemon.json
else
  echo "Warning: ${script_dir}/docker/daemon.json not found. Skipping Docker daemon configuration."
fi

# Install Docker and its components
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -q docker-ce docker-ce-cli containerd.io

# Add the ubuntu user to the Docker group to allow non-root Docker usage
sudo usermod -aG docker ubuntu
