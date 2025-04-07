#!/bin/bash

# Update package list
sudo DEBIAN_FRONTEND=noninteractive apt update

# Install Git
sudo DEBIAN_FRONTEND=noninteractive apt install -y git software-properties-common -y
sudo DEBIAN_FRONTEND=noninteractive add-apt-repository ppa:deadsnakes/ppa -y
sudo DEBIAN_FRONTEND=noninteractive apt update

# Install Python 3.11 and dependencies
sudo DEBIAN_FRONTEND=noninteractive apt install -y python3.11 python3.11-venv python3.11-dev

# Download and install pip for Python 3.11
curl -O https://bootstrap.pypa.io/get-pip.py
sudo python get-pip.py

# Install Development Tools (build-essential on Ubuntu)
sudo DEBIAN_FRONTEND=noninteractive apt install -y build-essential

# Install dependencies from requirements.txt
pip3 install -r requirements.txt
