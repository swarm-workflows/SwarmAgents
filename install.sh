sudo dnf install -y git
sudo dnf install -y python3.11
curl -O https://bootstrap.pypa.io/get-pip.py
sudo python3.11 get-pip.py
sudo dnf groupinstall -y "Development Tools"
pip3.11 install -r requirements.txt
sudo dnf config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
sudo dnf install -y docker-ce docker-ce-cli containerd.io
sudo systemctl start docker
sudo systemctl enable docker
sudo docker --version
