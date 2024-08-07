sudo dnf install -y git
sudo dnf install -y python3.11
curl -O https://bootstrap.pypa.io/get-pip.py
sudo python3.11 get-pip.py
sudo dnf groupinstall -y "Development Tools"
pip3.11 install -r requirements.txt