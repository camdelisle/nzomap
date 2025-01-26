#!/bin/bash
set -e

# Update and install essential packages
sudo apt-get update -y && sudo apt-get install -y python3 python3-venv python3-pip git curl unzip

# Install specific binaries (example: lastile and pullauta)
# Assuming binaries are available in a public S3 bucket or pre-installed in a custom AMI
curl -O https://downloads.rapidlasso.de/LAStools.tar.gz
curl -O https://github.com/karttapullautin/karttapullautin/releases/download/v2.5.0/karttapullautin-x86_64-linux.tar.gz
tar -xf LAStools.tar.gz
tar -xf karttapullautin-x86_64-linux.tar.gz


chmod +x lastile pullauta
sudo mv lastile pullauta /home/ubuntu/nzomap_processing

# Clone the Python script repository
git clone https://github.com/nzomap/nzomap_processing.git /home/ubuntu/nzomap_processing

# Navigate to the project directory
cd /home/ubuntu/nzomap_processing

# Set up a virtual environment
python3 -m venv venv
source venv/bin/activate

# Install Python dependencies
pip install -r requirements.txt

# Run the Python script
python processing_flow_v2.py