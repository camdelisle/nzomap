#!/bin/bash
set -e

# Function to download a file with retries
download_with_retries() {
  local url=$1
  local output_file=$2
  local retries=5
  local wait_time=5

  for ((i = 1; i <= retries; i++)); do
    echo "Attempt $i: Downloading $url..."
    if curl -fLo "$output_file" "$url"; then
      echo "Download succeeded: $url"
      return 0
    else
      echo "Download failed: $url (attempt $i)"
    fi

    if [ $i -lt $retries ]; then
      echo "Retrying in $wait_time seconds..."
      sleep $wait_time
    else
      echo "Max retries reached. Failed to download $url"
      return 1
    fi
  done
}

# Update and install essential packages
sudo yum update -y && sudo yum install -y python3 python3-pip git unzip tar

# Clone the Python script repository
git clone https://github.com/camdelisle/nzomap.git /home/ubuntu/nzomap_processing || { echo "Failed to clone repository"; exit 1; }

# Define file URLs and their target names
LASTOOLS_URL="https://downloads.rapidlasso.de/LAStools.tar.gz"
LASTOOLS_FILE="LAStools.tar.gz"

KARTTAPULLAUTIN_URL="https://github.com/karttapullautin/karttapullautin/releases/download/v2.5.0/karttapullautin-x86_64-linux.tar.gz"
KARTTAPULLAUTIN_FILE="karttapullautin-x86_64-linux.tar.gz"

# Download files with retries
download_with_retries "$LASTOOLS_URL" "$LASTOOLS_FILE" || exit 1
download_with_retries "$KARTTAPULLAUTIN_URL" "$KARTTAPULLAUTIN_FILE" || exit 1

# Extract binaries
tar -xvf "$LASTOOLS_FILE" || { echo "Failed to extract $LASTOOLS_FILE"; exit 1; }
tar -xvf "$KARTTAPULLAUTIN_FILE" || { echo "Failed to extract $KARTTAPULLAUTIN_FILE"; exit 1; }

# Set permissions and move binaries
chmod +x bin/lastile64 || { echo "Failed to set executable permissions for lastile"; exit 1; }
chmod +x pullauta || { echo "Failed to set executable permissions for pullauta"; exit 1; }
sudo mv bin/lastile64 pullauta /home/ubuntu/nzomap_processing || { echo "Failed to move binaries"; exit 1; }

# Navigate to the project directory
cd /home/ubuntu/nzomap_processing || { echo "Failed to change directory"; exit 1; }

# Set up a virtual environment
python3 -m venv venv || { echo "Failed to create virtual environment"; exit 1; }
source venv/bin/activate || { echo "Failed to activate virtual environment"; exit 1; }

# Install Python dependencies
pip install -r requirements.txt || { echo "Failed to install Python dependencies"; exit 1; }

# Run the Python script
python processing_flow_v2.py || { echo "Failed to run the Python script"; exit 1; }