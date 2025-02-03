#!/bin/bash
set -e
# this script assumes you have already run the install script

chmod +x processing_flow_py_39.py || { echo "Failed to set executable permissions for processing_flow_v2.py"; exit 1; }

# Run the Python script
python processing_flow_py_39.py || { echo "Failed to run the Python script"; exit 1; }
