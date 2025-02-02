# nzomap
Tools used to create NZ omap

Prerequisites:
1. AWS S3 access to NZ omap bucket
2. 15GB free disk space (20GB recommended) - to download lidar files

Setting up S3 access:
1. Contact Cameron for access key
2. Download and install aws cli
3. Run 'aws configure' command in a terminal
4. Use credentials as follows:

AWS Access Key ID [None]: {as provided}

AWS Secret Access Key [None]: {as provided}

Default region name [None]: us-east-2

Default output format [None]: json


Linux:
1. Ensure you have a x86 system (have not found an ARM version of lastile64)
2. Run a bash script like the example script (designed for Amazon Linux 2 - which is an RPM-based system, if you are using a different linux system type you may have issues) under processing to download dependencies and start processing
4. Note you need Python 3.9, so you may need to adjust the script to ensure you get the version correct


Windows:
1. Download dependencies manually (check bash script under processing for sources) - no batch script available yet
2. processing_flow_py_39.py should also work on windows (not tested)
