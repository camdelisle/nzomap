import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
import boto3
from botocore.config import Config
import shutil
import requests
import json

# S3 client configurations
s3_nz = boto3.client('s3')  # S3 client with full access

s3 = boto3.client('s3',endpoint_url='https://opentopography.s3.sdsc.edu', aws_access_key_id='', aws_secret_access_key='')
s3._request_signer.sign = (lambda *args, **kwargs: None)

# Command-line tool configurations
if os.name == 'nt':
    pullauta = 'pullauta'
    lastile = 'lastile64'
else:
    pullauta = './pullauta'
    lastile = './lastile64'

cores = 4  # Number of cores to use for processing

# Utility Functions

def ensure_dir(directory):
    """Creates a directory if it doesn't exist."""
    os.makedirs(directory, exist_ok=True)

def write_file(file_path, content_lines):
    """Writes a list of lines to a file."""
    with open(file_path, 'w') as f:
        f.writelines(f"{line}\n" for line in content_lines)


async def upload_files(output_folder, chunk_id, xmin, ymin):
    files = [f for f in os.listdir(output_folder) if f.endswith('.laz_depr.png')]

    async def upload(file):
        file_name = os.path.basename(file)
        file_name = file_name.replace('.laz_depr.png', '')
        x, y = map(int, file_name.split('_')[1:3])
        if xmin <= x < xmin + 5000 and ymin <= y < ymin + 5000:
            x_fix, y_fix = x // 200, (6553600 - y) // 200
            await asyncio.to_thread(
                s3_nz.upload_file, file, 'nzomap', f'tiles/15/{x_fix}/{y_fix}.png'
            )

    tasks = [upload(os.path.join(output_folder, file)) for file in files]
    await asyncio.gather(*tasks)

    payload = {'uuid': chunk_id}
    requests.post('https://fcghgojd5l.execute-api.us-east-2.amazonaws.com/dev/release_area', json=payload)
    print(f"Uploaded chunk {chunk_id}")
    return chunk_id

# Function to Run External Commands
async def run_command(command, cwd):
    """Runs an external command asynchronously."""
    process = await asyncio.create_subprocess_shell(
        command,
        cwd=cwd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    if process.returncode == 0:
        print(f"Command completed: {command}")
    else:
        print(f"Command failed: {command}\nError: {stderr.decode()}")

async def run_lastile(process_dir,cwd):
    """Runs the lastile tool."""
    cmd = f"{lastile} -i {os.path.join(process_dir, 'downloaded_files', '*.laz')} -tile_size 200 -odir {os.path.join(process_dir, 'tiles')} -o tile.laz"
    await run_command(cmd, cwd)

async def run_pullauta(cwd):
    """Runs the pullauta tool."""
    await run_command(pullauta, cwd)

def create_pullauta_file(cores, main_dir):
    """Generates the pullauta.ini configuration file."""
    content = [
        "vegemode=0",
        "undergrowth=0.35",
        "undergrowth2=0.56",
        "greenground=0.9",
        "greenhigh=2",
        "topweight=0.80",
        "vegezoffset=0",
        "greendetectsize=3",
        "zone1=1.0|2.65|99|1",
        "zone2=2.65|3.4|99|0.1",
        "zone3=3.4|5.5|8|0.2",
        "thresold1=0.20|3|0.1",
        "thresold2=3|4|0.1",
        "thresold3=4|7|0.1",
        "thresold4=7|20|0.1",
        "thresold5=20|99|0.1",
        "pointvolumefactor=0.1",
        "pointvolumeexponent=1",
        "firstandlastreturnfactor=1",
        "lastreturnfactor=1",
        "firstandlastreturnasground=3",
        "greenshades=0.2|0.35|0.5|0.7|1.3|2.6|4|99|99|99|99",
        "lightgreentone=200",
        "greendotsize=0",
        "groundboxsize=1",
        "medianboxsize=9",
        "medianboxsize2=1",
        "yellowheight=0.9",
        "yellowthresold=0.9",
        "cliff1=1.15",
        "cliff2=2.0",
        "cliffthin=1",
        "cliffsteepfactor=0.38",
        "cliffflatplace=3.5",
        "cliffnosmallciffs=5.5",
        "cliffdebug=0",
        "northlinesangle=0",
        "northlineswidth=0",
        "formline=2",
        "formlinesteepness=0.30",
        "formlineaddition=17",
        "minimumgap=30",
        "dashlength=60",
        "gaplength=12",
        "indexcontours=12.5",
        "smoothing=0.9",
        "curviness=1.1",
        "knolls=0.8",
        "coordxfactor=1",
        "coordyfactor=1",
        "coordzfactor=1",
        "thinfactor=1",
        "waterclass=9",
        "detectbuildings=0",
        "batch=1",
        f"processes={cores}",
        "batchoutfolder="+os.path.join(main_dir, 'output').replace("\\", "/"),
        "lazfolder="+os.path.join(main_dir, 'tiles').replace("\\", "/"),
        "vectorconf=osm.txt",
        "mtkskiplayers=",
        "buildingcolor=0,0,0",
        "savetempfiles=0",
        "savetempfolders=0",
        "basemapinterval=0",
        "scalefactor=1",
        "zoffset=0",
    ]
    write_file("pullauta.ini", content)

def create_osm_txt_file():
    """Generates the osm.txt configuration file."""
    content = [
        'power line|524|barrier!=',
        'power line|516|power!=',
        'trench|516 |tags="accident"=>"trench"',
        'building|526|building!=',
        'settlement|527|landuse=residential',
        'parking|529|amenity=parking',
        'farm|401|landuse=farm',
        'orchard|527|landuse=orchard',
        'pitch|529|leisure=pitch',
        'playground|529|leisure=playground',
        'railway|504|railway!=',
        'waterway|306|waterway!=',
        'water|301|natural=water',
        'water|301|water!=',
        'water|301|natural=bay',
        'road-path|503|highway=motorway&bridge!=yes',
        'road-path|503|highway=motorway_link&bridge!=yes',
        'road-path|503|highway=trunk&bridge!=yes',
        'road-path|503|highway=trunk_link&bridge!=yes',
        'road-path|503|highway=primary&bridge!=yes',
        'road-path|503|highway=primary_link&bridge!=yes',
        'road-path|503|highway=secondary&bridge!=yes',
        'road-path|503|highway=secondary_link&bridge!=yes',
        'road-path|503|highway=tertiary&bridge!=yes',
        'road-path|503|highway=tertiary_link&bridge!=yes',
        'road-path|504|highway=living_street&bridge!=yes',
        'road-path|505|highway=pedestrian&bridge!=yes',
        'road-path|503|highway=residential&bridge!=yes',
        'road-path|504|highway=unclassified&bridge!=yes',
        'road-path|504|highway=service&bridge!=yes',
        'road-path|505|highway=track&bridge!=yes',
        'road-path|504|highway=bus_guideway&bridge!=yes',
        'road-path|504|highway=raceway&bridge!=yes',
        'road-path|504|highway=road&bridge!=yes',
        'road-path|507|highway=path&bridge!=yes',
        'road-path|504|highway=footway&surface=paved&bridge!=yes',
        'road-path|504|highway=footway&surface!=paved&bridge!=yes',
        'road-path|507|highway=bridleway&bridge!=yes',
        'road-path|507|highway=steps&bridge!=yes',
        'road-path|505|highway=cycleway&bridge!=yes',
        'road-path|503|highway=lane&bridge!=yes',
        'road-path|503|highway=opposite&bridge!=yes',
        'road-path|503|highway=opposite_lane&bridge!=yes',
        'road-path|505|highway=track&bridge!=yes',
        'road-path|505|highway=opposite_track&bridge!=yes',
        'road-path|504|highway=shared&bridge!=yes',
        'road-path|504|highway=share_busway&bridge!=yes',
        'road-path|504|highway=shared_lane&bridge!=yes',
        'road-path|503T|highway=motorway&bridge=yes',
        'road-path|503T|highway=motorway_link&bridge=yes',
        'road-path|503T|highway=trunk&bridge=yes',
        'road-path|503T|highway=trunk_link&bridge=yes',
        'road-path|503T|highway=primary&bridge=yes',
        'road-path|503T|highway=primary_link&bridge=yes',
        'road-path|503T|highway=secondary&bridge=yes',
        'road-path|503T|highway=secondary_link&bridge=yes',
        'road-path|503T|highway=tertiary&bridge=yes',
        'road-path|503T|highway=tertiary_link&bridge=yes',
        'road-path|504T|highway=living_street&bridge=yes',
        'road-path|505T|highway=pedestrian&bridge=yes',
        'road-path|503T|highway=residential&bridge=yes',
        'road-path|504T|highway=unclassified&bridge=yes',
        'road-path|504T|highway=service&bridge=yes',
        'road-path|505T|highway=track&bridge=yes',
        'road-path|504T|highway=bus_guideway&bridge=yes',
        'road-path|504T|highway=raceway&bridge=yes',
        'road-path|504T|highway=road&bridge=yes',
        'road-path|507T|highway=path&bridge=yes',
        'road-path|504T|highway=footway&surface=paved&bridge=yes',
        'road-path|504T|highway=footway&surface!=paved&bridge=yes',
        'road-path|507T|highway=bridleway&bridge=yes',
        'road-path|507T|highway=steps&bridge=yes',
        'road-path|505T|highway=cycleway&bridge=yes',
        'road-path|503T|highway=lane&bridge=yes',
        'road-path|503T|highway=opposite&bridge=yes',
        'road-path|503T|highway=opposite_lane&bridge=yes',
        'road-path|505T|highway=track&bridge=yes',
        'road-path|505T|highway=opposite_track&bridge=yes',
        'road-path|504T|highway=shared&bridge=yes',
        'road-path|504T|highway=share_busway&bridge=yes',
        'road-path|504T|highway=shared_lane&bridge=yes'
    ]
    write_file( "osm.txt", content)


async def process_chunk(chunk_id, xmin, ymin, file_list, download_semaphore, pullauta_semaphore):
    """Processes a chunk of data."""
    process_dir = os.path.join("process", str(chunk_id))

    # ensure the process directory is empty
    try:
        shutil.rmtree(process_dir)
    except:
        pass

    cwd = os.getcwd()
    ensure_dir(process_dir)
    ensure_dir(os.path.join(process_dir, "tiles"))
    ensure_dir(os.path.join(process_dir, "output"))
    
    # Download files from S3
    print(f"Processing chunk {chunk_id}")
    downloaded_files_dir = os.path.join(process_dir, "downloaded_files")
    ensure_dir(downloaded_files_dir)
    
    # Ensure only one download runs at a time
    async with download_semaphore:
        print(f"Downloading data for chunk {chunk_id}")
        for file in file_list.split(','):
            file_name = os.path.basename(file)
            path_without_bucket = file.replace('https://opentopography.s3.sdsc.edu/pc-bulk/', '')
            s3.download_file('pc-bulk', path_without_bucket, os.path.join(downloaded_files_dir, file_name))

        print(f"Finished downloading data for chunk {chunk_id}")



    async with pullauta_semaphore:
        # Run lastile and pullauta tools
        # Generate new configuration files
        try:
            os.remove("pullauta.ini")
            os.remove("osm.txt")
            os.remove("osm.zip")
        except:
            pass

        # download the osm zip - must go in the input 'tiles' folder
        try:
            s3_nz.download_file('nzomap', f'osm/5000/{xmin}_{ymin}.zip', os.path.join(os.path.join(process_dir, "tiles"), 'osm.zip'))
        
        except:
            pass

        create_pullauta_file(cores, process_dir)
        create_osm_txt_file()

        await run_lastile(process_dir,cwd)
        await run_pullauta(cwd)

    # Upload results to S3
    output_folder = os.path.join(process_dir, "output")
    uploaded_chunk = await upload_files(output_folder, chunk_id, xmin, ymin)
    print(f"Finished processing and uploading chunk {uploaded_chunk}")

    # Clean up
    shutil.rmtree(process_dir)
    print(f"Cleaned up temporary files for chunk {chunk_id}")

async def main(chunks):
    """Main function to process all chunks."""

    # Semaphore instances to control concurrency
    download_semaphore = asyncio.Semaphore(1)  # Only one download at a time
    pullauta_semaphore = asyncio.Semaphore(1)  # Only one pullauta execution at a time

    tasks = [process_chunk(chunk['chunk_id'],chunk['xmin'],chunk['ymin'],chunk['file_list'],download_semaphore,pullauta_semaphore) for chunk in chunks]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    async def process_all_chunks():
        if os.path.exists("process"):
            shutil.rmtree("process")

        r = requests.get("https://fcghgojd5l.execute-api.us-east-2.amazonaws.com/dev/new_area")
        if r.status_code == 200:
            returned_json = json.loads(r.json()["body"])
            area_uuid = returned_json["uuid"]
            file_list = returned_json["files"]
            xmin = int(returned_json["xmin"])
            ymin = int(returned_json["ymin"])
            chunk_1 = {"chunk_id": area_uuid, "xmin": xmin, "ymin": ymin, "file_list": file_list}

            r = requests.get("https://fcghgojd5l.execute-api.us-east-2.amazonaws.com/dev/new_area")
            if r.status_code == 200:
                returned_json = json.loads(r.json()["body"])
                area_uuid = returned_json["uuid"]
                file_list = returned_json["files"]
                xmin = int(returned_json["xmin"])
                ymin = int(returned_json["ymin"])
                chunk_2 = {"chunk_id": area_uuid, "xmin": xmin, "ymin": ymin, "file_list": file_list}

                await main([chunk_1, chunk_2])
            
            else:
                await main([chunk_1])
            
        else:
            raise Exception("No new areas available")

    # Create the loop once
    while True:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(process_all_chunks())