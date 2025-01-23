import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
import boto3
from botocore.config import Config
import shutil
import requests
import json


# Configure S3 client
s3 = boto3.client('s3', 
                  endpoint_url='https://opentopography.s3.sdsc.edu',
                  config=Config(signature_version='unsigned'))


# Create a directory if it doesn't exist
def ensure_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

def create_pullauta_file(cores,main_dir):
    f = open(os.path.join(main_dir,"pullauta.ini"), "w")
    f.write("vegemode=0")
    f.write("undergrowth=0.35")
    f.write("undergrowth2=0.56")
    f.write("greenground=0.9")
    f.write("greenhigh=2")
    f.write("topweight=0.80")
    f.write("vegezoffset=0")
    f.write("greendetectsize=3")
    f.write("zone1=1.0^|2.65^|99^|1")
    f.write("zone2=2.65^|3.4^|99^|0.1")
    f.write("zone3=3.4^|5.5^|8^|0.2")
    f.write("thresold1=0.20^|3^|0.1")
    f.write("thresold2=3^|4^|0.1")
    f.write("thresold3=4^|7^|0.1")
    f.write("thresold4=7^|20^|0.1")
    f.write("thresold5=20^|99^|0.1")
    f.write("pointvolumefactor=0.1")
    f.write("pointvolumeexponent=1")
    f.write("firstandlastreturnfactor=1")
    f.write("lastreturnfactor")
    f.write("firstandlastreturnasground=3")
    f.write("greenshades=0.2^|0.35^|0.5^|0.7^|1.3^|2.6^|4^|99^|99^|99^|99")
    f.write("lightgreentone=200")
    f.write("greendotsize=0")
    f.write("groundboxsize=1")
    f.write("medianboxsize=9")
    f.write("medianboxsize2=1")
    f.write("yellowheight=0.9")
    f.write("yellowthresold=0.9")
    f.write("cliff1")
    f.write("cliff2")
    f.write("cliffthin=1")
    f.write("cliffsteepfactor=0.38")
    f.write("cliffflatplace=3.5")
    f.write("cliffnosmallciffs=5.5")
    f.write("cliffdebug=0")
    f.write("northlinesangle=0")
    f.write("northlineswidth=0")
    f.write("formline=2")
    f.write("formlinesteepness=0.30")
    f.write("formlineaddition=17")
    f.write("minimumgap")
    f.write("dashlength")
    f.write("gaplength")
    f.write("indexcontours=12.5")
    f.write("smoothing")
    f.write("curviness=1.1")
    f.write("knolls=0.8")
    f.write("coordxfactor=1")
    f.write("coordyfactor=1")
    f.write("coordzfactor=1")
    f.write("thinfactor")
    f.write("waterclass=9")
    f.write("detectbuildings=0")
    f.write("batch=1")
    f.write(f"processes={cores}")
    f.write(f"batchoutfolder={os.path.join(main_dir,'output')}")
    f.write(f"lazfolder={os.path.join(main_dir,'tiles')}")
    f.write("vectorconf=osm.txt")
    f.write("mtkskiplayers=")
    f.write("buildingcolor=0,0,0")
    f.write("savetempfiles=0")
    f.write("savetempfolders=0")
    f.write("basemapinterval=0")
    f.write("scalefactor=1")
    f.write("zoffset=0")
    f.close()

def create_osm_txt_file(main_dir):
    f = open(os.path.join(main_dir,"osm.txt"), "w")
    f.write('power line|524|barrier!=')
    f.write('power line|516|power!=')
    f.write('trench|516 |tags="accident"=>"trench"')
    f.write('building|526|building!=')
    f.write('settlement|527|landuse=residential')
    f.write('parking|529|amenity=parking')
    f.write('farm|401|landuse=farm')
    f.write('orchard|527|landuse=orchard')
    f.write('pitch|529|leisure=pitch')
    f.write('playground|529|leisure=playground')
    f.write('railway|504|railway!=')
    f.write('waterway|306|waterway!=')
    f.write('water|301|natural=water')
    f.write('water|301|water!=')
    f.write('water|301|natural=bay')
    f.write('road-path|503|highway=motorway&bridge!=yes')
    f.write('road-path|503|highway=motorway_link&bridge!=yes')
    f.write('road-path|503|highway=trunk&bridge!=yes')
    f.write('road-path|503|highway=trunk_link&bridge!=yes')
    f.write('road-path|503|highway=primary&bridge!=yes')
    f.write('road-path|503|highway=primary_link&bridge!=yes')
    f.write('road-path|503|highway=secondary&bridge!=yes')
    f.write('road-path|503|highway=secondary_link&bridge!=yes')
    f.write('road-path|503|highway=tertiary&bridge!=yes')
    f.write('road-path|503|highway=tertiary_link&bridge!=yes')
    f.write('road-path|504|highway=living_street&bridge!=yes')
    f.write('road-path|505|highway=pedestrian&bridge!=yes')
    f.write('road-path|503|highway=residential&bridge!=yes')
    f.write('road-path|504|highway=unclassified&bridge!=yes')
    f.write('road-path|504|highway=service&bridge!=yes')
    f.write('road-path|505|highway=track&bridge!=yes')
    f.write('road-path|504|highway=bus_guideway&bridge!=yes')
    f.write('road-path|504|highway=raceway&bridge!=yes')
    f.write('road-path|504|highway=road&bridge!=yes')
    f.write('road-path|507|highway=path&bridge!=yes')
    f.write('road-path|504|highway=footway&surface=paved&bridge!=yes')
    f.write('road-path|504|highway=footway&surface!=paved&bridge!=yes')
    f.write('road-path|507|highway=bridleway&bridge!=yes')
    f.write('road-path|507|highway=steps&bridge!=yes')
    f.write('road-path|505|highway=cycleway&bridge!=yes')
    f.write('road-path|503|highway=lane&bridge!=yes')
    f.write('road-path|503|highway=opposite&bridge!=yes')
    f.write('road-path|503|highway=opposite_lane&bridge!=yes')
    f.write('road-path|505|highway=track&bridge!=yes')
    f.write('road-path|505|highway=opposite_track&bridge!=yes')
    f.write('road-path|504|highway=shared&bridge!=yes')
    f.write('road-path|504|highway=share_busway&bridge!=yes')
    f.write('road-path|504|highway=shared_lane&bridge!=yes')
    f.write('road-path|503T|highway=motorway&bridge=yes')
    f.write('road-path|503T|highway=motorway_link&bridge=yes')
    f.write('road-path|503T|highway=trunk&bridge=yes')
    f.write('road-path|503T|highway=trunk_link&bridge=yes')
    f.write('road-path|503T|highway=primary&bridge=yes')
    f.write('road-path|503T|highway=primary_link&bridge=yes')
    f.write('road-path|503T|highway=secondary&bridge=yes')
    f.write('road-path|503T|highway=secondary_link&bridge=yes')
    f.write('road-path|503T|highway=tertiary&bridge=yes')
    f.write('road-path|503T|highway=tertiary_link&bridge=yes')
    f.write('road-path|504T|highway=living_street&bridge=yes')
    f.write('road-path|505T|highway=pedestrian&bridge=yes')
    f.write('road-path|503T|highway=residential&bridge=yes')
    f.write('road-path|504T|highway=unclassified&bridge=yes')
    f.write('road-path|504T|highway=service&bridge=yes')
    f.write('road-path|505T|highway=track&bridge=yes')
    f.write('road-path|504T|highway=bus_guideway&bridge=yes')
    f.write('road-path|504T|highway=raceway&bridge=yes')
    f.write('road-path|504T|highway=road&bridge=yes')
    f.write('road-path|507T|highway=path&bridge=yes')
    f.write('road-path|504T|highway=footway&surface=paved&bridge=yes')
    f.write('road-path|504T|highway=footway&surface!=paved&bridge=yes')
    f.write('road-path|507T|highway=bridleway&bridge=yes')
    f.write('road-path|507T|highway=steps&bridge=yes')
    f.write('road-path|505T|highway=cycleway&bridge=yes')
    f.write('road-path|503T|highway=lane&bridge=yes')
    f.write('road-path|503T|highway=opposite&bridge=yes')
    f.write('road-path|503T|highway=opposite_lane&bridge=yes')
    f.write('road-path|505T|highway=track&bridge=yes')
    f.write('road-path|505T|highway=opposite_track&bridge=yes')
    f.write('road-path|504T|highway=shared&bridge=yes')
    f.write('road-path|504T|highway=share_busway&bridge=yes')
    f.write('road-path|504T|highway=shared_lane&bridge=yes')
    f.close()


### WIP ###
# Function to upload files
async def upload_files(output_folder,chunk_id, valid_area):
    # identify valid files
    # valid files look like tile_1640000_5381800.laz_depr.png
    # there are also invalid files like tile_1640000_5381800.laz.png
    files = [os.path.join(output_folder, f) for f in os.listdir(output_folder) if f.endswith('.laz_depr.png')]

    with ThreadPoolExecutor(max_workers=10) as executor:
        loop = asyncio.get_event_loop()
        tasks = []
        for file in files:
            # we use the filename to identify where it sits in this chunk
            file_name = os.path.basename(file)
            x = file_name.split('_')[1]
            y = file_name.split('_')[2]

            tasks.append(loop.run_in_executor(executor, s3.upload_file, file, 'pc-bulk'))
        await asyncio.gather(*tasks)

    print(f"Uploaded chunk {chunk_id}")
    # on upload we should update some form of record that the files now exist, and should not be reprocessed
    return chunk_id

### WIP ###


# Function to run lastile command
async def run_lastile(process_dir):
    cmd = f"lastile {os.path.join(process_dir,'downloaded_files','*.laz')} -o {os.path.join(process_dir,'tiles')}"
    process = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    stdout, stderr = await process.communicate()
    if process.returncode == 0:
        print(f"lastile completed for {process_dir}")
    else:
        print(f"lastile failed for {process_dir}: {stderr.decode()}")

# Function to run pullauta command
async def run_pullauta(process_dir):
    cmd = f"pullauta"
    process = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    stdout, stderr = await process.communicate()
    if process.returncode == 0:
        print(f"pullauta completed for {process_dir}")
    else:
        print(f"pullauta failed for {process_dir}: {stderr.decode()}")


# Main function to orchestrate the workflow
async def main():
    await start_executors(2)

# v2

# Function to download files
async def download_files_v2(file_list,process_dir):
    with ThreadPoolExecutor(max_workers=4) as executor2:
        loop2 = asyncio.get_event_loop()
        tasks2 = []
        for file_path in file_list.split(','):
            local_path = os.path.join(process_dir,'downloaded_files', os.path.basename(file_path))
            tasks2.append(loop2.run_in_executor(executor2, s3.download_file, bucket_name, file_path, local_path))
        await asyncio.gather(*tasks2)

    print(f"Downloaded chunk")



async def process_chunk_v2(process_dir,file_list,area_uuid):
    download_dir = os.path.join(process_dir,'download')
    processing_dir = os.path.join(process_dir,'processing')
    while os.path.exists(download_dir):
        asyncio.sleep(15)

    # create download directory
    os.makedirs(download_dir)
    await download_files_v2(file_list,process_dir)

    # await release of processing capacity next
    while os.path.exists(processing_dir):
        asyncio.sleep(15)
    
    # claim processing capacity
    os.makedirs(processing_dir)
    # release downloads now processing capacity is free
    os.rmdir(download_dir)

    # run lastile and then pullauta
    await run_lastile(process_dir)
    await run_pullauta(process_dir)

    # release process capacity now we only need to upload
    os.rmdir(processing_dir)

    # upload files
    await upload_output(process_dir,area_uuid)

    # cleanup
    shutil.rmtree(process_dir)  # Clean up the processing directory


# Function to split processing into 2 executors, to make sure that we don't wait for 1 batch to finish before downloading the next
async def start_executors(workers,cores):
    with ThreadPoolExecutor(max_workers=workers) as executor:
        loop = asyncio.get_event_loop()
        tasks = []
        i = 0
        while True:
            r = requests.get('https://fcghgojd5l.execute-api.us-east-2.amazonaws.com/dev/new_area')
            if r.status_code == 200:
                returned_json = json.loads(r.json()['body'])
                area_uuid = returned_json['uuid']
                file_list = returned_json['files']
            else:
                print('Failed to get new area')
                break
            i = i + 1
            if i > 20:
                i = 5

            local_chunk_dir = os.path.join(local_base_dir, f'chunk_{area_uuid}')
            ensure_dir(local_chunk_dir)
            ensure_dir(os.path.join(local_chunk_dir,'downloaded_files'))
            ensure_dir(os.path.join(local_chunk_dir,'tiles'))
            ensure_dir(os.path.join(local_chunk_dir,'output'))

            shutil.copyfile(os.path.join(local_base_dir,'pullauta'), os.path.join(local_chunk_dir,'pullauta'))

            create_pullauta_file(cores,local_chunk_dir)
            create_osm_txt_file(local_chunk_dir)

            asyncio.sleep(i*2) # wait to prevent clashes
            tasks.append(loop.run_in_executor(executor, process_chunk_v2,local_chunk_dir,file_list,area_uuid))
        await asyncio.gather(*tasks)

    print(f"Completed all chunks")
    return True