# Script 1: run processing
# 1. determine tiling
# 2. download lidar (.laz+.lax files) that intersects tiling, using coordinates
#   2.1 if no .lax, run lasindex and index the files first. Command lasindex -i *.laz -cores 8
# 3. Run something like lastile64 -i *.laz -tile_size 200 -odir tiles -cores 8
# 4. Now we should have tiled lidar ready. We should also have OSM data available. Run kp
# 5. (check actual command)


# Script 2: prepare areas
# 1. Convert lidar index files into a list of download locations. Output in a txt file
# 2. 



# lidar S3 examples:
# Waikato: aws s3 ls s3://pc-bulk/NZ21_Waikato/ --recursive --endpoint-url https://opentopography.s3.sdsc.edu --no-sign-request

# we use 20km x 20km squares, except where osm density is too high
# then we break into 200m x 200m squares, which form our maptiles
# we could try something clever though where we process at the next scale level 
# eg 1600x1600, break apart into 200x200 squares, and desample the larger tiles a little..
# this might actually reduce storage size

# we should run 2 jobs here as well:
# 1. Processes and uploads tiles to webserver
# 2. Creates smaller and larger tiles

# another idea: store all the process files in web server, and then build a script which can reprocess a selected small area

from PIL import Image
import os
import pandas as pd
import geopandas as gpd
import boto3
from boto3.s3.transfer import TransferConfig
from botocore import UNSIGNED
from botocore.config import Config
import io
import glob
import math

import shutil

import requests

import json

def ensure_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

# this function takes a matrix of images, and 2 tile size parameters, and grids them
def join_tiles(images_xy,offset_x,offset_y):
    # create a blank image object, with correct size determined by tiling
    new_tile = Image.new("RGB", (offset_x * 8, offset_y * 8),(255, 255, 255))

    # go row by row
    for x in images_xy:
        # col by col
        for y in images_xy[x]:
            # first image will be at 0,0, subsequent will depend on offsets
            x_dist = x * offset_x
            y_dist = y * offset_y
            try:
                open_tile = Image.open(images_xy[x][y])
            except Exception:
                continue

            new_tile.paste(open_tile,(x_dist,y_dist))
    
    # scale to the correct tile size

    return new_tile.resize((offset_x,offset_y))



def tile_zoom_level(in_dir,out_dir,z):
    parents = {}
    total_cnt = 0
    for filename in glob.iglob(in_dir + f'{z}/*/*.png', recursive=True):
        y = int(os.path.splitext(os.path.basename(filename))[0])
        x = int(os.path.basename(os.path.dirname(filename)))
        if x >= -8888 and x < 88888:
            y_parent = math.floor(y/8)
            x_parent = math.floor(x/8)
            # identify plane coordinates. Bottom left will be 0,0
            x_off = x - int(x_parent)*8
            y_off = y - int(y_parent)*8

            parents[x_parent] = parents.setdefault(x_parent,{})
            parents[x_parent][y_parent] = parents[x_parent].setdefault(y_parent,{})
            parents[x_parent][y_parent][x_off] = parents[x_parent][y_parent].setdefault(x_off,{})
            parents[x_parent][y_parent][x_off][y_off] = parents[x_parent][y_parent][x_off].setdefault(y_off,os.path.join(in_dir,f'{z}/{x}/{y}.png'))
            total_cnt = total_cnt + 1

    running = 0
    total_cnt = total_cnt / 40
    for x in parents:
        for y in parents[x]:
            new_tile = join_tiles(parents[x][y],474,474)
            new_tile = new_tile.quantize(colors=32, method=2)
            directory = f'{out_dir}{z-3}/{x}/'
            ensure_dir(directory)
            new_tile.save(f'{directory}{y}.png')
            running = running+1

            print(f'Processed {running} of estimated {total_cnt}')
    
        


#out_dir = 'C:/Users/camer/nzomap_code/nzomap/processing/tiles/'
#in_dir = 'C:/wamp64/www/nzmap/tiles/'
#tile_zoom_level(out_dir,out_dir,3)
# this function tiles an image into a specified number of rows + columns
# it takes an input image as a PIL Image object
def tile_image(image,rows,cols,len_x,len_y):
    output_images = []
    for row in range(rows):
        for col in range(cols):
            box = (row*len_y, col*len_x, (row+1)*len_y, (col+1)*len_x)
            output_images.append(image.crop(box))

    return output_images


# this function takes a csv with cols wkt & tilename, and creates tiles that list all tilenames contained within
# a specified big tile size
def identify_lidar_tiles(shp,big_tile,small_tile,start_x,start_y,name):
    gdf = gpd.GeoDataFrame.from_file(shp)

    # get the bounding box of the geoseries, as tuple (xmin,ymin,xmax,ymax)
    bbox = gdf.total_bounds

    # normalise our bbox mins to our small_tile range using our start_x & start_y values
    x_diff = (bbox[0] - start_x) % small_tile
    xmin_norm = bbox[0] - x_diff

    y_diff = (bbox[1] - start_y) % small_tile
    ymin_norm = bbox[1] - y_diff

    # now we can loop until we exceed the bbox max values with our xmin & ymin incrementers
    xmin = xmin_norm
    output_payloads = []
    while xmin < bbox[2]:
        ymin = ymin_norm
        xmax = xmin + small_tile

        while ymin < bbox[3]:
            ymax = ymin + small_tile
            # get which big tile we are in by normalising (custom 'floor') back to last big tile
            xbig_adj = (xmin - start_x) % big_tile
            xbig = xmin - xbig_adj 
            ybig_adj = (ymin - start_y) % big_tile
            ybig = ymin - ybig_adj 

            gdf_filter = gdf.cx[xmin-5:xmax+5, ymin-5:ymax+5]
            if len(gdf_filter.index) > 0:
                # create a payload representing one processing block
                file_list = ''
                for row in gdf_filter.itertuples():
                    if file_list == '':
                        file_list = row.URL + ',' + row.URL[:-1] + 'x'
                    else:
                        file_list = file_list + ',' + row.URL + ',' + row.URL[:-1] + 'x'
                
                payload = {
                    "lidar_area_name": name,
                    "xmin": int(xmin),
                    "ymin": int(ymin),
                    "xmax": int(xmax),
                    "ymax": int(ymax),
                    "parent_block": f"{name}_{int(xbig)}_{int(ybig)}",
                    "file_list": file_list
                }
                output_payloads.append(payload)

            ymin = ymin + small_tile

        xmin = xmin + small_tile

    return output_payloads


def send_new_lidar_area(payload):
    endpoint = 'https://fcghgojd5l.execute-api.us-east-2.amazonaws.com/dev'

    r = requests.post(endpoint, json=payload)
    return r.status_code


#payloads = identify_lidar_tiles('./processing/NZ21_Waikato_TileIndex/NZ21_Waikato_TileIndex.shp',20000,5000,1000000,4800000,'NZ21_Waikato')
#for payload in payloads:
#    print(send_new_lidar_area(payload))

#r = requests.get('https://fcghgojd5l.execute-api.us-east-2.amazonaws.com/dev/new_area')

#print(r.status_code)
#print(r.text)
#print(json.loads(r.json()['body'])['available'])

# test case
#dfs = identify_lidar_tiles('waikato-lidar-index-tiles-2021.csv',12800,3200,1000000,4800000)

# data is stored in s3://pc-bulk/NZ21_Waikato/
#print(dfs[0])


#Server: opentopography.s3.sdsc.edu
#Port: 443
#Access Key ID: pc-bulk
#Secret Access Key: pc-bulk-3LoTa4
#Path: /pc-bulk/NZ21_Waikato

#my_bucket = s3_client.Bucket('s3://pc-bulk/NZ21_Waikato/')

#for my_bucket_object in my_bucket.objects.all():
#    print(my_bucket_object.key)

"""
# Configuring the S3 client for the specific endpoint and without signing requests
s3 = boto3.client('s3', 
                  endpoint_url='https://opentopography.s3.sdsc.edu',
                  config=Config(signature_version=UNSIGNED))

# Function to download all objects from a given S3 path
def download_s3_folder(bucket_name, prefix, local_dir):
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                # Strip the prefix from the object key to get the relative path
                relative_path = obj['Key'][len(prefix):]
                print(relative_path)
                local_path = os.path.join(local_dir, relative_path)
                # NZ21_Waikato/Addendum5/CL2_BE34_2021_1000_4939.lax
                # ./Addendum5/CL2_BE34_2021_1000_4939.lax

                # Create local directory structure if it does not exist
                #if not os.path.exists(os.path.dirname(local_path)):
                    #os.makedirs(os.path.dirname(local_path))

                # Download the file
                #s3.download_file(bucket_name, obj['Key'], local_path)
                print(f"Downloaded {obj['Key']} to {local_path}")

# Bucket and prefix based on the provided command
bucket_name = 'pc-bulk'
prefix = 'NZ21_Waikato/'

# Local directory to download the files
local_dir = './'

# Call the function to download the folder
download_s3_folder(bucket_name, prefix, local_dir)


# we need the workflow to be highly interruptible - using only a small chunk of an area at a time
# we can achieve this by breaking a 20km x 20km square for example, into 400x 1kmx1km chunks
# then as one chunk completes download, we start processing it, and keep downloading the next chunk async

# Threading would look like:
# Thread 1: downloading lidar chunk by chunk
# Thread 2: Wait for a complete chunk download then tile
# thread 3: Wait for a tiled dataset, then queue multiple pullauta processes
# thread 4: Wait for a full chunk, than upload, and cleanup

# Thread 1 should only have max 3 downloads waiting at once for thread 4 to finish
"""

def make_archive(source, destination, filename):
        shutil.make_archive(filename, 'zip', source)
        shutil.move(filename+'.zip', destination)

def create_osm_archives(scale):
    # code will create osm archives for a given scale

    raw_dir = "C:\\Users\\camer\\Maps\\Mapping\\kp\\ALL_DRIVE\\osm\\create\\100km"
    zip_dir = "C:\\Users\\camer\\Maps\\Mapping\\kp\\ALL_DRIVE\\osm\\" + str(scale)
    buffer = 10
    osms = []
    directory = os.fsencode(zip_dir)
    files = os.listdir(directory)
    for file in files:
        filename = os.fsdecode(file)
        if filename.endswith(".zip"): 
            xVal = filename[0:7]
            yVal = filename[8:15]
            tup = [int(xVal),int(yVal)]
            osms.append(tup)
        
    
    for x in range(1080000,2080000+scale,scale):
        for y in range(4820000,6180000+scale,scale):
            tup = [x,y]
            area_found = False
            for osm_z in osms:
                if tup == osm_z:
                    area_found = True
                    
            if not area_found:
                #create zip
                    
                #zero down to the 100km block.
                    
                x100 = int(x) - (int(x) % 100000)
                y100 = int(y) - (int(y) % 100000)
                    
                raw_100_dir = raw_dir + "\\" + str(x100) + "_" + str(y100)
                    
                osm_directory = os.fsencode(raw_100_dir)
                try:
                    osm_files = os.listdir(osm_directory)
                except:
                    continue
                    
                new_dir = zip_dir + "\\" + str(x) + "_" + str(y) + "\\"
                new_dir_fix = zip_dir + "\\" + str(x) + "_" + str(y)
                    
                if not os.path.exists(new_dir):
                    os.makedirs(new_dir)                    
                    
                minX = int(x) - buffer
                minY = int(y) - buffer
                maxX = int(x) + scale + buffer
                maxY = int(y) + scale + buffer
                
                for osm in osm_files:
                    osmName = os.fsdecode(osm)
                    if osmName.endswith(".shp"):
                        osm_gdf = gpd.read_file(raw_100_dir + "\\" + osmName)
                        osm_gdf = osm_gdf.cx[minX:maxX,minY:maxY]
                        osm_gdf.to_file(new_dir + osmName)
                                    
                                    
                    
                #make archive
                make_archive(new_dir,zip_dir,str(x) + "_" + str(y))
                if os.path.exists(new_dir):
                    shutil.rmtree(new_dir)


create_osm_archives(5000)