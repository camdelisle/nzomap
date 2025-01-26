from PIL import Image
import os
import io

import math
import glob

import boto3

s3 = boto3.client('s3')



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

            width, height = open_tile.size
            if width > 470 and height > 470:
                new_tile.paste(open_tile,(x_dist,y_dist))
    
    # scale to the correct tile size

    return new_tile.resize((offset_x,offset_y))


# 1. download files
# 2. run script to create new tiles
# 3. Upload to AWS
# 4. repeat for each zoom level

def main():
    in_x = 1860000
    in_y = 5770000
    
    for zoom in [12,9,6,3,0]:
        parents = {}
        scale_factor = 8 ** ((12 - zoom) // 3)
        x_min = math.floor(in_x // (200 * scale_factor) / 8) * 8
        x_max = math.ceil((in_x + 5000) // (200 * scale_factor) / 8) * 8
        y_max = math.ceil((6553600 - in_y) // (200 * scale_factor) / 8) * 8
        y_min = math.floor((6553600 - in_y - 5000) // (200 * scale_factor) / 8) * 8

        print(f'Processing zoom level {zoom}')
        for x in range(x_min,x_max):
            for y in range(y_min,y_max):
                try:
                    file_body = s3.get_object(Bucket='nzomap', Key=f'tiles/{zoom+3}/{x}/{y}.png')['Body']

                except:
                    continue

                else:
                    y_parent = math.floor(y/8)
                    x_parent = math.floor(x/8)
                    # identify plane coordinates. Bottom left will be 0,0
                    x_off = x - int(x_parent)*8
                    y_off = y - int(y_parent)*8

                    parents[x_parent] = parents.setdefault(x_parent,{})
                    parents[x_parent][y_parent] = parents[x_parent].setdefault(y_parent,{})
                    parents[x_parent][y_parent][x_off] = parents[x_parent][y_parent].setdefault(x_off,{})
                    parents[x_parent][y_parent][x_off][y_off] = parents[x_parent][y_parent][x_off].setdefault(y_off,file_body)
                
        
        running = 0
        for x in parents:
            for y in parents[x]:
                new_tile = join_tiles(parents[x][y],474,474)
                new_tile = new_tile.quantize(colors=32, method=2)

                file_stream = io.BytesIO()
                new_tile.save(file_stream, format='png')
                s3.put_object(Body=file_stream.getvalue(), Bucket='nzomap', Key=f'tiles/{zoom}/{x}/{y}.png')
                running = running+1

                print(f'{running}')

    
main()