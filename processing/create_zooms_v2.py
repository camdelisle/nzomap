from PIL import Image
import os
import io
import math
import boto3
import concurrent.futures
import requests
import json

s3 = boto3.client('s3')

# Join tiles function
def join_tiles(images_xy, offset_x, offset_y):
    """Combine 64 tiles (8x8 grid) into a single image and resize."""
    new_tile = Image.new("RGB", (offset_x * 8, offset_y * 8), (255, 255, 255))

    for x, row in images_xy.items():
        for y, img_bytes in row.items():
            try:
                open_tile = Image.open(io.BytesIO(img_bytes))
                if open_tile.size[0] > 470 and open_tile.size[1] > 470:
                    new_tile.paste(open_tile, (x * offset_x, y * offset_y))
            except Exception:
                continue

    return new_tile.resize((offset_x, offset_y), Image.LANCZOS)

# Download tiles in parallel
def download_tile(zoom, x, y):
    """Download tile from S3, return (x, y, image data)."""
    key = f'tiles/{zoom+3}/{x}/{y}.png'
    try:
        file_body = s3.get_object(Bucket='nzomap', Key=key)['Body'].read()
        return (x, y, file_body)
    except Exception:
        return None

# Upload tiles in parallel
def upload_tile(zoom, x, y, image):
    """Upload processed tile to S3."""
    file_stream = io.BytesIO()
    image.quantize(colors=32, method=2).save(file_stream, format='PNG')
    key = f'tiles/{zoom}/{x}/{y}.png'
    s3.put_object(Body=file_stream.getvalue(), Bucket='nzomap', Key=key)

# Main processing function
def main(in_x, in_y):

    for zoom in [12, 9, 6, 3, 0]:
        parents = {}
        scale_factor = 8 ** ((12 - zoom) // 3)
        x_min = math.floor(in_x // (200 * scale_factor) / 8) * 8
        x_max = math.ceil((in_x + 5000) // (200 * scale_factor) / 8) * 8
        y_max = math.ceil((6553600 - in_y) // (200 * scale_factor) / 8) * 8
        y_min = math.floor((6553600 - in_y - 5000) // (200 * scale_factor) / 8) * 8

        print(f'Processing zoom level {zoom}')

        # Use ThreadPoolExecutor for parallel tile downloading
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(download_tile, zoom, x, y): (x, y) for x in range(x_min, x_max) for y in range(y_min, y_max)}
            
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result:
                    x, y, file_body = result
                    x_parent, y_parent = x // 8, y // 8
                    x_off, y_off = x % 8, y % 8

                    parents.setdefault(x_parent, {}).setdefault(y_parent, {}).setdefault(x_off, {})[y_off] = file_body

        # Process and upload tiles
        running = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            upload_futures = []
            for x in parents:
                for y in parents[x]:
                    new_tile = join_tiles(parents[x][y], 474, 474)
                    upload_futures.append(executor.submit(upload_tile, zoom, x, y, new_tile))
                    running += 1
                    print(f'Uploaded {running} tiles')

            # Wait for all uploads to complete
            concurrent.futures.wait(upload_futures)



if __name__ == "__main__":

    r = requests.post('https://fcghgojd5l.execute-api.us-east-2.amazonaws.com/dev/create_zoom_tiles')
    returned_json = json.loads(r.json()["body"])
    for area in returned_json:
        x,y = area["xmin"], area["ymin"]
        print(f'Processing {x}, {y}')
        main(int(x),int(y))