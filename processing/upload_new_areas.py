
import pandas as pd
import geopandas as gpd
import requests


# this function takes a csv with cols wkt & tilename, and creates tiles that list all tilenames contained within
# a specified big tile size
def identify_lidar_tiles(shp,small_tile,grid_file,name):
    gdf = gpd.GeoDataFrame.from_file(shp)

    # get the grid tiles
    df = pd.read_csv(grid_file)

    output_payloads = []

    for row in df.itertuples():
        xmin = int(row.left)
        ymin = int(row.bottom)
        xmax = xmin + small_tile
        ymax = ymin + small_tile

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
                "parent_block": f"{name}_{int(xmin)}_{int(ymin)}",
                "file_list": file_list,
                'overwrite': True
            }
            output_payloads.append(payload)

    return output_payloads


def send_new_lidar_area(payload):
    endpoint = 'https://fcghgojd5l.execute-api.us-east-2.amazonaws.com/dev'

    r = requests.post(endpoint, json=payload)
    return r.status_code


payloads = identify_lidar_tiles('./processing/indexes/bop_prepared.shp',5000,'./processing/indexes/bop_grid.csv','NZ20_BOP')

for payload in payloads:
    print(payload)
    break
    #print(send_new_lidar_area(payload))
