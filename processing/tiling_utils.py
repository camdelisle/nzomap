import os
import rasterio
from rasterio.enums import Resampling
from rasterio.merge import merge
from rasterio.warp import reproject
from rasterio.windows import from_bounds
from rasterio.transform import Affine
from rasterio.crs import CRS
import numpy as np
import math

EXPECTED_CRS = CRS.from_wkt('''
PROJCRS["NZGD2000 / New Zealand Transverse Mercator 2000",
    BASEGEOGCRS["NZGD2000",
        DATUM["New Zealand Geodetic Datum 2000",
            ELLIPSOID["GRS 1980",6378137,298.257222101,
                LENGTHUNIT["metre",1]]],
        PRIMEM["Greenwich",0,
            ANGLEUNIT["degree",0.0174532925199433]],
        ID["EPSG",4167]],
    CONVERSION["New Zealand Transverse Mercator 2000",
        METHOD["Transverse Mercator",
            ID["EPSG",9807]],
        PARAMETER["Latitude of natural origin",-41,
            ANGLEUNIT["degree",0.0174532925199433],
            ID["EPSG",8801]],
        PARAMETER["Longitude of natural origin",173,
            ANGLEUNIT["degree",0.0174532925199433],
            ID["EPSG",8802]],
        PARAMETER["Scale factor at natural origin",0.9996,
            SCALEUNIT["unity",1],
            ID["EPSG",8805]],
        PARAMETER["False easting",1600000,
            LENGTHUNIT["metre",1],
            ID["EPSG",8806]],
        PARAMETER["False northing",10000000,
            LENGTHUNIT["metre",1],
            ID["EPSG",8807]]],
    CS[cartesian,2],
        AXIS["northing (N)",north,
            ORDER[1],
            LENGTHUNIT["metre",1]],
        AXIS["easting (E)",east,
            ORDER[2],
            LENGTHUNIT["metre",1]],
    ID["EPSG",2193]]
''')


async def crop_and_tile_pngs(process_dir,bounds,tile_size_m):
    """ function which takes a directory of pngs and crops them to the bounds specified
    and then tiles them into smaller pngs of the specified size
    """

    def snap_down(val, res):
        return math.floor(val / res) * res

    def snap_up(val, res):
        return math.ceil(val / res) * res

    nodata_value = 255  # white for 8-bit PNGs

    input_dir = os.path.join(process_dir, 'output')
    output_dir = os.path.join(process_dir, 'uploads')

    crs = EXPECTED_CRS  # Assuming all files have the same CRS

    # alternative approach:

    # === Step 1: Merge all input georeferenced PNGs - only depr files ===
    src_files = [rasterio.open(os.path.join(input_dir, f),crs=EXPECTED_CRS) for f in os.listdir(input_dir) if f.lower().endswith("depr.png")]
    if not src_files:
        raise ValueError("No PNGs found in input directory.")
    
    # we need to slightly increase the bounds to avoid seams
    temp_bounds = (bounds[0]-1, bounds[1]-1, bounds[2]+1, bounds[3]+1)

    mosaic, mosaic_transform = merge(
        src_files,
        bounds=temp_bounds,
        nodata=nodata_value,
    )
    # Force fill_value to be white across all bands
    nodata_rgb = 255
    mosaic[mosaic == nodata_value] = nodata_rgb  # just to be safe

    dtype = src_files[0].dtypes[0]
    for src in src_files:
        src.close()

    # === Step 2: Resample to 0.4237288136m resolution ===
    # required as NZOMAP was built using 0.4237288136m resolution (200m / 472px), input is at 0.4233333m resolution
    # we have to resample, or we will get an error seam every 5000m
    target_res = 0.4237288136

    # Snap bounds to exact pixel grid
    snapped_left = snap_down(temp_bounds[0], target_res)
    snapped_bottom = snap_down(temp_bounds[1], target_res)
    snapped_right = snap_up(temp_bounds[2], target_res)
    snapped_top = snap_up(temp_bounds[3], target_res)

    # Update width/height based on snapped bounds
    target_width = int(round((snapped_right - snapped_left) / target_res))
    target_height = int(round((snapped_top - snapped_bottom) / target_res))

    # Now your transform is exactly aligned
    dst_transform = Affine(
        target_res, 0, snapped_left,
        0, -target_res, snapped_top
    )

    resampled = np.full((mosaic.shape[0], target_height, target_width), nodata_value, dtype=dtype)

    for band in range(mosaic.shape[0]):
        reproject(
            source=mosaic[band],
            destination=resampled[band],
            src_transform=mosaic_transform,
            src_crs=crs,
            dst_transform=dst_transform,
            dst_crs=crs,
            resampling=Resampling.nearest,  
            src_nodata=nodata_value,
            dst_nodata=nodata_value,
            num_threads=4
        )
    
    # now crop back to the original bounds
    crop_window = from_bounds(*bounds, transform=dst_transform)
    row_off, col_off = int(np.round(crop_window.row_off)), int(np.round(crop_window.col_off))
    win_height, win_width = int(np.round(crop_window.height)), int(np.round(crop_window.width))
    cropped = resampled[:, row_off:row_off+win_height, col_off:col_off+win_width]
    cropped_transform = dst_transform * Affine.translation(col_off, row_off)
    
    # write the resampled image to a file for debugging
    #resampled_path = os.path.join(process_dir, 'resampled.png')
    #with rasterio.open(resampled_path, "w", driver="PNG", height=cropped.shape[1], width=cropped.shape[2], count=cropped.shape[0], dtype=dtype, crs=crs, transform=cropped_transform) as dst:
    #    dst.write(cropped)

    # === Step 3: Tile into 200m x 200m (472px x 472px) ===
    tile_width_px = 472
    tile_height_px = 472
    bands, height, width = cropped.shape

    for i in range(0, width, tile_width_px):
        for j in range(0, height, tile_height_px):

            w = min(tile_width_px, width - i)
            h = min(tile_height_px, height - j)

            # Extract the tile
            tile_data = np.full((bands, tile_height_px, tile_width_px), nodata_value, dtype=dtype)
            tile_data[:, 0:h, 0:w] = cropped[:, j:j+h, i:i+w]

            # Compute the transform for this tile
            tile_transform = cropped_transform * Affine.translation(i, j)

            # Save the tile
            tile_meta = {
                "driver": "PNG",
                "height": tile_height_px,
                "width": tile_width_px,
                "count": bands,
                "dtype": dtype,
                "count": 3,               # RGB
                "crs": crs,
                "transform": tile_transform,
                "nodata": None
            }

            tile_path = os.path.join(output_dir, f"tile_{int(bounds[0]+((i*200.0)/472.0))}_{int(bounds[3]-(((j+472)*200)/472))}.png")
            with rasterio.open(tile_path, "w", **tile_meta) as dst:
                dst.write(tile_data)