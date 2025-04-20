import os
import rasterio
from rasterio.merge import merge
from rasterio.windows import from_bounds
from rasterio.transform import Affine
import numpy as np

async def crop_and_tile_pngs(process_dir,bounds,tile_size_m):
    """ function which takes a directory of pngs and crops them to the bounds specified
    and then tiles them into smaller pngs of the specified size
    """

    nodata_value = 255  # white for 8-bit PNGs

    input_dir = os.path.join(process_dir, 'output')
    output_dir = os.path.join(process_dir, 'uploads')

    # === Step 1: Merge all input georeferenced PNGs - only depr files ===
    src_files = [rasterio.open(os.path.join(input_dir, f)) for f in os.listdir(input_dir) if f.lower().endswith("depr.png")]
    if not src_files:
        raise ValueError("No PNGs found in input directory.")

    mosaic, mosaic_transform = merge(src_files)
    meta = src_files[0].meta.copy()
    crs = src_files[0].crs
    for src in src_files:
        src.close()
    
    meta.update({
        "driver": "PNG",
        "height": mosaic.shape[1],
        "width": mosaic.shape[2],
        "transform": mosaic_transform,
        "count": mosaic.shape[0],
        "nodata": nodata_value
    })

    # === Step 2: Crop to bounding box ===
    crop_window = from_bounds(*bounds, transform=mosaic_transform)
    row_off, col_off = int(crop_window.row_off), int(crop_window.col_off)
    win_height, win_width = int(crop_window.height), int(crop_window.width)
    cropped = mosaic[:, row_off:row_off+win_height, col_off:col_off+win_width]
    cropped_transform = mosaic_transform * Affine.translation(col_off, row_off)

    # === Step 3: Tile with padding for edges ===
    pixel_size_x = cropped_transform.a
    pixel_size_y = -cropped_transform.e
    tile_width_px = int(tile_size_m / pixel_size_x)
    tile_height_px = int(tile_size_m / pixel_size_y)

    bands, height, width = cropped.shape

    for i in range(0, width, tile_width_px):
        for j in range(0, height, tile_height_px):
            # Create a blank tile and fill with nodata
            tile_data = np.full((bands, tile_height_px, tile_width_px), nodata_value, dtype=cropped.dtype)

            # Determine the actual window within the cropped data
            w = min(tile_width_px, width - i)
            h = min(tile_height_px, height - j)

            # Copy real data into the blank tile
            tile_data[:, 0:h, 0:w] = cropped[:, j:j+h, i:i+w]

            # Compute transform for this tile
            tile_transform = cropped_transform * Affine.translation(i, j)

            # Update profile
            tile_meta = meta.copy()
            tile_meta.update({
                "height": tile_height_px,
                "width": tile_width_px,
                "transform": tile_transform,
                "count": 3,               # RGB
                "dtype": tile_data.dtype,
                "driver": "PNG",
                "nodata": None            # important — don't let it apply transparency
            })

            # Save tile
            tile_path = os.path.join(output_dir, f"tile_{int(bounds[0]+((i*200.0)/472.0))}_{int(bounds[3]-(((j+472)*200)/472))}.png")
            with rasterio.open(tile_path, "w", **tile_meta) as dst:
                dst.write(tile_data)
