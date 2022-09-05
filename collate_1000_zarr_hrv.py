import logging

import dask
import numpy as np
import xarray as xr
from satip.jpeg_xl_float_with_nans import JpegXlFloatWithNaNs

def preprocess_function(xr_data: xr.Dataset) -> xr.Dataset:
    y_coords = xr_data.coords["y_geostationary"].values
    x_coords  = xr_data.coords["x_geostationary"].values
    x_dataarray = xr.DataArray(data=np.expand_dims(xr_data.coords["x_geostationary"].values, axis=0),
                               dims=["time", "x_geostationary"],
                               coords=dict(time=xr_data.coords["time"].values, x_geostationary=x_coords))
    y_dataarray = xr.DataArray(data=np.expand_dims(xr_data.coords["y_geostationary"].values, axis=0),
                               dims=["time", "y_geostationary"],
                               coords=dict(time=xr_data.coords["time"].values, y_geostationary=y_coords))
    xr_data["x_geostationary_coordinates"] = x_dataarray
    xr_data["y_geostationary_coordinates"] = y_dataarray
    return xr_data

if __name__ == "__main__":
    import glob
    import os

    logging.disable(logging.DEBUG)
    logging.disable(logging.INFO)

    import warnings
    import dask

    dask.config.set(**{"array.slicing.split_large_chunks": False})
    warnings.filterwarnings("ignore", category=RuntimeWarning)
    for year in range(2018,2013, -1):
        pattern = f"{year}"
        # Get all files for a month, and use that as the name for the empty one, zip up at end and download
        data_files = sorted(list(glob.glob(os.path.join("/mnt/storage_ssd_4tb/EUMETSAT_Zarr/", f"{pattern}*.zarr.zip"))))
        print(len(data_files))
        hrv_data_files = sorted(list(glob.glob(os.path.join("/mnt/storage_ssd_4tb/EUMETSAT_Zarr/", f"hrv_{pattern}*.zarr.zip"))))
        print(len(hrv_data_files))
        if len(data_files) == 0 or len(hrv_data_files) == 0:
            continue
        chunks = len(hrv_data_files) // 1000
        for i in range(chunks):
            dataset = xr.open_mfdataset(
                hrv_data_files[i*1000:(i+1)*1000],
                chunks="auto",  # See issue #456 for why we use "auto".
                mode="r",
                engine="zarr",
                concat_dim="time",
                consolidated=True,
                preprocess=preprocess_function,
                combine="nested",
            ).chunk({"time": 1,  "x_geostationary": int(5568/4), "y_geostationary": int(4176/4), "variable": 1})
            #dataset = xr.open_mfdataset(hrv_data_files[i*1000:(i+1)*1000], engine="zarr")
            print(dataset)
            compression_algos = {
                "jpeg-xl": JpegXlFloatWithNaNs(lossless=False, distance=0.4, effort=8),
            }
            compression_algo = compression_algos["jpeg-xl"]
            hrv_zarr_mode_to_extra_kwargs = {
                "a": {"append_dim": "time"},
                "w": {
                    "encoding": {
                        "data": {
                            "compressor": compression_algo,
                        },
                        "time": {"units": "nanoseconds since 1970-01-01"},
                    }
                },
            }
            extra_kwargs = hrv_zarr_mode_to_extra_kwargs["w"]
            dataset.to_zarr(f"/mnt/storage_ssd_4tb/1000_zarrs/hrv_{year}_{str(i).zfill(6)}-of-{str(chunks).zfill(6)}.zarr", compute=True, **extra_kwargs, consolidated=True)

