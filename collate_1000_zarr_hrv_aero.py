import logging

import zarr
import dask
import numpy as np
import xarray as xr
from satip.jpeg_xl_float_with_nans import JpegXlFloatWithNaNs

def preprocess_function(xr_data: xr.Dataset) -> xr.Dataset:
    attrs = xr_data.attrs
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
    xr_data.attrs = attrs
    return xr_data

if __name__ == "__main__":
    import glob
    import os

    logging.disable(logging.DEBUG)
    logging.disable(logging.INFO)

    import warnings
    import dask
    import random
    from huggingface_hub import HfApi

    dask.config.set(**{"array.slicing.split_large_chunks": False})
    warnings.filterwarnings("ignore", category=RuntimeWarning)
    years = list(range(2022,2018, -1))
    random.shuffle(years)
    api = HfApi()
    files = api.list_repo_files("openclimatefix/eumetsat-rss", repo_type="dataset")
    hf_files = [file for file in files if file.startswith("data/")]

    for year in years:
        pattern = f"{year}"
        # Get all files for a month, and use that as the name for the empty one, zip up at end and download
        data_files = sorted(list(glob.glob(os.path.join("/run/media/jacob/SSD2/modal/", f"{pattern}*.zarr.zip"))))
        print(len(data_files))
        hrv_data_files = sorted(list(glob.glob(os.path.join("/run/media/jacob/SSD2/modal/", f"hrv_{pattern}*.zarr.zip"))))
        print(len(hrv_data_files))
        if len(data_files) == 0 or len(hrv_data_files) == 0:
            continue
        chunks = len(hrv_data_files) // 1000
        shards = list(range(chunks + 1))
        random.shuffle(shards)
        for i in shards:
            files = api.list_repo_files("openclimatefix/eumetsat-rss", repo_type="dataset")
            hf_files = [file for file in files if file.startswith(f"data/{year}/hrv/")]
            shard_path_in_repo = f"data/{year}/hrv/{year}_{str(i).zfill(6)}-of-{str(chunks).zfill(6)}.zarr.zip"
            if shard_path_in_repo in hf_files:
                continue
            if os.path.exists(f"/run/media/jacob/data/1000_zarrs/hrv_{year}_{str(i).zfill(6)}-of-{str(chunks).zfill(6)}.zarr.zip"):
                continue
            try:
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
            except OSError:
                import time
                time.sleep(5)
                dataset = xr.open_mfdataset(
                    hrv_data_files[i * 1000:(i + 1) * 1000],
                    chunks="auto",  # See issue #456 for why we use "auto".
                    mode="r",
                    engine="zarr",
                    concat_dim="time",
                    consolidated=True,
                    preprocess=preprocess_function,
                    combine="nested",
                ).chunk({"time": 1, "x_geostationary": int(5568 / 4), "y_geostationary": int(4176 / 4), "variable": 1})
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
            out_filename = f"/run/media/jacob/data/1000_zarrs/hrv_{year}_{str(i).zfill(6)}-of-{str(chunks).zfill(6)}.zarr.zip"
            with zarr.ZipStore(
                    out_filename,
                    mode="w") as store:
                dataset.to_zarr(store, compute=True, **extra_kwargs, consolidated=True)
            dataset.close()
            del dataset
            shard_path_in_repo = f"data/{year}/hrv/{year}_{str(i).zfill(6)}-of-{str(chunks).zfill(6)}.zarr.zip"
            if shard_path_in_repo in hf_files:
                print(f"Skipping: {shard_path_in_repo}")
                os.remove(str(out_filename))
                continue
            try:
                api.upload_file(path_or_fileobj=out_filename,
                                path_in_repo=shard_path_in_repo,
                                repo_id="openclimatefix/eumetsat-rss",
                                repo_type="dataset",
                                )
                os.remove(str(out_filename))
            except Exception as e:
                print(e)
                continue

