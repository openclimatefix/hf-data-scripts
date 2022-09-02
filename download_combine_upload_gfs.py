import sys, os
import requests

def check_file_status(filepath, filesize):
    size = int(os.stat(filepath).st_size)
    percent_complete = (size/filesize)*100

url = 'https://rda.ucar.edu/cgi-bin/login'
values = {'email' : '<>', 'passwd' : '<>', 'action' : 'login'}
# Authenticate
ret = requests.post(url,data=values)
if ret.status_code != 200:
    print('Bad Authentication')
    print(ret.text)
    exit(1)
dspath = 'https://rda.ucar.edu/data/ds084.1/'
#dspath = 'https://rda.ucar.edu/data/ds084.3/'
filelist = []
import glob
from tqdm import tqdm
import pywgrib2_xr
import zarr
import numcodecs
from huggingface_hub import HfApi
api = HfApi()
files = api.list_repo_files("openclimatefix/gfs-reforecast", repo_type="dataset")
data_files = [file for file in files if file.startswith("data/forecasts/GFSv16/")]
for year in [2021,2022]:
    for month in range(1,13):
        for day in range(1,32):
            for hour in [0,6,12,18]:
                f = f"/run/media/jacob/7214E0FE36731680/{year}{str(month).zfill(2)}{str(day).zfill(2)}{str(hour).zfill(2)}.zarr.zip"
                shard_path_in_repo = str(f).split("gfs-reforecast/")[-1]  # Now path from root
                times = shard_path_in_repo.split("/")[-1].split(".")[0]
                year = times[:4]
                month = times[4:6]
                shard_path_in_repo = shard_path_in_repo.split("/")[0] + f"/forecasts/GFSv16/{year}/{month}/" + \
                                     shard_path_in_repo.split("/")[-1]
                print(shard_path_in_repo)
                if shard_path_in_repo in data_files:
                    continue
                for f_hour in [0,3,6,9,12,15,18,21,24,27,30,33,36,39,42,45,48]:
                    path = f"{year}/{year}{str(month).zfill(2)}{str(day).zfill(2)}/gfs.0p25.{year}{str(month).zfill(2)}{str(day).zfill(2)}{str(hour).zfill(2)}.f0{str(f_hour).zfill(2)}.grib2"
                    filelist.append(path)
                for file in tqdm(filelist):
                    filename=dspath+file
                    file_base = os.path.basename(file)
                    print('Downloading',file_base)
                    req = requests.get(filename, cookies = ret.cookies, allow_redirects=True, stream=True)
                    try:
                        filesize = int(req.headers['Content-length'])
                        with open(os.path.join(f"/run/media/jacob/Elements/GFS_025/", file_base), 'wb') as outfile:
                            chunk_size=1048576
                            for chunk in req.iter_content(chunk_size=chunk_size):
                                outfile.write(chunk)
                                if chunk_size < filesize:
                                    check_file_status(os.path.join(f"/run/media/jacob/Elements/GFS_025/", file_base), filesize)
                        check_file_status(os.path.join(f"/run/media/jacob/Elements/GFS_025/", file_base), filesize)
                    except Exception as e:
                        print(e)
                        continue
                    if len(filelist) != 17:  # All of them exist
                        continue
                    try:
                        tmpl = pywgrib2_xr.make_template(filelist)
                        dataset = pywgrib2_xr.open_dataset(filelist, template=tmpl)
                        times_to_drop = [time_var for time_var in dataset.dims if
                                         "time" in time_var and len(dataset[time_var]) != 17]
                        dataset = dataset.drop_dims(times_to_drop)
                        time_to_rename = {[time_var for time_var in dataset.dims if "time" in time_var][0]: "time"}
                        dataset = dataset.rename(time_to_rename)
                        # Rename the super long PV ones
                        data_vars_to_change = {
                            var: var.replace("2e-06_K*m2*kg-1*s-1", "2e06Km2kgs").replace("-", "Neg").replace("=", "")
                            for var in dataset.data_vars if "2e-06_K" in var}
                        dataset = dataset.rename(data_vars_to_change)
                        # compression = numcodecs.get_codec(dict(id="bz2", level=5))
                        # comp = dict(compressor=compression)
                        # encoding = {var: comp for var in dataset.data_vars}
                        encoding = {x: {"compressor": numcodecs.get_codec(dict(id="bz2", level=9))} for x in
                                    dataset.data_vars}
                        encoding.update({"reftime": dict(units="nanoseconds since 1970-01-01")})
                        dataset = dataset.chunk({"time": 1, "latitude": 721, "longitude": 1440})
                        with zarr.ZipStore(
                                f"/run/media/jacob/7214E0FE36731680/{year}{str(month).zfill(2)}{str(day).zfill(2)}{str(hour).zfill(2)}.zarr.zip",
                                mode="w") as store:
                            dataset.to_zarr(store, encoding=encoding, compute=True, consolidated=True, mode="w")
                        # Delete the original files
                        for f in filelist:
                            try:
                                os.remove(f)
                            except:
                                continue
                    except:
                        continue

                    from pathlib import Path
                    import os

                    base_dir = "/run/media/jacob/7214E0FE36731680/"
                    api = HfApi()
                    files = api.list_repo_files("openclimatefix/gfs-reforecast", repo_type="dataset")
                    data_files = [file for file in files if file.startswith("data/forecasts/GFSv16/")]
                    f = f"/run/media/jacob/7214E0FE36731680/{year}{str(month).zfill(2)}{str(day).zfill(2)}{str(hour).zfill(2)}.zarr.zip"
                    shard_path_in_repo = str(f).split("gfs-reforecast/")[-1]  # Now path from root
                    times = shard_path_in_repo.split("/")[-1].split(".")[0]
                    year = times[:4]
                    month = times[4:6]
                    shard_path_in_repo = shard_path_in_repo.split("/")[0] + f"/forecasts/GFSv16/{year}/{month}/" + \
                                         shard_path_in_repo.split("/")[-1]
                    print(shard_path_in_repo)
                    if shard_path_in_repo in data_files:
                        print(f"Skipping: {shard_path_in_repo}")
                        os.remove(str(f))
                        continue
                    try:
                        api.upload_file(path_or_fileobj=str(f),
                                        path_in_repo=shard_path_in_repo,
                                        repo_id="openclimatefix/gfs-reforecast",
                                        repo_type="dataset",
                                        )
                        os.remove(str(f))
                    except Exception as e:
                        print(e)
                        continue

                    filelist = []

