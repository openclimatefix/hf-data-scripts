from huggingface_hub import HfApi

api = HfApi()
files = api.list_repo_files("openclimatefix/gfs-reforecast", repo_type="dataset")
data_files = [file for file in files if file.startswith("data")]

for dfile in data_files:
    if "datadata" in dfile:
        api.delete_file(path_in_repo=dfile, repo_id="openclimatefix/gfs-reforecast", repo_type="dataset")
    elif "GFSv16/hrv_" in dfile:
        api.delete_file(path_in_repo=dfile, repo_id="openclimatefix/gfs-reforecast", repo_type="dataset")
    else:
        f = dfile.split("GFSv16/")[-1]
        # Satellite ones are longer than GFS ones
        # 4 year + 2 month + 12 + .zarr.zip
        f = f.split(".zarr.zip")[0].split("/")
        if len(f) == 12:
            print(dfile)
            #api.delete_file(path_in_repo=dfile, repo_id="openclimatefix/gfs-reforecast", repo_type="dataset")
