from huggingface_hub import HfApi
import glob
import os

files = glob.glob("/mnt/storage_ssd_4tb/1000_zarrs/*")
api = HfApi()
hf_files = api.list_repo_files("openclimatefix/eumetsat-rss", repo_type="dataset")

for f in files:
    if "hrv_" in f:
        path_in_repo = f"data/{f.split('hrv_')[-1][:4]}/{f.split('hrv_')[-1]}"
    else:
        path_in_repo = f"data/{f.split('/')[-1][:4]}/{f.split('/')[-1]}"
    if path_in_repo in hf_files:
        continue
    print(path_in_repo)
    try:
        api.upload_file(
                        path_or_fileobj=f,
                        path_in_repo=path_in_repo,
                        repo_id="openclimatefix/eumetsat-rss",
                        repo_type="dataset",
                    )
    except:
        continue
