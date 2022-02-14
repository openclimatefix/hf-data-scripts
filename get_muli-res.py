import xarray as xr
import s3fs
from collections import defaultdict

fs = s3fs.S3FileSystem(anon=True) #connect to s3 bucket!
# Get list of GOES data products to return

_URLS = {"goes_16": "s3://noaa-goes16", "goes_17": "s3://noaa-goes17",}

_PRODUCTS = {"rad": "ABI-L1b-Rad", "cmip": "ABI-L2-CMIP", "cmip_2km": "ABI-L2-MCMIP"}

for region in ["F", "C", "M"]:
    for product_name in ["rad", "cmip"]:
        product = _PRODUCTS[product_name]
        file_list = defaultdict(list)
        time_range = [2019, 2020]
        for year in time_range:
            for day in range(0,367):
                for hour in range(0,24):
                    for channel in range(1,17):
                        tmp_list = sorted(fs.glob(f'{filepath}/{product}{region}/{year}/{day}/{hour}/*M6C{str(channel).zfill(2)}*.nc'))
                        if tmp_list:
                            file_list[channel].append(tmp_list)
        # Save out Train time range for GOES-17, and GOES-16
        import json
        with open(f'train-17-{product_name}{region}.json', 'w') as f:
            json.dump(file_list, f)

        file_list = defaultdict(list)
        time_range = [2021]
        for year in time_range:
            for day in range(0,367):
                for hour in range(0,24):
                    for channel in range(1,17):
                        tmp_list = sorted(fs.glob(f'{filepath}/{product}{region}/{year}/{day}/{hour}/*M6C{str(channel).zfill(2)}*.nc'))
                        if tmp_list:
                            file_list[channel].append(tmp_list)
        with open(f'test-17-{product_name}{region}.json', 'w') as f:
            json.dump(file_list, f)

        filepath = "s3://noaa-goes16"
        file_list = defaultdict(list)
        time_range = [2019, 2020]
        for year in time_range:
            for day in range(0,367):
                for hour in range(0,24):
                    for channel in range(1,17):
                        tmp_list = sorted(fs.glob(f'{filepath}/{product}{region}/{year}/{day}/{hour}/*M6C{str(channel).zfill(2)}*.nc'))
                        if tmp_list:
                            file_list[channel].append(tmp_list)
        # Save out Train time range for GOES-17, and GOES-16
        import json
        with open(f'train-16-{product_name}{region}-2019-2020.json', 'w') as f:
            json.dump(file_list, f)

        time_range = [2017, 2018]
        for year in time_range:
            for day in range(0,367):
                for hour in range(0,24):
                    for channel in range(1,17):
                        tmp_list = sorted(fs.glob(f'{filepath}/{product}{region}/{year}/{day}/{hour}/*M6C{str(channel).zfill(2)}*.nc'))
                        if tmp_list:
                            file_list[channel].append(tmp_list)
        # Save out Train time range for GOES-17, and GOES-16
        import json
        with open(f'train-16-{product_name}{region}-2017-2020.json', 'w') as f:
            json.dump(file_list, f)

        file_list = defaultdict(list)
        time_range = [2021]
        for year in time_range:
            for day in range(0,367):
                for hour in range(0,24):
                    for channel in range(1,17):
                        tmp_list = sorted(fs.glob(f'{filepath}/{product}{region}/{year}/{day}/{hour}/*M6C{str(channel).zfill(2)}*.nc'))
                        if tmp_list:
                            file_list[channel].append(tmp_list)
        with open(f'test-16-{product_name}{region}.json', 'w') as f:
            json.dump(file_list, f)
