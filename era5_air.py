import cdsapi
import xarray as xr
import zarr
import numcodecs
import os
from huggingface_hub import HfApi
api = HfApi()
files = api.list_repo_files("openclimatefix/era5-reanalysis", repo_type="dataset")
data_files = [file for file in files if file.startswith("data/")]
c = cdsapi.Client()

for year in range(2022,2015,-1):
    for month in range(1,13):
        files = api.list_repo_files("openclimatefix/era5-reanalysis", repo_type="dataset")
        data_files = [file for file in files if file.startswith("data/")]
        for day in range(1,31):
            data_files = [file for file in files if file.startswith("data/")]
            if f"data/air/{year}/{str(month).zfill(2)}/{year}_{str(month).zfill(2)}_{str(day).zfill(2)}.zarr.zip" in data_files:
                continue
            try:
                c.retrieve(
                    'reanalysis-era5-pressure-levels',
                    {
                        'product_type': 'reanalysis',
                        'variable': [
                            'divergence', 'fraction_of_cloud_cover', 'geopotential',
                            'ozone_mass_mixing_ratio', 'potential_vorticity', 'relative_humidity',
                            'specific_cloud_ice_water_content', 'specific_cloud_liquid_water_content', 'specific_humidity',
                            'specific_rain_water_content', 'specific_snow_water_content', 'temperature',
                            'u_component_of_wind', 'v_component_of_wind', 'vertical_velocity',
                            'vorticity',
                        ],
                        'pressure_level': [
                            '1', '2', '3',
                            '5', '7', '10',
                            '20', '30', '50',
                            '70', '100', '125',
                            '150', '175', '200',
                            '225', '250', '300',
                            '350', '400', '450',
                            '500', '550', '600',
                            '650', '700', '750',
                            '775', '800', '825',
                            '850', '875', '900',
                            '925', '950', '975',
                            '1000',
                        ],
                        'year': str(year),
                        'month': str(month).zfill(2),
                        'day': str(day).zfill(2),
                        'time': [
                            '00:00', '01:00', '02:00',
                            '03:00', '04:00', '05:00',
                            '06:00', '07:00', '08:00',
                            '09:00', '10:00', '11:00',
                            '12:00', '13:00', '14:00',
                            '15:00', '16:00', '17:00',
                            '18:00', '19:00', '20:00',
                            '21:00', '22:00', '23:00',
                        ],
                        'format': 'netcdf',
                    },
                    f'download_air.nc')
                data = xr.open_dataset("/home/jacob/Downloads/download_air.nc", engine="netcdf4")
                print(data)
                encoding = {var: {"compressor": numcodecs.get_codec(dict(id="zlib", level=5))} for var in data.data_vars}
                for i in range(24):
                    if f"data/air/{year}/{str(month).zfill(2)}/{year}_{str(month).zfill(2)}_{str(day).zfill(2)}_{str(i).zfill(2)}.zarr.zip" in data_files:
                        continue
                    d = data.chunk({"time": 1})
                    with zarr.ZipStore(f'/run/media/jacob/Elements/{year}_{str(month).zfill(2)}_{str(day).zfill(2)}.zarr.zip', mode='w') as store:
                        d.to_zarr(store, encoding=encoding, compute=True)
                    api.upload_file(
                        path_or_fileobj=f'/run/media/jacob/Elements/{year}_{str(month).zfill(2)}_{str(day).zfill(2)}.zarr.zip',
                        path_in_repo=f"data/air/{year}/{str(month).zfill(2)}/{year}{str(month).zfill(2)}{str(day).zfill(2)}.zarr.zip",
                        repo_id="openclimatefix/era5",
                        repo_type="dataset",
                        )
                    os.remove(
                        f'/run/media/jacob/Elements/{year}_{str(month).zfill(2)}_{str(day).zfill(2)}.zarr.zip')
            except:
                continue

