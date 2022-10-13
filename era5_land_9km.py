import cdsapi
import xarray as xr
import zarr
import numcodecs
import os
from huggingface_hub import HfApi

api = HfApi()
files = api.list_repo_files("openclimatefix/era5-land", repo_type="dataset")
c = cdsapi.Client()

for year in range(2022, 2015, -1):
    for month in range(1, 13):
        for day in range(1, 31):
            files = api.list_repo_files("openclimatefix/era5-land", repo_type="dataset")
            data_files = [file for file in files if file.startswith("data/")]
            if f"data/{year}/{year}_{str(month).zfill(2)}_{str(day).zfill(2)}.zarr.zip" in data_files:
                continue
            try:
                c.retrieve(
    'reanalysis-era5-land',
    {
        'format': 'netcdf',
        'variable': [
            '10m_u_component_of_wind', '10m_v_component_of_wind', '2m_dewpoint_temperature',
            '2m_temperature', 'evaporation_from_bare_soil', 'evaporation_from_open_water_surfaces_excluding_oceans',
            'evaporation_from_the_top_of_canopy', 'evaporation_from_vegetation_transpiration', 'forecast_albedo',
            'lake_bottom_temperature', 'lake_ice_depth', 'lake_ice_temperature',
            'lake_mix_layer_depth', 'lake_mix_layer_temperature', 'lake_shape_factor',
            'lake_total_layer_temperature', 'leaf_area_index_high_vegetation', 'leaf_area_index_low_vegetation',
            'potential_evaporation', 'runoff', 'skin_reservoir_content',
            'skin_temperature', 'snow_albedo', 'snow_cover',
            'snow_density', 'snow_depth', 'snow_depth_water_equivalent',
            'snow_evaporation', 'snowfall', 'snowmelt',
            'soil_temperature_level_1', 'soil_temperature_level_2', 'soil_temperature_level_3',
            'soil_temperature_level_4', 'sub_surface_runoff', 'surface_latent_heat_flux',
            'surface_net_solar_radiation', 'surface_net_thermal_radiation', 'surface_pressure',
            'surface_runoff', 'surface_sensible_heat_flux', 'surface_solar_radiation_downwards',
            'surface_thermal_radiation_downwards', 'temperature_of_snow_layer', 'total_evaporation',
            'total_precipitation', 'volumetric_soil_water_layer_1', 'volumetric_soil_water_layer_2',
            'volumetric_soil_water_layer_3', 'volumetric_soil_water_layer_4',
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
                    f'/mnt/storage_ssd_4tb/land_download.nc')
                data = xr.open_dataset(f"/mnt/storage_ssd_4tb/land_download.nc", engine="netcdf4")
                print(data)
                encoding = {var: {"compressor": numcodecs.get_codec(dict(id="zlib", level=5))} for var in
                            data.data_vars}
                if f"data/{year}/{year}_{str(month).zfill(2)}_{str(day).zfill(2)}.zarr.zip" in data_files:
                    continue
                d = data.chunk({"time": 1})
                with zarr.ZipStore(
                        f'/mnt/storage_ssd_4tb/{year}{str(month).zfill(2)}{str(day).zfill(2)}.zarr.zip',
                        mode='w') as store:
                    d.to_zarr(store, encoding=encoding, compute=True)
                api.upload_file(
                    path_or_fileobj=f'/mnt/storage_ssd_4tb/{year}{str(month).zfill(2)}{str(day).zfill(2)}.zarr.zip',
                    path_in_repo=f"data/{year}/{year}_{str(month).zfill(2)}_{str(day).zfill(2)}.zarr.zip",
                    repo_id="openclimatefix/era5-land",
                    repo_type="dataset",
                )
                os.remove(
                    f'/mnt/storage_ssd_4tb/{year}{str(month).zfill(2)}{str(day).zfill(2)}.zarr.zip')
            except Exception as e:
                print(e)
                continue
