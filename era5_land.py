import cdsapi
import xarray as xr
import zarr
import numcodecs
import os
from huggingface_hub import HfApi

api = HfApi()
files = api.list_repo_files("openclimatefix/era5-reanalysis", repo_type="dataset")
c = cdsapi.Client()

for year in range(2022, 2015, -1):
    for month in range(1, 13):
        for day in range(1, 31):
            files = api.list_repo_files("openclimatefix/era5-reanalysis", repo_type="dataset")
            data_files = [file for file in files if file.startswith("data/")]
            if f"data/surface/{year}/{str(month).zfill(2)}/{year}_{str(month).zfill(2)}_{str(day).zfill(2)}_{str(23).zfill(2)}.zarr.zip" in data_files:
                continue
            try:
                c.retrieve(
                    'reanalysis-era5-single-levels',
                    {
                        'product_type': 'reanalysis',
                        'variable': [
                            '100m_u_component_of_wind', '100m_v_component_of_wind', '10m_u_component_of_neutral_wind',
                            '10m_u_component_of_wind', '10m_v_component_of_neutral_wind', '10m_v_component_of_wind',
                            '10m_wind_gust_since_previous_post_processing', '2m_dewpoint_temperature', '2m_temperature',
                            'air_density_over_the_oceans', 'angle_of_sub_gridscale_orography',
                            'anisotropy_of_sub_gridscale_orography',
                            'benjamin_feir_index', 'boundary_layer_dissipation', 'boundary_layer_height',
                            'charnock', 'clear_sky_direct_solar_radiation_at_surface', 'cloud_base_height',
                            'coefficient_of_drag_with_waves', 'convective_available_potential_energy',
                            'convective_inhibition',
                            'convective_precipitation', 'convective_rain_rate', 'convective_snowfall',
                            'convective_snowfall_rate_water_equivalent', 'downward_uv_radiation_at_the_surface',
                            'duct_base_height',
                            'eastward_gravity_wave_surface_stress', 'eastward_turbulent_surface_stress', 'evaporation',
                            'forecast_albedo', 'forecast_logarithm_of_surface_roughness_for_heat',
                            'forecast_surface_roughness',
                            'free_convective_velocity_over_the_oceans', 'friction_velocity', 'geopotential',
                            'gravity_wave_dissipation', 'high_cloud_cover', 'high_vegetation_cover',
                            'ice_temperature_layer_1', 'ice_temperature_layer_2', 'ice_temperature_layer_3',
                            'ice_temperature_layer_4', 'instantaneous_10m_wind_gust',
                            'instantaneous_eastward_turbulent_surface_stress',
                            'instantaneous_large_scale_surface_precipitation_fraction', 'instantaneous_moisture_flux',
                            'instantaneous_northward_turbulent_surface_stress',
                            'instantaneous_surface_sensible_heat_flux', 'k_index', 'lake_bottom_temperature',
                            'lake_cover', 'lake_depth', 'lake_ice_depth',
                            'lake_ice_temperature', 'lake_mix_layer_depth', 'lake_mix_layer_temperature',
                            'lake_shape_factor', 'lake_total_layer_temperature', 'land_sea_mask',
                            'large_scale_precipitation', 'large_scale_precipitation_fraction', 'large_scale_rain_rate',
                            'large_scale_snowfall', 'large_scale_snowfall_rate_water_equivalent',
                            'leaf_area_index_high_vegetation',
                            'leaf_area_index_low_vegetation', 'low_cloud_cover', 'low_vegetation_cover',
                            'maximum_2m_temperature_since_previous_post_processing', 'maximum_individual_wave_height',
                            'maximum_total_precipitation_rate_since_previous_post_processing',
                            'mean_direction_of_total_swell', 'mean_direction_of_wind_waves',
                            'mean_period_of_total_swell',
                            'mean_period_of_wind_waves', 'mean_sea_level_pressure', 'mean_square_slope_of_waves',
                            'mean_vertical_gradient_of_refractivity_inside_trapping_layer', 'mean_wave_direction',
                            'mean_wave_direction_of_first_swell_partition',
                            'mean_wave_direction_of_second_swell_partition',
                            'mean_wave_direction_of_third_swell_partition', 'mean_wave_period',
                            'mean_wave_period_based_on_first_moment',
                            'mean_wave_period_based_on_first_moment_for_swell',
                            'mean_wave_period_based_on_first_moment_for_wind_waves',
                            'mean_wave_period_based_on_second_moment_for_swell',
                            'mean_wave_period_based_on_second_moment_for_wind_waves',
                            'mean_wave_period_of_first_swell_partition',
                            'mean_wave_period_of_second_swell_partition', 'mean_wave_period_of_third_swell_partition',
                            'mean_zero_crossing_wave_period',
                            'medium_cloud_cover', 'minimum_2m_temperature_since_previous_post_processing',
                            'minimum_total_precipitation_rate_since_previous_post_processing',
                            'minimum_vertical_gradient_of_refractivity_inside_trapping_layer', 'model_bathymetry',
                            'near_ir_albedo_for_diffuse_radiation',
                            'near_ir_albedo_for_direct_radiation', 'normalized_energy_flux_into_ocean',
                            'normalized_energy_flux_into_waves',
                            'normalized_stress_into_ocean', 'northward_gravity_wave_surface_stress',
                            'northward_turbulent_surface_stress',
                            'ocean_surface_stress_equivalent_10m_neutral_wind_direction',
                            'ocean_surface_stress_equivalent_10m_neutral_wind_speed', 'peak_wave_period',
                            'period_corresponding_to_maximum_individual_wave_height', 'potential_evaporation',
                            'precipitation_type',
                            'runoff', 'sea_ice_cover', 'sea_surface_temperature',
                            'significant_height_of_combined_wind_waves_and_swell', 'significant_height_of_total_swell',
                            'significant_height_of_wind_waves',
                            'significant_wave_height_of_first_swell_partition',
                            'significant_wave_height_of_second_swell_partition',
                            'significant_wave_height_of_third_swell_partition',
                            'skin_reservoir_content', 'skin_temperature', 'slope_of_sub_gridscale_orography',
                            'snow_albedo', 'snow_density', 'snow_depth',
                            'snow_evaporation', 'snowfall', 'snowmelt',
                            'soil_temperature_level_1', 'soil_temperature_level_2', 'soil_temperature_level_3',
                            'soil_temperature_level_4', 'soil_type', 'standard_deviation_of_filtered_subgrid_orography',
                            'standard_deviation_of_orography', 'sub_surface_runoff', 'surface_latent_heat_flux',
                            'surface_net_solar_radiation', 'surface_net_solar_radiation_clear_sky',
                            'surface_net_thermal_radiation',
                            'surface_net_thermal_radiation_clear_sky', 'surface_pressure', 'surface_runoff',
                            'surface_sensible_heat_flux', 'surface_solar_radiation_downward_clear_sky',
                            'surface_solar_radiation_downwards',
                            'surface_thermal_radiation_downward_clear_sky', 'surface_thermal_radiation_downwards',
                            'temperature_of_snow_layer',
                            'toa_incident_solar_radiation', 'top_net_solar_radiation',
                            'top_net_solar_radiation_clear_sky',
                            'top_net_thermal_radiation', 'top_net_thermal_radiation_clear_sky', 'total_cloud_cover',
                            'total_column_cloud_ice_water', 'total_column_cloud_liquid_water', 'total_column_ozone',
                            'total_column_rain_water', 'total_column_snow_water',
                            'total_column_supercooled_liquid_water',
                            'total_column_water', 'total_column_water_vapour', 'total_precipitation',
                            'total_sky_direct_solar_radiation_at_surface', 'total_totals_index',
                            'trapping_layer_base_height',
                            'trapping_layer_top_height', 'type_of_high_vegetation', 'type_of_low_vegetation',
                            'u_component_stokes_drift', 'uv_visible_albedo_for_diffuse_radiation',
                            'uv_visible_albedo_for_direct_radiation',
                            'v_component_stokes_drift', 'vertical_integral_of_divergence_of_cloud_frozen_water_flux',
                            'vertical_integral_of_divergence_of_cloud_liquid_water_flux',
                            'vertical_integral_of_divergence_of_geopotential_flux',
                            'vertical_integral_of_divergence_of_kinetic_energy_flux',
                            'vertical_integral_of_divergence_of_mass_flux',
                            'vertical_integral_of_divergence_of_moisture_flux',
                            'vertical_integral_of_divergence_of_ozone_flux',
                            'vertical_integral_of_divergence_of_thermal_energy_flux',
                            'vertical_integral_of_divergence_of_total_energy_flux',
                            'vertical_integral_of_eastward_cloud_frozen_water_flux',
                            'vertical_integral_of_eastward_cloud_liquid_water_flux',
                            'vertical_integral_of_eastward_geopotential_flux',
                            'vertical_integral_of_eastward_heat_flux',
                            'vertical_integral_of_eastward_kinetic_energy_flux',
                            'vertical_integral_of_eastward_mass_flux', 'vertical_integral_of_eastward_ozone_flux',
                            'vertical_integral_of_eastward_total_energy_flux',
                            'vertical_integral_of_eastward_water_vapour_flux', 'vertical_integral_of_energy_conversion',
                            'vertical_integral_of_kinetic_energy',
                            'vertical_integral_of_mass_of_atmosphere', 'vertical_integral_of_mass_tendency',
                            'vertical_integral_of_northward_cloud_frozen_water_flux',
                            'vertical_integral_of_northward_cloud_liquid_water_flux',
                            'vertical_integral_of_northward_geopotential_flux',
                            'vertical_integral_of_northward_heat_flux',
                            'vertical_integral_of_northward_kinetic_energy_flux',
                            'vertical_integral_of_northward_mass_flux', 'vertical_integral_of_northward_ozone_flux',
                            'vertical_integral_of_northward_total_energy_flux',
                            'vertical_integral_of_northward_water_vapour_flux',
                            'vertical_integral_of_potential_and_internal_energy',
                            'vertical_integral_of_potential_internal_and_latent_energy',
                            'vertical_integral_of_temperature', 'vertical_integral_of_thermal_energy',
                            'vertical_integral_of_total_energy', 'vertically_integrated_moisture_divergence',
                            'volumetric_soil_water_layer_1',
                            'volumetric_soil_water_layer_2', 'volumetric_soil_water_layer_3',
                            'volumetric_soil_water_layer_4',
                            'wave_spectral_directional_width', 'wave_spectral_directional_width_for_swell',
                            'wave_spectral_directional_width_for_wind_waves',
                            'wave_spectral_kurtosis', 'wave_spectral_peakedness', 'wave_spectral_skewness',
                            'zero_degree_level',
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
                    f'download.nc')
                data = xr.open_dataset(f"/home/jacob/Downloads/download.nc", engine="netcdf4")
                print(data)
                encoding = {var: {"compressor": numcodecs.get_codec(dict(id="zlib", level=5))} for var in
                            data.data_vars}
                if f"data/surface/{year}/{str(month).zfill(2)}/{year}_{str(month).zfill(2)}_{str(day).zfill(2)}.zarr.zip" in data_files:
                    continue
                d = data.chunk({"time": 1})
                with zarr.ZipStore(
                        f'/run/media/jacob/Elements/surface/{year}/{str(month).zfill(2)}/{year}{str(month).zfill(2)}{str(day).zfill(2)}.zarr.zip',
                        mode='w') as store:
                    d.to_zarr(store, encoding=encoding, compute=True)
                api.upload_file(
                    path_or_fileobj=f'/run/media/jacob/Elements/surface/{year}/{str(month).zfill(2)}/{year}{str(month).zfill(2)}{str(day).zfill(2)}.zarr.zip',
                    path_in_repo=f"data/surface/{year}/{str(month).zfill(2)}/{year}{str(month).zfill(2)}{str(day).zfill(2)}.zarr.zip",
                    repo_id="openclimatefix/era5-reanalysis",
                    repo_type="dataset",
                )
                os.remove(
                    f'/run/media/jacob/Elements/surface/{year}/{str(month).zfill(2)}/{year}{str(month).zfill(2)}{str(day).zfill(2)}.zarr.zip')
            except Exception as e:
                print(e)
                continue
