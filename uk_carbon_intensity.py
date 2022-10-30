"""
Getting carbon intensity for the United Kingdom and storing in Zarr format.

References:
- https://carbonintensity.org.uk
- https://carbon-intensity.github.io/api-definitions
"""

import numpy as np
import pandas as pd
import xarray as xr


# %%
# Temporal date range
start_year: str = "2020"
end_year: str = "2022"

# %%
# Retrieve UK National carbon density data in JSON format from API
# Each API call is limited to 31 days so we build a list of URLs for each month
api_urls: list = []
for start_date in pd.date_range(start=start_year, end=end_year, freq="MS"):
    # Start date like 2020-02-01T00:00:00, end date like 2020-02-28T00:00:00
    end_date = start_date + pd.offsets.MonthEnd()
    api_urls.append(
        f"https://api.carbonintensity.org.uk/intensity/{start_date.isoformat()}Z/{end_date.isoformat()}Z"
    )

# Parse JSON from API call, concat data from every month into one dataframe
# Example JSON:
# {
#     "data": [
#         {
#             "from": "2018-01-20T12:00Z",
#             "to": "2018-01-20T12:30Z",
#             "intensity": {"forecast": 266, "actual": 263, "index": "moderate"},
#         }
#     ]
# }
df: pd.DataFrame = pd.concat(
    objs=[
        pd.read_json(path_or_buf=api_url, orient="split", convert_dates=["from", "to"])
        for api_url in api_urls
    ]
)
assert list(df.columns) == ["from", "to", "intensity"]
assert list(df.dtypes) == [
    pd.DatetimeTZDtype(unit="ns", tz="UTC"),
    pd.DatetimeTZDtype(unit="ns", tz="UTC"),
    np.object_,
]

# Split intensity column (dictionary format) into forecast, actual and index values
df_intensity: pd.DataFrame = pd.json_normalize(data=df.intensity)
assert list(df_intensity.columns) == ["forecast", "actual", "index"]
assert list(df_intensity.dtypes) == [np.int64, np.float64, np.object_]

# Create dataset by joining from/to dates with forecast/actual/index carbon intensity
time_coords: pd.Series = df["from"]  # use start date as index coordinate
ds: xr.Dataset = xr.Dataset(
    data_vars={
        "start_date": ("time", df["from"]),
        "end_date": ("time", df["to"]),
        "forecast": ("time", df_intensity["forecast"]),
        "actual": ("time", df_intensity["actual"]),
        "intensity_index": ("time", df_intensity["index"].astype(str)),
    },
    coords={"time": time_coords},
    attrs={
        "description": "UK National forecasted and actual carbon intensity data at half-hourly intervals"
    },
)
assert dict(ds.coords.dtypes) == {"time": np.dtype("<M8[ns]")}

# Add more metadata to the xarray.Dataset
# See https://carbon-intensity.github.io/api-definitions/#intensity-1
ds["forecast"].attrs = dict(
    description="The forecast Carbon Intensity for the half hour in units gCO2/kWh."
)
ds["actual"].attrs = dict(
    description="The estimated actual Carbon Intensity for the half hour in units gCO2/kWh."
)
ds["intensity_index"].attrs = dict(
    description="The index is a measure of the Carbon Intensity represented on a scale "
    "between 'very low', 'low', 'moderate', 'high', 'very high'."
)


# Save to Zarr format
ds.to_zarr(store="uk_national_carbon_intensity.zarr", consolidated=True)
