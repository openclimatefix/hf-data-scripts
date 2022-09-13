import sys
import pandas as pd
import geopandas as gpd
from matplotlib import pyplot as plt

sys.path.append("..")
from pvoutput.grid_search import GridSearch
from pvoutput import PVOutput

def search_using_grid(grid, search_radius=25):
    systems = None
    for i, loc in grid.iterrows():
        print(f"Searching for systems within {search_radius}km of {loc.latitude}, {loc.longitude} ({i} of {len(grid)})...")
        new_systems = pv.search(query=f"{search_radius}km", lat=loc.latitude, lon=loc.longitude,
                                wait_if_rate_limit_exceeded=True)
        if len(new_systems) >= 30: # PVOutput returns a maximum of 30 systems, so we'll drill down and conduct a finer search
            print("PVOutput returned 30 systems... drilling down")
            grid_ = grd.generate_grid(
                radial_clip=(loc.latitude, loc.longitude, 25),
                buffer=0,
                search_radius=5,
                show=False
            )
             # Call this function recursively with a finer search radius
            new_systems = search_using_grid(grid_, search_radius=search_radius / 5.)
        if systems is None:
            systems = new_systems
        else:
            systems = pd.concat((systems, new_systems))
        print(f"Found {len(new_systems)} new systems")
    # Remove any duplicates due to overlapping search radii
    return systems[~systems.index.duplicated()]

plt.rcParams['figure.dpi'] = 200
api_key =  None
system_id = None
pv = PVOutput(api_key, system_id)
CACHE_DIR = "/tmp"
grd = GridSearch(cache_dir=CACHE_DIR)
for country in ["France", "Italy", "Spain", "Germany", "Egypt", "Denmark", "Belgium", "Luxembourg", "Switzerland", "Austria", "Sweden", "Norway", "Portugal", "Ukraine", "Malta", "Iceland", "Liechtenstein",
                   "Bosnia and Herz.", "Croatia", "Hungary", "Romania", "Bulgaria", "Lithuania", "Moldova", "Monaco", "Morocco", "Netherlands", "Poland", "Slovakia", "Czechia", "Vatican",
               "North Macedonia", "Kosovo", "Albania", "Montenegro", "Serbia", "Finland", "Belarus", "Greece", "Cyprus", "Ireland", "Estonia", "Algeria", "Libya", "Latvia", "Turkey", "Israel", "Lebanon", "Saudi Arabia", "Andorra"]:
    try:
        ukgrid = grd.generate_grid(
            countries=[country],  # List as many countries as you want, or set to None for world-wide
            radial_clip=None,  # Only include search points within a certain radius of a location (see Example 3)
            buffer=10,  # Increase this if you'd like to consider systems "near" the target region (see Example 2)
            search_radius=24.5,  # Allow some extra overlap due to inaccuracies in measuring distance
            local_crs_epsg=3034,  # EPSG:27700 is OSGB36 / British National Grid
            show=False  # Gives a nice plot of the region and grid
        )
        ukgrid.head()

        systems = search_using_grid(ukgrid)
        systems.to_csv(f"/mnt/storage_ssd_4tb/PVOutput_{country}_systems.csv")
        get_metadata = lambda system_id: pv.get_metadata(system_id, wait_if_rate_limit_exceeded=True)
        metadata = systems.reset_index().system_id.apply(get_metadata)
        metadata.to_csv(f"/mnt/storage_ssd_4tb/PVOutput_{country}_systems_metadata.csv")
        metadata.head()
    except Exception as e:
        print(e)
        continue
