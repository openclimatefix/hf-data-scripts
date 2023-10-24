"""
An example script to sequentially download data from a bunch of long term
ASOS sites, for only a few specific variables, and save the result to
individual CSV files.

You are free to use this however you want.

Author: daryl herzmann akrherz@iastate.edu
"""
import os
from datetime import date, datetime, timedelta

import requests

import json
import os
import sys
import time
from urllib.request import urlopen


def fetch(station_id):
    """Download data we are interested in!"""
    localfn = f"{station_id}.csv"
    if os.path.isfile(localfn):
        print(f"- Cowardly refusing to over-write existing file: {localfn}")
        return
    print(f"+ Downloading for {station_id}")
    startts = datetime(2008, 1, 1)
    endts = datetime(2023, 10, 24)
    now = startts
    while now < endts:
        enddt = endts #now + timedelta(hours=24)
        uri = (
            "http://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?"
            f"station={station_id}&data=all&year1={now.year}&month1={now.month}&day1={now.day}&"
            f"year2={enddt.year}&month2={enddt.month}&day2={enddt.day}&"
            "tz=Etc/UTC&format=comma&latlon=yes&elev=yes&missing=M&trace=T&"
            "direct=yes&report_type=2"
        )
        print(f"Downloading: {now}")
        data = download_data(uri)
        outfn = f"/run/media/jacob/data/ASOS/{station_id}.txt"
        print(data)
        with open(outfn, "w", encoding="ascii") as fh:
            fh.write(data)
        now = enddt

MAX_ATTEMPTS = 6
# HTTPS here can be problematic for installs that don't have Lets Encrypt CA
SERVICE = "http://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?"

def download_data(uri):
    """Fetch the data from the IEM

    The IEM download service has some protections in place to keep the number
    of inbound requests in check.  This function implements an exponential
    backoff to keep individual downloads from erroring.

    Args:
      uri (string): URL to fetch

    Returns:
      string data
    """
    attempt = 0
    while attempt < MAX_ATTEMPTS:
        try:
            data = urlopen(uri, timeout=300).read().decode("utf-8")
            if data is not None and not data.startswith("ERROR"):
                return data
        except Exception as exp:
            print(f"download_data({uri}) failed with {exp}")
            time.sleep(5)
        attempt += 1

    print("Exhausted attempts to download, returning empty data")
    return ""




def main():
    """Main loop."""
    # Step 1: Fetch global METAR geojson metadata
    # https://mesonet.agron.iastate.edu/sites/networks.php
    req = requests.get(
        "http://mesonet.agron.iastate.edu/geojson/network/AZOS.geojson",
        timeout=60,
    )
    geojson = req.json()
    for feature in geojson["features"]:
        station_id = feature["id"]
        props = feature["properties"]
        # We want stations with data to today (archive_end is null)
        #if props["archive_end"] is None:
        #    continue
        # We want stations with data to at least 1943
        #if props["archive_begin"] is None:
        #    continue
        if props["country"] != "IN":
            continue
        #archive_begin = datetime.strptime(props["archive_begin"], "%Y-%m-%d")
        #if archive_begin.year > 2008:
        #    continue
        # Horray, fetch data
        fetch(station_id)


if __name__ == "__main__":
    main()
