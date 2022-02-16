#!/usr/bin/env python
#################################################################
# Python Script to retrieve 5 online Data files of 'ds084.1',
# total 1.08G. This script uses 'requests' to download data.
#
# Highlight this script by Select All, Copy and Paste it into a file;
# make the file executable and run it on command line.
#
# You need pass in your password as a parameter to execute
# this script; or you can set an environment variable RDAPSWD
# if your Operating System supports it.
#
# Contact rpconroy@ucar.edu (Riley Conroy) for further assistance.
#################################################################


import sys, os
import requests

def check_file_status(filepath, filesize):
    sys.stdout.write('\r')
    sys.stdout.flush()
    size = int(os.stat(filepath).st_size)
    percent_complete = (size/filesize)*100
    sys.stdout.write('%.3f %s' % (percent_complete, '% Completed'))
    sys.stdout.flush()

url = 'https://rda.ucar.edu/cgi-bin/login'
values = {'email' : '', 'passwd' : '', 'action' : 'login'}
# Authenticate
ret = requests.post(url,data=values)
if ret.status_code != 200:
    print('Bad Authentication')
    print(ret.text)
    exit(1)
dspath = 'https://rda.ucar.edu/data/ds084.1/'
#dspath = 'https://rda.ucar.edu/data/ds084.3/'
filelist = []
device = "Square2"
for year in range(2015,2016):
    for month in range(1,13):
        for day in range(1,32):
            for hour in [0,6,12,18]:
                for f_hour in [0,3,6,9]:
                    path = f"{year}/{year}{str(month).zfill(2)}{str(day).zfill(2)}/gfs.0p25.{year}{str(month).zfill(2)}{str(day).zfill(2)}{str(hour).zfill(2)}.f0{str(f_hour).zfill(2)}.grib2"
                    filelist.append(path)
for file in filelist:
    filename=dspath+file
    file_base = os.path.basename(file)
    print(file_base)
    print('Downloading',file_base)
    req = requests.get(filename, cookies = ret.cookies, allow_redirects=True, stream=True)
    try:
        filesize = int(req.headers['Content-length'])
        with open(os.path.join(f"/run/media/jacob/{device}/GFS_025/", file_base), 'wb') as outfile:
            chunk_size=1048576
            for chunk in req.iter_content(chunk_size=chunk_size):
                outfile.write(chunk)
                if chunk_size < filesize:
                    check_file_status(os.path.join(f"/run/media/jacob/{device}/GFS_025/", file_base), filesize)
        check_file_status(os.path.join(f"/run/media/jacob/{device}/GFS_025/", file_base), filesize)
        print()
    except Exception as e:
        print(e)
        continue
