import subprocess
import time
import sys

max_processes = 6 # Half the number running
processes = []
while True:
    while len(processes) < max_processes:
        processes.append(subprocess.Popen(['/home/jacob/miniconda3/envs/modal/bin/python', '/home/jacob/collate_1000_zarr_hrv.py'], close_fds=True))
        processes.append(subprocess.Popen(
            ['/home/jacob/miniconda3/envs/modal/bin/python', '/home/jacob/collate_1000_zarr_nonhrv.py'],
            close_fds=True))
    time.sleep(5)  # give it a second to launch
    while len(processes) >= max_processes / 2:
        for p in processes:
            if p.poll() is None: # Still alive
                time.sleep(5)
        for i in range(len(processes)):
            if processes[i].poll():
                processes.pop(i) # Remove from processes