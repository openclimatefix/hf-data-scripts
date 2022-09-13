import subprocess
import time
import sys

max_processes = 4 # Half the number running
processes = []
while True:
    while len(processes) < max_processes:
        processes.append(subprocess.Popen(['/home/jacob/anaconda3/envs/modal/bin/python', '/home/jacob/Development/modal/collate_1000.py'], close_fds=True))
        processes.append(subprocess.Popen(
            ['/home/jacob/anaconda3/envs/modal/bin/python', '/home/jacob/Development/modal/collat_1000_nonhrv.py'],
            close_fds=True))
    time.sleep(5)  # give it a second to launch
    while len(processes) >= max_processes / 2:
        for p in processes:
            if p.poll() is None: # Still alive
                time.sleep(5)
        for i in range(len(processes)):
            if processes[i].poll():
                processes.pop(i) # Remove from processes