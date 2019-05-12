#!/dali/lgrandi/strax/miniconda3/envs/strax/bin/python

import argparse
import tempfile
import time
import os.path as osp

import random
import string
import os
import subprocess


parser = argparse.ArgumentParser(
    description='Start a strax jupyter notebook server on the dali batch queue')
parser.add_argument('--port', type=int, 
                    default=-1,
                    help='Port number to use. Picks random one if omitted.')
parser.add_argument('--cpu', default=4, 
                    type=int, help='Number of CPUs to request.')
args = parser.parse_args()
port = args.port
n_cpu = args.cpu

if port == -1:
    port = random.randrange(15000, 20000)

jupyter_job = """#!/bin/bash
#SBATCH --partition dali
#SBATCH --qos dali
#SBATCH --account=pi-lgrandi
#SBATCH --ntasks=1
#SBATCH --job-name=straxlab
#SBATCH --cpus-per-task={n_cpu}
#SBATCH --mem-per-cpu=4480
#SBATCH --time=24:00:00
#SBATCH --output={log_fn}
#SBATCH --error={log_fn}

cd ${{HOME}}
eval "$(/dali/lgrandi/strax/miniconda3/bin/conda shell.bash hook)"
conda activate strax

ipnport={port}
ipnip=$(hostname -i)
echo Starting jupyter job
echo Host: $ipnip
echo User: ${{USER}}

jupyter lab --no-browser --port=$ipnport --ip=$ipnip 2>&1
"""

# Dir for temporary files
# Must be shared between batch queue and login node
# (i.e. not /tmp)
tmp_dir = '/dali/lgrandi/strax/jupyter_job_launcher'

def make_executable(path):
    """Make the file at path executable, see """
    mode = os.stat(path).st_mode
    mode |= (mode & 0o444) >> 2    # copy R bits to X
    os.chmod(path, mode)
    
    
url_cache_fn = osp.join(
    os.environ['HOME'],
    '.last_jupyter_url')
username = os.environ['USER']

q = subprocess.check_output(['squeue', '-u', username])
for line in q.decode().splitlines():
    if 'straxlab' in line:
        print("You still have a running jupyter job; retrieving the URL.")
        job_id = int(line.split()[0])
        with open(url_cache_fn) as f:
            url = f.read()
        break
        
else:
    print("Submitting a new jupyter job")
    job_fn = tempfile.NamedTemporaryFile(
        delete=False, dir=tmp_dir).name
    log_fn = tempfile.NamedTemporaryFile(
        delete=False, dir=tmp_dir).name
    with open(job_fn, mode='w') as f:
        f.write(jupyter_job.format(
            log_fn=log_fn,
            port=port,
            n_cpu=n_cpu))
    make_executable(job_fn)
    result = subprocess.check_output(['sbatch', job_fn])
    job_id = int(result.decode().split()[-1])
    
    while not osp.exists(log_fn):
        print("Waiting for your job to start...")
        time.sleep(1)

    slept = 0
    url = None
    while url is None and slept < 60:
        with open(log_fn, mode='r') as f:
            for line in f.readlines():
                if '?token' in line:
                    url = line.split()[-1]
                    break
            else:
                print("Waiting for jupyter server to start inside job...")
                time.sleep(2)
                slept += 2
    if url is None:
        raise RuntimeError("Jupyter did not start inside your job. Check the job logfile {log_fn}!".format(log_fn=log_fn))
    
    with open(url_cache_fn, mode='w') as f:
        f.write(url)
    os.remove(job_fn)
    os.remove(log_fn)

ip, port = url.split('/')[2].split(':')
token = url.split('?')[1].split('=')[1]

print("""
Success! If you have linux, execute the following command on your laptop:

ssh -fN -L {port}:{ip}:{port} {username}@dali-login1.rcc.uchicago.edu && sensible-browser http://localhost:{port}/?token={token}

If you have a mac, instead do:

ssh -fN -L {port}:{ip}:{port} {username}@dali-login1.rcc.uchicago.edu && open http://localhost:{port}/?token={token}

Happy strax analysis!
""".format(ip=ip, port=port, token=token, username=username))
