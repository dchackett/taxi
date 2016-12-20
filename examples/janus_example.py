# -*- coding: utf-8 -*-
"""
"""

import os, sys

## Place to find taxi shell script, runners, etc.
install_dir = os.path.abspath('..')

### Localization
if install_dir not in sys.path:
    sys.path.insert(0, install_dir)
import local_util

## Tell abstract job classes where their runner scripts can be found on this machine
local_util.specify_dir_with_runner_scripts(install_dir + '/runners/')

## Tell abstract job classes where their binaries can be found on this machine
# To see how to point at your own binaries, look in local_util.py
local_util.use_dan_binary_paths()


### Make abstract job forest
import dispatch_tools

## Make seed HMC stream, starts from fresh
seed_stream = dispatch_tools.make_hmc_job_stream(Ns=4, Nt=6, beta=7.75, k4=0.128, k6=0.1285, # Physical parameters to run
                                               N_configs=10, # Generate 10 configurations
                                               nsteps=18,    # 18 outer integrator steps per trajectory (no Hasenbuch)
                                               req_time=120, # Estimate max 2 minutes to run one HMC task
                                               starter=None) # starter=None -> Fresh start
                                               
## Make second HMC stream nearby in parameter space, forks off from first stream after 5 trajectories
fork_stream = dispatch_tools.make_hmc_job_stream(Ns=4, Nt=6, beta=7.76, k4=0.128, k6=0.1285, # Physical parameters to run (beta changed)
                                               N_configs=10, # Generate 10 configurations
                                               nsteps=18,    # 18 outer integrator steps per trajectory (no Hasenbunch)
                                               req_time=120, # Estimate max 2 minutes to run one HMC task
                                               starter = seed_stream[5]) # Start from 5th job in seed stream

hmc_pool = seed_stream + fork_stream

## Generate auxiliary jobs: measure spectroscopy and flow observables
aux_job_pool = []

# Spectroscopy helper functions -- provide a list of HMC jobs, and it will make a spectroscopy job for each one (modulo start_count)
aux_job_pool += dispatch_tools.spectro_jobs_for_hmc_jobs(hmc_pool,          # Takes relevant physical parameters from HMC jobs
                                                         irrep='f', r0=6.0, # Physical parameters novel to spectroscopy
                                                         screening=True, p_plus_a=True, # Currently just used for file naming conventions, what happens depends on binary
                                                         req_time=120,      # Estimate max 2 minutes to run a spectro task
                                                         start_at_count=10) # Don't measure spectroscopy for a configuration until count=10 in to stream
aux_job_pool += dispatch_tools.spectro_jobs_for_hmc_jobs(hmc_pool,            # Takes relevant physical parameters from HMC jobs
                                                         irrep='as2', r0=6.0, # Physical parameters novel to spectroscopy
                                                         screening=True, p_plus_a=True, # Currently just used for file naming conventions, what happens depends on binary
                                                         req_time=120,        # Estimate max 2 minutes to run a spectro task
                                                         start_at_count=10)   # Don't measure spectroscopy for a configuration until count=10 in to stream

# Flow helper function -- provide a list of HMC jobs, and it will make a Wilson Flow job for each one (modulo start_count)
aux_job_pool += dispatch_tools.flow_jobs_for_hmc_jobs(hmc_pool,     # Takes relevant physical parameters from HMC jobs
                                                      epsilon=.01,  # Flow integrator step size
                                                      tmax=1,       # Maximum flow time to integrate out to
                                                      req_time=120, # Estimate max 2 minutes to run flow task
                                                      start_at_count=10) # Don't measure Wilson Flow for a configuration until count=10 in to stream

                                                      
## Gather the jobs together
# Priority implicit in list ordering -- putting aux jobs in front of hmc jobs means the aux jobs will pre-empt hmc jobs, if they're ready to go
job_pool = aux_job_pool + hmc_pool 

## Numerical runner scripts don't bother with directory structure -- too much for one thing to keep track of, just dump output in working dir 
# Instead, make separate copy tasks to put output files, gauge files, prop files in to whatever directory structure
# Convenience function acts on HMC tasks and HMC auxiliary tasks, provide copy tasks
job_pool = dispatch_tools.copy_jobs_to_sort_output(job_pool,
                                                   data_dir='./data',    # Sends files to folders like ./data/4x6/hmc/7.75/0.128_0.1285/ or ./data/4x6/spec4_r6/7.75/0.128_0.1285/
                                                   gauge_dir='./gauge'   # Send files to folders like ./gauge/4x6/7.75/0.128_0.1285
                                                   ) + job_pool          # Copy tasks go first so they happen ASAP


### Dispatch class needs to know how to uniquely name taxis and where to store their log files
#  Solve both problems at once -- have a "pool" directory where each taxi has a sub-directory
# wherein there logs go, like {pool_dir}/{taxi_tag}0  (e.g., 'taxi-pool/mrep0/')
#  If a folder exists in the pool directory, taxi name is taken, try next name (e.g. 'taxi-pool/mrep1/')
from manage_taxi_pool import next_available_taxi_name_in_pool, log_dir_for_taxi

    
### Compile abstract task forest in to sqlite DB that can be read by taxis running on compute nodes
forest_name = "./forest.db"  # File to save sqlite DB to -- must be accessible from compute nodes
pool_dir = "./taxi-pool/"    # File to use as taxi pool dir (for logs and taxi-name-availabiity tracking)
taxi_tag = "test"            # Name taxis like 'mrep0', 'mrep1', ...
workspace_dir = '/lustre/janus_scratch/daha5747/taxi/work/' # Working folder: Outputs go here

## Use dispatch class to build a dispatch
# Automatically figures out how many independent taxis can run simultaneously, and where to launch them
# Automatically compiles abstract task forest provided in job_pool in to sqlite DB
d = dispatch_tools.dispatch(
    job_pool=job_pool, # Abstract job forest
    N_nodes=1,         # Number of nodes taxis will run on (Currently same for all taxis, but could be modified easily to have many taxi sizes)
    taxi_time=23*3600+50*60, # Maximum time in seconds a taxi can run until it needs to resubmit itself
    
    # taxi_name_generatior is a function which, passed N, provides an iterable of N available taxi names.  Use our taxi_pool scheme with specified pool_dir, taxi_tag
    taxi_name_generator=lambda N: next_available_taxi_name_in_pool(pool_dir=pool_dir, taxi_tag=taxi_tag, N=N),
    
    # taxi_log_dir_for_name is a function which, passed the name of the taxi, provides a directory in which to place the taxi's logs
    taxi_log_dir_for_name=lambda name: log_dir_for_taxi(pool_dir=pool_dir, taxi_name=name))

### Prep
d.prepare_workspace(workspace_dir) # Make sure working directory and taxi log dirs exist (making taxi log dirs "occupies" their name in pool_dir scheme)
d.save_forest_db(forest_name)      # Save the sqlite DB file to disk, and record its location to tell the initial taxis about

### Launch

# taxi_launcher is a function that, provided a bunch of parameters, submits a new taxi to the queue
# In general, need a different taxi launcher on each computer
from local_taxi import taxi_launcher

# taxi_shell_script is the path to the shell script wrapper submitted to the queue
# Shell script wrapper calls taxi.py in turn
taxi_shell_script = install_dir + '/taxi.sh'

# Launch dispatches the initial taxis, getting the stream moving
d.launch(taxi_launcher=taxi_launcher, taxi_shell_script=taxi_shell_script)
