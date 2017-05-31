# -*- coding: utf-8 -*-
"""
"""

import os, sys

### Localization -- Things change a bit depending on machine

# Tell script where to find taxi shell script, runners, etc.
install_dir = os.path.abspath('..')

# Look for packages in the install_dir
if install_dir not in sys.path:
    sys.path.insert(0, install_dir)

import local_util # Utilities for localization

# Tell abstract job classes where their runner scripts can be found on this machine
local_util.specify_dir_with_runner_scripts(install_dir + '/runners/')

# Tell abstract job classes where their binaries can be found on this machine
# To see how to point at your own binaries, look in local_util.py
local_util.use_dan_binary_paths()


### Make abstract task forest
# Note: everything in this section is completely abstract and machine independent
import dispatch_tools

## HMC jobs
# Make seed HMC stream, starts from fresh
seed_stream = dispatch_tools.make_hmc_job_stream(Ns=4, Nt=4, beta=7.75, k4=0.128, k6=0.1285, # Physical parameters to run
                                               N_configs=10, # Generate 40 configurations
                                               nsteps=18,    # 18 outer integrator steps per trajectory (no Hasenbuch)
                                               req_time=120, # Estimate max 2 minutes to run one HMC task
                                               starter=None) # starter=None -> Fresh start
                                               
# Make second HMC stream nearby in parameter space, forks off from first stream after 5 trajectories
fork_stream = dispatch_tools.make_hmc_job_stream(Ns=4, Nt=4, beta=7.76, k4=0.128, k6=0.1285, # Physical parameters to run (beta changed)
                                               N_configs=10, # Generate 40 configurations
                                               nsteps=18,    # 18 outer integrator steps per trajectory (no Hasenbunch)
                                               req_time=120, # Estimate max 2 minutes to run one HMC task
                                               starter = seed_stream[5]) # Start from 5th job in seed stream

# Get all of our hmc jobs together in a pool
hmc_pool = seed_stream + fork_stream


## Auxiliary jobs: Flow, spectroscopy
aux_job_pool = []

# Spectroscopy helper functions -- provide a list of HMC jobs, and it will make a spectroscopy job for each one (after and including start_count)
aux_job_pool += dispatch_tools.spectro_jobs_for_hmc_jobs(hmc_pool,          # Takes relevant physical parameters from HMC jobs
                                                         irrep='f', r0=6.0, # Physical parameters novel to spectroscopy
                                                         screening=True, p_plus_a=True, # Specifies which binaries to use, and how to name files
                                                         req_time=20,       # Estimate max 20 seconds to run a spectro task
                                                         start_at_count=5)  # Don't measure spectroscopy for a configuration until count=5 in to stream
aux_job_pool += dispatch_tools.spectro_jobs_for_hmc_jobs(hmc_pool,            # Takes relevant physical parameters from HMC jobs
                                                         irrep='as2', r0=6.0, # Physical parameters novel to spectroscopy
                                                         screening=True, p_plus_a=True, # Specifies which binaries to use, and how to name files
                                                         req_time=20,         # Estimate max 20 seconds
                                                         start_at_count=5)    # Don't measure spectroscopy for a configuration until count=5 in to stream

# Flow helper function -- provide a list of HMC jobs, and it will make a gradient flow job for each one (after and including start_count)
aux_job_pool += dispatch_tools.flow_jobs_for_hmc_jobs(hmc_pool,     # Takes relevant physical parameters from HMC jobs
                                                      epsilon=.01,  # Flow integrator step size
                                                      tmax=1,       # Maximum flow time to integrate out to
                                                      req_time=20,  # Estimate max 20 seconds to run flow task
                                                      start_at_count=5) # Don't measure Wilson Flow for a configuration until count=5 in to each stream

                                                      
## Gather together numerical running tasks
# Priority implicit in list ordering -- putting aux jobs in front of hmc jobs means the aux jobs will pre-empt hmc jobs, if they're ready to go
job_pool = aux_job_pool + hmc_pool 

## Runner scripts don't bother with directory structure -- too much for one thing to be in charge of.  Instead, they just dump output in working dir 
# Use separate copy tasks to put output files, gauge files, prop files in to whatever directory structure
# This convenience function takes a list of HMC tasks and HMC auxiliary tasks, and provides a bunch of copy tasks to file things away
# To use your own directory structure conventions, make your own version of dispatch_tools.copy_jobs_to_sort_output
# Priority is implicit in list ordering -- copy jobs pre-empt running further numerics
job_pool = dispatch_tools.copy_jobs_to_sort_output(job_pool,
                                                   data_dir='./data',    # Sends files to folders like ./data/4x6/hmc/7.75/0.128_0.1285/ or ./data/4x6/spec4_r6/7.75/0.128_0.1285/
                                                   gauge_dir='./gauge'   # Send files to folders like ./gauge/4x6/7.75/0.128_0.1285
                                                   ) + job_pool          # Copy tasks go first so they happen ASAP


## Use adaptive nsteps
# After each HMC run is completed, check the accept rate, and adjust the number of steps in the
# outer layer of integration accordingly. Convenience function adds NstepAdjustor tasks to the pool.
#  Note: Unlike other convenience functions, this doesn't return a list of jobs for you to put wherever you please.
#  This is because the HMC tasks now depend on the NstepAdjustor tasks, and so excluding them from the pool
#  would cause issues worse than just some tasks not being run.
job_pool = dispatch_tools.use_adaptive_nsteps(job_pool, AR_from_last_N = 1)


### Taxi management
# Dispatch class needs to know how to uniquely name taxis and where to store their log files.
# Solve both problems at once -- have a "pool" directory where each taxi has a sub-directory
# wherein there logs go, like {pool_dir}/{taxi_tag}0  (e.g., 'taxi-pool/test0/').
# If a folder exists in the pool directory, taxi name is taken.
from manage_taxi_pool import next_available_taxi_name_in_pool, log_dir_for_taxi


### Compile abstract task forest in to sqlite DB that can be read by taxis running on compute nodes

## Filesystem
# This section is not machine-independent purely because of filesystem considerations, e.g.:
# CU has no fast scratch directory, so workspace just goes wherever.  Meanwhile, Janus
# doesn't like tasks to talk to the file system anywhere but the fast scratch directory,
# so workspace must be put on lustre.
forest_name = "./forest.db"  # File to save sqlite DB to -- must be accessible from compute nodes
pool_dir = "./taxi-pool/"    # File to use as taxi pool dir (for logs and taxi-name-availabiity tracking)
taxi_tag = "test"            # Name taxis like 'mrep0', 'mrep1', ...
workspace_dir = './work/' # Working folder: Outputs go here


## Compile using the dispatch class (dispatch is a noun, here)
# Automatically figures out how many independent taxis can run simultaneously, and where to launch them
# Automatically compiles abstract task forest provided in job_pool in to sqlite DB
d = dispatch_tools.dispatch(
    job_pool=job_pool, # Provide abstract job forest
    N_nodes=1,         # Number of nodes taxis will run on (Currently same for all taxis, but could be modified straightforwardly to have varying taxi sizes)
    taxi_time=5*60,    # Maximum time in seconds a taxi can run until it needs to resubmit itself
    
    # taxi_name_generator is a function which, passed N, provides an iterable of N available taxi names.  Use our taxi_pool scheme with specified pool_dir, taxi_tag
    taxi_name_generator=lambda N: next_available_taxi_name_in_pool(pool_dir=pool_dir, taxi_tag=taxi_tag, N=N),
    
    # taxi_log_dir_for_name is a function which, passed the name of the taxi, provides a directory in which to place the taxi's logs
    taxi_log_dir_for_name=lambda name: log_dir_for_taxi(pool_dir=pool_dir, taxi_name=name))

### Launch

## Prepare filesystem
d.prepare_workspace(workspace_dir) # Make sure working directory exists (If we wanted to copy scripts, etc. to a scratch fs, this would be the place to do it)
d.save_forest_db(forest_name)      # Save the sqlite DB file to disk, and record its location to tell the initial taxis about


## Launch the initial taxis
# taxi_launcher is a function that, provided a bunch of parameters, submits a new taxi to the queue
# In general, need a different taxi launcher on each computer
from local_taxi import taxi_launcher

# taxi_shell_script is the path to the shell script wrapper submitted to the queue
# Shell script wrapper calls taxi.py in turn
taxi_shell_script = install_dir + '/taxi.sh'

# Launch dispatches the initial taxis, getting the stream moving
# Also at launch time, make taxi log dirs.  This "occupies" their name in pool_dir scheme
d.launch(taxi_launcher=taxi_launcher, taxi_shell_script=taxi_shell_script)