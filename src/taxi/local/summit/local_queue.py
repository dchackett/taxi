#!/usr/bin/env python

## Local queue implementation for "cu_hep" machine

import os
import subprocess
import re
import time, datetime

from taxi.batch_queue import *
from taxi._utility import ensure_path_exists

def _total_seconds(td):
    """datetime.timedelta.total_seconds() is not available in Python 2.7
    """
    return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / 10**6

def _taxi_status_from_slurm_line(line):
    taxi_queue_info = line.split()
    slurm_status = taxi_queue_info[4]

    status_map_slurm_to_taxi = {
        'PD':'Q', 'R':'R', 'CF':'R', 'CG':'R', 
        'CA':'X', 'F':'X', 'NF':'X', 'T':'X', 'RV':'X'
    }
    assert slurm_status in status_map_slurm_to_taxi.keys(), "Don't know how to interpret Slurm status {ss}".format(ss=slurm_status)

    return status_map_slurm_to_taxi[slurm_status]


class LocalQueue(BatchQueue):

    def report_taxi_status_by_name(self, taxi_name):
        taxi_status = 'X'
        taxi_job_number = None

        # Get all queued jobs matching taxi_name
        proc = subprocess.Popen("squeue -n {taxi_name}".format(taxi_name=taxi_name),
            shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        qout, qerr = proc.communicate()

        if qerr:
            print qout
            print qerr
            raise Exception("Exception encountered when trying to call squeue")
        else:
            lines = qout.strip().split("\n") # Strip removes trailing newline

            # First block should be the SLURM header
            header = lines[0].split()
            try:
                assert(header[0] == 'JOBID')
            except AssertionError:
                raise Exception("Failed to parse squeue output - missing header")

            # All other lines should correspond to the correct job
            if len(lines) == 1:
                return {
                    'status': 'X',
                    'job_number': None,
                    'running_time': None,
                }
            elif len(lines) == 2:
                # Expected outcome - one taxi with this name
                taxi_line = lines[1]
            elif len(lines) > 2:
                # Multiple taxis with this name were found
                # This can be okay, if the taxi has just respawned -- expect one running and one queued
                taxi_statuses = [_taxi_status_from_slurm_line(line) for line in lines[1:] ]

                if len([ts for ts in taxi_statuses if ts == 'R']) > 1:
                    for l in lines[1:]: print l
                    raise Exception("Multiple running taxis with name {tn} found in queue!".format(tn=taxi_name))
                if len([ts for ts in taxi_statuses if ts == 'Q']) == 0:
                    for l in lines[1:]: print l
                    raise Exception("Multiple taxis with name {tn}, none queued?  Statuses: {ts}".format(tn=taxi_name, ts=taxi_statuses))
                if len([ts for ts in taxi_statuses if ts == 'Q']) > 1:
                    for l in lines[1:]: print l
                    raise Exception("Multiple queued taxis with name {tn} found in queue!".format(tn=taxi_name))
                
                # Find the single queued taxi; this is the 'active' taxi now
                idx = [tt for (tt, ts) in enumerate(taxi_statuses) if ts == 'Q'][0]
                taxi_line = lines[idx+1]
            
            # Get job ID
            taxi_queue_info = taxi_line.split()
            taxi_job_number = taxi_queue_info[0]

            # Get status
            taxi_status = _taxi_status_from_slurm_line(taxi_line)
            
            # Get running time
            walltime = taxi_queue_info[5]
            try:
                walltime = time.strptime(walltime, "%H:%M:%S")
            except ValueError:
                # Default format can drop the hour block if it's zero
                walltime = time.strptime(walltime, "%M:%S")
            except:
                raise Exception("Failed to parse walltime {wt}".format(wt=walltime))

            walltime = datetime.timedelta(hours=walltime.tm_hour, minutes=walltime.tm_min, seconds=walltime.tm_sec)#.total_seconds()
            walltime = _total_seconds(walltime)

        return {
            'status': taxi_status,
            'job_number': taxi_job_number,
            'running_time': walltime
        }
        

    def launch_taxi(self, taxi, respawn=False):
        super(LocalQueue, self).launch_taxi(taxi, respawn=respawn)
        
        ## Time logistics
        bash_time = taxi.time_limit
        
        # Translate taxi_time from seconds to hh:mm:ss format
        # Include a buffer between bash script requested time and taxi time budget 
        bash_time += 300 # HARDCODE: 5 minute buffer
        hours = int(bash_time / 3600)
        minutes = int(bash_time - 3600*hours)/60
        seconds = int(bash_time - 3600*hours - 60*minutes)
        time_str = "{hours:02d}:{minutes:02d}:{seconds:02d}".format(hours=hours, minutes=minutes, seconds=seconds)
        
        ## Arguments for Slurm
        taxi_call = "sbatch "
        taxi_call += " --job-name {taxi_name} ".format(taxi_name=taxi.name)
        taxi_call += " --time {walltime} ".format(walltime=time_str)
        taxi_call += " --nodes {taxi_nodes} ".format(taxi_nodes=taxi.nodes)

        ## Pass along allocation, qos
        assert taxi.allocation is not None, "Must specify an allocation"
        taxi_call += " --account {0} ".format(taxi.allocation)

        assert taxi.queue is not None, "Must specify a queue (partition)"
        taxi_call += " --partition {0} ".format(taxi.queue)
        
        ## Pass location of virtual environment to taxi, if we're inside one
        ## ALL passes along the rest of the environment, which is the default behavior for Slurm
        if os.environ.get('VIRTUAL_ENV', None) is not None:
            taxi_call += " --export TAXI_PYENV='{0}',ALL ".format(os.environ['VIRTUAL_ENV'])
        else:
            taxi_call += " --export TAXI_PYENV='',ALL "

        
        # Logs go in taxi dir, like dtaxi/taxi-name_1241234.log
        if taxi.log_dir is None:
            raise Exception("Taxi {t} doesn't have log_dir set, cannot submit".format(t=repr(taxi)))
        logfn = "{dtaxi}/{taxi_name}_{subtime}".format(dtaxi=taxi.log_dir,
            taxi_name=taxi.name, subtime=time.strftime("%Y%b%d-%H%M"))
        
        taxi_call += " --open-mode append --output {logfn} ".format(logfn=logfn)
    
        ## Call taxi wrapper bash script
        ## which call makes sure we find the correctly installed script
        taxi_call += " $(which taxi.sh) "
        
        
        ## Pass argument to run_taxi
        taxi_dict = taxi.to_dict()
        key_arg_list = ['name', 'cores', 'nodes', 'pool_name', 'time_limit', 'dispatch_path', 'pool_path']

        taxi_args = [ "--{0} {1}".format(k, taxi_dict[k]) for k in key_arg_list ]
        taxi_args_str = " ".join(taxi_args)
        taxi_call += " " + taxi_args_str + " "

        
        # Submit the job, catch stdout and stderr
        batch_out, batch_err = subprocess.Popen(taxi_call, 
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True).communicate()

        print batch_out.strip()
        if batch_err != '':
            print batch_err
            raise RuntimeError("Error in taxi invocation: \n", taxi_call)
            
        # Return job ID
        return batch_out.split()[-1]


    def cancel_job(self, job_number):
        subprocess.Popen('scancel {job_number}'.format(job_number=job_number))

    def get_current_job_id(self):
        """Returns job_id on queue of currently running process (to be called by
        taxis to determine their own job_id).
        """
        return os.environ['SLURM_JOB_ID']

