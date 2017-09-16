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

class LocalQueue(BatchQueue):

    def report_taxi_status_by_name(self, taxi_name):
        taxi_status = 'X'
        taxi_job_number = None

        # Get full qstat output
        proc = subprocess.Popen("qstat -f".format(taxi_name=taxi_name),
            shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        qout, qerr = proc.communicate()

        if qerr:
            print qout
            print qerr
            raise Exception("Exception encountered when trying to call qstat")
        else:
            # Parse output in to blocks
            blocks = qout.split("\n\n")
            
            # Find block corresponding to correct job
            blocks = filter(lambda b: "Job_Name = {tn}\n".format(tn=taxi_name) in b, blocks)
            if len(blocks) == 0:
                return {
                    'status' : 'X',
                    'job_number' : None,
                    'running_time' : None
                }
            elif len(blocks) > 1:
                print blocks
                raise Exception("Multiple taxis with name {tn} found in queue!".format(tn=taxi_name))
            block = blocks[0]
            
            # Get job ID
            taxi_job_number = re.findall("Job Id: (\S+)\n", block)

            # Get status
            pbs_status = re.findall("job_state = (\S+)\n", block)
            assert len(pbs_status) == 1, "Found too many PBS job_states = [{0}] in block: {1}".format(pbs_status, block)
            pbs_status = pbs_status[0]
            status_map_pbs_to_taxi = {'Q':'Q', 'R':'R', 'E':'R', 'H':'Q', 'T':'R', 'W':'Q', 'S':'Q'}
            assert pbs_status in status_map_pbs_to_taxi.keys(), "Don't know how to interpret PBS status {ps}".format(ps=pbs_status)
            taxi_status = status_map_pbs_to_taxi[pbs_status]
            
            # Get running time
            walltime = re.findall("Resource_List.walltime = (\S+)\n", block)
            assert len(walltime) == 1, "Too many (or no) walltimes [{0}] found in block: {1}".format(walltime, block)
            walltime = walltime[0]
            walltime = time.strptime(walltime, "%H:%M:%S")
            walltime = datetime.timedelta(hours=walltime.tm_hour, minutes=walltime.tm_min, seconds=walltime.tm_sec)#.total_seconds()
            walltime = _total_seconds(walltime)

        return {
            'status': taxi_status,
            'job_number': taxi_job_number,
            'running_time': walltime
        }
        

    def launch_taxi(self, taxi):
        ## Time logistics
        bash_time = taxi.time_limit
        
        # Translate taxi_time from seconds to hh:mm:ss format
        # Include a buffer between bash script requested time and taxi time budget 
        bash_time += 300 # HARDCODE: 5 minute buffer
        hours = int(bash_time / 3600)
        minutes = int(bash_time - 3600*hours)/60
        seconds = int(bash_time - 3600*hours - 60*minutes)
        time_str = "{hours:02d}:{minutes:02d}:{seconds:02d}".format(hours=hours, minutes=minutes, seconds=seconds)
        
        ## Arguments for PBS
        taxi_call = "/usr/local/pbs/bin/qsub "
        taxi_call += " -N {taxi_name} ".format(taxi_name=taxi.name)
        taxi_call += " -l walltime={walltime} ".format(walltime=time_str)
        taxi_call += " -l nodes={taxi_nodes} ".format(taxi_nodes=taxi.nodes)

        ## Pass along allocation
        assert taxi.allocation is not None, "FNAL requires an allocation to be specified"
        taxi_call += " -A {0} ".format(taxi.allocation)

        ## Pass location of virtual environment to taxi, if we're inside one
        if os.environ.get('VIRTUAL_ENV', None) is not None:
            taxi_call += " -v TAXI_PYENV='{0}' ".format(os.environ['VIRTUAL_ENV'])
        else:
            taxi_call += " -v TAXI_PYENV='' "

        # FNAL oddity: For virtualenv compatibility: pass PATH along manually, or we won't know where qstat/etc are
        taxi_call += " -v PATH='{0}' ".format(os.environ['PATH'])

        
        # Logs go in taxi dir, like dtaxi/taxi-name_1241234.log
        # PBS should replace $PBS_JOBID in -o to the job id, but it doesn't.
        #logfn = "{taxi_dir}/{taxi_name}_$PBS_JOBID.log".format(taxi_dir=taxi_dir, taxi_name=taxi_name)
        if taxi.log_dir is None:
            raise Exception("Taxi {t} doesn't have log_dir set, cannot submit".format(t=repr(taxi)))
        logfn = "{dtaxi}/{taxi_name}_{subtime}".format(dtaxi=taxi.log_dir,
            taxi_name=taxi.name, subtime=time.strftime("%Y%b%d-%H%M"))
        
        taxi_call += " -o {logfn} ".format(logfn=logfn)
    
        ## Call taxi wrapper bash script
        ## which call makes sure we find the correctly installed script
        taxi_call += " $(which taxi.sh) "
        
        
        ## Pass argument to run_taxi
        taxi_dict = taxi.to_dict()
        key_arg_list = ['name', 'cores', 'nodes', 'pool_name', 'time_limit', 'dispatch_path', 'pool_path']

        taxi_args = [ "--{0} {1}".format(k, taxi_dict[k]) for k in key_arg_list ]
        taxi_args_str = " ".join(taxi_args)
        
        ## PBS-specific -- command line args to script must be passed as string to -F
        if '"' in taxi_args or "'" in taxi_args:
            print "LAUNCH FAILED: Taxi launcher tried to use command line arguments with quotes in them."
            print "taxi_args:", taxi_args
            return False
        taxi_call += """ -F \\"{taxi_args}\\" """.format(taxi_args=taxi_args_str)
    
        ## FNAL oddity -- Apparently on worker nodes, can only qsub via restricted shell
        taxi_call = """/usr/bin/rsh bc1p "{taxi_call}" """.format(taxi_call=taxi_call)

        
        # Submit the job, catch stdout and stderr
        batch_out, batch_err = subprocess.Popen(taxi_call, 
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True).communicate()

        print batch_out
        if batch_err != '':
            print batch_err
            raise RuntimeError("Error in taxi invocation: \n", taxi_call)


    def cancel_job(self, job_number):
        subprocess.Popen('qdel {job_number}'.format(job_number=job_number))



