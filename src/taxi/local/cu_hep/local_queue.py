#!/usr/bin/env python

## Local queue implementation for "cu_hep" machine

import os
import subprocess
import re
import time

from taxi.batch_queue import *
from taxi._utility import mkdir_p

class LocalQueue(BatchQueue):

    def __init__(self):
        pass


    def report_taxi_status_by_name(self, taxi_name):
        ## The configuration of this queueing system is unbelievably unfriendly...
        ## so this is messy.
        taxi_status = 'X'
        taxi_job_number = None

        proc = subprocess.Popen("qstat -j {taxi_name}".format(taxi_name=taxi_name),
            shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        qout, qerr = proc.communicate()

        if not qerr:
            taxi_job_number = re.findall("job_number:\W+(\d+)", qout)

            usage_lines = re.findall("usage:", qout)
            if len(usage_lines) == 0:
                taxi_status = 'Q'
            else:
                taxi_status = 'R'

        return {
            'status': taxi_status,
            'job_number': taxi_job_number,
            'running_time': None            ## I have no idea how to get this from the Beowulf queue
        }


    def launch_taxi(self, taxi):
        if taxi.cores is not None and taxi.cores != 8:
            raise Exception("Can only run single-node jobs on beowulf")

        # Arguments to SGE (Note, don't specify nodes -- always 1)
        taxi_call = "Qsub -e -G beowulf "
        taxi_call += " -N {taxi_name} ".format(taxi_name=taxi.name)

        # Logs go in taxi dir, like dtaxi/taxi-name_1241234.log
        if taxi.log_dir is None:
            raise Exception("Taxi {t} doesn't have log_dir set, cannot submit".format(t=repr(taxi)))
        logfn = "{dtaxi}/{taxi_name}_{subtime}".format(dtaxi=taxi.log_dir,
            taxi_name=taxi.name, subtime=time.strftime("%Y%b%d-%H%M"))

        taxi_call += " -o {logfn} ".format(logfn=logfn)
        taxi_call += " taxi.sh"

        ## Pass argument to run_taxi
        taxi_dict = taxi.to_dict()
        key_arg_list = ['name', 'cores', 'nodes', 'pool_name', 'time_limit', 'dispatch_path', 'pool_path']

        taxi_args = [ "--{} {}".format(k, taxi_dict[k]) for k in key_arg_list ]
        taxi_args_str = " ".join(taxi_args)

        taxi_call += " " + taxi_args_str

        # Submit the job, catch stdout and stderr
        batch_out, batch_err = subprocess.Popen(taxi_call, 
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True).communicate()

        print batch_out
        if batch_err != '':
            print batch_err
            raise RuntimeError("Error in taxi invocation: \n", taxi_call)


    def cancel_job(self, job_number):
        subprocess.Popen('qdel {job_number}'.format(job_number=job_number))



