#!/usr/bin/env python

import taxi
import taxi.dispatcher
import taxi.pool
import taxi.jobs

## All runners to be used must be imported here as well!
import taxi.apps.mrep_milc.flow
import taxi.apps.mrep_milc.pure_gauge_ora

import os
import sys
import argparse
import time
import json
import traceback # For full error output

import taxi.local.local_taxi as local_taxi
import taxi.local.local_queue as local_queue

from taxi._utility import sanitized_path
from taxi._utility import flush_output

## Diagnostic output
os.system('hostname') # Print which machine we're working on
print "Working in dir:", os.getcwd()

## Parse arguments from command line
parser = argparse.ArgumentParser(description="Workflow taxi (NOTE: not intended to be called by user directly!)")

parser.add_argument('--name', type=str, required=True, help='Taxi name (provided by shell wrapper).')
parser.add_argument('--cores', type=int, required=True, help='Number of CPUs to tell mpirun about (provided by shell wrapper).')
parser.add_argument('--nodes', type=int, required=True, help='Number of nodes taxi is running on (provided by shell wrapper).')
parser.add_argument('--pool_path', type=str, required=True, help='Path of pool backend DB.')
parser.add_argument('--pool_name', type=str, required=True, help='Name of pool this taxi is assigned to.')
parser.add_argument('--dispatch_path', type=str, required=True, help='Path of dispatch backend DB.')
parser.add_argument('--time_limit',  type=float, required=True, help='Number of seconds in time budget.')
#parser.add_argument('--log_dir', type=str, required=True, help='Logs go here.')
#parser.add_argument('--work_dir', type=str, required=True, help='Working directory for taxi is here.')

print sys.argv
# TODO: shouldn't this work without the sys.argv explicit specification?
parg = parser.parse_args(sys.argv[1:]) # Call like "python run_taxi.py ...args..."

taxi_obj = taxi.Taxi(name=parg.name, time_limit=parg.time_limit, cores=parg.cores)

## Record starting time
taxi_obj.start_time = time.time()
print "Running on", taxi_obj.cores, "cores"

my_dispatch = taxi.dispatcher.SQLiteDispatcher(parg.dispatch_path)
my_pool = taxi.pool.SQLitePool(
    db_path=parg.pool_path,
    pool_name=parg.pool_name,
)
my_queue = local_queue.LocalQueue()

with my_pool:
    my_pool.register_taxi(taxi_obj)
    my_dispatch.register_taxi(taxi_obj, my_pool)

## Decoding for runner objects; relevant TaskRunner subclasses
## should be imported in local_taxi above!
runner_decoder = taxi.jobs.runner_rebuilder_factory()

## Main control loop
while True:

    ## Check with pool for jobs to launch
    with my_pool:
        my_pool.update_all_taxis_queue_status(my_queue)
        my_pool.spawn_idle_taxis(my_queue)

    ## Check with dispatch for tasks to execute
    with my_dispatch:
        # Ask dispatcher for next job
        task = my_dispatch.request_next_task(for_taxi=taxi_obj)

        # Flag task for execution
        try:
            my_dispatch.claim_task(taxi_obj, task)
        except taxi.dispatcher.TaskClaimException, e:
            ## Race condition safeguard: skips and tries again if the task status has changed
            print str(e)
            continue


    ## Execute task
    task_start_time = time.time()
    print "EXECUTING TASK ", task
    #print task.__dict__
    
    if isinstance(task, taxi.jobs.Die):
        ## "Die" is a special task
        print task.message # Die comes with a reason why
        
        with my_dispatch:
            task_run_time = time.time() - task_start_time
            my_dispatch.update_task(task, 'complete', run_time=task_run_time, by_taxi=taxi_obj)

        with my_pool:
            my_pool.update_taxi_status(taxi_obj, 'H')

        sys.exit(0)

    sys.stdout.flush()
    
    ## Dispatcher now reconstructs every object
#    # Alright, this is a little odd-looking with dumps and loads in the same line...
#    task_obj = json.loads(json.dumps(task), object_hook=runner_decoder)

    failed_task = False
    try:
        task.execute(cores=taxi_obj.cores)
        sys.stdout.flush()
    except:
        ## TODO: some exception logging here?
        ## Record task as failed
        failed_task = True
        print "RUNNING FAILED:"
        traceback.print_exc()

    ## Record exit status, time taken, etc.
    taxi_obj.task_finish_time = time.time()
    task_run_time = taxi_obj.task_finish_time - task_start_time

    if failed_task:
        task_status = 'failed'
    else:
        if (task.is_recurring):
            task_status = 'pending'
        else:
            task_status = 'complete'

    with my_dispatch:
        my_dispatch.update_task(task, status=task_status, start_time=task_start_time, run_time=task_run_time, by_taxi=taxi_obj)

    sys.stdout.flush()
    
