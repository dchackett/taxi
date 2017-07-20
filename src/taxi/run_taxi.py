#!/usr/bin/env python
import time
_start_time = time.time() # Get this ASAP for accurate accounting

import os
import sys
import argparse
import traceback # For full error output
import imp # For dynamical imports

import taxi
import taxi.dispatcher
import taxi.pool
import taxi.jobs

import taxi.local.local_taxi as local_taxi
import taxi.local.local_queue as local_queue

from taxi._utility import sanitized_path
from taxi._utility import flush_output


if __name__ == '__main__':
    
    ### Initialization
    
    os.system('hostname') # Print which machine we're working on
    print "Working dir:", os.getcwd()


    ## Parse arguments from command line
    parser = argparse.ArgumentParser(description="Workflow taxi (NOTE: not intended to be called by user directly!)")
    
    parser.add_argument('--name', type=str, required=True, help='Taxi name (provided by shell wrapper).')
    parser.add_argument('--cores', type=int, required=True, help='Number of CPUs to tell mpirun about (provided by shell wrapper).')
    parser.add_argument('--nodes', type=int, required=True, help='Number of nodes taxi is running on (provided by shell wrapper).')
    parser.add_argument('--pool_path', type=str, required=True, help='Path of pool backend DB.')
    parser.add_argument('--pool_name', type=str, required=True, help='Name of pool this taxi is assigned to.')
    parser.add_argument('--dispatch_path', type=str, required=True, help='Path of dispatch backend DB.')
    parser.add_argument('--time_limit',  type=float, required=True, help='Number of seconds in time budget.')

    print sys.argv
    # TODO: shouldn't this work without the sys.argv explicit specification?
    parg = parser.parse_args(sys.argv[1:]) # Call like "python run_taxi.py ...args..."


    ## "Connect with dispatcher"
    my_dispatch = taxi.dispatcher.SQLiteDispatcher(parg.dispatch_path)
    my_pool = taxi.pool.SQLitePool(
        db_path=parg.pool_path,
        pool_name=parg.pool_name,
    )
    my_queue = local_queue.LocalQueue()
    
    
    ## "Who am I?" -- Get information about this taxi from pool
    with my_pool:
        taxi_obj = my_pool.get_taxi(parg.name) # Get taxi object from pool
        
    if taxi_obj is None:
        # Taxi object with our name was not found in pool
        # Construct taxi object...
        taxi_obj = taxi.Taxi(name=parg.name, pool_name=parg.pool_name,
                             time_limit=parg.time_limit,
                             cores=parg.cores, nodes=parg.nodes)
        taxi_obj.pool_path=parg.pool_path
        taxi_obj.dispatch_path=parg.dispatch_path
        # ...and add it to the pool
        with my_pool:
            my_pool.register_taxi(taxi_obj)
            my_dispatch.register_taxi(taxi_obj, my_pool)
            
    ## Imports
    # Must have entered my_pool context at least once for this to work
    imports = taxi_obj.imports + my_pool.imports
    # Just need to get these in to the global namespace somewhere so the
    # task subclasses can be found
    _imported = [imp.load_source('mod%d'%ii, I) for ii, I in enumerate(imports)]
    
    # Print valid task classes that have been loaded
    #print "Loaded Task subclasses:", taxi.jobs.valid_task_classes().keys()
    print "Loaded Task subclasses:", taxi.jobs.valid_task_classes()
            

    ## Decoding for runner objects; relevant TaskRunner subclasses
    ## should be imported in local_taxi above!
    runner_decoder = taxi.jobs.runner_rebuilder_factory()

    ## Record starting time
    taxi_obj.start_time = _start_time
    
    print "Running on", taxi_obj.cores, "cores"

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
        taxi_obj.task_start_time = time.time()
        print "EXECUTING TASK ", task
        print task.__dict__


        if isinstance(task, taxi.jobs.Die) or isinstance(task, taxi.jobs.Sleep):
            ## "Die" and "Sleep" are special tasks
            print task.message # Print reason why we're dying or sleeping
            
            with my_dispatch:
                task_run_time = time.time() - taxi_obj.task_start_time
                my_dispatch.update_task(task, 'complete', run_time=task_run_time, by_taxi=taxi_obj)
    
            with my_pool:
                if isinstance(task, taxi.jobs.Die):
                    my_pool.update_taxi_status(taxi_obj, 'H')
                else:
                    my_pool.update_taxi_status(taxi_obj, 'I')
    
            sys.exit(0)
    
        sys.stdout.flush()

    
        try:
            task.execute(cores=taxi_obj.cores)
            sys.stdout.flush()
        except:
            ## TODO: some exception logging here?
            ## Record task as failed
            task.status = 'failed'
            print "RUNNING FAILED:"
            sys.stdout.flush()
            traceback.print_exc()
            
        ## Record exit status, time taken, etc.
        taxi_obj.task_finish_time = time.time()
    
        with my_dispatch:
            my_dispatch.finalize_task_run(taxi_obj, task)
            
        sys.stdout.flush()
