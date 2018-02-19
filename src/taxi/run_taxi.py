#!/usr/bin/env python
import time
_start_time = time.time() # Get this ASAP for accurate accounting

import os
import sys
import argparse
import datetime

import taxi
import taxi.dispatcher
import taxi.pool
import taxi.tasks

import taxi.local.local_queue as local_queue

from taxi import flush_output, work_in_dir, print_traceback


if __name__ == '__main__':
    
    ### Initialization
    os.system('date')
    os.system('hostname') # Print which machine we're working on
    print "Starting in directory:", os.getcwd()


    ## Parse arguments from command line
    parser = argparse.ArgumentParser(description="Workflow taxi (NOTE: not intended to be called by user directly!)")
    
    parser.add_argument('--name', type=str, required=True, help='Taxi name (provided by shell wrapper).')
    parser.add_argument('--cores', type=int, required=True, help='Number of CPUs to tell mpirun about (provided by shell wrapper).')
    parser.add_argument('--nodes', type=int, required=True, help='Number of nodes taxi is running on (provided by shell wrapper).')
    parser.add_argument('--pool_path', type=str, required=True, help='Path of pool backend DB.')
    parser.add_argument('--pool_name', type=str, required=True, help='Name of pool this taxi is assigned to.')
    parser.add_argument('--dispatch_path', type=str, required=True, help='Path of dispatch backend DB.')
    parser.add_argument('--time_limit',  type=float, required=True, help='Number of seconds in time budget.')

    # TODO: shouldn't this work without the sys.argv explicit specification?
    parg = parser.parse_args(sys.argv[1:]) # Call like "python run_taxi.py ...args..."


    ## Connect with Dispatcher, Pool, Queue
    print "Connecting with dispatch: {0}".format(parg.dispatch_path)
    my_dispatch = taxi.dispatcher.SQLiteDispatcher(parg.dispatch_path)
    
    print "Connecting with pool: {0}({1})".format(parg.pool_path, parg.pool_name)
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
            

    ## Record starting time
    taxi_obj.start_time = _start_time
    print "Start time:", datetime.datetime.fromtimestamp(_start_time).isoformat(' ')
    
    
    ## Diagnostic outputs: where are we running?
    print "Running on", taxi_obj.cores, "cores"
    print "Working dir:", my_pool.work_dir
    
    ## Control variables
    keep_running = True
    tasks_run = 0
    
    ## Main control loop
    with work_in_dir(my_pool.work_dir):
        
        loops_without_executing_task = 0 # ANTI-THRASH
        
        while keep_running:
            loops_without_executing_task += 1 # Value is incorrect for a while, but increment here for maximum safety
            
            MAX_WASTED_LOOPS = 20
            if loops_without_executing_task > MAX_WASTED_LOOPS:
                print "ANTI-THRASH: Entered main loop {0} times without executing a task. Killing taxi.".format(MAX_WASTED_LOOPS)
                break
            
            ### Maintain taxi pool
            with my_pool:
                my_pool.update_all_taxis_queue_status(queue=my_queue, dispatcher=my_dispatch)
                my_pool.spawn_idle_taxis(queue=my_queue, dispatcher=my_dispatch)
        
        
            ### Check with dispatch for tasks to execute
            with my_dispatch:
                # Ask dispatcher for next task
                task = my_dispatch.request_next_task(for_taxi=taxi_obj)
        
                # Flag task for execution
                try:
                    my_dispatch.claim_task(taxi_obj, task)
                except taxi.dispatcher.TaskClaimException, e:
                    ## Race condition safeguard: skips and tries again if the task status has changed
                    print str(e)
                    continue
                
            
            ### Execute task
            print "EXECUTING TASK {0}".format(getattr(task, 'id', None))
            print task
            
            ## Timing
            task.start_time = time.time()
            print "Time remaining:", taxi_obj.time_limit - (task.start_time - taxi_obj.start_time)
            
            flush_output()
            
            ## Special behavior -- Die/Sleep
            if isinstance(task, taxi.tasks.Die) or isinstance(task, taxi.tasks.Sleep):
                ## "Die" and "Sleep" are special tasks
                print task.message # Print reason why we're dying or sleeping

                with my_pool:
                    if isinstance(task, taxi.tasks.Die):
                        my_pool.update_taxi_status(taxi_obj, 'H')
                    else:
                        my_pool.update_taxi_status(taxi_obj, 'I')
                    
                task.status = 'complete'
                keep_running = False
                
            ## Special behavior -- Respawning
            elif isinstance(task, taxi.tasks.Respawn):
                print "RESPAWNING", taxi_obj
                
                with my_pool:
                    if tasks_run > 0:
                        # Resubmit self as queued taxi
                        my_pool.submit_taxi_to_queue(my_taxi=taxi_obj, queue=my_queue, respawn=True)
                    else:
                        # ANTI-THRASHING: This taxi accomplished nothing while it was alive
                        my_pool.update_taxi_status(taxi_obj, 'E')
                        print "ANTI-THRASH: {t} ran no tasks while alive, not respawning.".format(t=str(taxi_obj))
                    
                keep_running = False
                
            ## 'Normal' behavior -- Task running
            elif hasattr(task, 'execute') and callable(getattr(task, 'execute')): # duck typing
                try:
                    task.execute(cores=taxi_obj.cores)
                except:
                    ## TODO: some exception logging here?
                    ## Record task as failed
                    task.status = 'failed'
                    print "RUNNING FAILED:"
                    print_traceback()
            else:
                print "WARNING: Task type {t} does nothing".format(type(task))
                print task.to_dict()
            
            flush_output()
            
            
            ### Record exit status, time taken, etc.
            task.run_time = time.time() - task.start_time
            with my_dispatch:
                tasks_run += 1
                my_dispatch.finalize_task_run(taxi_obj, task)    
            flush_output()
            
            loops_without_executing_task = 0 # ANTI-THRASH: Not thrashing if we've made it this far

## Exit
os.system('date')