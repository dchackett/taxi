#!/usr/bin/env python

import taxi
import dispatcher
import pool
import tasks
import task_runners

import sys
import argparse
import time

import local_taxi
import local_queue

## Utility functions
def flush_output():
    sys.stdout.flush()
    sys.stderr.flush()


## Parse arguments from command line
parser = argparse.ArgumentParser(description="Workflow taxi (NOTE: not intended to be called by user directly!)")

parser.add_argument('--name', type=str, required=True, help='Taxi name (provided by shell wrapper).')
parser.add_argument('--cores', type=int, required=True, help='Number of CPUs to tell mpirun about (provided by shell wrapper).')
#parser.add_argument('--nodes', type=int, required=True, help='Number of nodes the taxi is running on (provided by shell wrapper).')
parser.add_argument('--pool_path', type=str, required=True, help='Path of pool backend DB.')
parser.add_argument('--pool_name', type=str, required=True, help='Name of pool this taxi is assigned to.')
parser.add_argument('--dispatch_path', type=str, required=True, help='Path of dispatch backend DB.')
parser.add_argument('--time',  type=float, required=True, help='Number of seconds in time budget.')
#parser.add_argument('--log_dir', type=str, required=True, help='Logs go here.')
#parser.add_argument('--work_dir', type=str, required=True, help='Working directory for taxi is here.')
#    parser.add_argument('--shell',  type=str, required=True, help='Path to wrapper shell script.')

print sys.argv
# TODO: shouldn't this work without the sys.argv explicit specification?
parg = parser.parse_args(sys.argv[1:]) # Call like "python run_taxi.py ...args..."

taxi_obj = taxi.Taxi(name=parg.name, time_limit=parg.time, cores=parg.cores, pool_name=parg.pool_name)


my_dispatch = dispatcher.SQLiteDispatcher(parg.dispatch_path)
my_pool = pool.SQLitePool(
    db_path=parg.pool_path,
    pool_name=parg.pool_name,
)
my_queue = local_queue.LocalQueue()

## Decoding for runner objects; relevant TaskRunner subclasses
## should be imported in local_taxi above!
runner_decoder = task_runners.runner_rebuilder_factory()

## Record starting time
taxi_obj.start_time = time.time()

## Main control loop
while True:

    ## Check with pool for jobs to launch
    with my_pool:
        my_pool.update_all_taxis_queue_status(my_queue)
        my_pool.spawn_idle_taxis(my_queue)

    ## Check with dispatch for tasks to execute
    with my_dispatch:
        task_blob = my_dispatch.get_task_blob(taxi_obj)

        N_pending_tasks = 0
        found_ready_task = False

        # Order tasks in blob by priority
        task_priority_ids = [ t['id'] for t in sorted(task_blob.values(), cmp=dispatcher.task_priority_sort) ]

        for task_id in task_priority_ids:
            task = task_blob[task_id]

            # Only try to do pending tasks
            if task['status'] != 'pending':
                continue
            
            N_pending_tasks += 1
                
            # Check whether task is ready to go
            N_unresolved, N_failed = my_dispatch.count_unresolved_dependencies(task=task, task_blob=task_blob)
            sufficient_time = my_dispatch.enough_time_for_task(taxi_obj, task)
            
            # Look deeper in priority list if task not ready
            if N_unresolved > 0 or not sufficient_time:
                continue
            
            # Task ready; stop looking for new task to run
            found_ready_task = True
            break


        # If there are no tasks, finish up
        if N_pending_tasks == 0:
            print "WORK COMPLETE: no tasks pending"
            ## TODO: I think this will break both the with: and the outer while True:,
            ## but add a test case!

            # No work to be done, so place the taxi on hold
            with my_pool:
                my_pool.update_taxi_status(taxi_obj, 'H')

            break
        if not found_ready_task:
            ## TODO: we could add another status code that puts the taxi to sleep,
            ## but allows it to restart after some amount of time...
            ## Either that, or another script somewhere that checks the Pool
            ## and un-holds taxis that were waiting for dependencies to resolved
            ## once it sees that it's happened.
            ## Also need to be wary of interaction with insufficient time check,
            ## which we should maybe track separately.

            print "WORK COMPLETE: no tasks ready, but %d pending"%N_pending_tasks
            break

        # Otherwise, flag task for execution
        try:
            my_dispatch.claim_task(taxi, task_id)
        except dispatcher.TaskClaimException, e:
            ## Race condition safeguard: skips and tries again if the task status has changed
            print str(e)
            continue


    ## Execute task
    if task['task_type'] == 'die':
        ## "Die" is a special task
        with my_dispatch:
            task_run_time = time.time() - taxi_obj.task_start_time
            my_dispatch.update_task(task['id'], 'complete', run_time=task_run_time, by_taxi=taxi_obj)

        sys.exit(0)

    # Alright, this is a little odd...
    task_obj = json.loads(json.dumps(task), object_hook=runner_decoder)

    failed_task = False
    try:
        task_obj.execute()
    except:
        ## TODO: some exception logging here?
        ## Record task as failed
        failed_task = True

    ## Record exit status, time taken, etc.
    taxi_obj.task_finish_time = time.time()
    task_run_time = taxi_obj.task_finish_time - taxi_obj.task_start_time

    if failed_task:
        task_status = 'failed'
    else:
        if (task['is_recurring']):
            task_status = 'pending'
        else:
            task_status = 'complete'

    with my_dispatch:
        my_dispatch.update_task(task['id'], task_status, run_time=task_run_time, by_taxi=taxi_obj)



    
