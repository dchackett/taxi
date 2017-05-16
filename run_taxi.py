#!/usr/bin/env python

import taxi
import dispatcher
import pool
import tasks

import sys
import argparse
import time

import local_taxi

## Utility functions
def flush_output():
    sys.stdout.flush()
    sys.stderr.flush()


## Parse arguments from command line
parser = argparse.ArgumentParser(description="Workflow taxi (NOTE: not intended to be called by user directly!)")

parser.add_argument('--name', type=str, required=True, help='Taxi name (provided by shell wrapper).')
parser.add_argument('--cpus', type=int, required=True, help='Number of CPUs to tell mpirun about (provided by shell wrapper).')
parser.add_argument('--nodes', type=int, required=True, help='Number of nodes the taxi is running on (provided by shell wrapper).')
parser.add_argument('--pool_path', type=str, required=True, help='Path of pool backend DB.')
parser.add_argument('--pool_name', type=str, required=True, help='Name of pool this taxi is assigned to.')
parser.add_argument('--dispatch_path', type=str, required=True, help='Path of dispatch backend DB.')
parser.add_argument('--time',  type=float, required=True, help='Number of seconds in time budget.')
parser.add_argument('--log_dir', type=str, required=True, help='Logs go here.')
parser.add_argument('--work_dir', type=str, required=True, help='Working directory for taxi is here.')
#    parser.add_argument('--shell',  type=str, required=True, help='Path to wrapper shell script.')

print sys.argv
# TODO: shouldn't this work without the sys.argv explicit specification?
parg = parser.parse_args(sys.argv[1:]) # Call like "python run_taxi.py ...args..."

taxi_obj = taxi.Taxi(name=parg.name, time_limit=parg.time, node_limit=parg.nodes, pool_name=parg.pool_name)


my_dispatch = dispatcher.SQLiteDispatcher(parg.dispatch_path)
my_pool = pool.SQLitePool(
    db_path=parg.pool_path,
    pool_name=parg.pool_name,
    work_dir=parg.work_dir,
    log_dir=parg.log_dir,
)
my_queue = local_taxi.LocalQueue()

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
        task_priority = my_dispatch.get_priorities(taxi_obj)

        N_pending_tasks = 0
        found_ready_task = False

        # TODO: "task_ii" is never used, is there a better syntax for this?
        for task_ii, task_id in enumerate(task_priority):
            task = task_blob[task_id]

            # We only asked for pending and recurring tasks, excluding "failed", "complete", and "active" tasks
            if not task_blob.has_key(task_id):
                continue
            task = task_blob[task_id]

            # Only try to do pending or recurring tasks
            if task['status'] not in ['pending', 'recurring']:
                continue

            # Count number of pending tasks
            if task['status'] == 'pending':
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
            break
        if not found_ready_task:
            print "WORK COMPLETE: no tasks ready, but %d pending"%N_pending_tasks
            break

        # Otherwise, flag task for execution
        try:
            my_dispatch.claim_task(taxi, task_id)
        except dispatcher.TaskClaimException, e:
            ## Race condition safeguard: skips and tries again if the task status has changed
            print str(e)
            continue

        ## TODO: Create task object?
        pass

    ## Execute task
    try:
        pass
    except:
        raise

    ## Record exit status, time taken, etc.
    pass
