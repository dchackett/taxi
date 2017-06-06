#!/usr/bin/env python

import taxi
import taxi.dispatcher
import taxi.pool
import taxi.jobs

## All runners to be used must be imported here as well!
import taxi.runners.flow
import taxi.runners.mrep_milc.pure_gauge_ora

import sys
import argparse
import time
import json

import taxi.local.local_taxi as local_taxi
import taxi.local.local_queue as local_queue

class Taxi(object):

    def __init__(self, name=None, pool_name=None, time_limit=None, cores=None):
        self.name = name
        self.pool_name = pool_name
        self.time_limit = time_limit
        self.cores = cores
        self.time_last_submitted = None
        self.start_time = None  ## Not currently saved to DB, but maybe it should be?
        self.status = 'I'

    def __eq__(self, other):
        eq = (self.name == other.name)
        eq = eq and (self.pool_name == other.pool_name)
        eq = eq and (self.time_limit == other.time_limit)
        eq = eq and (self.cores == other.cores)
        eq = eq and (self.time_last_submitted == other.time_last_submitted)
        eq = eq and (self.start_time == other.start_time)
        eq = eq and (self.status == other.status)

        return eq

    def taxi_name(self):
        return '{0:s}_{1:d}'.format(self.pool_name, self.name)

    def rebuild_from_dict(self, taxi_dict):
        try:
            self.name = taxi_dict['name']
            self.pool_name = taxi_dict['pool_name']
            self.time_limit = taxi_dict['time_limit']
            self.cores = taxi_dict['cores']
            self.time_last_submitted = taxi_dict['time_last_submitted']
            self.status = taxi_dict['status']
            self.dispatch_path = taxi_dict['dispatch']
        except KeyError:
            print "Error: attempted to rebuild taxi from malformed dictionary:"
            print taxi_dict
            raise
        except TypeError:
            print "Error: type mismatch in rebuilding taxi from dict:"
            print taxi_dict
            raise

    def to_dict(self):
        self_dict = {
            'name': self.name,
            'pool_name': self.pool_name,
            'time_limit': self.time_limit,
            'cores': self.cores,
            'time_last_submitted': self.time_last_submitted,
            'start_time': self.start_time,
            'status': self.status,
        }
        if hasattr(self, 'pool_path'):
            self_dict['pool_path'] = self.pool_path
        if hasattr(self, 'dispatch_path'):
            self_dict['dispatch_path'] = self.dispatch_path

        return self_dict

    def __repr__(self):
        return "Taxi<{},{},{},{},{},'{}'>".format(self.name, self.pool_name, self.time_limit,
            self.cores, self.time_last_submitted, self.status)


if __name__ == '__main__':

    ## Parse arguments from command line
    parser = argparse.ArgumentParser(description="Workflow taxi (NOTE: not intended to be called by user directly!)")

    parser.add_argument('--name', type=str, required=True, help='Taxi name (provided by shell wrapper).')
    parser.add_argument('--cores', type=int, required=True, help='Number of CPUs to tell mpirun about (provided by shell wrapper).')
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
            if (task_blob is None) or (len(task_blob) == 0):
                task_priority_ids = []
            else:
                task_priority_ids = [ t['id'] for t in sorted(task_blob.values(), cmp=taxi.dispatcher.task_priority_sort) ]
    
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
                my_dispatch.claim_task(taxi_obj, task_id)
            except taxi.dispatcher.TaskClaimException, e:
                ## Race condition safeguard: skips and tries again if the task status has changed
                print str(e)
                continue


        ## Execute task
        task_start_time = time.time()
        print "EXECUTING TASK ", task
        
        if task['task_type'] == 'die':
            ## "Die" is a special task
            with my_dispatch:
                task_run_time = time.time() - task_start_time
                my_dispatch.update_task(task['id'], 'complete', run_time=task_run_time, by_taxi=taxi_obj)

            with my_pool:
                my_pool.update_taxi_status(taxi_obj, 'H')

            sys.exit(0)

        # Alright, this is a little odd-looking with dumps and loads in the same line...
        task_obj = json.loads(json.dumps(task), object_hook=runner_decoder)

        failed_task = False
        try:
            task_obj.execute(cores=taxi_obj.cores)
        except:
            ## TODO: some exception logging here?
            ## Record task as failed
            failed_task = True
            raise

        ## Record exit status, time taken, etc.
        taxi_obj.task_finish_time = time.time()
        task_run_time = taxi_obj.task_finish_time - task_start_time

        if failed_task:
            task_status = 'failed'
        else:
            if (task['is_recurring']):
                task_status = 'pending'
            else:
                task_status = 'complete'

        with my_dispatch:
            my_dispatch.update_task(task['id'], task_status, start_time=task_start_time, run_time=task_run_time, by_taxi=taxi_obj)

