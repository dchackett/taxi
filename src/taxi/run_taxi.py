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
            try:
                my_dispatch.get_task_to_run(taxi_obj)
            except taxi.dispatcher.TaskClaimException, e:
                ## Race condition safeguard: skips and tries again if the task status has changed
                print str(e)
                continue

        ## Execute task
        taxi_obj.task_start_time = time.time()
        print "EXECUTING TASK ", task

        special_task_status_flag = None

        if task['task_type'] == 'die':
            ## "Die" kills the taxi and puts it on hold in the pool
            special_task_status_flag = 'H'
        elif task['task_type'] == 'sleep':
            ## "Sleep" kills the taxi temporarily, until another taxi or launch script restarts it
            special_task_status_flag = 'I'

        if special_task_status_flag:
            with my_dispatch:
                task_run_time = time.time() - taxi_obj.task_start_time
                my_dispatch.update_task(task['id'], 'complete', run_time=task_run_time, by_taxi=taxi_obj)
                
            with my_pool:
                my_pool.update_taxi_status(taxi_obj, special_task_status_flag)

            sys.exit(0)

        # Alright, this is a little odd-looking with dumps and loads in the same line...
        task_obj = json.loads(json.dumps(task), object_hook=runner_decoder)
        
        # TODO: This line may not be necessary, depending on how runner_decoder works
        # in future updates...
        task_obj.job_id = task['id']    
        
        try:
            task_obj.execute(cores=taxi_obj.cores)
        except:
            ## TODO: some exception logging here?
            ## Record task as failed
            task_obj.status = 'failed'

        ## Record exit status, time taken, etc.
        taxi_obj.task_finish_time = time.time()

        with my_dispatch:
            my_dispatch.finalize_task_run(taxi_obj, task_obj)

