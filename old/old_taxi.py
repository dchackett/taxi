# -*- coding: utf-8 -*-

## Temporarily moved to "old_taxi.py" while I refactor everything across taxi, pool, dispatch.

### Header -- imports
import time
start_time = time.time() # Record start time as early as possible for accurate accounting

import os, sys
import sqlite3
import json

# Need to bring our own argparse, so make sure PYTHONPATH includes taxi dir
import argparse


### Parse command line arguments
parser = argparse.ArgumentParser(description="Workflow taxi")

parser.add_argument('--name', type=str, required=True, help='Taxi name (provided by shell wrapper).')
parser.add_argument('--cpus', type=int, required=True, help='Number of CPUs to tell mpirun about (provided by shell wrapper).')
parser.add_argument('--nodes', type=int, required=True, help='Number of nodes the taxi is running on (provided by shell wrapper).')
parser.add_argument('--forest', type=str, required=True, help='Filename of sqlite3 DB containing task forest.')
parser.add_argument('--dhome', type=str, required=True, help='Directory to work in (outputs generated here).')
parser.add_argument('--dtaxi', type=str, required=True, help='Logs go here.')
parser.add_argument('--time',  type=float, required=True, help='Number of seconds in time budget.')
parser.add_argument('--shell',  type=str, required=True, help='Path to wrapper shells cript.')

print sys.argv
parg = parser.parse_args(sys.argv[1:]) # Call like "python taxi.py ...args..."


### General utilities
def mkdir_p(path):
    if not os.path.exists(path):
        os.makedirs(path)

def flush_output():
    sys.stdout.flush()
    sys.stderr.flush()
    
### Utilities for interacting with job forest sqlite DB
def get_priorities(taxi_name):
    """Extracts priority list from the SQLite job forest DB table 'priority'.
    If there's a taxi-specific list for taxi_name, extracts that.  Otherwise, finds
    the list labeled 'all'."""
    with conn:
        res = conn.execute("""SELECT * FROM priority WHERE taxi=? OR taxi='all'""", (taxi_name,)).fetchall()
    
    res_dict = {}
    for r in map(dict, res):
        res_dict[r['taxi']] = r
           
    if res_dict.has_key(taxi_name):
        return json.loads(dict(res_dict[taxi_name])['list'])
    else:
        return json.loads(dict(res_dict['all'])['list'])

        
def get_task_blob(taxi_name):
    """Get all incomplete tasks pertinent to this taxi.
    Returns a dict like { task_id : task_info }.
    Nice graceful behavior: taxis where for_taxi=null automatically match with the provided taxi name."""
    with conn:
        res = conn.execute("""
                           SELECT * FROM tasks
                           WHERE (for_taxi=? OR for_taxi IS null)
                           AND (status != 'complete')""",
        (taxi_name,)).fetchall()
    if len(res) == 0:
        return None
    
    # Dictionaryize everything
    res = map(dict, res)
    for r in res:
        # SQLite doesn't support arrays -- Parse dependency JSON in to list of integers
        if r['depends_on'] is not None:
            r['depends_on'] = json.loads(r['depends_on'])
        
        # Big complicated dictionary of task args in JSON format
        if r['task_args'] is not None:
            r['task_args'] = json.loads(r['task_args'])
    
    # Package as task_id : task dict
    res_dict = {}
    for r in res:
        res_dict[r['id']] = r
    
    return res_dict
    
    
def check_task_status(task_id):
    """Quick query of task status for task with id=task_id from job forest DB.
    
    For last-minute checks that job hasn't been claimed by another job."""
    
    with conn:
        res = conn.execute("""SELECT status FROM tasks WHERE id=?""", (task_id,)).fetchall()
        
    # No task with this ID
    if len(res) == 0:
        return None
    
    return dict(res[0])['status']


def update_task(task_id, status, run_time=None, by_taxi=None):
    """Change the status of task with task_id in job forest DB.  For claiming
    task as 'active', marking tasks as 'complete', 'failed', etc.  At end of run,
    used to record runtime and update status simultaneously (one less DB interaction)."""
    
    update_str = """UPDATE tasks SET status=?"""
    values = [status]
    if run_time is not None:
        update_str += """, run_time=?"""
        values.append(run_time)
    if by_taxi is not None:
        update_str += """, by_taxi=?"""
        values.append(by_taxi)
    update_str += """WHERE id=?"""
    values.append(task_id)
    
    with conn:
        conn.execute(update_str, values)        
        
def count_unresolved_dependencies(task, task_blob):
    """Looks at the status of all jobs in the job forest DB that 'task' depends upon.
    Counts up number of jobs that are not complete, and number of jobs that are failed.
    Returns tuple (n_unresolved, n_failed)"""
    
    dependencies = task['depends_on']
    
    # Sensible behavior for dependency-tree roots
    if dependencies is None:
        return 0, 0
    
    # Count up number of incomplete, number of failed
    N_unresolved = 0
    N_failed = 0
    for dependency_id in dependencies:
        if not task_blob.has_key(dependency_id):
            continue # Completes weren't requested in task blob
        dependency_status = task_blob[dependency_id]['status']
        if dependency_status != 'complete':
            N_unresolved += 1
        if dependency_status == 'failed':
            N_failed += 1
    return N_unresolved, N_failed
    
    
def enough_time_for_task(task):
    """Checks if this taxi has enough time left to execute this task."""
    
    elapsed_time = time.time() - start_time
    time_remaining = parg.time - elapsed_time
    return time_remaining > task['req_time']


### Queue interaction
from local_taxi import taxi_in_queue, taxi_launcher
def spawn_new_taxi(task_args):
    """Execute a spawn task.  Calls taxi_launcher.
    
    Returns: True if job submission seems to have worked correctly."""
    
    # Don't spawn duplicate taxis, but allow respawning
    if task_args['taxi_name'] != parg.name and taxi_in_queue(task_args['taxi_name']):
        print "spawn: Taxi {taxi_name} already in queue".format(taxi_name = task_args['taxi_name'])
        return False
        
    # Any taxi spawned by a taxi should have same work dir (all output files kept in the same place)
    return taxi_launcher(taxi_name=task_args['taxi_name'],
                         taxi_forest=parg.forest,
                         home_dir=parg.dhome,
                         taxi_dir=task_args['taxi_dir'],
                         taxi_time=task_args['taxi_time'],
                         taxi_nodes=task_args['taxi_nodes'],
                         taxi_shell_script=parg.shell)

def respawn():
    """Function to respawn the current taxi, if we've run out of time.  Wraps spawn_new_taxi."""
    return spawn_new_taxi(task_args={
            'taxi_nodes' : parg.nodes,
            'taxi_name'  : parg.name,
            'taxi_dir'   : parg.dtaxi,
            'taxi_forest': parg.forest,
            'taxi_time'  : parg.time
        })
    
    
### Script runner
def run_script(task_args):
    """Calls python with os.system to run whatever script is specified, with the
    specified command line arguments.  Tells the script about the number of CPUs
    available..
    
    Checks if the script exited happily, returns True if so and False if not."""

    print "Running script", task_args['script']
    flush_output()

    cmd_line_args = []
    if task_args.has_key('cmd_line_args'):
        # Args as list are legacy, cut out when all old dispatches are complete
        if isinstance(task_args['cmd_line_args'], list):
            cmd_line_args += task_args['cmd_line_args']
        elif isinstance(task_args['cmd_line_args'], dict):
            cmd_line_args += ["--{k} {v}".format(k=k, v=v) for (k,v) in task_args['cmd_line_args'].items()]
    
    # Specify number of cpus per the provided formatter
    if task_args.has_key('ncpu_fmt'):
        cmd_line_args.append(task_args['ncpu_fmt'].format(cpus=parg.cpus))
        
    # Explicitly call python to run scripts
    shell_call = " ".join(['python', task_args['script']] + map(str,cmd_line_args))
    
    # Run the script, catch exit code
    exit_code = os.system(shell_call)
    exit_code = exit_code >> 8 # only care about second byte
    
    if exit_code != 0:
        print "run_script: Script exit code = {errcode} != 0".format(errcode=exit_code)
        return False
    
    return True
    

### Nstep adaptor
#def adjust_hmc_nstep(task_args, task_blob):
#    adjust_id = task_args['adjust_job']
#    adjust_task = task_blob[adjust_id]
#
#    if adjust_task['status'] != 'pending':
#        print "Tried to adjust nsteps for non-pending job", adjust_id
#        return False
#    
#    # Look at files, figure out AR and new nstep
#    new_nstep = get_adjusted_nstep(task_args['files'], min_AR=task_args['min_AR'],
#                               max_AR=task_args['max_AR'], die_AR=task_args['die_AR'],
#                               delta_nstep=task_args['delta_nstep'])
#    if new_nstep is None:
#        print "Accept rate dropped below", task_args['die_AR'], "aborting"
#        return False
#
#    # Edit local copy of HMC task
#    # Legacy code for cmd_line_args as a list; cut this out when old dispatches are done
#    new_task_args = adjust_task['task_args']
#    if isinstance(new_task_args['cmd_line_args'], list):
#        cmd_line_args = new_task_args['cmd_line_args']
#        for aa, arg in enumerate(cmd_line_args):
#            if "--nsteps1 " in arg:
#                words = arg.split()
#                for ww, word in enumerate(words):
#                    if '--nsteps1' in word:
#                        words[ww+1] = nstep
#                        break
#                cmd_line_args[aa] = " ".join(map(str,words))
#                break
#        new_task_args['cmd_line_args'] = cmd_line_args # Probably redundant
#    elif isinstance(new_task_args['cmd_line_args'], dict):
#        new_task_args['cmd_line_args']['nsteps1'] = new_nstep
#
#    # Write change to DB
#    conn.execute("""UPDATE tasks SET task_args=? WHERE id=?""", (json.dumps(new_task_args), adjust_id))
#
#    print "New nstep", new_nstep
#    return True
            
    
### Open connection to job forest sqlite DB
conn = sqlite3.connect(parg.forest, timeout=30.0)
conn.row_factory = sqlite3.Row # Row factory for return-as-dict

    
### Change to working dir
backup_cwd = os.getcwd() # backup current directory
if not os.path.exists(os.path.abspath(parg.dhome)):
    mkdir_p(os.path.abspath(parg.dhome)) # Dig out working directory if it doesn't exist
os.chdir(os.path.abspath(parg.dhome)) # move to desired working directory


### Main loop
tasks_completed = 0 # For anti-thrash -- die if we can't complete a single non-recurring task

while True:
    flush_output()

    ## Get current priority list for this taxi
    ## and any tasks that are pending or recurring and belong to us / have no owners
    print ""
    print "Looking for a new task"
    task_priority = get_priorities(parg.name)
    task_blob = get_task_blob(parg.name)
    
    ## (Probably) redundant anti-thrashing safety measure
    if len(task_priority) == 0:
        print "FATAL: No tasks in priority list"
        break
    
    ## Run first task in the forest that:
    #  - Has no unresolved dependencies
    #  - Fits in our remaining time budget
    # Watch out for thrashing
    N_insufficient_time = 0
    N_fail_by_dependency = 0
    N_pending_tasks = 0
    found_ready_task = False
    for task_ii, task_id in enumerate(task_priority):
        # We only asked for pending and recurring tasks, excluding "failed", "complete", and "active" tasks
        if not task_blob.has_key(task_id):
            continue
        task = task_blob[task_id]

        # Only try to do pending or recurring tasks
        if task['status'] not in ['pending', 'recurring']:
            continue
            
        # ANTI-THRASH: Don't spawn self, only explicit respawn allowed to do that
        if task['task_type'] == 'spawn' and task['task_args']['taxi_name'] == parg.name:
            continue
            
        # Count number of pending tasks
        if task['status'] == 'pending':
            N_pending_tasks += 1
            
        # Check whether task is ready to go
        n_unresolved, n_failed = count_unresolved_dependencies(task=task, task_blob=task_blob)
        sufficient_time = enough_time_for_task(task) or task['task_type'] == 'respawn'
        
        # Keep track of reasons tasks aren't ready to go
        if n_failed > 0:
            N_fail_by_dependency += 1
        if not sufficient_time:
            N_insufficient_time += 1
        
        # Look deeper in priority list if task not ready
        if n_unresolved > 0 or not sufficient_time:
            continue
        
        # Task ready; stop looking for new task to run
        found_ready_task = True
        break
        
    
    ## No tasks ready: either done, or thrashing risk
    if N_pending_tasks == 0:
        print "WORK COMPLETE: No tasks pending"
        break
    if not found_ready_task:
        print "ANTI-THRASH: No tasks are ready, but %d pending"%N_pending_tasks
        break
        
    
    ## Last-second double-check that task hasn't switched to active, then switch to active
    status_check = check_task_status(task_id)
    if status_check not in ['pending', 'recurring']:
        print "Task {task_id} pre-empted at last second {status0}->{status1}"\
            .format(task_id=task_id, status0=task['status'], status1=status_check)
        continue
    
    ## Let other taxis know we've claimed this task, unless it's recurring
    if task['status'] == 'pending':
        update_task(task_id, status='active', by_taxi=parg.name)

    ## Perform task
    task_type = task["task_type"]
    print "Performing task", task_id, task_type
    task_start_time = time.time()
    flush_output()

    
    # Kill the taxi
    if task_type == "die":
        print "DIE: Killing taxi."
        break
    
    
    # Spawn a new taxi (for forking)
    elif task_type == "spawn":
        task_args = task["task_args"]
        
        # Respawn tasks must explicitly be respawns
        if task_args["taxi_name"] == parg.name:
            print "ANTI-THRASH: Non-respawn spawn task would have respawned taxi.  Respawns must be explicit."
            break # Kill the taxi
            
        print "Spawning", task_args['taxi_name']
        
        task_successful = spawn_new_taxi(task_args=task_args)
        
        
    # Respawn this taxi
    elif task_type == "respawn":        
        if N_pending_tasks == 0:
            print "WORK COMPLETE: No tasks pending, blocking respawn"
            break
        
        # ANTI-THRASHING MECHANISM : Before we spawn another taxi assigned
        # to the same forest we're working on, make sure that this task accomplished anything at all
        if tasks_completed == 0:
            print "ANTI-THRASH: Tried to resubmit, but completed no tasks first."
            break # Kill the taxi
            
        task_successful = respawn()
        
        print "Respawn successful: ", task_successful
        print "Killing taxi after respawn"
        break
        
        
    # Run a script (numerics, moving files around, etc)
    elif task_type == "run_script":
        task_successful = run_script(task["task_args"])
        
    # Copy a file
    elif task_type == "copy":
        task_args = task['task_args']
        if not os.path.exists(task_args['src']):
            print "Copy failed: file", task_args['src'], "does not exist, cannot copy it"
            task_successful = False
        else:
            # Dig out destination directory if it's not there already
            if not os.path.exists(os.path.split(task_args['dest'])[0]):
                mkdir_p(os.path.split(task_args['dest'])[0])            
            # Copy!
            print "Copying", task_args['src'], "->", task_args['dest']
            task_successful = (os.system('rsync -Paz {src} {dest}'.format(**task_args)) >> 8) == 0
        

#    # Adaptive nstep
#    elif task_type == 'adjust_nstep':
#        task_successful = adjust_hmc_nstep(task['task_args'], task_blob)

    # Print something (debug)
    elif task_type == "print":
        print task["task_args"]["print_text"]
        task_successful = True

    
    ## Task completion logistics
    if task_successful:
        print "Task {task_id} completed successfully".format(task_id=task_id)
        tasks_completed += 1
    else:
        print "Task {task_id} failed".format(task_id=task_id)   
        
    # Update job status and record runtime
    task_run_time = time.time() - task_start_time
    if task["status"] == "pending": # We kept track of original task status; Don't alter 'recurring' tasks
        if task_successful:
            update_task(task_id, status='complete', run_time=task_run_time)
        else:
            update_task(task_id, status='failed', run_time=task_run_time)
    print "Run time:", task_run_time
    print "Time remaining:", parg.time - (time.time() - start_time)
    
    flush_output()

print "TAXI {taxi_name} DONE".format(taxi_name=parg.name)


### Tear-down
conn.close() # be safe
os.chdir(backup_cwd) # restore original working directory
