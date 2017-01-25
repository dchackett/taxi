# -*- coding: utf-8 -*-

import os, sys, shutil
import sqlite3, json
import argparse

### Parse command line arguments
parser = argparse.ArgumentParser(description="Workflow forest task rollback tool")

parser.add_argument('--forest', type=str, required=True, help='Forest file to modify.')
parser.add_argument('--id', type=int, required=True, help='Job ID to roll back to (including this job)')
parser.add_argument('--droll', type=str, default=None, help='Folder to move rolled-back files to. Must be provided if --rollback is provided.')
parser.add_argument('--dwork', type=str, default=None, help='Working folder to find files to roll back. Must be provided if --rollback is provided.')
parser.add_argument('--rollback', dest='rollback', action='store_true', help='If provided, perform specified rollback. If not provided, list jobs that would be rolled back.')
parser.set_defaults(rollback=False)

parg = parser.parse_args(sys.argv[1:]) # Call like "python taxi.py ...args..."


### Defaults, error checking, and massaging
forest_file = os.path.abspath(parg.forest)
if not os.path.exists(forest_file):
    raise Exception("Specified forest {fn} does not exist".format(fn=forest_file))
    
if parg.rollback and parg.droll is None:
    raise Exception("Must provide directory --droll to move rollback files to.")

if parg.rollback and parg.dwork is None:
    raise Exception("Must provide working directory --dwork to find files to roll back in.")

droll = os.path.abspath(parg.droll)
dwork = os.path.abspath(parg.dwork)
    
### Open forest db
conn = sqlite3.connect(forest_file)
conn.row_factory = sqlite3.Row


### Get all failed tasks in the forest
with conn:
    tasks = conn.execute("""
        SELECT * FROM tasks
        WHERE status IN ('complete', 'failed', 'active')
    """).fetchall()

# Process task (parse json, etc)
tasks = map(dict, tasks)
for task in tasks:
    task['depends_on'] = json.loads(task['depends_on'])
    task['task_args'] = json.loads(task['task_args'])

# Dictionaryize tasks like id->task
task_dict = {}
for task in tasks:
    task_dict[task['id']] = task
tasks = task_dict

if not tasks.has_key(parg.id):
    raise Exception("Specified forest {fn} does not have task with id {task_id}".format(fn=forest_file, task_id=parg.id))

# Invert dependency tree
for task_id, task in tasks.items():
    if not task.has_key('depends_on') or task['depends_on'] is None:
        continue # No dependencies
    for dep_id in task['depends_on']:
        parent = tasks[dep_id]
        if not parent.has_key('dependents'):
            parent['dependents'] = []
        parent['dependents'].append(task_id)

# Slice out rollback tasks and tasks after
rollback_tasks = [tasks[parg.id]]
for task in rollback_tasks:
    if not task.has_key('dependents'):
        continue # leaf
    for dep_id in task['dependents']:
        dep_task = tasks[dep_id]
        if dep_task['status'] in ['failed', 'complete', 'active']:
            rollback_tasks.append(tasks[dep_id])
            
# Check for active tasks
for task in rollback_tasks:
    if task['status'] == 'active':
        err_msg = 'Task-to-rollback {task_id} is active'.format(task_id=task['id'])
        if parg.rollback:
            raise Exception(err_msg)
        else:
            print "WARNING: " + err_msg

## If not performing the rollback, print out rollback tasks in detail
def pretty_print_dict(D, prefix=""):
    for k, v in D.items():
        if isinstance(v, dict):
            print prefix + str(k), ":"
            pretty_print_dict(v, prefix+"  ")
        else:
            print prefix + str(k), ":", v

if not parg.rollback:  
    for task in rollback_tasks:
        print "TASK", task['id']
        pretty_print_dict(task)
        print ''


## Perform rollback
def find_available_filename(to_fn):
    file_idx = 0
    check_fn = to_fn
    while os.path.exists(check_fn):
        file_idx += 1
        check_fn = to_fn + "[%d]"%file_idx
    return check_fn
    

# Move files from dwork to droll
N_rolled_back = 0
for task in rollback_tasks:
    print 'ROLLING BACK TASKS' + ('' if parg.rollback else ' (DRY RUN)'), task['id']
    
    ## Some script ran
    if task['task_type'] == 'run_script':
        cmd_line_args = task['task_args']['cmd_line_args']
        
        # Move output files where they exist
        for fkey in ['fout', 'saveg', 'savep']:
            # Check that the script generates this kind of output file
            if not cmd_line_args.has_key(fkey):
                continue
            
            # Check that the output filename is actually provided
            fn = cmd_line_args[fkey]
            if fn is None:
                continue
            
            # Check that the output file was created/exists, move if so
            from_fn = os.path.join(dwork, fn)
            to_fn   = find_available_filename(os.path.join(droll, fn))
            if os.path.exists(from_fn):
                print fkey, to_fn
                if parg.rollback:
                    shutil.move(from_fn, to_fn)
    
    ## Copied a file
    elif task['task_type'] == 'copy':
        dest = task['task_args']['dest']
        if os.path.exists(dest):
            to_fn = find_available_filename(os.path.join(droll, os.path.basename(dest) + '(COPIED)'))
            print 'copy_dest', to_fn
            if parg.rollback:
                shutil.move(dest, to_fn)                    

    ## Update task status to pending
    if parg.rollback:
        with conn:
            conn.execute("""
                UPDATE tasks
                SET status='pending'
                WHERE id=?
            """, (task['id'],))
            
    N_rolled_back += 1

if parg.rollback:
    print "Rolled back", N_rolled_back, "tasks"
