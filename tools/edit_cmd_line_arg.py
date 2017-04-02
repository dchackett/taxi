#!/usr/bin/env python

# -*- coding: utf-8 -*-

import os, sys
import sqlite3, json
import argparse

### Parse command line arguments
parser = argparse.ArgumentParser(description="Workflow forest run_script command line argument editor")

parser.add_argument('--forest', type=str, required=True, help='Forest file to edit.')
parser.add_argument('--id', type=int, required=True, help='Job ID to edit')
parser.add_argument('--cla', type=str, required=True, help='Command line argument to edit')
parser.add_argument('--value', type=str, required=True, help='New value for command line argument')
parser.add_argument('--cascade', dest='cascade', action='store_true', help='If provided and task is an HMC task, apply change to all rest of HMC tasks in stream as well (does not propagate to other streams that fork off).')
parser.set_defaults(cascade=False)
parser.add_argument('--new', dest='new_cla', action='store_true', help='Must provide this to set a new command line argument, versus altering an existing one.')
parser.set_defaults(new_cla=False)


parg = parser.parse_args(sys.argv[1:]) # Call like "python taxi.py ...args..."


### Defaults, error checking, and massaging
forest_file = os.path.abspath(parg.forest)
if not os.path.exists(forest_file):
    raise Exception("Specified forest {fn} does not exist".format(fn=forest_file))


### Open forest db
conn = sqlite3.connect(forest_file)
conn.row_factory = sqlite3.Row


### Get all failed tasks in the forest
with conn:
    tasks = conn.execute("""
        SELECT * FROM tasks
        WHERE task_type='run_script'
    """).fetchall()

assert len(tasks) > 0

# Process tasks
tasks = map(dict, tasks)
for task in tasks:
    task['depends_on'] = json.loads(task['depends_on'])
    task['task_args'] = json.loads(task['task_args'])

# Only care about run_script tasks
def is_hmc_task(task):
    if task['task_type'] != 'run_script':
        return False
    if os.path.basename(task['task_args']['script']) not in ['SU4_nds_mrep.py', 'SU4_nds_sextet.py', 'SU4_nds_fund.py']:
        return False
    return True

def is_run_script_task(task):
    if task['task_type'] != 'run_script':
        return False
    return True
tasks = filter(is_run_script_task, tasks)

# Dictionaryize tasks like id->task
task_dict = {}
for task in tasks:
    task_dict[task['id']] = task
tasks = task_dict

if not tasks.has_key(parg.id):
    raise Exception("Specified forest {fn} does not have task with id {task_id}".format(fn=forest_file, task_id=parg.id))

# Reduce forest size per-case
filtered_tasks = {}
if is_hmc_task(tasks[parg.id]):
    # Only keep HMC tasks
    for task_id, task in tasks.items():
        if is_hmc_task(task):
            filtered_tasks[task_id] = task
else:
    # Don't know how to cascade for non-HMC tasks, keep only this one
    filtered_tasks[parg.id] = tasks[parg.id]
tasks = filtered_tasks

    
def hmc_tasks_from_same_stream(task1, task2):
#    if not is_hmc_task(task1) or not is_hmc_task(task2):
#        return False

    for param in ['Ns', 'Nt', 'beta', 'k4', 'k6', 'gammarat']:
        if task1['task_args']['cmd_line_args'].has_key(param) != task2['task_args']['cmd_line_args'].has_key(param):
            return False
        if not task1['task_args']['cmd_line_args'].has_key(param):
            continue
        if task1['task_args']['cmd_line_args'][param] != task2['task_args']['cmd_line_args'][param]:
            return False
    return True
        

target_tasks = [tasks[parg.id]]
if parg.cascade and is_hmc_task(tasks[parg.id]):
    # Invert dependency tree
    for task_id, task in tasks.items():
        if not task.has_key('depends_on') or task['depends_on'] is None:
            continue # No dependencies
        for dep_id in task['depends_on']:
            if not tasks.has_key(dep_id):
                continue # Allows for HMC tasks to depend on non-hmc tasks
            parent = tasks[dep_id]
            if not parent.has_key('dependents'):
                parent['dependents'] = []
            parent['dependents'].append(task_id)
            
    # Trace through tree
    for task in target_tasks:
        if not task.has_key('dependents'):
            continue
        for dep_id in task['dependents']:
            if hmc_tasks_from_same_stream(task, tasks[dep_id]):
                target_tasks.append(tasks[dep_id])
    

# Diagnostic output
print "Tasks to modify:", [t['id'] for t in target_tasks]


for task in target_tasks:
    # Update
    if task['task_args']['cmd_line_args'].has_key(parg.cla):
        if parg.new_cla:
            print "WARNING: Task", task['id'], "already has command line arg", parg.cla
        #task['task_args']['cmd_line_args'][parg.cla] = type(task['task_args']['cmd_line_args'][parg.cla])(parg.value)        
    else:
        if not parg.new_cla:
            print "Task", task['id'], "does not have command line arg", parg.cla
            print "CLAs present:", task['task_args']['cmd_line_args']
            continue # Don't make new CLAs if not told to do so explicitly
        
    task['task_args']['cmd_line_args'][parg.cla] = str(parg.value)
    
    # Recompile, post to DB
    task['task_args'] = json.dumps(task['task_args'])
    with conn:
        conn.execute("""
            UPDATE tasks
            SET task_args=?
            WHERE id=?
        """, (task['task_args'], task['id']))
