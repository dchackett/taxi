#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys
import sqlite3, json
import argparse

### Parse command line arguments
parser = argparse.ArgumentParser(description="Workflow forest HMC job nstep adjustor")

parser.add_argument('--forest', type=str, required=True, help='Forest file to adjust nsteps in.')
parser.add_argument('--id', type=int, required=True, help='Job ID to adjust nstep in')
parser.add_argument('--nstep', type=int, required=True, help='New value for nstep')
parser.add_argument('--cascade', dest='cascade', action='store_true', help='If provided, apply changed nsteps to all following HMC tasks as well (does not propagate to other streams that fork off).')
parser.set_defaults(cascade=False)

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

# Only care about hmc tasks
def is_hmc_task(task):
    if task['task_type'] != 'run_script':
        return False
    if os.path.basename(task['task_args']['script']) not in ['SU4_nds_mrep.py', 'SU4_nds_sextet.py', 'SU4_nds_fund.py']:
        return False
    return True
tasks = filter(is_hmc_task, tasks)

# Dictionaryize tasks like id->task
task_dict = {}
for task in tasks:
    task_dict[task['id']] = task
tasks = task_dict

if not tasks.has_key(parg.id):
    raise Exception("Specified forest {fn} does not have task with id {task_id}".format(fn=forest_file, task_id=parg.id))


    
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
if parg.cascade:
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
    task['task_args']['cmd_line_args']['nsteps1'] = parg.nstep
    
    # Recompile, post to DB
    task['task_args'] = json.dumps(task['task_args'])
    with conn:
        conn.execute("""
            UPDATE tasks
            SET task_args=?
            WHERE id=?
        """, (task['task_args'], task['id']))
