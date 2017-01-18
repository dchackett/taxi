# -*- coding: utf-8 -*-

import os, sys
import sqlite3, json
import argparse

### Parse command line arguments
parser = argparse.ArgumentParser(description="Workflow forest summarizer")

parser.add_argument('--forest', type=str, required=True, help='Forest file to look in.')
parser.add_argument('--failed', dest='failed', action='store_true', help='Print details of failed tasks.')
parser.set_defaults(failed=False)
parser.add_argument('--active', dest='active', action='store_true', help='Print details of active tasks.')
parser.set_defaults(active=False)
parser.add_argument('--taxis', dest='taxis', action='store_true', help='Find which taxis are associated with active tasks.')
parser.set_defaults(taxis=False)


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
    """).fetchall()

# Process task (parse json, etc)
tasks = map(dict, tasks)
for task in tasks:
    task['depends_on'] = json.loads(task['depends_on'])
    if task.has_key('task_args') and task['task_args'] is not None:
        task['task_args'] = json.loads(task['task_args'])

# Dictionaryize tasks like id->task
task_dict = {}
for task in tasks:
    task_dict[task['id']] = task
tasks = task_dict

# Find blocked tasks
for task_id, task in tasks.items():
    task['blocked'] = False
    
    if not task.has_key('depends_on') or task['depends_on'] is None:        
        continue

    failed_parent = False    
    for dep_id in task['depends_on']:
        if tasks[dep_id]['status'] == 'failed':
            failed_parent = True
            
    if failed_parent:
        task['blocked'] = True

# Sort tasks in to interesting piles
pending_tasks  = []
active_tasks   = []
complete_tasks = []
failed_tasks   = []
blocked_tasks  = []

for task_id, task in tasks.items():
    if task['status'] == 'pending':
        pending_tasks.append(task)
    elif task['status'] == 'active':
        active_tasks.append(task)
    elif task['status'] == 'complete':
        complete_tasks.append(task)
    elif task['status'] == 'failed':
        failed_tasks.append(task)
    elif task['blocked']:
        blocked_tasks.append(task)


## Output

def pretty_print_dict(D, prefix=""):
    for k, v in D.items():
        if isinstance(v, dict):
            print prefix + str(k), ":"
            pretty_print_dict(v, prefix+"  ")
        else:
            print prefix + str(k), ":", v

# If requested, print details
if parg.active:
    print "ACTIVE TASKS"
    for task in active_tasks:
        print "TASK", task['id']
        pretty_print_dict(task)
        print ""
else:
    print "ACTIVE TASKS:", [task['id'] for task in active_tasks]
        
if parg.failed:
    print "FAILED TASKS"
    for task in failed_tasks:
        print "TASK", task['id']
        pretty_print_dict(task)
        print ""
else:
    print "FAILED TASKS:", [task['id'] for task in failed_tasks]
    
if parg.taxis:
    print "ACTIVE TAXIS"
    for task in active_tasks:
        print task['by_taxi'], task['id']
        
# Print summary
print "PENDING %d  ACTIVE %d  COMPLETE %d  FAILED %d  BLOCKED %d"%(len(pending_tasks), len(active_tasks), len(complete_tasks), len(failed_tasks), len(blocked_tasks))
