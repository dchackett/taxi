# -*- coding: utf-8 -*-

import os, sys
import sqlite3, json
import argparse

### Parse command line arguments
parser = argparse.ArgumentParser(description="Workflow forest HMC job nstep adjustor")

parser.add_argument('--forest', type=str, required=True, help='Forest file to adjust nsteps in.')
parser.add_argument('--id', type=int, required=True, help='Job ID to adjust nstep in')
parser.add_argument('--nstep', type=int, required=True, help='New value for nstep')

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
    task = conn.execute("""
        SELECT * FROM tasks
        WHERE id=?
    """, (parg.id,)).fetchall()

if len(task) == 0:
    print "No task with id", parg.id, "found!"
    sys.exit(1)

# Process task
task = dict(task[0])

if task['task_type'] != 'run_script':
    print "Task isn't a run_script task"
    print task
    sys.exit(1)

# Big complicated dictionary of task args in JSON format
if task['task_args'] is not None:
    task['task_args'] = json.loads(task['task_args'])

if not task['task_args'].has_key('cmd_line_args'):
    print "Command line args missing"
    print task
    sys.exit(1)

if not task['task_args']['cmd_line_args'].has_key('nsteps1'):
    print "nsteps1 missing from cmd_line_args"
    print task
    sys.exit(1)

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
