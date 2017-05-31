#!/usr/bin/env python

# -*- coding: utf-8 -*-

import os, sys
import sqlite3, json
import argparse

### Parse command line arguments
parser = argparse.ArgumentParser(description="Workflow forest deactivator")

parser.add_argument('--forest', type=str, required=True, help='Forest file to deactivate tasks in.')
parser.add_argument('--status', type=str, default=None, help='If provided, change status of all active tasks to this status.')
parser.set_defaults(unfail=False)

parg = parser.parse_args(sys.argv[1:]) # Call like "python taxi.py ...args..."



### Defaults, error checking, and massaging
forest_file = os.path.abspath(parg.forest)
if not os.path.exists(forest_file):
    raise Exception("Specified forest {fn} does not exist".format(fn=forest_file))

if parg.status is not None and parg.status not in ['complete', 'pending', 'failed']:
    raise Exception("Unknown status %s"%str(parg.status))

### Open forest db
conn = sqlite3.connect(forest_file)
conn.row_factory = sqlite3.Row


### Get all failed tasks in the forest
with conn:
    active_tasks = conn.execute("""
        SELECT * FROM tasks
        WHERE status='active'
    """).fetchall()

print "Found", len(active_tasks), "active tasks"

def pretty_print_dict(D, prefix=""):
    for k, v in D.items():
        if isinstance(v, dict):
            print prefix + str(k), ":"
            pretty_print_dict(v, prefix+"  ")
        else:
            print prefix + str(k), ":", v

if len(active_tasks) > 0:
    active_tasks = map(dict, active_tasks)
    for r in active_tasks:
        if r['depends_on'] is not None:
            r['depends_on'] = json.loads(r['depends_on'])
        if r['task_args'] is not None:
            r['task_args'] = json.loads(r['task_args'])
            

    
    if parg.status is None:
        # Print all failed tasks
        for task in active_tasks:
            pretty_print_dict(task)
            print ""
    else:
        with conn:
            for task in active_tasks:
                print task['id'], ":", task['task_type']
                conn.execute("""
                    UPDATE tasks
                    SET status=?
                    WHERE id=?
                """, (parg.status, task['id'],))
        print "DONE"
