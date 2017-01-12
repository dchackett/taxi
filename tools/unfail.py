# -*- coding: utf-8 -*-

import os, sys
import sqlite3, json
import argparse

### Parse command line arguments
parser = argparse.ArgumentParser(description="Workflow forest unfailer")

parser.add_argument('--forest', type=str, required=True, help='Forest file to fix failed tasks in.')
parser.add_argument('--unfail', dest='unfail', action='store_true', help='If provided, change failed tasks to pending. If not provided, list failed tasks.')
parser.add_argument('--id', type=int, default=None, help='If provided, only target this task.  Still need unfail to unfail.')
parser.set_defaults(unfail=False)

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
    if parg.id is None:
        failed_tasks = conn.execute("""
            SELECT * FROM tasks
            WHERE status='failed'
        """).fetchall()
    else:
        failed_tasks = conn.execute("""
            SELECT * FROM tasks
            WHERE status='failed'
            AND id={task_id}
        """.format(task_id=parg.id)).fetchall()

print "Found", len(failed_tasks), "failed tasks"

if len(failed_tasks) > 0:
    failed_tasks = map(dict, failed_tasks)
    for r in failed_tasks:
        # SQLite doesn't support arrays -- Parse dependency JSON in to list of integers
        if r['depends_on'] is not None:
            r['depends_on'] = json.loads(r['depends_on'])
        
        # Big complicated dictionary of task args in JSON format
        if r['task_args'] is not None:
            r['task_args'] = json.loads(r['task_args'])

    
    if not parg.unfail:
        # Print all failed tasks
        for task in failed_tasks:
            print "ID:", task['id']
            print "     BY", task['by_taxi']
            print "   TYPE", task['task_type']
            print " STATUS", task['status']
            print "   DEPS", task['depends_on']
            if task.has_key('task_args') and task['task_args'] is not None:
                print "   ARGS", task['task_args']
    else:
        with conn:
            for task in failed_tasks:
                print task['id'], ":", task['task_type']
                conn.execute("""
                    UPDATE tasks
                    SET status='pending'
                    WHERE id=?
                """, (task['id'],))
        print "DONE"
