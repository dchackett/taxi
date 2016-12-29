# -*- coding: utf-8 -*-

import os, sys
import sqlite3, json
import argparse

### Parse command line arguments
parser = argparse.ArgumentParser(description="Workflow forest task status forcer")

parser.add_argument('--forest', type=str, required=True, help='Forest file to modify.')
parser.add_argument('--id', type=int, required=True, help='Job ID to change status of')
parser.add_argument('--status', type=str, required=True, help='New status')

parg = parser.parse_args(sys.argv[1:]) # Call like "python taxi.py ...args..."


if parg.status not in ['pending', 'complete', 'failed', 'recurring', 'active']:
    print "WARNING: Nonstandard status", parg.status, " -- beware undefined behavior"

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

# Recompile, post to DB
with conn:
    conn.execute("""
        UPDATE tasks
        SET status=?
        WHERE id=?
    """, (parg.status, task['id']))
