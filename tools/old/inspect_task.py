#!/usr/bin/env python

# -*- coding: utf-8 -*-

import os, sys
import sqlite3, json
import argparse

### Parse command line arguments
parser = argparse.ArgumentParser(description="Workflow forest task inspector")

parser.add_argument('--forest', type=str, required=True, help='Forest file to look in.')
parser.add_argument('--id', type=int, required=True, help='Job ID to inspect')

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

# Big complicated dictionary of task args in JSON format
if task['task_args'] is not None:
    task['task_args'] = json.loads(task['task_args'])


def pretty_print_dict(D, prefix=""):
    for k, v in D.items():
        if isinstance(v, dict):
            print prefix + str(k), ":"
            pretty_print_dict(v, prefix+"  ")
        else:
            print prefix + str(k), ":", v

pretty_print_dict(task)
