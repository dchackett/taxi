# -*- coding: utf-8 -*-

import os, sys
import sqlite3, json
import argparse

### Parse command line arguments
parser = argparse.ArgumentParser(description="Workflow forest run_script req_time adjustor")

parser.add_argument('--forest', type=str, required=True, help='Forest file to modify.')
parser.add_argument('--time', type=int, default=None, help='If provided, time in seconds to change required time to.  If not provided, list jobs to be modified.')
parser.add_argument('--type', type=str, default='hmc', help='Base filename of runner script (e.g., spectro.py) or "hmc"')
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
        WHERE task_type IN ('run_script')
    """).fetchall()


if parg.type == 'hmc':
    script_names = ['SU4_nds_mrep.py', 'SU4_nds_sextet.py', 'SU4_nds_fund.py']
else:
    script_names = [parg.type]

# Process task (parse json, etc)
tasks = map(dict, tasks)
adjust_tasks = []
for task in tasks:
    task['depends_on'] = json.loads(task['depends_on'])
    task['task_args'] = json.loads(task['task_args'])
    
    if task['task_type'] == 'run_script' and os.path.basename(task['task_args']['script']) in script_names:
        adjust_tasks.append(task)

print "Tasks to adjust time for (%d)"%len(adjust_tasks)
print [t['id'] for t in adjust_tasks]

# Move files from dwork to droll
if parg.time is not None:
    with conn:
        conn.execute("""
            UPDATE tasks
            SET req_time=?
            WHERE id IN {}
        """.format(tuple([t['id'] for t in adjust_tasks])), (parg.time,))
            
