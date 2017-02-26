# -*- coding: utf-8 -*-
"""
Created on Fri Dec 16 17:52:23 2016

@author: Dan
"""

import os, sys
import sqlite3

# HACKY: Localization, make sure we can find taxi root dir
install_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) # /taxi/tools/../
sys.path.insert(0, install_dir)

import argparse

### Parse command line arguments
parser = argparse.ArgumentParser(description="Workflow additional taxi launcher")

parser.add_argument('--forest', type=str, required=True, help='Forest file to look for taxis in.')
parser.add_argument('--dwork',  type=str, default=None, help='Work directory for taxis.')
parser.add_argument('--dpool',  type=str, default=None, help='Taxi pool directory.')
parser.add_argument('--tag', type=str, required=True, help='Taxi tag to name new taxis with.')
parser.add_argument('--launch', dest='launch', action='store_true', help='If provided, launch taxis.')
parser.set_defaults(launch=False)
parser.add_argument('--N', type=int, default=1, help='Number of new taxis to launch')
parser.add_argument('--time', type=int, default=6*3600, help='Taxi lifetime in seconds before it must respawn; default 6h')
parser.add_argument('--nodes', type=int, default=1, help='Number of nodes for taxi to run on')

parg = parser.parse_args(sys.argv[1:]) # Call like "python taxi.py ...args..."


### Defaults, error checking, and massaging
forest_file = os.path.abspath(parg.forest)
if not os.path.exists(forest_file):
    raise Exception("Specified forest {fn} does not exist".format(fn=forest_file))
    
#if parg.dshell is None:
#    taxi_shell_script = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + '/taxi.sh' # ../taxi.sh
#else:
#    taxi_shell_script = os.path.abspath(parg.dshell) + '/taxi.sh'
taxi_shell_script = os.path.abspath(os.path.join(install_dir, 'taxi.sh'))
if not os.path.exists(taxi_shell_script):
    raise Exception("{fn} does not exist.".format(fn=taxi_shell_script))

work_dir = parg.dwork
if work_dir is None:
    if parg.launch:
        raise Exception("Must specify working directory to launch taxi.")    
else:
    work_dir = os.path.abspath(work_dir)

pool_dir = parg.dpool
if pool_dir is None:
    if parg.launch:
        raise Exception("Must specify pool directory to launch new taxis.")
else:
    pool_dir = os.path.abspath(pool_dir)


# Relaunch the specified taxi
if parg.launch:
    from local_taxi import taxi_in_queue, taxi_launcher
    from manage_taxi_pool import next_available_taxi_name_in_pool, log_dir_for_taxi


    taxi_names = next_available_taxi_name_in_pool(pool_dir=pool_dir, taxi_tag=parg.tag, N=parg.N)

    for tn in taxi_names:
        # Don't spawn duplicate taxis
        if taxi_in_queue(tn):
            print "Can't launch: Taxi {taxi_name} already in queue".format(taxi_name = tn)
            continue

        td = log_dir_for_taxi(taxi_name=tn, pool_dir=pool_dir)
        if not os.path.exists(td):
            os.makedirs(td)

        # Launch taxis
        success = taxi_launcher(
             taxi_name=tn,
             taxi_forest=forest_file,
             home_dir=work_dir,
             taxi_dir=td,
             taxi_time=parg.time,
             taxi_nodes=parg.nodes,
             taxi_shell_script=taxi_shell_script)
    
