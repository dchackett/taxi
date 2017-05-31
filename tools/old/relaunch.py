#!/usr/bin/env python

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
parser = argparse.ArgumentParser(description="Workflow taxi relauncher")

parser.add_argument('--forest', type=str, required=True, help='Forest file to look for taxis in.')
parser.add_argument('--dwork',  type=str, default=None, help='Work directory for taxi.')
#parser.add_argument('--dshell', type=str, default=None, help='Directory to find taxi.sh in (default: .. from relaunch.py)')
parser.add_argument('--taxi', type=str, default=None, help='Taxi to launch.  If not provided, launch all initial taxis.')
parser.add_argument('--launch', dest='launch', action='store_true', help='If provided, launch taxis. If not provided, list taxis.')
parser.set_defaults(launch=False)

parg = parser.parse_args(sys.argv[1:]) # Call like "python taxi.py ...args..."


### Defaults, error checking, and massaging
def truepath(fn):
    return os.path.abspath(os.path.realpath(os.path.expanduser(fn)))

forest_file = truepath(parg.forest)
if not os.path.exists(forest_file):
    raise Exception("Specified forest {fn} does not exist".format(fn=forest_file))
    
#if parg.dshell is None:
#    taxi_shell_script = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + '/taxi.sh' # ../taxi.sh
#else:
#    taxi_shell_script = os.path.abspath(parg.dshell) + '/taxi.sh'
taxi_shell_script = truepath(os.path.join(install_dir, 'taxi.sh'))
if not os.path.exists(taxi_shell_script):
    raise Exception("{fn} does not exist.".format(fn=taxi_shell_script))

work_dir = parg.dwork
if work_dir is None:
    if parg.launch:
        raise Exception("Must specify working directory to launch taxi.")    
else:
    work_dir = truepath(work_dir)

### Open forest db
conn = sqlite3.connect(forest_file)
conn.row_factory = sqlite3.Row

# List all taxis associated with this forest
with conn:
    taxis = conn.execute("""
        SELECT * FROM taxis
    """).fetchall()

if len(taxis) > 0:
    taxis = map(dict, taxis)
    
    if not parg.launch:
        for taxi in taxis:
            print taxi['taxi_name']
            print "   TIME", taxi['taxi_time']
            print "  NODES", taxi['nodes']
            print "  DTAXI", taxi['taxi_dir']
            print " LAUNCH", taxi['is_launch_taxi'] == 1
    else:
        # Relaunch the specified taxi
        from local_taxi import taxi_in_queue, taxi_launcher
        
        if parg.taxi is not None:
            # If taxi provided, only launch that one
            taxis = [taxi for taxi in taxis if taxi['taxi_name'] == parg.taxi]
        else:
            # If no taxis provided, launch all launch taxis
            taxis = [taxi for taxi in taxis if taxi['is_launch_taxi'] == 1]

        # Check that we have any taxis left to launch
        if len(taxis) == 0:
            raise Exception("No taxis to launch".format(fn=forest_file))

        for taxi in taxis:
            # Don't spawn duplicate taxis
            if taxi_in_queue(taxi['taxi_name']):
                print "Can't relaunch: Taxi {taxi_name} already in queue".format(taxi_name = taxi['taxi_name'])
                continue
            
            # Launch taxis
            success = taxi_launcher(
                     taxi_name=taxi['taxi_name'],
                     taxi_forest=forest_file,
                     home_dir=work_dir,
                     taxi_dir=taxi['taxi_dir'],
                     taxi_time=taxi['taxi_time'],
                     taxi_nodes=taxi['nodes'],
                     taxi_shell_script=taxi_shell_script)
            
