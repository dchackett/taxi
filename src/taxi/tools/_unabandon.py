#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
"""

import _utility

from taxi.local.local_queue import LocalQueue

def unabandon(dispatch, pool, rollback_dir=None, delete_files=False):
    ## Connect to queue to check taxi existence
    q = LocalQueue()
    
    pool.update_all_taxis_queue_status(dispatcher=dispatch) # Marks tasks as abandoned, updates taxis as missing
    
    tasks = dispatch.get_all_tasks(include_complete=True) # Task blob is an id : task dict
    active_tasks, abandoned_tasks = _utility.classify_abandoned_tasks(tasks=tasks, queue=q)
    
    relevant_taxis = []
    for task in abandoned_tasks:
        print "UNABANDONING", task.id, task
        task.status = 'abandoned'
        dispatch.write_tasks([task])
        dispatch.rollback(task, rollback_dir=rollback_dir, delete_files=delete_files)
        pool.update_taxi_status(my_taxi=task.by_taxi, status='I')
        relevant_taxis.append(task.by_taxi)
    
    print "RELAUNCHING MISSING TAXIS", relevant_taxis
    for my_taxi in pool.get_all_taxis_in_pool():
        if str(my_taxi) in relevant_taxis:
            q.launch_taxi(my_taxi)
