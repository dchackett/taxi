#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
"""

def classify_abandoned_tasks(tasks, queue):
    active_tasks    = []
    abandoned_tasks = []
    
    # Find abandoned tasks
    for task in [t for t in tasks.values() if t.status == 'active']:
        running_taxi_status = queue.report_taxi_status_by_name(task.by_taxi)
        if running_taxi_status != 'R':
            abandoned_tasks.append(task)
        else:
            active_tasks.append(task)
            
    return active_tasks, abandoned_tasks