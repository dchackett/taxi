#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
"""

import load
from taxi.dispatcher import Dispatcher
from taxi.local.local_queue import LocalQueue

def summary(dispatch):
    """
    """
    
    ## Make sure we're talking to Dispatcher
    if isinstance(dispatch, Dispatcher):
        pass
    elif isinstance(dispatch, str):
        dispatch = load.dispatch(dispatch)
    else:
        raise Exception("Not sure what to do with specified dispatch {d}, must be DB filename or Dispatcher instance".format(d=dispatch))
    
    ## Connect to queue to check taxi existence
    q = LocalQueue()
    
    tasks = dispatch.get_task_blob(include_complete=True) # Task blob is an id : task dict
    
    ## Classify tasks
    pending_tasks   = []
    ready_tasks     = []
    blocked_tasks   = []
    active_tasks    = []
    abandoned_tasks = []
    failed_tasks    = []
    completed_tasks = []
    
    # Find abandoned tasks
    for task in [t for t in tasks.values() if t.status == 'active']:
        running_taxi_status = q.report_taxi_status_by_name(task.by_taxi)
        if running_taxi_status != 'R':
            abandoned_tasks.append(task)
        else:
            active_tasks.append(task)
    
    # Classify remaining tasks
    for task_id, task in tasks.items():
        if task.status == 'complete':
            completed_tasks.append(task)
        elif task.status == 'pending':
            if task.depends_on is not None and any([dep in abandoned_tasks for dep in task.depends_on]):
                blocked_tasks.append(task)
            else:
                npending, nfailed = task.count_unresolved_dependencies()
                if nfailed > 0:
                    blocked_tasks.append(task)
                elif npending == 0:
                    ready_tasks.append(task)
                else:
                    pending_tasks.append(task)
        elif task.status == 'failed':
            failed_tasks.append(task)
            
    # Print summary info
    print "PENDING %d  READY %d  BLOCKED %d  ACTIVE %d  ABANDONED %d  FAILED %d  COMPLETE %d"%\
        (len(pending_tasks), len(ready_tasks), len(blocked_tasks), len(active_tasks), len(abandoned_tasks), len(failed_tasks), len(completed_tasks))

    # Print diagnostics
    if len(active_tasks) > 0:
        print "ACTIVE TASKS:"
        for t in [" {0} ({1}) {2}".format(t.id, t.by_taxi, t) for t in active_tasks]: print t
    if len(abandoned_tasks) > 0:
        print "ABANDONED TASKS:"
        for t in [" {0} ({1}) {2}".format(t.id, t.by_taxi, t) for t in abandoned_tasks]: print t
    if len(failed_tasks) > 0:
        print "FAILED TASKS:"
        for t in ["{0} ({1}) {2}".format(t.id, t.by_taxi, t) for t in failed_tasks]: print t
    if len(blocked_tasks) > 0:
        print "BLOCKED TASKS:"
        for tt in [" {0} ({1}) {2}".format(t.id, ",".join([str(dep.id) for dep in t.depends_on if dep in abandoned_tasks+failed_tasks]), t) for t in blocked_tasks]: print tt