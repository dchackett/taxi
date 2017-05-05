#!/usr/bin/env python

# Definition of "Dispatcher" class - manages task forest and assigns tasks to Taxis.

import os
import tasks
import time

def mkdir_p(path):
    if not os.path.exists(path):
        os.makedirs(path)


class Dispatcher(object):

    def __init__(self):
        pass

    def get_priorities(self, taxi_name):
        """If there's a taxi-specific list for taxi_name, extracts that priority list.
        
        Otherwise, finds the list labeled 'all'."""
        raise NotImplementedError

    def get_task_blob(self, taxi_name):
        """Get all incomplete tasks pertinent to this taxi."""
        raise NotImplementedError

    def check_task_status(self, task_id):
        """Quick query of task status for task with id=task_id from job forest DB.

        For last-minute checks that job hasn't been claimed by another job."""

        raise NotImplementedError
    
    def update_task(self, task_id, status, run_time=None, by_taxi=None):
        """Change the status of task with task_id in job forest DB.  For claiming
        task as 'active', marking tasks as 'complete', 'failed', etc.  At end of run,
        used to record runtime and update status simultaneously (one less DB interaction)."""

        raise NotImplementedError


    def enough_time_for_task(self, my_taxi, task):
        """Checks if this taxi has enough time left to execute this task."""
        
        elapsed_time = time.time() - my_taxi.start_time
        time_remaining = my_taxi.time_limit - elapsed_time
        return time_remaining > task.req_time

    


    

    
    


class SQLiteDispatcher(Dispatcher):
    """
    Implementation of the Dispatcher abstract class using SQLite as a backend.
    """ 

    def __init__(self, db_path):
        self.db_path = db_path

        ## Open connection

    
    ## Add destructor when I remember the magic syntax

    