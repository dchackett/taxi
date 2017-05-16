#!/usr/bin/env python

# Definition of "Dispatcher" class - manages task forest and assigns tasks to Taxis.

import os
import tasks
import time
import json

import taxi

def mkdir_p(path):
    if not os.path.exists(path):
        os.makedirs(path)

class TaskClaimException(Exception):
    pass

class Dispatcher(object):

    def __init__(self):
        pass

    def _taxi_name(self, my_taxi):
        ## Polymorphism!  (I wonder if there's a cleaner Python way to do this...)
        if type(my_taxi) == taxi.Taxi:
            return my_taxi.name
        elif type(my_taxi) == str:
            return my_taxi
        else:
            raise TypeError

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

    


    

    
    
import sqlite3

class SQLiteDispatcher(Dispatcher):
    """
    Implementation of the Dispatcher abstract class using SQLite as a backend.
    """ 

    ## NOTE: There is a little bit of code duplication between this and SQLitePool.
    ## However, the SQL is too deeply embedded in the pool/dispatched logic
    ## for a separate "DB Backend" object to make much sense to me.
    ##
    ## The clean way to remove the duplication would be multiple inheritance of
    ## an SQLite interface class...but I think multiple inheritance is kind of
    ## weird in Python2 and earlier.  We can look into it.


    def __init__(self, db_path):
        self.db_path = db_path


    ## NOTE: enter/exit means we can use "with <SQLiteDispatcher>:" syntax
    def __enter__(self):
        self.conn = sqlite3.connect(self.db_path, timeout=30.0)
        self.conn.row_factory = sqlite3.Row # Row factory for return-as-dict

        self.write_table_structure()


    def __exit__(self, exc_type, exc_val, exc_traceback):
        self.conn.close()


    def write_table_structure(self):
        create_task_str = """
            CREATE TABLE IF NOT EXISTS tasks (
                id integer PRIMARY KEY,
                task_type text,
                depends_on text,
                status text,
                for_taxi text,
                by_taxi text,
                req_time integer DEFAULT 0,
                run_time real DEFAULT -1,
                task_args text
            )"""
        create_priority_str = """
            CREATE TABLE IF NOT EXISTS priority (
                id integer PRIMARY KEY,
                taxi_name text,
                list text,
                CONSTRAINT priority_taxi_unique UNIQUE (taxi) 
            )"""

        with self.conn:
            self.conn.execute(create_task_str)
            self.conn.execute(create_priority_str)

        return

    def create_taxi_object(self, db_taxi):
        """
        Interface to translate Taxi representation in the DB to a Taxi object.
        """
        new_taxi = taxi.Taxi()
        new_taxi.rebuild_from_dict(db_taxi)
        return new_taxi


    def execute_select(self, query, *query_args):
        try:
            with self.conn:
                res = self.conn.execute(query, query_args).fetchall()
        except:
            raise

        return res

    def execute_update(self, query, *query_args):
        try:
            with self.conn:
                self.conn.execute(query, query_args)
        except:
            print "Failed to execute query: "
            print query
            print "with arguments: "
            print query_args
            raise

        return

    def get_priorities(self, my_taxi):
        """If there's a taxi-specific list for taxi_name, extracts that priority list.
        
        Otherwise, finds the list labeled 'all'."""

        taxi_name = self._taxi_name(my_taxi)

        priority_query = """SELECT * FROM priority WHERE taxi=? OR taxi='all'"""
        priority_res = self.execute_select(priority_query, taxi_name)

        res_dict = {}
        for r in map(dict, priority_res):
            res_dict[r['taxi']] = r

        if res_dict.has_key(taxi_name):
            return json.loads(dict(res_dict[taxi_name])['list'])
        else:
            return json.loads(dict(res_dict['all'])['list'])


    def get_task_blob(self, my_taxi):
        """Get all incomplete tasks pertinent to this taxi."""

        taxi_name = self._taxi_name(my_taxi)

        task_query = """
            SELECT * FROM tasks
            WHERE (for_taxi=? OR for_taxi IS null)
            AND (status != 'complete')"""
        
        task_res = self.execute_select(task_query, taxi_name)

        if len(task_res) == 0:
            return None

        # Dictionaryize everything
        task_res = map(dict, task_res)
        for r in task_res:
            # SQLite doesn't support arrays -- Parse dependency JSON in to list of integers
            if r['depends_on'] is not None:
                r['depends_on'] = json.loads(r['depends_on'])
            
            # Big complicated dictionary of task args in JSON format
            if r['task_args'] is not None:
                r['task_args'] = json.loads(r['task_args'])
        
        # Package as task_id : task dict
        res_dict = {}
        for r in task_res:
            res_dict[r['id']] = r
        
        return res_dict


    def check_task_status(self, task_id):
        """Quick query of task status for task with id=task_id from job forest DB.

        For last-minute checks that job hasn't been claimed by another job."""

        task_res = self.execute_select("""SELECT status FROM tasks WHERE id=?""", task_id)
        
        if len(task_res) == 0:
            return None
        
        return dict(task_res[0])['status']
        
   
    def update_task(self, task_id, status, run_time=None, by_taxi=None):
        """Change the status of task with task_id in job forest DB.  For claiming
        task as 'active', marking tasks as 'complete', 'failed', etc.  At end of run,
        used to record runtime and update status simultaneously (one less DB interaction)."""

        update_str = """UPDATE tasks SET status=?"""
        values = [status]
        if run_time is not None:
            update_str += """, run_time=?"""
            values.append(run_time)
        if by_taxi is not None:
            update_str += """, by_taxi=?"""
            values.append(self._taxi_name(by_taxi))
        update_str += """WHERE id=?"""
        values.append(task_id)

        self.execute_update(update_str, values)

    def count_unresolved_dependencies(self, task, task_blob):
        """Looks at the status of all jobs in the job forest DB that 'task' depends upon.
        Counts up number of jobs that are not complete, and number of jobs that are failed.
        Returns tuple (n_unresolved, n_failed)"""
        
        dependencies = task['depends_on']
        
        # Sensible behavior for dependency-tree roots
        if dependencies is None:
            return 0, 0
        
        # Count up number of incomplete, number of failed
        N_unresolved = 0
        N_failed = 0
        for dependency_id in dependencies:
            if not task_blob.has_key(dependency_id):
                continue # Completes weren't requested in task blob
            dependency_status = task_blob[dependency_id]['status']
            if dependency_status != 'complete':
                N_unresolved += 1
            if dependency_status == 'failed':
                N_failed += 1
        return N_unresolved, N_failed
        
    def enough_time_for_task(self, taxi, task):
        """Checks whether a taxi has enough time to complete the given task."""

        elapsed_time = time.time() - taxi.start_time
        time_remaining = taxi.time_limit - elapsed_time

        return time_remaining > task['req_time']

    def claim_task(self, taxi, task_id):
        """Attempt to claim task for a given taxi.  Fails if the status has been changed from pending."""

        task_status = self.check_task_status(task_id)
        if task_status not in ['pending', 'recurring']:
            raise TaskClaimException("Failed to claim task {}: status {}".format(task_id, task_status))

        self.update_task(task_id=task_id, status='active', by_taxi=taxi.name)