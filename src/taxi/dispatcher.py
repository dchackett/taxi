#!/usr/bin/env python

# Definition of "Dispatcher" class - manages task forest and assigns tasks to Taxis.

import os
import time
import json

import taxi

def task_priority_sort(a, b):
    """
    Order tasks by their priority score.
    Negative priority (default -1) is the lowest.
    For positive priority, smaller numbers are higher priority.
    """
    if a['priority'] < 0:
        if b['priority'] > 0:
            return 1
        else:
            return 0
    elif b['priority'] < 0:
        ## At this point a['priority'] must be positive
        return -1
    elif a['priority'] < b['priority']:
        return 1
    elif a['priority'] > b['priority']:
        return -1
    else:
        return 0

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
            raise TypeError("{} is not a Taxi or taxi name!".format(my_taxi))

    def get_task_blob(self, taxi_name):
        """Get all incomplete tasks pertinent to this taxi."""
        raise NotImplementedError

    def check_task_status(self, task_id):
        """Quick query of task status for task with id=task_id from job forest DB.

        For last-minute checks that job hasn't been claimed by another job."""

        raise NotImplementedError
    
    def update_task(self, task_id, status, start_time=None, run_time=None, by_taxi=None):
        """Change the status of task with task_id in job forest DB.  For claiming
        task as 'active', marking tasks as 'complete', 'failed', etc.  At end of run,
        used to record runtime and update status simultaneously (one less DB interaction)."""

        raise NotImplementedError


    def enough_time_for_task(self, my_taxi, task):
        """Checks if this taxi has enough time left to execute this task."""
        
        elapsed_time = time.time() - my_taxi.start_time
        time_remaining = my_taxi.time_limit - elapsed_time
        return time_remaining > task.req_time

    ## Initialization

    def _find_trees(self, job_pool):
        ## Scaffolding
        # Give each task an identifier, reset dependents
        for jj, job in enumerate(job_pool):
            job._dependents = []

        # Let dependencies know they have a dependent
        for job in job_pool:
            for dependency in job.depends_on:
                dependency._dependents.append(job)
                
        ## Break apart jobs into separate trees
        # First, find all roots
        self.trees = []
        for job in job_pool:
            if job.depends_on is None or len(job.depends_on) == 0:
                self.trees.append([job])

        ## Build out from roots
        # TODO:
        # - If dependent has different number of nodes, make it a new tree
        # - If job is a trunk job and two dependents are trunk jobs, make one of them a new tree
        for tree in self.trees:
            for tree_job in tree:
                if not tree_job.trunk:
                    continue
                n_trunks_found = 0
                for d in tree_job._dependents:
                    # Count number of trunk tasks encountered in dependents, fork if this isn't the first
                    if d.trunk:
                        n_trunks_found += 1
                        if n_trunks_found > 1:
                            self.trees.append([d]) # Break branch off in to a new tree
                            continue
                    # Normal behavior: build on current tree
                    tree.append(d)
                
    def _find_lowest_job_priority(self, job_pool):
        lowest_priority = 0
        for job in job_pool:
            if job.priority > lowest_priority:
                lowest_priority = job.priority

        return lowest_priority


    def _assign_priorities(self, job_pool, priority_method='tree'):
        """
        Assign task priorities to the newly tree-structured job pool.  Respects
        any user-assigned priority values that already exist.  All auto-assigned
        tasks have lower priority than user-chosen ones.

        'priority_method' describes the algorithm to be used for assigning priority.
        Currently, the following options are available:

        - 'tree': Tree-first priority: the workflow will attempt to finish an entire tree
        of jobs, before moving on to the next one.
        - 'trunk': Trunk-first priority: the workflow will attempt to finish all available
        tasks at the same tree depth, before moving deeper.
        - 'anarchy': No priorities are automatically assigned.  In the absence of user-determined
        priorities, the tasks will be run in arbitrary order, except that dependencies will be
        resolved first.
        """
        lowest_priority = self._find_lowest_job_priority(job_pool)

        if priority_method == 'tree':
            for tree in self.trees:
                tree_priority = lowest_priority + 1
                lowest_priority = tree_priority

                for tree_job in tree:
                    if (tree_job.priority < 0):
                        tree_job.priority = tree_priority
            
            return

        elif priority_method == 'trunk':
            ## I'll need to think about this implementation...not sure exactly the right way to assign
            ## depth-first priorities based on the current tree-construction algorithm.
            raise NotImplementedError

        elif priority_method == 'anarchy':
            ## Do nothing
            return

        else:
            raise ValueError("Invalid choice of priority assignment method: {}".format(priority_method))


    def _compile(self, job_pool, start_id):
        # Give each job an integer id
        for jj, job in enumerate(job_pool):
            job.job_id = jj + start_id + 1

        # Tell all jobs to compile themselves
        for job in job_pool:
            job.compile()

        self.task_pool = [job.compiled for job in job_pool]

    def _populate_task_table(self):
        raise NotImplementedError

    def _get_max_task_id(self):
        raise NotImplementedError

    def initialize_new_job_pool(self, job_pool, priority_method='tree'):
        # If we are adding a new pool to an existing dispatcher, 
        # start enumerating task IDs at the end

        start_id = self._get_max_task_id()
        self._find_trees(job_pool)
        self._assign_priorities(job_pool, priority_method=priority_method)
        self._compile(job_pool, start_id)
        self._populate_task_table()



    

    
    
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
                task_args text,
                depends_on text,
                status text,
                for_taxi text,
                by_taxi text,
                is_recurring bool,
                req_time integer DEFAULT 0,
                start_time real DEFAULT -1,
                run_time real DEFAULT -1,
                priority integer DEFAULT -1
            )"""

        with self.conn:
            self.conn.execute(create_task_str)

        return

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
                self.conn.commit()
        except:
            print "Failed to execute query: "
            print query
            print "with arguments: "
            print query_args
            raise

        return

    def register_taxi(self, my_taxi, my_pool):
        my_taxi.dispatch_path = self.db_path
        my_pool.update_taxi_dispatch(my_taxi, self.db_path)

        return

    def get_task_blob(self, my_taxi=None, include_complete=False):
        """Get all incomplete tasks pertinent to this taxi (or to all taxis.)"""

        if (my_taxi is None):
            task_query = """
                SELECT * FROM tasks
                WHERE (for_taxi IS null)"""
            if (not include_complete):
                task_query += """AND (status != 'complete')"""

            task_res = self.execute_select(task_query)
        else:
            taxi_name = self._taxi_name(my_taxi)

            task_query = """
                SELECT * FROM tasks
                WHERE (for_taxi=? OR for_taxi IS null)"""
            if (not include_complete):
                task_query += """AND (status != 'complete')"""
        
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
        
   
    def update_task(self, task_id, status, start_time=None, run_time=None, by_taxi=None):
        """Change the status of task with task_id in job forest DB.  For claiming
        task as 'active', marking tasks as 'complete', 'failed', etc.  At end of run,
        used to record runtime and update status simultaneously (one less DB interaction)."""

        update_str = """UPDATE tasks SET status=?"""
        values = [status]
        if start_time is not None:
            update_str += """, start_time=?"""
            values.append(start_time)
        if run_time is not None:
            update_str += """, run_time=?"""
            values.append(run_time)
        if by_taxi is not None:
            update_str += """, by_taxi=?"""
            values.append(self._taxi_name(by_taxi))
        update_str += """ WHERE id=?"""
        values.append(task_id)

        self.execute_update(update_str, *values)

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
        
    def enough_time_for_task(self, my_taxi, task):
        """Checks whether a taxi has enough time to complete the given task."""

        elapsed_time = time.time() - my_taxi.start_time
        time_remaining = my_taxi.time_limit - elapsed_time

        return time_remaining > task['req_time']

    def claim_task(self, my_taxi, task_id):
        """Attempt to claim task for a given taxi.  Fails if the status has been changed from pending."""

        task_status = self.check_task_status(task_id)
        if task_status != 'pending':
            raise TaskClaimException("Failed to claim task {}: status {}".format(task_id, task_status))

        self.update_task(task_id=task_id, status='active', by_taxi=my_taxi.name)

    def _populate_task_table(self):
        ## TODO: is there a more efficient way than generating N queries using a Python for loop?

        for task in self.task_pool:
            task_query = """INSERT OR REPLACE INTO tasks
            (task_type, task_args, depends_on, status, for_taxi, is_recurring, req_time, priority)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)"""

            task_values = (task['task_type'], 
                        json.dumps(task['task_args']) if task.has_key('task_args') else None,
                        json.dumps(task['depends_on']),
                        task['status'], 
                        task['for_taxi'] if task.has_key('for_taxi') else None, 
                        task['is_recurring'],
                        task['req_time'], 
                        task['priority'])

            self.execute_update(task_query, *task_values)
            
        
    def _get_max_task_id(self):
        task_id_query = """SELECT id FROM tasks ORDER BY id DESC LIMIT 1;"""
        max_id_query = self.execute_select(task_id_query)

        if len(max_id_query) == 0:
            return 0
        else:
            return max_id_query[0]

