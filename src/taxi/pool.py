#!/usr/bin/env python

# Definition of "Pool" class - manages Taxi attributes and queue status.

import os

import taxi
import time

import json

import traceback

import __main__ # To get filename of calling script

class Pool(object):

    taxi_status_codes = [
        'Q',    # queued
        'R',    # running
        'I',    # idle
        'H',    # on hold
        'E',    # error in queue submission
    ]

    def __init__(self, work_dir, log_dir, imports=None, thrash_delay=300):
        self.work_dir = taxi.expand_path(work_dir)
        self.log_dir = taxi.expand_path(log_dir)
        
        if imports is None:
            # Default behavior: import the file that called this pool (run-spec script)
            self.imports = [taxi.expand_path(__main__.__file__)]
        else:
            self.imports = imports

        ## thrash_delay sets the minimum time between taxi resubmissions, in seconds.
        ## Default is 5 minutes.
        self.thrash_delay = thrash_delay
    

    ### Backend interaction ###
    
    def __enter__(self):
        raise NotImplementedError
        
    def __exit__(self):
        raise NotImplementedError
    
    def get_all_taxis_in_pool(self):
        raise NotImplementedError

    def get_taxi(self, my_taxi):
        raise NotImplementedError
    
    def update_taxi_status(self, my_taxi, status):
        raise NotImplementedError

    def update_taxi_last_submitted(self, my_taxi, last_submit_time):
        raise NotImplementedError

    def register_new_taxi(self, my_taxi):
        raise NotImplementedError

    def delete_taxi_from_pool(self, my_taxi):
        # Pseudocode:
        # If taxi is a taxi object, extract id.
        # Otherwise, if it's an integer assume it's already an id.
        # Otherwise, complain!

        raise NotImplementedError

    ### Queue interaction ###

    def check_for_thrashing(self, my_taxi):
        last_submit = self.get_taxi(my_taxi).time_last_submitted

        if last_submit is None:
            return False
        else:
            current_time = time.time()
            time_since_last_submit = current_time - last_submit
            if time_since_last_submit < self.thrash_delay:
                print "Thrashing detected: {a}-{b} = {c} < {d} (Taxi resubmitted too quickly)".format(a=current_time,
                           b=last_submit, c=time_since_last_submit, d=self.thrash_delay)
            return time_since_last_submit < self.thrash_delay


    def submit_taxi_to_queue(self, my_taxi, queue, **kwargs):
        # Don't submit hold/error status taxis
        pool_taxi = self.get_taxi(my_taxi)
        taxi_status = pool_taxi.status
        if (taxi_status in ('H', 'E')):
            print "WARNING: did not submit taxi {} due to status flag {}.".format(my_taxi, taxi_status)
            return

        if self.check_for_thrashing(my_taxi):
            # Put taxi on hold to prevent thrashing
            print "Thrashing detected for taxi {}; set to hold.".format(my_taxi)
            self.update_taxi_status(my_taxi, 'H')
            return

        try:
            queue.launch_taxi(my_taxi, **kwargs)
        except:
            self.update_taxi_status(my_taxi, 'E')
            print "Failed to submit taxi {t}".format(t=str(my_taxi))
            traceback.print_exc()

        self.update_taxi_last_submitted(my_taxi, time.time())


    def remove_taxi_from_queue(self, my_taxi, queue):
        """
        Remove all jobs from the queue associated with the given taxi.
        """
        taxi_status = queue.report_taxi_status(my_taxi)

        for job in taxi_status['job_numbers']:
            queue.cancel_job(job)

        return


    ### Control logic ###

    def update_all_taxis_queue_status(self, queue):
        taxi_list = self.get_all_taxis_in_pool()
        for my_taxi in taxi_list:
            self.update_taxi_queue_status(my_taxi, queue)


    def update_taxi_queue_status(self, my_taxi, queue):
        queue_status = queue.report_taxi_status(my_taxi)['status']
        pool_status = self.get_taxi(my_taxi).status

        if queue_status in ('Q', 'R'):
            if pool_status in ('E', 'H'):
                # Hold and error status must be changed explicitly
                return
            else:
                self.update_taxi_status(my_taxi, queue_status)
                return
        elif queue_status == 'X':
            if pool_status in ('E', 'H'):
                return
            else:
                self.update_taxi_status(my_taxi, 'I')
                return
        else:
            print "Invalid queue status code - '{}'".format(queue_status)
            raise BaseException


    def spawn_idle_taxis(self, queue):
        taxi_list = self.get_all_taxis_in_pool()
        for my_taxi in taxi_list:
            pool_status = self.get_taxi(my_taxi).status
            if pool_status == 'I':
                self.submit_taxi_to_queue(my_taxi, queue)



import sqlite3
    
    
class SQLitePool(Pool):
    """
    Concrete implementation of the Pool class using an SQLite backend.
    """
    
    def __init__(self, db_path, pool_name,
                 work_dir=None, log_dir=None, imports=None, thrash_delay=300):
        """
        Argument options: either [db_path, pool_name(, thrash_delay)] are specified, or
        [work_dir, log_dir(, imports, thrash_delay)] are specified.  If all are specified,
        then [db_path, pool_name] takes priority and the remaining inputs
        are ignored.
        """
        super(SQLitePool, self).__init__(work_dir=work_dir, log_dir=log_dir,
             imports=imports, thrash_delay=thrash_delay)

        self.db_path = taxi.expand_path(db_path)
        self.pool_name = pool_name

        self.conn = None
        
        with self:
            pass # Semi-kludgey creation/retrieval of pool DB
    

    def write_table_structure(self):
        create_taxi_str = """
            CREATE TABLE IF NOT EXISTS taxis (
                name text PRIMARY KEY,
                pool_name text REFERENCES pools (name),
                time_limit real,
                cores integer,
                nodes integer,
                time_last_submitted real,
                status text,
                dispatch text,
                imports text
            )"""
        create_pool_str = """
            CREATE TABLE IF NOT EXISTS pools (
                name text PRIMARY KEY,
                working_dir text,
                log_dir text,
                imports text
            )"""

        with self.conn:
            self.conn.execute(create_taxi_str)
            self.conn.execute(create_pool_str)

        return
    
    
    def _get_or_create_pool(self):
        self.write_table_structure()

        # Make sure pool details are written
        pool_query = """SELECT * FROM pools WHERE name = ?"""
        this_pool_row = self.execute_select(pool_query, self.pool_name)

        if len(this_pool_row) == 0:
            # Did not find this pool in the pool DB; add it
            pool_write_query = """INSERT OR REPLACE INTO pools
                (name, working_dir, log_dir, imports)
                VALUES (?, ?, ?, ?)"""
            self.execute_update(pool_write_query, self.pool_name, self.work_dir,
                                self.log_dir, json.dumps(self.imports))
        else:
            # Found this pool in the pool DB; retrieve info about it from DB
            if self.work_dir is None: # Allow overrides
                self.work_dir = this_pool_row[0]['working_dir']
            if self.log_dir is None: # Allow overrides
                self.log_dir = this_pool_row[0]['log_dir']
            self.imports = json.loads(this_pool_row[0]['imports']) # Don't allow overrides
            
        
    ## Note: definition of "enter/exit" special functions allows usage of the "with" operator, i.e.
    ## with SQLitePool(...) as pool:
    ##      ...code...
    ## This automatically takes care of setup/teardown without any try/finally clause.
    
    def __enter__(self):
        self.conn = sqlite3.connect(self.db_path, timeout=30.0)
        self.conn.row_factory = sqlite3.Row # Row factory for return-as-dict
        
        self._get_or_create_pool() # Also retrieves info about pool from DB, including working dir, so must occur here
        
        taxi.mkdir_p(os.path.abspath(self.work_dir)) # Dig out working directory if it doesn't exist
        taxi.mkdir_p(os.path.abspath(self.log_dir)) # Dig out log directory if it doesn;t exist

        


    def __exit__(self, exc_type, exc_val, exc_traceback):
        self.conn.close()
#        os.chdir(self.backup_cwd) # restore original working directory


    def _create_taxi_object(self, db_taxi):
        """Interface to translate Taxi representation in the DB to a Taxi object.
        """
        db_taxi = dict(db_taxi)
        db_taxi['imports'] = json.loads(db_taxi['imports'])
        
        new_taxi = taxi.Taxi()
        new_taxi.rebuild_from_dict(db_taxi)
        new_taxi.pool_path = self.db_path
        new_taxi.log_dir = self.log_dir # Tell taxi where log_dir for this pool is

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
                self.conn.commit()
        except:
            print "Failed to execute query: "
            print query
            print "with arguments: "
            print query_args
            raise

        return


    def get_all_taxis_in_pool(self):
        query = """SELECT * FROM taxis;"""
        taxi_raw_info = self.execute_select(query)

        all_taxis = []
        for row in taxi_raw_info:
            row_taxi = self._create_taxi_object(row)
            all_taxis.append(row_taxi)

        return all_taxis


    def get_taxi(self, my_taxi):
        taxi_name = str(my_taxi)

        query = """SELECT * FROM taxis WHERE name == ?"""
        db_rows = self.execute_select(query, taxi_name)
        
        if len(db_rows) == 0:
            return None
        elif len(db_rows) > 1:
            print map(dict, db_rows)
            raise Exception("Multiple taxis with name=%s found in pool"%taxi_name)
        else:
            this_taxi = self._create_taxi_object(db_rows[0])
            return this_taxi
    
    
    def update_taxi_status(self, my_taxi, status):
        taxi_name = str(my_taxi)

        update_query = """UPDATE taxis SET status = ? WHERE name = ?"""
        self.execute_update(update_query, status, taxi_name)

        return


    def update_taxi_last_submitted(self, my_taxi, last_submit_time):
        taxi_name = str(my_taxi)
        
        update_query = """UPDATE taxis SET time_last_submitted = ? WHERE name = ?"""
        self.execute_update(update_query, last_submit_time, taxi_name)

        return


    def update_taxi_dispatch(self, my_taxi, dispatch_path):
        taxi_name = str(my_taxi)

        update_query = """UPDATE taxis SET dispatch = ? WHERE name = ?"""
        self.execute_update(update_query, dispatch_path, taxi_name)


    def register_taxi(self, my_taxi):
        """
        Register a taxi with the pool.  (Adds to the pool if the taxi is new;
        otherwise, sets taxi pool attributes.)
        """
        my_taxi.pool_name = self.pool_name
        my_taxi.pool_path = self.db_path
        my_taxi.log_dir = self.log_dir

        taxi_name_query = """SELECT name FROM taxis;"""
        all_taxi_names = map(lambda t: t['name'], self.execute_select(taxi_name_query))

        if str(my_taxi) in all_taxi_names:
            # Already registered in pool DB
            return
        else:
            self._add_taxi_to_pool(my_taxi)


    def _add_taxi_to_pool(self, my_taxi):
        """Add an already-initialized taxi to the pool.
        To be used by register_new_taxi and potentially
        other helper functions down the road.
        """

        insert_taxi_query = """INSERT OR REPLACE INTO taxis
            (name, pool_name, time_limit, cores, nodes, time_last_submitted, status, imports)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.execute_update(insert_taxi_query, my_taxi.name, my_taxi.pool_name, my_taxi.time_limit, 
            my_taxi.cores, my_taxi.nodes, my_taxi.time_last_submitted, my_taxi.status,
            json.dumps(my_taxi.imports))

        return


    def delete_taxi_from_pool(self, my_taxi):
        """Delete a particular taxi from the pool.
        """
        taxi_name = str(my_taxi)

        remove_query = """DELETE FROM taxis WHERE name = ?"""

        self.execute_update(remove_query, taxi_name)

        return