#!/usr/bin/env python

# Definition of "Pool" class - manages Taxi attributes and queue status.

import os

import taxi
import time
import batch_queue

def mkdir_p(path):
    if not os.path.exists(path):
        os.makedirs(path)


class Pool(object):

    taxi_status_codes = [
        'Q',    # queued
        'R',    # running
        'I',    # idle
        'H',    # on hold
        'E',    # error in queue submission
    ]

    def __init__(self, work_dir, log_dir, thrash_delay=300):
        self.work_dir = work_dir
        self.log_dir = log_dir

        ## thrash_delay sets the minimum time between taxi resubmissions, in seconds.
        ## Default is 5 minutes.
        self.thrash_delay = thrash_delay

    def _taxi_id(self, my_taxi):
        ## Polymorphism!  (I wonder if there's a cleaner Python way to do this...)
        if type(my_taxi) == taxi.Taxi:
            return my_taxi.id
        elif type(my_taxi) == int:
            return my_taxi
        else:
            raise TypeError

    ### Backend interaction ###
    
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
            return (time.time() - last_submit) < self.thrash_delay


    def submit_taxi_to_queue(self, my_taxi, queue):
        # Don't submit hold/error status taxis
        taxi_status = self.get_taxi(my_taxi).status
        if (taxi_status in ('H', 'E')):
            print "Warning: did not submit taxi {} due to status flag {}.".format(my_taxi, taxi_status)
            return

        if (self.check_for_thrashing(my_taxi)):
            # Put taxi on hold to prevent thrashing
            print "Thrashing detected for taxi {}; set to hold.".format(my_taxi)
            self.update_taxi_status(my_taxi, 'H')
            return

        try:
            queue.launch_taxi(my_taxi)
        except:
            ## TO-DO: add some error handling here
            self.update_taxi_status(my_taxi, 'E')
            raise

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
            queue_status = queue.report_taxi_status(taxi)['status']
            if queue_status in ('Q', 'R'):
                self.update_taxi_status(my_taxi, queue_status)
            elif queue_status == 'X':
                pool_status = self.get_taxi(my_taxi).status
                if pool_status in ('E', 'H'):
                    continue
                else:
                    self.update_taxi_status(my_taxi.id, 'I')
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

    def __init__(self, db_path, pool_name, work_dir, log_dir, thrash_delay=300):
        self.db_path = db_path
        self.pool_name = pool_name
        self.work_dir = work_dir
        self.log_dir = log_dir
        self.thrash_delay = thrash_delay

        self.conn = None

        return

    ## Note: definition of "enter/exit" special functions allows usage of the "with" operator, i.e.
    ## with SQLitePool(...) as pool:
    ##      ...code...
    ## This automatically takes care of setup/teardown without any try/finally clause.

    def __enter__(self):
        self.conn = sqlite3.connect(self.db_path, timeout=30.0)
        self.conn.row_factory = sqlite3.Row # Row factory for return-as-dict

        self.backup_cwd = os.getcwd() # backup current directory
        if not os.path.exists(os.path.abspath(self.work_dir)):
            mkdir_p(os.path.abspath(self.work_dir)) # Dig out working directory if it doesn't exist
        os.chdir(os.path.abspath(self.work_dir)) # move to desired working directory

        self.write_table_structure()

        # Make sure pool details are written
        pool_query = """SELECT * FROM pools WHERE name == ?"""
        this_pool_row = self.execute_select(pool_query, self.pool_name)

        if len(this_pool_row) == 0:
            pool_write_query = """INSERT OR REPLACE INTO pools
                (name, working_dir, log_dir)
                VALUES (?, ?, ?)"""
            self.execute_update(pool_write_query, self.pool_name, self.work_dir, self.log_dir)


    def __exit__(self, exc_type, exc_val, exc_traceback):
        self.conn.close()
        os.chdir(self.backup_cwd) # restore original working directory

    def write_table_structure(self):
        create_taxi_str = """
            CREATE TABLE IF NOT EXISTS taxis (
                id integer PRIMARY KEY,
                pool_name text REFERENCES pools (name),
                time_limit real,
                node_limit integer,
                time_last_submitted real,
                status text
            )"""
        create_pool_str = """
            CREATE TABLE IF NOT EXISTS pools (
                name text PRIMARY KEY,
                working_dir text,
                log_dir text
            )"""

        with self.conn:
            self.conn.execute(create_taxi_str)
            self.conn.execute(create_pool_str)

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

    def get_all_taxis_in_pool(self):
        query = """SELECT * FROM taxis;"""
        taxi_raw_info = self.execute_select(query)

        all_taxis = []
        for row in taxi_raw_info:
            row_taxi = self.create_taxi_object(row)
            all_taxis.append(row_taxi)

        return all_taxis

    def get_taxi(self, my_taxi):
        taxi_id = self._taxi_id(my_taxi)

        query = """SELECT * FROM taxis WHERE id == ?"""
        this_taxi = self.create_taxi_object(self.execute_select(query, taxi_id)[0])

        return this_taxi
    
    def update_taxi_status(self, my_taxi, status):
        taxi_id = self._taxi_id(my_taxi)

        update_query = """UPDATE taxis SET status = ? WHERE id = ?"""
        self.execute_update(update_query, status, taxi_id)

        return

    def update_taxi_last_submitted(self, my_taxi, last_submit_time):
        taxi_id = self._taxi_id(my_taxi)
        
        update_query = """UPDATE taxis SET time_last_submitted = ? WHERE id = ?"""
        self.execute_update(update_query, last_submit_time, taxi_id)

        return

    def register_new_taxi(self, my_taxi):
        taxi_id_query = """SELECT id FROM taxis;"""
        all_taxi_ids = map(lambda t: t['id'], self.execute_select(taxi_id_query))

        if len(all_taxi_ids) == 0:
            new_id = 1
        else:
            new_id = max(all_taxi_ids) + 1
        
        my_taxi.id = new_id
        my_taxi.pool_name = self.pool_name

        self._add_taxi_to_pool(my_taxi)

        return my_taxi

    def _add_taxi_to_pool(self, my_taxi):
        """
        Add an already-initialized taxi to the pool.
        To be used by register_new_taxi and potentially
        other helper functions down the road.
        """

        insert_taxi_query = """INSERT OR REPLACE INTO taxis
            (id, pool_name, time_limit, node_limit, time_last_submitted, status)
            VALUES (?, ?, ?, ?, ?, ?)
        """
        self.execute_update(insert_taxi_query, my_taxi.id, my_taxi.pool_name, my_taxi.time_limit, 
            my_taxi.node_limit, my_taxi.time_last_submitted, my_taxi.status)

        return


    def delete_taxi_from_pool(self, my_taxi):
        # Pseudocode:
        # If taxi is a taxi object, extract id.
        # Otherwise, if it's an integer assume it's already an id.
        # Otherwise, complain!
        taxi_id = self._taxi_id(my_taxi)

        remove_query = """DELETE FROM taxis WHERE ID = ?"""

        self.execute_update(remove_query, taxi_id)

        return