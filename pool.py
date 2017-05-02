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

    def __init__(self, batch_queue, work_dir):
        self.batch_queue = batch_queue
        self.work_dir = work_dir

    ### Backend interaction ###

    def get_all_taxis_in_pool(self):
        raise NotImplementedError

    def get_taxi_status(self, taxi_id):
        raise NotImplementedError
    
    def update_taxi_status(self, taxi_id, status):
        raise NotImplementedError

    def update_taxi_last_submitted(self, taxi_id, last_submit_time):
        raise NotImplementedError

    def add_taxi_to_pool(self, taxi):
        raise NotImplementedError

    def delete_taxi_from_pool(self, taxi):
        # Pseudocode:
        # If taxi is a taxi object, extract id.
        # Otherwise, if it's an integer assume it's already an id.
        # Otherwise, complain!

        raise NotImplementedError

    def delete_taxi_id_from_pool(self, taxi_id):
        raise NotImplementedError

    ### Queue interaction ###

    def check_for_thrashing(self, taxi):
        raise NotImplementedError

    def submit_taxi_to_queue(self, taxi):
        # Don't submit hold/error status taxis
        taxi_status = self.get_taxi_status(taxi.id)
        if (taxi_status in ('H', 'E')):
            print "Warning: did not submit taxi {} due to status flag {}.".format(taxi, taxi_status)
            return

        if (self.check_for_thrashing(taxi)):
            # Put taxi on hold to prevent thrashing
            print "Thrashing detected for taxi {}; set to hold.".format(taxi)
            self.update_taxi_status(taxi.id, 'H')
            return

        try:
            self.batch_queue.launch_taxi(taxi)
        except:
            ## TO-DO: add some error handling here
            self.update_taxi_status(taxi.id, 'E')
            raise

        self.update_taxi_last_submitted(taxi, time.time())

    def remove_taxi_from_queue(self, taxi):
        """
        Remove all jobs from the queue associated with the given taxi.
        """
        taxi_status = self.batch_queue.report_taxi_status(taxi)

        for job in taxi_status['job_numbers']:
            self.batch_queue.cancel_job(job)

        return


    ### Control logic ###

    def update_all_taxis_queue_status(self):
        taxi_list = self.get_all_taxis_in_pool()
        for taxi in taxi_list:
            queue_status = self.batch_queue.report_taxi_status(taxi)
            if queue_status in ('Q', 'R'):
                self.update_taxi_status(taxi.id, queue_status)
            elif queue_status == 'X':
                pool_status = self.get_taxi_status(taxi.id)
                if pool_status in ('E', 'H'):
                    continue
                else:
                    self.update_taxi_status(taxi.id, 'I')
            else:
                print "Invalid queue status code, {}".format(queue_status)
                raise BaseException

    def spawn_idle_taxis(self):
        taxi_list = self.get_all_taxis_in_pool()
        for taxi in taxi_list:
            pool_status = self.get_taxi_status(taxi.id)
            if pool_status == 'I':
                self.submit_taxi_to_queue(taxi)
    




import sqlite3
    
    
class SQLitePool(Pool):
    """
    Concrete implementation of the Pool class using an SQLite backend.
    """

    def __init__(self, batch_queue, db_path, work_dir):
        self.batch_queue = batch_queue
        self.db_path = db_path
        self.work_dir = work_dir

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

    def __exit__(self, exc_type, exc_val, exc_traceback):
        self.conn.close()
        os.chdir(self.backup_cwd) # restore original working directory
        


    ## Add destructor when I remember the magic syntax

    def create_taxi_object(self, db_row):
        """
        Interface to translate Taxi representation in the DB to a Taxi object.
        """
        pass

    def execute_select(self, query, *query_args):
        try:
            res = self.conn.execute(query, query_args).fetchall()
        except:
            raise

        return res

    def execute_update(self, query, *query_args):
        try:
            self.conn.execute(query, query_args)
        except:
            raise

        return

    def get_all_taxis_in_pool(self):
        query = """SELECT * FROM taxis;"""
        taxi_raw_info = self.execute_select(query)

        all_taxis = []
        for row in taxi_raw_info:
            taxi = create_taxi_object(row)
            all_taxis.append(taxi)

        return all_taxis

    def get_taxi_status(self, taxi_id):
        pass


