#!/usr/bin/env python

## Local queue implementation for "scalar" generic machine
## Serial "queue" based on sqlite; one-taxi limit!

import sqlite3
import os

from batch_queue import *
from taxi import mkdir_p

class LocalQueue(BatchQueue):
    
    def __init__(self):
        self.queue_db_path = os.path.expanduser("~") + "/.taxi/"
        if not (os.path.exists(self.queue_db_path)):
            mkdir_p(self.queue_db_path)

        self.conn = sqlite3.connect(self.queue_db_path + "serial_queue.sqlite3", timeout=30.0)
        self.conn.row_factory = sqlite3.Row

        self._write_table_structure()

    def __del__(self):
        self.conn.close()

    def _delete_queue_db(self):
        os.unlink(self.queue_db_path + "serial_queue.sqlite3")

    def _write_table_structure(self):
        create_queue_str = """
            CREATE TABLE IF NOT EXISTS queue (
                id integer PRIMARY KEY,
                job_name string,
                status string,
                taxi_args string,
                start_time int DEFAULT -1
            )"""
        
        with self.conn:
            self.conn.execute(create_queue_str)

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

    
    def report_taxi_status_by_name(self, taxi_name):
        taxi_status_query = """SELECT id, status, start_time FROM queue
            WHERE job_name LIKE ?;
        """

        res = self.execute_select(taxi_status_query, '%' + taxi_name + '%')

        if (len(res) > 1):
            print "WARNING: non-unique taxi status returned by queue: {} entires".format(len(res))
            print res

        if (len(res) == 0):
            return {
                'status': 'X',
                'job_number': None,
                'running_time': None
            }

        res = res[0]

        if (res['status']) == 'R':
            run_time = time.time() - res['start_time']
        else:
            run_time = None

        return {
            'status': res['status'],
            'job_number': res['id'],
            'running_time': run_time,
        }

    def launch_taxi(self, taxi, pool_path, dispatch_path):
        taxi_launch_query = """INSERT INTO queue
            (job_name, status, taxi_args) VALUES (?, ?, ?);"""

        taxi_dict = taxi.to_dict()
        taxi_dict['pool_path'] = pool_path
        taxi_dict['dispatch_path'] = dispatch_path
        key_arg_list = ['name', 'cores', 'pool_name', 'time_limit', 'dispatch_path', 'pool_path']

        taxi_args = [ "--{} {}".format(k, taxi_dict[k]) for k in key_arg_list ]
        taxi_args_str = " ".join(taxi_args)

        self.execute_update(taxi_launch_query, taxi.name, 'Q', taxi_args_str)


    def cancel_job(self, job_number):
        cancel_query = """DELETE FROM queue
            WHERE id = ?;"""

        self.execute_update(cancel_query, job_number)

    def report_queue_status(self):
        raise NotImplementedError
    





