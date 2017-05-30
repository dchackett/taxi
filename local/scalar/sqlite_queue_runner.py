#!/usr/bin/env python

## For interaction with a real queue backend, we just need local_queue.
## Here we're mocking up a queue with simple, serial behavior, so we need a way to actually run it!

import sqlite3
import os
import subprocess

class SQLiteSerialQueue:

    def __init__(self):
        self.db_path = os.path.expanduser("~") + "/.taxi/serial_queue.sqlite3"

    def __enter__(self):
        self.conn = sqlite3.connect(self.db_path, timeout=30.0)
        self.conn.row_factory = sqlite3.Row # Row factory for return-as-dict

    def __exit__(self, exc_type, exc_val, exc_traceback):
        self.conn.close()


    def count_queued(self):
        """
        Return number of jobs that are currently queued.
        """
        raise NotImplementedError

    def run_next_job(self):
        """
        Run the first available job in the queue.
        """
        queued_list = self.conn.execute("""SELECT * FROM queue WHERE status = 'Q' ORDER BY id ASC;""").fetchall()
        if len(queued_list) == 0:
            print "Warning: attempted to execute with no jobs in queue!"
            return

        taxi_call = "./taxi.sh " + queued_list[0]['taxi_args']
        batch_out, batch_err = subprocess.Popen(taxi_call, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True).communicate()

        print "BO: ", batch_out
        print "BE: ", batch_err

        self.conn.execute("""DELETE FROM queue WHERE id = ?;""", (queued_list[0]['id'],))
        self.conn.commit()

