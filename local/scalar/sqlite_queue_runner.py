#!/usr/bin/env python

## For interaction with a real queue backend, we just need local_queue.
## Here we're mocking up a queue with simple, serial behavior, so we need a way to actually run it!

import sqlite3
import os

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
        queued_list = self.conn.query("""SELECT * FROM queue WHERE status = 'Q' ORDER BY id ASC;""").fetchall()

        os.system("taxi.sh " + queued_list[0]['taxi_args'])


