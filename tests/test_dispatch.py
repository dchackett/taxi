#!/usr/bin/env python

import unittest
import mock
import os
import sqlite3

from dispatcher import *
from jobs import *

class TestSQLiteBase(unittest.TestCase):
    def setUp(self):
        self.test_filename = './tests/db_test_dispatch.sqlite'
        self.test_dispatch = SQLiteDispatcher(self.test_filename)

    def tearDown(self):
#        pass
        os.unlink(self.test_filename)
    


class TestSQLiteEmptyDispatch(TestSQLiteBase):

    def setUp(self):
        super(TestSQLiteEmptyDispatch, self).setUp()

    def tearDown(self):
        super(TestSQLiteEmptyDispatch, self).tearDown()

    def test_initialize(self):
        with self.test_dispatch:

            empty_query = self.test_dispatch.conn.execute("""SELECT id, task_type, task_args, priority FROM tasks""").fetchall()
            self.assertEqual(empty_query, [])

            with self.assertRaises(sqlite3.OperationalError):
                self.test_dispatch.conn.execute("""SELECT fake_column FROM taxis""")

            
    def test_populate_task_pool(self):
        test_job = Job(req_time=33)
        test_job_two = Job(req_time=44)
        
        test_job.trunk = True
        test_job.status = 'complete'
        test_job_two.is_recurring = True

        test_job_pool = [test_job, test_job_two]

        with self.test_dispatch:
            self.test_dispatch.initialize_new_job_pool(test_job_pool)

            task_blob = self.test_dispatch.get_task_blob(None, include_complete=True)

            read_task = task_blob[0]
            read_task_two = task_blob[1]

            self.assertEqual(read_task['status'], 'complete')
            self.assertEqual(read_task_two['is_recurring'], True)
            self.assertEqual(read_task_two['status'], test_job_two.status)

            task_blob_no_complete = self.test_dispatch.get_task_blob(None, include_complete=False)

            self.assertEqual(task_blob_no_complete.keys(), [1])


if __name__ == '__main__':
    suite1 = unittest.TestLoader().loadTestsFromTestCase(TestSQLiteEmptyDispatch)

    all_tests = unittest.TestSuite([suite1])
    unittest.TextTestRunner(verbosity=2).run(all_tests)
