#!/usr/bin/env python

import unittest
import mock
import os
import sqlite3
import json

from taxi.dispatcher import *
from taxi.tasks import *

class TestSQLiteBase(unittest.TestCase):
    def setUp(self):
        self.test_filename = './tests/db_test_dispatch.sqlite'
        self.test_dispatch = SQLiteDispatcher(self.test_filename)

    def tearDown(self):
        os.unlink(self.test_filename)
    


class TestSQLiteEmptyDispatch(TestSQLiteBase):

    def setUp(self):
        super(TestSQLiteEmptyDispatch, self).setUp()

    def tearDown(self):
        super(TestSQLiteEmptyDispatch, self).tearDown()

    def test_initialize(self):
        with self.test_dispatch:

            empty_query = self.test_dispatch.conn.execute("""SELECT id FROM tasks""").fetchall()
            self.assertEqual(empty_query, [])

            with self.assertRaises(sqlite3.OperationalError):
                self.test_dispatch.conn.execute("""SELECT fake_column FROM taxis""")

    def test_json_rebuild(self):
        json_one = {'task_type': 'Copy', 'src': 'abc', 'dest': 'xyz' }
        json_two = {'task_type': 'Copy', 'src': 'abc', 'dest': 'xyz', 'bad_arg': 4 }
        json_three = {'task_type': 'FakeRunner'}

        obj_one = self.test_dispatch.rebuild_json_task(json_one)
        self.assertTrue(isinstance(obj_one, Copy))
        self.assertEqual(obj_one.src, 'abc')
        self.assertEqual(obj_one.dest, 'xyz')

        obj_two = self.test_dispatch.rebuild_json_task(json_two)
        self.assertTrue(isinstance(obj_two, Copy))
        self.assertEqual(obj_two.src, 'abc')
        self.assertEqual(obj_two.dest, 'xyz')
        
        with self.assertRaises(TypeError):
            obj_three = self.test_dispatch.rebuild_json_task(json_three)
        
    def test_populate_task_pool(self):
        test_task = Task(req_time=33)
        test_task_two = Task(req_time=44)
        
        test_task.trunk = True
        test_task.status = 'complete'
        test_task_two.is_recurring = True

        test_task_pool = [test_task, test_task_two]

        with self.test_dispatch:
            self.test_dispatch.initialize_new_task_pool(test_task_pool)

            task_blob = self.test_dispatch.get_all_tasks(None, include_complete=True)
            read_task = task_blob[1]
            read_task_two = task_blob[2]

            self.assertEqual(read_task.status, 'complete')
            self.assertEqual(read_task_two.is_recurring, True)
            self.assertEqual(read_task_two.status, test_task_two.status)

            task_blob_no_complete = self.test_dispatch.get_all_tasks(None, include_complete=False)

            self.assertEqual(task_blob_no_complete.keys(), [2])


if __name__ == '__main__':
    suite1 = unittest.TestLoader().loadTestsFromTestCase(TestSQLiteEmptyDispatch)

    all_tests = unittest.TestSuite([suite1])
    unittest.TextTestRunner(verbosity=2).run(all_tests)
