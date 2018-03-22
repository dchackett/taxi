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


class TestSQLiteDispatchDelete(TestSQLiteBase):

    def setUp(self):
        super(TestSQLiteDispatchDelete, self).setUp()
        self.test_dump_filename = './tests/db_dump_dispatch.sqlite'

    def tearDown(self):
        super(TestSQLiteDispatchDelete, self).tearDown()
        os.unlink(self.test_dump_filename)

    def test_delete(self):
        test_task = Task()
        with self.test_dispatch:
            # Write test task to dispatch
            self.test_dispatch.initialize_new_task_pool([test_task])
            
            # Find list of tasks in dispatch, delete all of them
            task_blob = self.test_dispatch.get_all_tasks()
            self.assertEqual(len(task_blob), 1)
            self.test_dispatch.delete_tasks(task_blob)
            
            # Make sure deletion has occurred
            task_blob = self.test_dispatch.get_all_tasks()
            self.assertEqual(len(task_blob), 0)

    def test_trim(self):
        # Make a test stream of Tasks
        test_pool1 = [Task() for t in range(5)]
        for tt in range(1, len(test_pool1)):
            test_pool1[tt].depends_on = [test_pool1[tt-1]]
            
        # Branch a second test stream off the first
        test_pool2 = [Task() for t in range(5)]
        for tt in range(1, len(test_pool2)):
            test_pool2[tt].depends_on = [test_pool2[tt-1]]
        test_pool2[0].depends_on = [test_pool1[2]] # branch off from third task
        
        # Put in correct structure for branch finder; assign ids so we know what to look for
        test_pool1[0].branch_root = True
        test_pool2[0].branch_root = True
        for tt, t in enumerate(test_pool1 + test_pool2):
            t.id = tt
            t.trunk = True
            
        # Mark branch in test_pool1 as complete
        for t in test_pool1:
            t.status = 'complete'
            
        with self.test_dispatch:
            # Write test pool to dispatch
            self.test_dispatch.initialize_new_task_pool(test_pool1 + test_pool2)
            
            # Make sure all desired tasks are present initially
            tb = self.test_dispatch.get_all_tasks()
            self.assertEqual(sorted([t.id for t in test_pool1 + test_pool2]), sorted(tb.keys()))
            
            # Call trimmer - should remove test_pool2, dumping tasks to provided dump DB path
            self.test_dispatch.trim_completed_branches(dump_dispatch=self.test_dump_filename)
            
            # Check that only test_pool1 tasks are still present
            tb = self.test_dispatch.get_all_tasks()
            self.assertEqual(sorted([t.id for t in test_pool2]), sorted(tb.keys()))
            
        # Check that tasks in test_pool2 were moved to dump dispatch
        dump_dispatch = SQLiteDispatcher(self.test_dump_filename)
        with dump_dispatch:
            tb = dump_dispatch.get_all_tasks()
            self.assertEqual(sorted([t.id for t in test_pool1]), sorted(tb.keys()))
            
        # Now, mark test_pool2 complete and trim
        for t in test_pool2:
            t.status = 'complete'
            
        with self.test_dispatch:
            # Mark tasks in test_pool1 as completed
            self.test_dispatch.write_tasks(test_pool2)
            
            tb = self.test_dispatch.get_all_tasks()
            for t in tb.values():
                print t, t.status, t.depends_on
            
            # Call trimmer - should remove test_pool1, deleting tasks (because no dump DB provided)
            self.test_dispatch.trim_completed_branches()
            
            # Check that no tasks are left
            tb = self.test_dispatch.get_all_tasks()
            self.assertEqual(len(tb), 0)
            
            

if __name__ == '__main__':
    suite1 = unittest.TestLoader().loadTestsFromTestCase(TestSQLiteEmptyDispatch)

    all_tests = unittest.TestSuite([suite1])
    unittest.TextTestRunner(verbosity=2).run(all_tests)
