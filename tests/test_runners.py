#!/usr/bin/env python

import unittest
import mock
import os
import sqlite3
import json

from taxi.jobs import *

class TestBaseTaskRunner(unittest.TestCase):
    
    def test_json_rebuild(self):
        json_one = json.dumps({'task_type': 'CopyRunner', 'task_args': {'src': 'abc', 'dest': 'xyz'} })
        json_two = json.dumps({'task_type': 'CopyRunner', 'task_args': {'src': 'abc', 'dest': 'xyz', 'bad_arg': 4} })
        json_three = json.dumps({'task_type': 'FakeRunner'})

        runner_parser = runner_rebuilder_factory()

        obj_one = json.loads(json_one, object_hook=runner_parser)
        self.assertTrue(isinstance(obj_one, CopyRunner))
        self.assertEqual(obj_one.dest, 'xyz')

        obj_two = json.loads(json_two, object_hook=runner_parser)
        self.assertTrue(isinstance(obj_two, CopyRunner))
        with self.assertRaises(AttributeError):
            fail_assign = obj_two.bad_arg

        obj_three = json.loads(json_three, object_hook=runner_parser)
        self.assertTrue(isinstance(obj_three, dict))


class TestCopyRunner(unittest.TestCase):

    def setUp(self):
        self.src = 'test_file_abc'
        self.dest = 'test_file_xyz'

        self.runner_parser = runner_rebuilder_factory()

        os.system("touch {}".format(self.src))

    def tearDown(self):
        os.remove(self.src)
        os.remove(self.dest)

    def test_copy(self):
        json_copy = json.dumps({'task_type': 'CopyRunner', 'task_args': {'src': self.src, 'dest': self.dest } })

        copy_run = json.loads(json_copy, object_hook=self.runner_parser)
        copy_run.execute(cores=1)

        self.assertTrue(os.path.exists(self.dest))


if __name__ == '__main__':
    suite1 = unittest.TestLoader().loadTestsFromTestCase(TestBaseTaskRunner)
    suite2 = unittest.TestLoader().loadTestsFromTestCase(TestCopyRunner)

    all_tests = unittest.TestSuite([suite1, suite2])
    unittest.TextTestRunner(verbosity=2).run(all_tests)
