#!/usr/bin/env python

import unittest
import mock
import os
import sqlite3

import task_runners
import jobs
import pool
import dispatcher
import taxi

import local.scalar.local_queue as local_queue
import local.scalar.local_taxi as local_taxi
import local.scalar.sqlite_queue_runner as qrun

## TaskRunner that always fails, for testing purposes
class FailRunner(task_runners.TaskRunner):
    def execute(self):
        raise BaseException



class TestScalarRunTaxiIntegration(unittest.TestCase):
    ## Integration testing: run_taxi using the "scalar" localization and simple tasks.

    def setUp(self):

        if not os.path.exists('./test_run'):
            taxi.mkdir_p('./test_run')
 
        ## Source/destination for copy test jobs
        self.src_files = ['./test_run/test_ab', './test_run/test_cd', './test_run/test_ef']
        self.dst_files = ['./test_run/test_uv', './test_run/test_wx', './test_run/test_yz']

        for i in range(len(self.src_files)):
            with open(self.src_files[i], 'w') as test_file:
                test_file.write(self.src_files[i])

        ## Set up queue system
        self.my_queue = local_queue.LocalQueue()

        ## Set up test pool
        self.my_taxi = taxi.Taxi('test1', 'test_pool', 60, 1)

        self.pool_path = './test_run/test-pool.sqlite'
        self.pool_wd = './test_run/pool'
        self.pool_ld = './test_run/pool/log'
        self.my_pool = pool.SQLitePool(self.pool_path, 'test_pool', self.pool_wd, self.pool_ld)

        with self.my_pool:
            self.my_pool.register_new_taxi(self.my_taxi)

        ## Set up test dispatch
        self.disp_path = './test_run/test-disp.sqlite'
        self.my_disp = dispatcher.SQLiteDispatcher(self.disp_path)

        init_job_pool = [ jobs.CopyJob(self.src_files[0], self.dst_files[0]) ]

        with self.my_disp:
            self.my_disp.initialize_new_job_pool(init_job_pool)



    def tearDown(self):
        ## Remove test files
#        os.unlink('./test_run/')
        pass

    ## Test initialization
    def test_pool_init(self):
        with self.my_pool:
            all_taxis = self.my_pool.get_all_taxis_in_pool()
            self.assertEqual(len(all_taxis), 1)
            self.assertEqual(all_taxis[0], self.my_taxi)


    ## Test 'die' job

    ## Test taxi hold

    ## Test single copy job, round-trip

    ## Test two copy jobs, with dependency

    ## Test re-launch detection after job

    ## Test work completion (no tasks pending)

    ## Test task failure

    ## Test attempting to run a previously failed task

    ## Test attempting to run an unresolved dependency




if __name__ == '__main__':
    suite1 = unittest.TestLoader().loadTestsFromTestCase(TestScalarRunTaxiIntegration)

    all_tests = unittest.TestSuite([suite1])
    unittest.TextTestRunner(verbosity=2).run(all_tests)
