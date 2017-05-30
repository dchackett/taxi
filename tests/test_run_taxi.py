#!/usr/bin/env python

import unittest
import mock
import os
import sqlite3
import time

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
        self.squeue = qrun.SQLiteSerialQueue()

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

        # init_job_pool = [ jobs.CopyJob(self.src_files[0], self.dst_files[0]) ]

        # with self.my_disp:
        #     self.my_disp.initialize_new_job_pool(init_job_pool)

        with self.my_disp:
            pass


    def tearDown(self):
        ## Remove test files
        os.unlink(self.pool_path)
        os.unlink(self.disp_path)

        for f in self.src_files:
            if os.path.exists(f):
                os.unlink(f)
        
        for f in self.dst_files:
            if os.path.exists(f):
                os.unlink(f)

        self.my_queue._delete_queue_db()

    ## Test initialization
    def test_pool_init(self):
        with self.my_pool:
            all_taxis = self.my_pool.get_all_taxis_in_pool()
            self.assertEqual(len(all_taxis), 1)
            self.assertEqual(all_taxis[0], self.my_taxi)

    def test_reregister_taxi(self):
        with self.my_pool:
            with self.assertRaises(RuntimeError):
                self.my_pool.register_new_taxi(self.my_taxi)

    ## Test 'die' job
    def test_die(self):
        test_job = jobs.DieJob(for_taxi=self.my_taxi.taxi_name)
        with self.my_disp:
            task_blob = self.my_disp.get_task_blob(self.my_taxi)
            self.assertEqual(task_blob, None)

            self.my_disp.initialize_new_job_pool([test_job])

            task_blob = self.my_disp.get_task_blob(self.my_taxi)
            self.assertEqual(len(task_blob.keys()), 1)

        with self.my_pool:
            self.my_pool.submit_taxi_to_queue(self.my_taxi, self.my_queue, 
                pool_path=self.pool_path, dispatch_path=self.disp_path)

        queue_stat = self.my_queue.report_taxi_status(self.my_taxi)
        self.assertEqual(queue_stat['status'], 'Q')
        self.assertEqual(queue_stat['running_time'], None)


        ## Check the aftermath; first, queue should be empty now
        queue_stat = self.my_queue.report_taxi_status(self.my_taxi)
        print "QSTAT: ", queue_stat

        ## Manually trigger the queue - wouldn't be needed with a real queueing system
        with self.squeue:
            self.squeue.run_next_job()
            
        ## Check the aftermath; first, queue should be empty now
        queue_stat = self.my_queue.report_taxi_status(self.my_taxi)
        print "QSTAT: ", queue_stat
        self.assertEqual(queue_stat['status'], None)


        ## Second, taxi should be held and show recent execution
        with self.my_pool:
            pool_stat = self.my_pool.get_all_taxis_in_pool()
            print "PSTAT: ", pool_stat
            self.assertEqual(len(pool_stat), 1)
            self.assertEqual(pool_stat[0].status, 'H')
            self.assertLess(pool_stat[0].time_last_submitted - time.time(), 60.0)


        ## Third, 'die' task should be complete
        with self.my_disp:
            task_blob = self.my_disp.get_task_blob(self.my_taxi, include_complete=True)
            print "TB: ", task_blob
            self.assertEqual(task_blob[1]['task_type'], 'die')
            self.assertEqual(task_blob[1]['status'], 'complete')
            self.assertGreater(task_blob[1]['run_time'], 0.0)
            self.assertEqual(task_blob[1]['by_taxi'], 'test1')



    ## Test taxi hold by user
    def test_user_hold(self):
        test_job = jobs.CopyJob(self.src_files[0], self.dst_files[0])
        with self.my_disp:
            self.my_disp.initialize_new_job_pool([test_job])
        
        with self.my_pool:
            self.my_pool.update_taxi_status(self.my_taxi, 'H')

        with self.my_pool:
            self.my_pool.submit_taxi_to_queue(self.my_taxi, self.my_queue,
                pool_path=self.pool_path, dispatch_path=self.disp_path)

            self.assertEqual(self.my_pool.get_taxi(self.my_taxi).status, 'H')

        ## Manual queue trigger
        with self.squeue:
            self.squeue.run_next_job()

        ## With one taxi on hold, nothing should have happened
        with self.my_pool:
            pool_stat = self.my_pool.get_all_taxis_in_pool()
            self.assertEqual(pool_stat[0].status, 'H')
            self.assertEqual(pool_stat[0].time_last_submitted, None)
        
        with self.my_disp:
            task_blob = self.my_disp.get_task_blob(self.my_taxi)
            self.assertEqual(task_blob[1]['status'], 'pending')
            self.assertEqual(task_blob[1]['run_time'], -1.0)

    ## Test single copy job, round-trip
    def test_single_copy(self):
        pass

    ## Test two copy jobs, with dependency

    ## Test re-launch detection after job

    ## Test work completion (no tasks pending)

    ## Test task failure

    ## Test attempting to run a previously failed task

    ## Test attempting to run an unresolved dependency




if __name__ == '__main__':
    local_files = ['taxi.sh', 'local_taxi.py', 'local_queue.py']

    for lf in local_files:
        if not (os.path.lexists('./' + lf)):
            os.symlink('./local/scalar/' + lf, './' + lf)

    suite1 = unittest.TestLoader().loadTestsFromTestCase(TestScalarRunTaxiIntegration)

    all_tests = unittest.TestSuite([suite1])
    unittest.TextTestRunner(verbosity=2).run(all_tests)

    for lf in local_files:
        try:
            os.unlink('./' + lf)
        except:
            continue