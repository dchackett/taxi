#!/usr/bin/env python

import unittest
#import mock
import os
#import sqlite3
import time

import taxi.jobs
import taxi.pool
import taxi.dispatcher
import taxi

from taxi._utility import ensure_path_exists

## Note: 'scalar' localization is required
import taxi.local.local_queue as local_queue
import taxi.local.local_taxi as local_taxi
import taxi.local.sqlite_queue_runner as qrun

## TaskRunner that always fails, for testing purposes
class FailRunner(taxi.jobs.Runner):
    def execute(self):
        raise BaseException

class TestScalarRunTaxiIntegration(unittest.TestCase):
    ## Integration testing: run_taxi using the "scalar" localization and simple tasks.

    def setUp(self):

        if not os.path.exists('./test_run'):
            ensure_path_exists('./test_run')
 
        ## Source/destination for copy test jobs
        self.src_files = ['./test_run/test_ab', './test_run/test_cd', './test_run/test_ef']
        self.dst_files = ['./test_run/test_uv', './test_run/test_wx', './test_run/test_yz']
        
        # Use abspaths
        # TODO: Change test to use relative paths, becomes an implicit test of working dir functionality
        self.src_files = map(taxi.expand_path, self.src_files)
        self.dst_files = map(taxi.expand_path, self.dst_files)

        for i in range(len(self.src_files)):
            with open(self.src_files[i], 'w') as test_file:
                test_file.write(self.src_files[i])

        ## Set up queue system
        self.my_queue = local_queue.LocalQueue()
        self.squeue = qrun.SQLiteSerialQueue()

        ## Set up test pool
        self.my_taxi = taxi.Taxi(name='test1', pool_name='test_pool', time_limit=120, nodes=1, cores=1)

        self.pool_path = './test_run/test-pool.sqlite'
        self.pool_wd = './test_run/pool'
        self.pool_ld = './test_run/pool/log'
        self.my_pool = taxi.pool.SQLitePool(self.pool_path, 'test_pool', self.pool_wd, self.pool_ld)

        ## Set up test dispatch
        self.disp_path = './test_run/test-disp.sqlite'
        self.my_disp = taxi.dispatcher.SQLiteDispatcher(self.disp_path)

        with self.my_disp:
            pass
        
        ## Registration
        with self.my_pool:
            self.my_pool.register_taxi(self.my_taxi)
            self.my_disp.register_taxi(self.my_taxi, self.my_pool)


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
            self.my_pool.register_taxi(self.my_taxi)
            pool_stat = self.my_pool.get_all_taxis_in_pool()

        self.assertEqual(len(pool_stat), 1)

    ## Test 'die' job
    def test_die(self):
        test_job = taxi.jobs.Die(message="Testing", for_taxi=self.my_taxi.name)
        with self.my_disp:
            task_blob = self.my_disp.get_task_blob(self.my_taxi)
            self.assertEqual(task_blob, None)

            self.my_disp.initialize_new_job_pool([test_job])
            
            task_blob = self.my_disp.get_task_blob(self.my_taxi)
            self.assertEqual(len(task_blob.keys()), 1)

        with self.my_pool:
            self.my_pool.submit_taxi_to_queue(self.my_taxi, self.my_queue)

        queue_stat = self.my_queue.report_taxi_status(self.my_taxi)
        self.assertEqual(queue_stat['status'], 'Q')
        self.assertEqual(queue_stat['running_time'], None)

        ## Manually trigger the queue - wouldn't be needed with a real queueing system
        with self.squeue:
            self.squeue.run_next_job()
            
        ## Check the aftermath; first, queue should be empty now
        queue_stat = self.my_queue.report_taxi_status(self.my_taxi)
        self.assertEqual(queue_stat['status'], 'X')


        ## Second, taxi should be held and show recent execution
        with self.my_pool:
            pool_stat = self.my_pool.get_all_taxis_in_pool()
            self.assertEqual(len(pool_stat), 1)
            self.assertEqual(pool_stat[0].status, 'H')
            self.assertLess(time.time() - pool_stat[0].time_last_submitted, 60.0)


        ## Third, 'die' task should be complete
        with self.my_disp:
            task_blob = self.my_disp.get_task_blob(self.my_taxi, include_complete=True)
            self.assertEqual(task_blob[1].task_type, 'Die')
            self.assertTrue(task_blob[1], taxi.jobs.Die)
            self.assertEqual(task_blob[1].status, 'complete')
            self.assertGreater(task_blob[1].run_time, 0.0)
            self.assertEqual(task_blob[1].by_taxi, 'test1')



    ## Test taxi hold by user
    def test_user_hold(self):
        test_job = taxi.jobs.Copy(self.src_files[0], self.dst_files[0])
        with self.my_disp:
            self.my_disp.initialize_new_job_pool([test_job])
        
        with self.my_pool:
            self.my_pool.update_taxi_status(self.my_taxi, 'H')

        with self.my_pool:
            self.my_pool.submit_taxi_to_queue(self.my_taxi, self.my_queue)

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
            self.assertEqual(task_blob[1].status, 'pending')
            self.assertEqual(task_blob[1].run_time, -1.0)

    ## Test single copy job, round-trip
    def test_single_copy(self):

        ## Target file shouldn't exist yet
        files = os.listdir('./test_run')
        self.assertFalse(self.dst_files[0].split('/')[-1] in files)

        test_job = taxi.jobs.Copy(self.src_files[0], self.dst_files[0])

        with self.my_disp:
            self.my_disp.initialize_new_job_pool([test_job])

        with self.my_pool:
            self.my_pool.submit_taxi_to_queue(self.my_taxi, self.my_queue)

        with self.squeue:
            self.squeue.run_next_job()

        ## Check for successful copy
        files = os.listdir('./test_run')
        self.assertTrue(self.dst_files[0].split('/')[-1] in files)

        ## Check for correct pool status
        with self.my_pool:
            self.my_pool.update_all_taxis_queue_status(self.my_queue)
            pool_stat = self.my_pool.get_all_taxis_in_pool()
            self.assertEqual(pool_stat[0].status, 'H')
            self.assertLess(time.time() - pool_stat[0].time_last_submitted, 60.0)

        ## Check for correct dispatcher status
        with self.my_disp:
            task_blob = self.my_disp.get_task_blob(self.my_taxi, include_complete=True)
            self.assertEqual(len(task_blob), 1)
            self.assertEqual(task_blob.values()[0].status, 'complete')
            self.assertEqual(task_blob.values()[0].task_type, 'Copy')


    ## Test multiple copy jobs, with dependency
    def test_copy_with_dependency(self):

        files = os.listdir('./test_run')
        for i in range(3):
            self.assertFalse(self.dst_files[i].split('/')[-1] in files)

        job_pool = []
        for i in range(3):
            job_pool.append(taxi.jobs.Copy(self.src_files[i], self.dst_files[i]))

        job_pool[1].depends_on = [job_pool[0]]
        job_pool[2].depends_on = [job_pool[0], job_pool[1]]
        job_pool.reverse()  ## Reversing so the execution order doesn't trivially match the dependency

        with self.my_disp:
            self.my_disp.initialize_new_job_pool(job_pool)
            task_blob = self.my_disp.get_task_blob(self.my_taxi)
        
        with self.my_pool:
            self.my_pool.submit_taxi_to_queue(self.my_taxi, self.my_queue)

        with self.squeue:
            self.squeue.run_next_job()

        ## Check for successful copy of both files
        files = os.listdir('./test_run')
        for i in range(3):
            self.assertTrue(self.dst_files[i].split('/')[-1] in files)

        ## Check that execution happened in the expected order
        ## TODO: Currently done based on filesystem info; should we store start_time for tasks, as well?
        with self.my_disp:
            task_blob = self.my_disp.get_task_blob(include_complete=True)
            for task in task_blob.values():
                self.assertEqual(task.status, 'complete')

            # Should have run in reverse order, since we reversed the job pool above
            self.assertGreater(task_blob[1].start_time, task_blob[2].start_time)
            self.assertGreater(task_blob[2].start_time, task_blob[3].start_time)





    ## Test re-launch detection for idle taxis
    def test_taxi_relaunch(self):
        # Set up single copy task pool, registered to test1
        test_job = taxi.jobs.Copy(self.src_files[0], self.dst_files[0], for_taxi=self.my_taxi.name)

        with self.my_disp:
            self.my_disp.initialize_new_job_pool([test_job])

        # Add a second taxi to the pool and queue it
        taxi_two = taxi.Taxi(name='test2', pool_name='test_pool', time_limit=120, nodes=1, cores=1)
        with self.my_pool:
            self.my_pool.register_taxi(taxi_two)
            self.my_disp.register_taxi(taxi_two, self.my_pool)
            self.my_pool.submit_taxi_to_queue(taxi_two, self.my_queue)

        with self.my_pool:
            pool_stat = self.my_pool.get_all_taxis_in_pool()

        # Run queue with the second, jobless taxi
        with self.squeue:
            self.squeue.run_next_job()

        # After one step, the second taxi should be held, the first still idle
        with self.my_pool:
            pool_stat = self.my_pool.get_all_taxis_in_pool()
            self.assertEqual(pool_stat[0].status, 'I')
            self.assertEqual(pool_stat[1].status, 'H')

        # The first taxi should now be queued into the 'batch' system
        qstat = self.my_queue.report_taxi_status(self.my_taxi)
        self.assertEqual(qstat['job_number'], 2)

        # And the copy task should still be unfinished
        with self.my_disp:
            task_blob = self.my_disp.get_task_blob(self.my_taxi, include_complete=False)
            self.assertEqual(len(task_blob), 1)

        # Step into the queue one more time
        with self.squeue:
            self.squeue.run_next_job()

        # Now both taxis should be held
        with self.my_pool:
            pool_stat = self.my_pool.get_all_taxis_in_pool()
            self.assertEqual(pool_stat[0].status, 'H')
            self.assertEqual(pool_stat[1].status, 'H')

        # The task should be finished
        with self.my_disp:
            task_blob = self.my_disp.get_task_blob(self.my_taxi, include_complete=True)
            self.assertEqual(len(task_blob), 1)
            self.assertEqual(task_blob[1].status, 'complete')

        # And the destination file should exist
        files = os.listdir('./test_run')
        self.assertTrue(self.dst_files[0].split('/')[-1] in files)

    ## Test work completion (no tasks pending)

    ## Test task failure

    ## Test attempting to run a previously failed task

    ## Test attempting to run an unresolved dependency

    ## Test attempting to run with insufficient time to complete




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