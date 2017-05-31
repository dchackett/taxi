#!/usr/bin/env python

import unittest
import mock
import os
import sqlite3

from taxi.pool import *
from taxi.batch_queue import *

class TestSQLiteBase(unittest.TestCase):
    def setUp(self):
        self.test_filename = './tests/db_test_pool.sqlite'
        test_pool_name = 'test_pool'
        test_work_dir = './tests/work/'
        test_log_dir = './tests/log/'

        self.test_pool = SQLitePool(self.test_filename, test_pool_name, test_work_dir, test_log_dir)

    def tearDown(self):
#        pass
        os.unlink(self.test_filename)
    

class TestSQLiteEmptyPool(TestSQLiteBase):

    def setUp(self):
        super(TestSQLiteEmptyPool, self).setUp()

    def tearDown(self):
        super(TestSQLiteEmptyPool, self).tearDown()

    def test_write_tables(self):
        with self.test_pool:
            empty_taxis = self.test_pool.conn.execute("""SELECT name, pool_name, time_limit, time_last_submitted, 
                status, cores FROM taxis""").fetchall()
            pools = map(dict, self.test_pool.conn.execute("""SELECT name, working_dir, log_dir FROM pools""").fetchall())

            self.assertItemsEqual(empty_taxis, [])
            self.assertEqual(len(pools), 1)
            self.assertEqual(pools[0]['name'], 'test_pool')

            with self.assertRaises(sqlite3.OperationalError):
                self.test_pool.conn.execute("""SELECT fake_column FROM taxis""")

    def test_read_write_taxi(self):
        with self.test_pool:
            taxi_one = taxi.Taxi(time_limit=4000, cores=8, name="one")
            self.test_pool.register_taxi(taxi_one)

            taxi_list = self.test_pool.get_all_taxis_in_pool()
            self.assertEqual(len(taxi_list), 1)

            taxi_one_return = taxi_list[0]

            self.assertEqual(taxi_one_return.name, "one")
            self.assertEqual(taxi_one_return.time_limit, taxi_one.time_limit)
            self.assertEqual(taxi_one_return.cores, taxi_one.cores)
            self.assertEqual(taxi_one_return.time_last_submitted, None)
            self.assertEqual(taxi_one_return.status, 'I')

    def test_create_second_pool(self):
        second_pool_name = 'pool2'
        second_work_dir = './tests/work/'
        second_log_dir = './tests/log/'
        
        second_pool = SQLitePool(self.test_filename, second_pool_name, second_work_dir, second_log_dir)
        with self.test_pool:
            pass

        with second_pool:
            pools = map(dict, second_pool.conn.execute("""SELECT name, working_dir, log_dir FROM pools""").fetchall())
            self.assertEqual(len(pools), 2)
            self.assertEqual(pools[1]['name'], 'pool2')

    def test_read_log_dirs(self):
        dupe_pool_obj = SQLitePool(self.test_filename, 'test_pool')

        with self.test_pool:
            pass

        with dupe_pool_obj:
            self.assertEqual(dupe_pool_obj.work_dir, './tests/work/')
            self.assertEqual(dupe_pool_obj.log_dir, './tests/log/')


class TestSQLitePoolWithTaxis(TestSQLiteBase):

    def setUp(self):
        super(TestSQLitePoolWithTaxis, self).setUp()

        with self.test_pool:
            self.taxi_one = taxi.Taxi(time_limit=4000, cores=8, name="one")
            self.taxi_two = taxi.Taxi(time_limit=9999, cores=1, name="two")

            self.test_pool.register_taxi(self.taxi_one)
            self.test_pool.register_taxi(self.taxi_two)


    def tearDown(self):
        super(TestSQLitePoolWithTaxis, self).tearDown()

    def test_read_write_two_taxis(self):
        with self.test_pool:
            taxi_list = self.test_pool.get_all_taxis_in_pool()
            self.assertEqual(len(taxi_list), 2)

            taxi_two_return = taxi_list[1]

            self.assertEqual(taxi_two_return.name, "two")
            self.assertEqual(taxi_two_return.time_limit, self.taxi_two.time_limit)

            taxi_one_return = self.test_pool.get_taxi("one")

            self.assertEqual(taxi_one_return.name, "one")
            self.assertEqual(taxi_one_return.time_limit, self.taxi_one.time_limit)

    def test_get_taxi_by_object_or_name(self):
        with self.test_pool:
            taxi_one_return = self.test_pool.get_taxi(self.taxi_one)
            self.assertEqual(taxi_one_return.time_limit, self.taxi_one.time_limit)

            taxi_two_return = self.test_pool.get_taxi(self.taxi_two.name)
            self.assertEqual(taxi_two_return.time_limit, self.taxi_two.time_limit)

    def test_update_status_and_last_submit(self):
        with self.test_pool:
            self.test_pool.update_taxi_status(self.taxi_one, 'Q')
            taxi_one_return = self.test_pool.get_taxi(self.taxi_one)
            self.assertEqual(taxi_one_return.status, 'Q')

            self.test_pool.update_taxi_last_submitted(self.taxi_two, 13985738.5)
            taxi_two_return = self.test_pool.get_taxi(self.taxi_two.name)
            self.assertEqual(taxi_two_return.time_last_submitted, 13985738.5)


    def test_delete_taxi_from_pool(self):
        with self.test_pool:
            self.test_pool.delete_taxi_from_pool(self.taxi_one)

            all_taxis = self.test_pool.get_all_taxis_in_pool()

            self.assertEqual(len(all_taxis), 1)
            self.assertEqual(all_taxis[0].time_limit, self.taxi_two.time_limit)
            
class TestSQLitePoolQueueInteraction(TestSQLiteBase):

    def setUp(self):
        super(TestSQLitePoolQueueInteraction, self).setUp()

        with self.test_pool:
            self.taxi_one = taxi.Taxi(time_limit=4000., cores=8, name="one")
            self.taxi_two = taxi.Taxi(time_limit=9999., cores=1, name="two")
            self.taxi_three = taxi.Taxi(time_limit=4040., cores=8, name="three")

            self.test_pool.register_taxi(self.taxi_one)
            self.test_pool.register_taxi(self.taxi_two)
            self.test_pool.register_taxi(self.taxi_three)

            self.test_pool.update_taxi_status(self.taxi_three, 'H')

    def tearDown(self):
        super(TestSQLitePoolQueueInteraction, self).tearDown()

    def test_spawn_idle_taxis(self):
        my_queue = BatchQueue()
        my_queue.launch_taxi = mock.MagicMock()
    
        with self.test_pool:
            self.test_pool.spawn_idle_taxis(my_queue)

            taxi_one_return = self.test_pool.get_taxi(self.taxi_one)
            self.assertGreater(taxi_one_return.time_last_submitted, 0.)

        self.assertTrue(my_queue.launch_taxi.call_args_list[0][0][0] == self.taxi_one)
        self.assertTrue(my_queue.launch_taxi.call_args_list[1][0][0] == self.taxi_two)
        self.assertEqual(my_queue.launch_taxi.call_count, 2)  ## Only taxis 1 and 2 should be submitted

    def test_check_thrashing(self):
        with self.test_pool:
            self.test_pool.update_taxi_last_submitted(self.taxi_one, time.time())
            self.test_pool.update_taxi_last_submitted(self.taxi_two, time.time() - 3600.)

            self.assertTrue(self.test_pool.check_for_thrashing(self.taxi_one))
            self.assertFalse(self.test_pool.check_for_thrashing(self.taxi_two))


    def test_submit_to_queue(self):
        my_queue = BatchQueue()
        my_queue.launch_taxi = mock.MagicMock()

        with self.test_pool:
            self.test_pool.update_taxi_last_submitted(self.taxi_two, time.time())
            self.test_pool.update_taxi_status(self.taxi_three, 'I')

            self.test_pool.submit_taxi_to_queue(self.taxi_one, my_queue)
            self.test_pool.submit_taxi_to_queue(self.taxi_two, my_queue)

            self.assertEqual(my_queue.launch_taxi.call_count, 1)
            self.assertEqual(self.test_pool.get_taxi(self.taxi_two).status, 'H')

            my_queue.launch_taxi.side_effect = BaseException()
            self.assertRaises(BaseException, self.test_pool.submit_taxi_to_queue, self.taxi_three, my_queue)

            self.assertEqual(self.test_pool.get_taxi(self.taxi_three).status, 'E')


    def test_delete_from_queue(self):
        my_queue = BatchQueue()
        mock_status = {
            'status': 'R',
            'job_numbers': [123, 456, 789],
            'running_time': [4750.2, 3889.3, 1341.4], 
        }
        mock_status_two = {
            'status': 'X',
            'job_numbers': [],
            'running_time': [],
        }

        my_queue.report_taxi_status = mock.MagicMock(return_value=mock_status)
        my_queue.cancel_job = mock.MagicMock()

        with self.test_pool:
            self.test_pool.remove_taxi_from_queue(self.taxi_one, my_queue)
            self.assertEquals(my_queue.cancel_job.call_args_list, [mock.call(123),mock.call(456),mock.call(789)])

            my_queue.report_taxi_status = mock.MagicMock(return_value=mock_status_two)
            my_queue.cancel_job.call_count = 0

            self.test_pool.remove_taxi_from_queue(self.taxi_two, my_queue)
            self.assertEqual(my_queue.cancel_job.call_count, 0)


            
    def test_update_all_queue_status(self):
        my_queue = BatchQueue()

        mock_status = {
            'status': 'Q',
            'job_numbers': [193],
            'running_time': [4902.4],
        }

        status_list = [ mock_status.copy() for i in range(3) ]

        status_list[1]['status'] = 'X'
        status_list[2]['status'] = 'X'

        with self.test_pool:
            self.test_pool.update_taxi_status(self.taxi_two, 'R')

            self.assertEqual(self.test_pool.get_taxi(self.taxi_one).status, 'I')
            self.assertEqual(self.test_pool.get_taxi(self.taxi_two).status, 'R')
            self.assertEqual(self.test_pool.get_taxi(self.taxi_three).status, 'H')

            my_queue.report_taxi_status = mock.MagicMock(side_effect=status_list)
            self.test_pool.update_all_taxis_queue_status(my_queue)

            self.assertEqual(self.test_pool.get_taxi(self.taxi_one).status, 'Q')
            self.assertEqual(self.test_pool.get_taxi(self.taxi_two).status, 'I')
            self.assertEqual(self.test_pool.get_taxi(self.taxi_three).status, 'H')
    


if __name__ == '__main__':
    suite1 = unittest.TestLoader().loadTestsFromTestCase(TestSQLiteEmptyPool)
    suite2 = unittest.TestLoader().loadTestsFromTestCase(TestSQLitePoolWithTaxis)
    suite3 = unittest.TestLoader().loadTestsFromTestCase(TestSQLitePoolQueueInteraction)


    all_tests = unittest.TestSuite([suite1, suite2, suite3])
    unittest.TextTestRunner(verbosity=2).run(all_tests)
