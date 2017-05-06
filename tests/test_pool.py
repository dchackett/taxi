#!/usr/bin/env python

import unittest
import mock
import os
import sqlite3

from pool import *
from batch_queue import *

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
            empty_taxis = self.test_pool.conn.execute("""SELECT id, pool_name, time_limit, time_last_submitted, 
                status, node_limit FROM taxis""").fetchall()
            pools = map(dict, self.test_pool.conn.execute("""SELECT name, working_dir, log_dir FROM pools""").fetchall())

            self.assertItemsEqual(empty_taxis, [])
            self.assertEqual(len(pools), 1)
            self.assertEqual(pools[0]['name'], 'test_pool')

            with self.assertRaises(sqlite3.OperationalError):
                self.test_pool.conn.execute("""SELECT fake_column FROM taxis""")

    def test_read_write_taxi(self):
        with self.test_pool:
            taxi_one = taxi.Taxi(time_limit=4000, node_limit=8)
            taxi_one = self.test_pool.register_new_taxi(taxi_one)

            taxi_list = self.test_pool.get_all_taxis_in_pool()
            self.assertEqual(len(taxi_list), 1)

            taxi_one_return = taxi_list[0]

            self.assertEqual(taxi_one_return.id, 1)
            self.assertEqual(taxi_one_return.time_limit, taxi_one.time_limit)
            self.assertEqual(taxi_one_return.node_limit, taxi_one.node_limit)
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


class TestSQLitePoolWithTaxis(TestSQLiteBase):

    def setUp(self):
        super(TestSQLitePoolWithTaxis, self).setUp()

        with self.test_pool:
            self.taxi_one = taxi.Taxi(time_limit=4000, node_limit=8)
            self.taxi_two = taxi.Taxi(time_limit=9999, node_limit=1)

            self.taxi_one = self.test_pool.register_new_taxi(self.taxi_one)
            self.taxi_two = self.test_pool.register_new_taxi(self.taxi_two)


    def tearDown(self):
        super(TestSQLitePoolWithTaxis, self).tearDown()

    def test_read_write_two_taxis(self):
        with self.test_pool:
            taxi_list = self.test_pool.get_all_taxis_in_pool()
            self.assertEqual(len(taxi_list), 2)

            taxi_two_return = taxi_list[1]

            self.assertEqual(taxi_two_return.id, 2)
            self.assertEqual(taxi_two_return.time_limit, self.taxi_two.time_limit)

            taxi_one_return = self.test_pool.get_taxi(1)

            self.assertEqual(taxi_one_return.id, 1)
            self.assertEqual(taxi_one_return.time_limit, self.taxi_one.time_limit)

    def test_get_taxi_by_object_or_id(self):
        with self.test_pool:
            taxi_one_return = self.test_pool.get_taxi(self.taxi_one)
            self.assertEqual(taxi_one_return.time_limit, self.taxi_one.time_limit)

            taxi_two_return = self.test_pool.get_taxi(self.taxi_two.id)
            self.assertEqual(taxi_two_return.time_limit, self.taxi_two.time_limit)

    def test_update_status_and_last_submit(self):
        with self.test_pool:
            self.test_pool.update_taxi_status(self.taxi_one, 'Q')
            taxi_one_return = self.test_pool.get_taxi(self.taxi_one)
            self.assertEqual(taxi_one_return.status, 'Q')

            self.test_pool.update_taxi_last_submitted(self.taxi_two, 13985738.5)
            taxi_two_return = self.test_pool.get_taxi(self.taxi_two.id)
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
            self.taxi_one = taxi.Taxi(time_limit=4000., node_limit=8)
            self.taxi_two = taxi.Taxi(time_limit=9999., node_limit=1)
            self.taxi_three = taxi.Taxi(time_limit=4040., node_limit=8)

            self.taxi_one = self.test_pool.register_new_taxi(self.taxi_one)
            self.taxi_two = self.test_pool.register_new_taxi(self.taxi_two)
            self.taxi_three = self.test_pool.register_new_taxi(self.taxi_three)

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



    


if __name__ == '__main__':
    suite1 = unittest.TestLoader().loadTestsFromTestCase(TestSQLiteEmptyPool)
    suite2 = unittest.TestLoader().loadTestsFromTestCase(TestSQLitePoolWithTaxis)
    suite3 = unittest.TestLoader().loadTestsFromTestCase(TestSQLitePoolQueueInteraction)


    all_tests = unittest.TestSuite([suite1, suite2, suite3])
    unittest.TextTestRunner(verbosity=2).run(all_tests)
