#!/usr/bin/env python

import unittest
#import mock
import os
#import sqlite3
import json

from taxi.tasks import Copy # Must import * to get all Task classes in globals() scope
import taxi.apps.mrep_milc.flow as flow
import taxi.apps.mrep_milc.pure_gauge_ora as pure_gauge_ora

class TestBaseTaskRunner(unittest.TestCase):
    pass


class TestCopy(unittest.TestCase):

    def setUp(self):
        self.src = 'test_file_abc'
        self.dest = 'test_file_xyz'

        os.system("touch {}".format(self.src))

    def tearDown(self):
        os.remove(self.src)
        os.remove(self.dest)

    def test_copy(self):
        copy_run = Copy(src=self.src, dest=self.dest)
        copy_run.execute(cores=1)

        self.assertTrue(os.path.exists(self.dest))


class TestFlowRunner(unittest.TestCase):

    def setUp(self):
        self.flow_spec = {
            'Ns': 16,
            'Nt': 32,
            'tmax': 4.0,
            'epsilon': 0.01,
            'minE': 0.0,
            'mindE': 0.0,
        }

        self.hmc_spec = {
            'Ns': 16,
            'Nt': 32,
            'beta': 7.75,
            'nsteps': 4,
            'qhb_steps': 1,
            'tpm': 10,
            'ntraj': 100,
            'warms': 0,
            'seed': 10859285,
            'label': 'test_run',
        }

        self.hmc_task = pure_gauge_ora.PureGaugeORATask(req_time=360, **self.hmc_spec)

    def tearDown(self):
        pass

    def test_create_aux_task(self):
        hmc_flow_task = flow.FlowTask(measure_on=self.hmc_task, req_time=60, tmax=4.0)

        self.assertEqual(hmc_flow_task.Ns, 16)


    def test_generate_input(self):
        pass

    def test_parse_from_ensemble(self):
        pass


    def test_flow_for_hmc(self):
        pass


if __name__ == '__main__':
    suite1 = unittest.TestLoader().loadTestsFromTestCase(TestBaseTaskRunner)
    suite2 = unittest.TestLoader().loadTestsFromTestCase(TestCopy)
    suite3 = unittest.TestLoader().loadTestsFromTestCase(TestFlowRunner)

    all_tests = unittest.TestSuite([suite1, suite2, suite3])
    unittest.TextTestRunner(verbosity=2).run(all_tests)
