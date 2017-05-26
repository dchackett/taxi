#!/usr/bin/env python

import unittest
import mock
import os
import sqlite3

import task_runners

## TaskRunner that always fails, for testing purposes
class FailRunner(task_runners.TaskRunner):
    def execute(self):
        raise BaseException



class TestScalarRunTaxiIntegration(unittest.TestCase):
    ## Integration testing: run_taxi using the "scalar" localization and simple tasks.

    def setUp(self):
        ## Set up symbolic links for scalar localization

        ## Source/destination for copy test jobs

        ## Set up test pool

        ## Set up test dispatch

        pass



    def tearDown(self):
        ## Remove symbolic links
        pass

    ## Test local imports

    ## Test 'die' job

    ## Test taxi hold

    ## Test single copy job, round-trip

    ## Test two copy jobs, with dependency

    ## Test re-launch detection after job

    ## Test work completion (no tasks pending)

    ## Test task failure

    ## Test attempting to run a previously failed task

    ## Test attempting to run an unresolved dependency

