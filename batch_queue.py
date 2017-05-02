#!/usr/bin/env python

# Interface for system queues.  Used by the Pool class to interact with the batch system.
# This is an abstract class; implementations to be provided for specific machines in "local" subdir.

class BatchQueue(object):

    # Enumeration of taxi status codes
    queue_status_codes = [
        'Q',    ## Queued
        'R',    ## Running
        'X',    ## Not found
    ]

    def __init__(self):
        pass

    def report_taxi_status_by_name(self, taxi_name):
        """
        - Interact with the batch system, search for a job associated with a particular taxi.
        - Return a dictionary containing information on the taxi:
            * 'status': code indicating current queue status, enumerated in queue_status_codes
            * 'job_number': list of job numbers associated with this taxi
            * 'running_time': if running, amount of time currently elapsed
        """

        raise NotImplementedError

    def report_taxi_status(self, taxi):
        return self.report_taxi_status_by_name(taxi.name)
        

    def launch_taxi(self, taxi):
        """
        - Tell the batch system to start a new job associated with the given taxi.
        - No return, but raises exceptions if a failure is detected.
        """
        raise NotImplementedError

    def cancel_job(self, job_number):
        """
        Request to cancel the specified job from the queue.
        """
        raise NotImplementedError

    def report_queue_status(self):
        """
        - Look at all of the jobs reported by the batch system.
        - Return a dictionary containing quantities of interest:
            * 'nodes_free': # of nodes available
            * 'utilization_pct': % of nodes currently utilized
        """
        raise NotImplementedError