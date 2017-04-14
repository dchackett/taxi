#!/usr/bin/env python

# Definition of "Pool" class - manages Taxi attributes and status

import taxi

class AbstractTaxiPool(object):

    def __init__(self):
        pass

    def get_all_taxis(self):
        raise NotImplementedError

    def get_taxi_status(self, taxi):
        raise NotImplementedError
    
    def set_taxi_status(self, taxi, status):
        raise NotImplementedError

    def check_queue(self):
        raise NotImplementedError

    def add_taxi(self, taxi):
        raise NotImplementedError

    def remove_taxi(self, taxi):
        raise NotImplementedError

    def submit_taxi_to_queue(self, taxi):
        raise NotImplementedError

    
    