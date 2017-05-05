#!/usr/bin/env python

class Taxi(object):

    def __init__(self, time_limit=None, node_limit=None):
        self.id = None
        self.pool_name = None
        self.time_limit = time_limit
        self.node_limit = node_limit
        self.time_last_submitted = None
        self.status = 'I'

    def taxi_name(self):
        return '{0:s}_{1:d}'.format(self.pool_name, self.id)

    def rebuild_from_dict(self, taxi_dict):
        try:
            self.id = taxi_dict['id']
            self.pool_name = taxi_dict['pool_name']
            self.time_limit = taxi_dict['time_limit']
            self.node_limit = taxi_dict['node_limit']
            self.time_last_submitted = taxi_dict['time_last_submitted']
            self.status = taxi_dict['status']
        except KeyError:
            print "Error: attempted to rebuild taxi from malformed dictionary:"
            print taxi_dict
            raise
        except TypeError:
            print "Error: type mismatch in rebuilding taxi from dict:"
            print taxi_dict
            raise

    def __repr__(self):
        return "Taxi<{},{},{},{},{},'{}'>".format(self.id, self.pool_name, self.time_limit,
            self.node_limit, self.time_last_submitted, self.status)
        

