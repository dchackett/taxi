__version__ = '0.2.0'

import os



def mkdir_p(path):
    if not os.path.exists(path):
        os.makedirs(path)
        
class Taxi(object):

    def __init__(self, name=None, pool_name=None, time_limit=None, cores=None):
        self.name = name
        self.pool_name = pool_name
        self.time_limit = time_limit
        self.cores = cores
        self.time_last_submitted = None
        self.start_time = None  ## Not currently saved to DB, but maybe it should be?
        self.status = 'I'

    def __eq__(self, other):
        eq = (self.name == other.name)
        eq = eq and (self.pool_name == other.pool_name)
        eq = eq and (self.time_limit == other.time_limit)
        eq = eq and (self.cores == other.cores)
        eq = eq and (self.time_last_submitted == other.time_last_submitted)
        eq = eq and (self.start_time == other.start_time)
        eq = eq and (self.status == other.status)

        return eq

    def taxi_name(self):
        return '{0:s}_{1:d}'.format(self.pool_name, self.name)

    def rebuild_from_dict(self, taxi_dict):
        try:
            self.name = taxi_dict['name']
            self.pool_name = taxi_dict['pool_name']
            self.time_limit = taxi_dict['time_limit']
            self.cores = taxi_dict['cores']
            self.time_last_submitted = taxi_dict['time_last_submitted']
            self.status = taxi_dict['status']
            self.dispatch_path = taxi_dict['dispatch']
        except KeyError:
            print "Error: attempted to rebuild taxi from malformed dictionary:"
            print taxi_dict
            raise
        except TypeError:
            print "Error: type mismatch in rebuilding taxi from dict:"
            print taxi_dict
            raise

    def to_dict(self):
        self_dict = {
            'name': self.name,
            'pool_name': self.pool_name,
            'time_limit': self.time_limit,
            'cores': self.cores,
            'time_last_submitted': self.time_last_submitted,
            'start_time': self.start_time,
            'status': self.status,
        }
        if hasattr(self, 'pool_path'):
            self_dict['pool_path'] = self.pool_path
        if hasattr(self, 'dispatch_path'):
            self_dict['dispatch_path'] = self.dispatch_path

        return self_dict

    def __repr__(self):
        return "Taxi<{},{},{},{},{},'{}'>".format(self.name, self.pool_name, self.time_limit,
            self.cores, self.time_last_submitted, self.status)