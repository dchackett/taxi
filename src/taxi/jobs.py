#!/usr/bin/env python

import os
import taxi.local.local_taxi as local_taxi

# Helper function
def sanitized_path(path):
    if path is None:
        return None

    return "".join([c for c in path if c.isalpha() or c.isdigit() or c==' ' or c=='_' or c=='.' or c=='/']).rstrip()


### Job classes
class Job(object):
    def __init__(self, req_time=0, for_taxi=None, **kwargs):
        self.req_time = req_time
        self.for_taxi = for_taxi
        self.status = 'pending'
        self.is_recurring = False

        # Tree properties
        self.trunk = False
        self.branch_root = False
        self.priority = -1
        
        # Want a default, but don't clobber anything set by a subclass before calling superconstructor
        if not hasattr(self, 'depends_on'):
            self.depends_on = []
        
    def compile(self):
        # Package in to JSON forest format
        self.compiled = {
            'task_type' :   'none',
            'id'        :   self.job_id,
            'req_time'  :   self.req_time,
            'status'    :   self.status,
            'is_recurring': self.is_recurring,
            'priority':     self.priority,
            'for_taxi':     self.for_taxi,
        }
        
        # Get dependencies in job_id format
        if not hasattr(self, 'depends_on') or self.depends_on is None or len(self.depends_on) == 0:
            self.compiled['depends_on'] = None
        else:
            self.compiled['depends_on'] = [d.job_id for d in self.depends_on]

class DieJob(Job):
    def __init_(self, req_time=0, **kwargs):
        super(DieJob, self).__init__(req_time=req_time, **kwargs)

    def compile(self):
        super(DieJob, self).compile()

        self.compiled.update({
            'task_type': 'die',
        })


class CopyJob(Job):
    def __init__(self, src, dest, req_time=60, **kwargs):
        super(CopyJob, self).__init__(req_time=req_time, **kwargs)
        
        self.src = src
        self.dest = dest
        
        
    def compile(self):
        super(CopyJob, self).compile()
        
        self.compiled.update({
                'task_type' : 'CopyRunner',
                'task_args' : {
                    'src' : self.src,
                    'dest' : self.dest
                }
            })    

## Task runner classes
class TaskRunner(object):
    def __init__(self, **kwargs):
        self.binary = "echo"

        self.cores = kwargs['cores']
        self.use_mpi = False

    def build_input_string(self):
        return ""

    def execute(self, cores):
        if self.use_mpi:
            exec_str = local_taxi.mpirun_str.format(cores)
        else:
            exec_str = ""

        exec_str += self.binary + " "
        exec_str += self.build_input_string()

        os.system(exec_str)

class CopyRunner(TaskRunner):

    def __init__(self, **kwargs):
        for arg in ['src', 'dest']:
            assert arg in kwargs.keys()

        self.binary = "rsync -Paz"

        self.src = kwargs['src']
        self.dest = kwargs['dest']

        # Sanitize file paths
        assert isinstance(self.src, basestring)
        assert isinstance(self.dest, basestring)

        self.src = sanitized_path(self.src)
        self.dest = sanitized_path(self.dest)

        self.use_mpi = False

    def build_input_string(self):
        return "{} {}".format(self.src, self.dest)

def runner_rebuilder_factory():
    """
    Returns a function that turns JSON payloads into TaskRunner objects,
    according to the "task_type" field in the JSON.  (This function
    should be passed to the 'object_hook' argument of json.loads.)

    Searches the namespace for all defined subclasses of TaskRunner in
    order to get the list of valid task types.
    """

    valid_runner_classes = globals()['TaskRunner'].__subclasses__()
    valid_class_names = [ cls.__name__ for cls in valid_runner_classes ]

    class_dict = dict(zip(valid_class_names, valid_runner_classes))

    def runner_decoder(s):
        if s.get('task_type') in valid_class_names:
            return class_dict[s['task_type']](**s['task_args'])

        return s

    return runner_decoder
