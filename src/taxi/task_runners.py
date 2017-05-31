#!/usr/bin/env python

import os

def sanitized_path(path):
    if path is None:
        return None

    return "".join([c for c in path if c.isalpha() or c.isdigit() or c==' ' or c=='_' or c=='.' or c=='/']).rstrip()

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



class TaskRunner(object):
    def __init__(self, **kwargs):
        self.binary = "echo"

        self.cores = kwargs['cores']
        self.use_mpi = False

    def build_input_string(self):
        return ""

    def execute(self):
        if self.use_mpi:
            exec_str = local_taxi.mpirun_str.format(self.cores)
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






