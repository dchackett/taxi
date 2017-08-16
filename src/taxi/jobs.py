#!/usr/bin/env python

import os
import taxi.local.local_taxi as local_taxi

from taxi import sanitized_path, all_subclasses_of

from copy import copy, deepcopy

special_keys = ['task_type', 'depends_on', 'status', 'for_taxi', 'is_recurring', 'req_time', 'priority']

class Task(object):
    """Abstract task superclass"""
    
    def __init__(self, req_time=0, for_taxi=None, **kwargs):
        # Provided arguments
        self.req_time = req_time
        self.for_taxi = for_taxi
                
        # Defaults
        self.status = 'pending'
        self.is_recurring = False
        if not hasattr(self, 'depends_on'): # Don't clobber anything set by a subclass
            self.depends_on = [] 

        # Tree properties
        self.trunk = False
        self.branch_root = False
        self.priority = -1
        
        
    def to_dict(self):
        """Returns a dictionaryized version of the object, for use in compilation
        in to a form that can be stored in a dispatch DB.  Unlike simply inspecting
        task.__dict__, this returns a copy of __dict__ with all 'private' attributes
        removed (private attributes in Python are indicated by a leading underscore
        like task._private_var)."""
        
        # to_dict accidental recursion handling
        # (to_dict evaluates all properties, but some property getters call to_dict, recursing infinitely)
        # TODO: Better way to do this? Contexts? Decorator? traceback? Afraid of it breaking.
        if not hasattr(self, '_to_dict_recursion_depth'):
            self._to_dict_recursion_depth = 0
        self._to_dict_recursion_depth += 1
        
        d = copy(self.__dict__)
        
        # Evaluate all properties, loading in to dict
        # TODO: Write a unit test for this
        if self._to_dict_recursion_depth == 1: # Don't infinitely recurse if some property getter calls to_dict
            for k in dir(self):
                if k.startswith('_'):
                    continue # Enforce privacy
                if not hasattr(self.__class__, k):
                    continue # Non-property attributes aren't present in both class and instance
                try: # Try to evaluate property
                    if isinstance(getattr(self.__class__, k), property):    
                        d[k] = getattr(self, k)
                except AttributeError:
                    pass # Don't die if getter not implemented
            
        
        # Don't include private variables in dict version of object
        for k in d.keys():
            if k.startswith('_'):
                d.pop(k)
        
        # depends_on gets modified in compilation, so preserve the original
        d['depends_on'] = deepcopy(d['depends_on'])
        
        self._to_dict_recursion_depth -= 1
        
        return d
    
    
    def compiled(self):
        # Break apart task metainfo and task payload, loading payload in to task dict
        payload = self.to_dict()
        compiled = {}
        for special_key in special_keys:
            if payload.has_key(special_key):
                compiled[special_key] = payload.pop(special_key)
        compiled['payload'] = payload
        
        compiled['task_type'] = self.__class__.__name__ # e.g., 'Task'
        
        # Get dependencies in job_id format
        if not hasattr(self, 'depends_on') or self.depends_on is None or len(self.depends_on) == 0:
            compiled['depends_on'] = None
        else:
            compiled['depends_on'] = [d.id for d in self.depends_on]
            
        return compiled
    
    
    def count_unresolved_dependencies(self):
        """Looks at the status of all jobs in the job forest DB that 'task' depends upon.
        Counts up number of jobs that are not complete, and number of jobs that are failed.
        Returns tuple (n_unresolved, n_failed)"""
        
        # Sensible behavior for dependency-tree roots
        if self.depends_on is None or len(self.depends_on) == 0:
            return 0, 0
        
        # Count up number of incomplete, number of failed
        N_unresolved = 0
        N_failed = 0
        for dependency in self.depends_on:
            if not isinstance(dependency, Task):
                # Completes weren't requested in task blob OR removed dirtily from dispatch
                # If they were not found in task blob, the entries in depends_on are still task_id (instead of task_obj)
                continue
            if dependency.status != 'complete':
                N_unresolved += 1
            if dependency.status == 'failed':
                N_failed += 1
        return N_unresolved, N_failed

        
### Special jobs
class Die(Task):
    """Tells taxi to die"""
    
    def __init__(self, message, req_time=0, **kwargs):        
        super(Die, self).__init__(req_time=req_time, **kwargs)        
        self.message = message
    
    
class Sleep(Task):
    """Tells taxi to sleep"""
    
    def __init__(self, message, req_time=0, **kwargs):        
        super(Sleep, self).__init__(req_time=req_time, **kwargs)        
        self.message = message
        

class Respawn(Task):
    """Special task tells taxi to respawn itself."""
    
    def __init__(self, req_time=0, **kwargs):
        super(Respawn, self).__init__(req_time=req_time, **kwargs)
        
        
class Runner(Task):
    """Abstract superclass to run some external program"""
    
    def __init__(self, cores=None, use_mpi=None, **kwargs):
        super(Runner, self).__init__(**kwargs)
        
        # MPI - Don't clobber anything set by a subclass
        if not hasattr(self, 'cores'):
            self.cores = cores
        if not hasattr(self, 'use_mpi'):
            self.use_mpi = use_mpi
        
        # Defaults
        if not hasattr(self, 'binary'): # Don't clobber anything set by a subclass
            self.binary = 'echo' # For testing purposes

    def build_input_string(self):
        return ""
    
    def verify_output(self):
        pass
    
    def execute(self, cores=None):          
        if cores is None or self.cores is None:
            if self.cores is not None:
                cores = self.cores
            elif cores is None:
                cores = 1
        elif cores < self.cores:
            print "WARNING: Running with {n0} cores for taxi < {n1} cores for task.".format(n0=cores, n1=self.cores)
        elif cores > self.cores:
            print "WARNING: Running with {n1} cores for task < {n0} cores for taxi.".format(n0=cores, n1=self.cores)
            cores = self.cores
            
        if self.use_mpi is not None:
            use_mpi = self.use_mpi
        else:
            use_mpi = cores > 1
            
        if not use_mpi and cores > 1:
            print "WARNING: use_mpi=False, ignoring cores=%d"%cores
            
        if use_mpi:
            exec_str = local_taxi.mpirun_str.format(cores) + " "
        else:
            exec_str = ""

        exec_str += self.binary + " "
        exec_str += self.build_input_string().strip() # Remove leading and trailing whitespace from input string

        #print "exec:", exec_str
        os.system(exec_str)

        self.verify_output()


### Standard runners
class Copy(Runner):
    """Copy a file from src to dest using rsync"""
    
    def __init__(self, src, dest, req_time=60, **kwargs):
        super(Copy, self).__init__(req_time=req_time, **kwargs)

        self.binary = "rsync -Paz"
        
        # Store sanitized file paths
        assert isinstance(src, basestring)
        assert isinstance(dest, basestring)

        self.src = sanitized_path(src)
        self.dest = sanitized_path(dest)
        

    def build_input_string(self):
        return "{} {}".format(self.src, self.dest)
    

### Task rebuilder    
class BlankObject(object):
    def __init__(self):
        pass # Need an __init__ function to have a __dict__


def runner_rebuilder_factory():
    """
    Returns a function that turns JSON payloads into TaskRunner objects,
    according to the "task_type" field in the JSON.  (This function
    should be passed to the 'object_hook' argument of json.loads.)

    Searches the namespace for all defined subclasses of TaskRunner in
    order to get the list of valid task types.
    """

    class_dict = all_subclasses_of(Task)
    
    def runner_decoder(s):
        if s.get('task_type') in class_dict.keys():
            rebuilt = BlankObject()
            rebuilt.__dict__.update(**s)
            rebuilt.__class__ = class_dict[s['task_type']]
            assert rebuilt.__class__.__name__ == s['task_type'], \
                "Failed to reconstruct Task subclass %s, instead ended up with %s"%(rebuilt.__class__.__name__, s['task_type'])
            return rebuilt

        return s

    return runner_decoder
