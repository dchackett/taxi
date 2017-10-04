#!/usr/bin/env python

import os
import shutil
import taxi.local.local_taxi as local_taxi

from taxi import sanitized_path, expand_path, all_subclasses_of, copy_nested_list, ensure_path_exists

from copy import copy, deepcopy

import hashlib # For checksum comparisons by Copy

special_keys = ['id', 'task_type', 'depends_on', 'status', 'for_taxi', 'is_recurring', 'req_time', 'priority']

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
        if not hasattr(self, '_currently_evaluating_properties'):
            self._currently_evaluating_properties = set([])
        
        d = copy(self.__dict__)
        
        # Evaluate all properties, loading in to dict
        # TODO: Write a unit test for this            
        for k in dir(self):
            if k.startswith('_'):
                continue # Enforce privacy
            if not hasattr(self.__class__, k):
                continue # Non-property attributes aren't present in both class and instance
            if k in self._currently_evaluating_properties:
                continue # Don't let evaluating a property depend on evaluating a property
            try: # Try to evaluate property
                if isinstance(getattr(self.__class__, k), property):
                    self._currently_evaluating_properties.add(k)
                    d[k] = getattr(self, k)
            except AttributeError:
                pass # Don't die if getter not implemented
            
            try:
                self._currently_evaluating_properties.remove(k) # Make sure this isn't on the stack anymore once we've evaluated it
            except KeyError:
                pass
                
        # Don't include private variables in dict version of object
        for k in d.keys():
            if k.startswith('_'):
                d.pop(k)
        
        # depends_on gets modified in compilation, so preserve the original
        d['depends_on'] = copy_nested_list(d['depends_on'])
        
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
            compiled['depends_on'] = [d.id if isinstance(d, Task) else d for d in self.depends_on]
            
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
    
    
    def _rollback(self):
        assert self.status != 'active', "Task {0} is active, cannot roll it back. Kill it first.".format(self)
        self.status = 'pending'

        
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
    
    binary = 'echo' # Default: For testing purposes
    
    def __init__(self, cores=None, use_mpi=None, **kwargs):
        super(Runner, self).__init__(**kwargs)
        
        # MPI - Don't clobber anything set by a subclass
        if not hasattr(self, 'cores'):
            self.cores = cores
        if not hasattr(self, 'use_mpi'):
            self.use_mpi = use_mpi
            
        self.output_files = []
        

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
        
        
    def _rollback(self, rollback_dir=None, delete_files=False):
        super(Runner, self)._rollback()
        
        
        if self.output_files is not None and len(self.output_files) > 0:
            assert not (rollback_dir is None and delete_files == False),\
                "Must either provide a rollback_dir to copy files to or give permission to delete_files"
            
            if rollback_dir is not None:
                rollback_dir = expand_path(rollback_dir)
                assert os.path.exists(rollback_dir), \
                    "Provided rollback dir (expanded) '{0}' does not exist".format(rollback_dir)
        
            self.output_files = [fn for fn in self.output_files if fn is not None] # Happens when e.g. MCMC passes saveg up, but saveg was None
            
            for fn in self.output_files:
                
                if not os.path.exists(fn):
                    print "Rollback unable to find file: '{0}'".format(fn)
                    continue
                
                if rollback_dir is not None:
                    to_path = os.path.join(rollback_dir, os.path.basename(fn))
                    
                    # Don't clobber any files in the rollback directory -- rename duplicate files like hmc_output(1)
                    counter = 0
                    while os.path.exists(to_path):
                        counter += 1
                        new_fn = os.path.basename(fn) + '({0})'.format(counter)
                        to_path = os.path.join(rollback_dir, new_fn)
                    
                    print "Rollback: '{0}' -> '{1}'".format(fn, to_path)
                    shutil.move(fn, to_path)
                    
                
                elif delete_files:
                    # Safety: Don't delete files even if granted permission if a rollback_dir is provided
                    print "Rollback: deleting '{0}'".format(fn)
                    os.remove(fn)
                
            # Output files are cleared, don't need to keep track of them anymore
            self.output_files = []
            

class Copy(Runner):
    """Copy a file from src to dest. Does not overwrite anything unless told to.
    
    Unlike the usual runner, doesn't call a binary."""
    
    binary = "rsync -Paz"
    
    def __init__(self, src, dest, allow_overwrite=False, req_time=60, **kwargs):
        super(Copy, self).__init__(req_time=req_time, **kwargs)
        
        # Store sanitized file paths
        assert isinstance(src, basestring)
        assert isinstance(dest, basestring)

        self.src = sanitized_path(src)
        self.dest = sanitized_path(dest)
        
        self.allow_overwrite = allow_overwrite
        
    
    def execute(self, *args, **kwargs):
        assert os.path.exists(self.src), "Source file '{0}' must exist".format(self.src)
        
        if os.path.exists(self.dest):
            if self.allow_overwrite:
                ## Only overwrite if file is updated.  Try to avoid hashing.
                file_updated = False
                if os.path.getmtime(self.src) > os.path.getmtime(self.dest):
                    # Source was modifiedly more recently than dest
                    file_updated = True
                elif os.stat(self.src).st_size != os.stat(self.dest).st_size:
                    # Sizes are not the same
                    file_updated = True
                else:
                    # Do the hash. MD5 should be good enough.
                    hash_src = hashlib.md5(open(self.src, 'rb').read()).digest()
                    hash_dest = hashlib.md5(open(self.dest, 'rb').read()).digest()
                    if hash_src != hash_dest:
                        file_updated = True
                if not file_updated:
                    print "Skipping copy: '{0}' = '{1}'".format(self.src, self.dest)
                    return
                    
            else:
                raise Exception("Path '{0}' already exists and overwriting not allowed".format(self.dest))
        
        ensure_path_exists(os.path.dirname(self.dest))       
        print "{0} -> {1}".format(self.src, self.dest)
        shutil.copy2(self.src, self.dest)
        
    
    def rollback(self, rollback_dir=None, delete_files=False):
        self.output_files.append(self.dest)
        
        super(Copy, self).rollback(rollback_dir=rollback_dir, delete_files=delete_files)
    

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
