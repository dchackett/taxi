#!/usr/bin/env python

import os
import shutil
import taxi.local.local_taxi as local_taxi

from taxi import sanitized_path, expand_path, copy_nested_list, ensure_path_exists

from taxi.file import File, should_save_file, output_file_attributes_for_task

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
        
        # Load any unconsumed kwargs in to the object
        for k, v in kwargs.items():
            setattr(self, k, v)
        
        
    def to_dict(self):
        """Returns a dictionaryized version of the object, for use in compilation
        in to a form that can be stored in a dispatch DB.  Unlike simply inspecting
        task.__dict__, this returns a copy of __dict__ with all 'private' attributes
        removed (private attributes in Python are indicated by a leading underscore
        like task._private_var).  Also evaluates any dynamic attributes (i.e.,
        properties and descriptors like Files) to get a static value for storage,
        avoiding recursive evaluation of this function.
        """
        
        # to_dict accidental recursion handling
        # (to_dict evaluates all properties, but some property getters call to_dict, recursing infinitely)
        # TODO: Better way to do this? Contexts? Decorator? traceback? Afraid of it breaking.
        if not hasattr(self, '_currently_evaluating_properties'):
            self._currently_evaluating_properties = set([])
        
        # Retrieve all attributes and evaluate all properties, loading in to dict
        # Also retrieves class attributes (defaults) and loads in to dict, which
        # is in some sense a promotion of a default to an instance-level attr.
        # TODO: Write a unit test for this            
        d = {}
        for k in dir(self):
            if k.startswith('_'):
                continue # Enforce privacy
            if k in self._currently_evaluating_properties:
                continue # Don't let evaluating a property depend on evaluating a property
            self._currently_evaluating_properties.add(k)
            
            try:
                v = getattr(self, k) # Try to retrieve attribute or evaluate property
            except AttributeError:
                pass # Don't die if getter not implemented
            
            if not callable(v): # Don't save methods -- NOTE: might cause unpredictable behavior with callable properties
                d[k] = v 
                
            try:
                self._currently_evaluating_properties.remove(k) # Make sure this isn't on the stack anymore once we've evaluated it
            except KeyError:
                pass
                
        # Redundancy for safety: don't include private variables in dict version of object
        for k in d.keys():
            if k.startswith('_'):
                d.pop(k)
        
        # depends_on gets modified in compilation, so preserve the original
        d['depends_on'] = copy_nested_list(d['depends_on'])
        
        return d
    
    
    def compiled(self):
        """Returns a dictionaryized version of the task which has all non-standard
        attributes stored in a 'payload' dict attribute and all dependencies resolved
        from pointers to other Task instances to the ids of those tasks. This format
        is necessary for storage in SQL DBs.
        """
        # Break apart task metainfo and task payload, loading payload in to task dict
        payload = self.to_dict()
        compiled = {}
        for special_key in special_keys:
            if payload.has_key(special_key):
                compiled[special_key] = payload.pop(special_key)
        compiled['payload'] = payload
        
        compiled['task_type'] = self.__class__.__name__ # e.g., 'Task'
        
        # Get dependencies in task_id format
        if not hasattr(self, 'depends_on') or self.depends_on is None or len(self.depends_on) == 0:
            compiled['depends_on'] = None
        else:
            compiled['depends_on'] = [d.id if isinstance(d, Task) else d for d in self.depends_on]
            
        return compiled
    
    
    def count_unresolved_dependencies(self):
        """Looks at the status of all tasks in the task forest DB that 'task' depends upon.
        Counts up number of tasks that are not complete, and number of tasks that are failed.
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
    
    def is_ready(self):
        """Is the task ready to run?"""
        return self.status == 'pending' and self.count_unresolved_dependencies==(0,0)
    
    
    def _rollback(self):
        print "Rolling back task {0}: {1}".format(getattr(self, 'id', None), self)
        assert self.status != 'active', "Task {0} is active, cannot roll it back. Kill it first.".format(self)
        self.status = 'pending'
        
    def __repr__(self):
        """For easier diagnostics, render Tasks like <{Task class name}({task id})>"""
        return "<{class_name}({task_id})>".format(class_name=self.__class__.__name__, task_id=getattr(self, 'id', None))

        
### Special tasks
class Die(Task):
    """Tells taxi to die"""
    
    def __init__(self, message, req_time=0, **kwargs):        
        super(Die, self).__init__(req_time=req_time, **kwargs)        
        self.message = message
        
    def __repr__(self):
        return "<Die:{msg}>".format(msg=self.message)
    
    
class Sleep(Task):
    """Tells taxi to sleep"""
    
    def __init__(self, message, req_time=0, **kwargs):        
        super(Sleep, self).__init__(req_time=req_time, **kwargs)        
        self.message = message
        
    def __repr__(self):
        return "<Sleep:{msg}>".format(msg=self.message)
        

class Respawn(Task):
    """Special task tells taxi to respawn itself."""
    
    def __init__(self, req_time=0, **kwargs):
        super(Respawn, self).__init__(req_time=req_time, **kwargs)
        
    def __repr__(self):
        return "<Respawn>"
        
        
class Runner(Task):
    """Abstract superclass to run some external program"""
    
    binary = 'echo' # Default: For testing purposes
    
    def __init__(self, cores=None, use_mpi=None, allow_output_clobbering=False, **kwargs):
        super(Runner, self).__init__(**kwargs)
        
        # MPI - Don't clobber anything set by a subclass
        if not hasattr(self, 'cores'):
            self.cores = cores
        if not hasattr(self, 'use_mpi'):
            self.use_mpi = use_mpi
            
        self.allow_output_clobbering = allow_output_clobbering
        self.output_files = []
        

    def build_input_string(self):
        """Convenience function to generate an input string (and/or input file) to be
        fed to the relevant binary. Default execute uses this string like:
            (binary name) (input string)
        which allows for feeding of simple strings, heredocs, or specification
        of an input file.
        
        Returns the input string.
        """
        return ""
    
    def verify_output(self):
        """Called after execution is complete to check whether the binary has
        generated the desired output, and that that output is well-formatted.
        Raises errors if any issues are detected.
        """
        pass
    
    def execute(self, cores=None):
        """Calls the binary specified in self.binary, using mpirun (if self.use_mpi==True)
        as specified in local_taxi.mpirun_str and feeding the binary the input string
        generated by build_input_string.
        
        Smart behavior regarding output files:
            - Will not overwrite an existing file unless self.allow_output_clobbering;
            this is also useful to avoid race conditions where multiple taxis start
            working on the same task.
            - Stores the location of all output files written in self.output_files,
            which can then be used by rollback to remove outputs.
        """
        ## Core logic -- reconcile task cores and taxi cores
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
        
        
        ## Prepare to use MPI, if desired
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
            
        
        ## Non-clobbering behavior
        if not self.allow_output_clobbering:
            # Find files that the task intends to save, check if they already exist            
            to_clobber = []
            for ofa in output_file_attributes_for_task(self):
                ofn = getattr(self, ofa, None)
                if should_save_file(ofn) and os.path.exists(str(ofn)):
                    print "WARNING: File {0}={1} already exists, attempting to verify output.".format(ofa, ofn)
                    to_clobber.append(ofn)
                    
            if len(to_clobber) > 0:
                self.verify_output()
                # Verify output throws an error and blocks rest of function if output isn't correct
                print "WARNING: Pre-existing well-formatted output (according to verify_output()) detected; skipping running"
                return # Never clobber

        
        ## Keep track of absolute paths of output files created, for rollbacking
        # For user-friendliness, only have to provide a list of attributes that may contain output filenames
        # Track these before execution. If output fails, want to have a list of output files that may have been created.
        for ofa in output_file_attributes_for_task(self):
            ofn = getattr(self, ofa, None)
            if should_save_file(ofn):
                self.output_files.append(expand_path(str(ofn)))

        ## Construct binary call and execute
        exec_str += self.binary + " "
        exec_str += self.build_input_string().strip() # Remove leading and trailing whitespace from input string

        #print "exec:", exec_str
        os.system(exec_str)

        # Only keep track of files that were actually created
        self.output_files = [ofn for ofn in self.output_files if os.path.exists(str(ofn))]

        ## Verify output
        self.verify_output()
        
        
    def _rollback(self, rollback_dir=None, delete_files=False):
        """Called by Dispatcher.rollback() to roll back this Runner.
        Removes all output files generated in executing the task (which are
        stored in self.output_files) by either deleting them (if delete_files) or
        by moving them to rollback_dir (if specified).
        """
        super(Runner, self)._rollback()
        
        
        if self.output_files is not None and len(self.output_files) > 0:
            assert not (rollback_dir is None and delete_files == False),\
                "Must either provide a rollback_dir to copy files to or give permission to delete_files"
            
            if rollback_dir is not None:
                rollback_dir = expand_path(rollback_dir)
                if not os.path.exists(rollback_dir):
                    os.makedirs(rollback_dir) # Dig out the rollback directory
        
            self.output_files = [fn for fn in self.output_files if fn is not None] # Happens when e.g. MCMC passes saveg up, but saveg was None
            
            for fn in [str(ss) for ss in self.output_files]:
                
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
        else:
            print "No output files tracked for task {0} ({1})".format(getattr(self, 'id', None), self)
            
            

class Copy(Runner):
    """Copy a file from src to dest. Does not overwrite anything unless told to.
    
    Unlike the usual runner, doesn't call a binary."""
    
    binary = None
    
    ## Modularized file naming conventions
    # src = InputFile(...) # Unnecessary to track src, don't ever parse the fn
    dest = File(conventions=None, save=True) # Let rollbacker know to track this file
    
    def __init__(self, src, dest, allow_overwrite=False, req_time=60, **kwargs):
        super(Copy, self).__init__(req_time=req_time, **kwargs)
        
        # Store sanitized file paths
        assert isinstance(src, basestring)
        assert isinstance(dest, basestring)

        self.src = sanitized_path(src)
        self.dest = sanitized_path(dest)
        
        self.allow_overwrite = allow_overwrite
        
    
    def execute(self, *args, **kwargs):
        """Uses shutil.copy2 to copy a file from self.src to self.dest.  Unless
        self.allow_overwrite, will not overwrite an existing file.  If self.allow_overwrite,
        then only overwrites the file if it has been updated (determined by looking
        at modification times and MD5 hashes if necessary).
        """
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
        
        dest_dirname = os.path.dirname(self.dest)
        if len(dest_dirname) > 0:
            ensure_path_exists(os.path.dirname(self.dest))
        print "{0} -> {1}".format(self.src, self.dest)
        shutil.copy2(self.src, self.dest) 
    
    
    def __repr__(self):
        """Render Copy objects to strings like <Copy(id):{filename}>"""
        filename = os.path.basename(self.src)
        return "<{class_name}({task_id}):{filename}>".format(class_name=self.__class__.__name__, task_id=getattr(self, 'id', None), filename=filename)