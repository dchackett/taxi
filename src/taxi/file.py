#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
"""

import os
import parse
import weakref

_IS_RUNTIME = False # Want to disable some behavior on compute nodes, set this flag to do so

def _decompose_path(fn):
    decomposed = []
    split = (fn, '')
    while True:
        split = os.path.split(split[0])
        if len(split[1]) == 0:
            break
        decomposed.append(split[1])
    return decomposed[::-1]


def _subclass_cmp(a, b):
    if issubclass(a, b):
        return -1
    elif issubclass(b, a):
        return 1
    else:
        return 0
    

def parse_filename(fn, conventions, postprocessor=None):
    """Uses the parse library to parse parameters out of a filename.
    
    Arguments:
        fn: Filename to parse (can include some directory structure, including
            excess directory structure; any directory structure not included in the
            convention will be ignored).
        conventions: Either a single parsing convention, e.g.,
                "{prefix}_{traj:d}" for a filename like "cfg_100", or
                "{prefix}_{beta:f}_{traj:d}" for a filename like "pg_6.0_100"
            or a list of such conventions.  If a list is provided, the function
            will try them in order, returning the first one that works.
            Can also match with relative path structure like
                "{beta}/{prefix}_{traj:d}" for a filename like "6.0/cfg_100"
            or absolute path structure like
                "/data/{beta}/{prefix}_{traj:d}" for an absolute path like "/data/6.0/cfg_100".
            Note that function will never read out a '/' in any parameter, so absolute
            paths must be provided with the correct number of directories in the path.
        postprocessor: If desired, can provide a function that takes a dictionary
            of parsed parameters and returns a postprocessed version of that dictionary.
            Can also provide a list of functions, which are applied in order.
        
    Returns:
        A dictionary of parameters parsed out of the filename using the first
        provided convention string that successfully parses the filename,
    """
    
    # To disable filename parsing
    if conventions is None:
        return None

    fn = os.path.normpath(fn) # Get filename in standardized form without excess / or /../ or /./
        
    if not hasattr(conventions, '__iter__'):
        conventions = [conventions]
        
    parsed = None
    for convention in conventions:
        convention = os.path.normpath(convention) # Remove any redundant or trailing /, /./, /../, etc.

        ## Path-handling: don't want to match any {parse field} with something        
        decomp_conv = _decompose_path(convention)
        decomp_fn = _decompose_path(fn)
        if os.path.isabs(convention):
            # Match abspath
            if len(decomp_conv) != len(decomp_fn):
                continue
            parse_fn = fn
        else:
            # Match relpath -- keep only last (number convention wants to match with) path components
            if len(decomp_conv) > len(decomp_fn):
                continue
            parse_fn = os.path.join(*(decomp_fn[-len(decomp_conv):]))
        
        parsed = parse.parse(convention, parse_fn)
        if parsed is not None:
            parsed = parsed.named # Pull dictionary out of parse.Result object
            break
    
    if postprocessor is not None:
        if not hasattr(postprocessor, '__iter__'):
            postprocessor = [postprocessor]
        for pp in postprocessor:
            parsed = pp(parsed)
        
    return parsed


class FileInterface(object):
    """Asbtract interface to a File object."""
    
    def __init__(self, file_in_class=None, conventions=None, save=None, postprocessor=None):
        """
        Args:
            file_in_class - The File object, stored in some class, that this
             instance is an interface to.
            conventions - (List of) string(s) specifying file naming conventions
             for this instance.  If specified, loads the conventions in to this
             instance as an instance-level override. See taxi.file.parse_filename.
            save - Specifies whether or not this File will be saved.  If specified,
             loads the flag in to this instance as an instance-level override.
            postprocessor - (List of) postprocessors to apply after parsing a filename.
             Each postprocessor is some function that takes a dict of parameters
             parsed out of a filename, and returns a postprocessed version of that
             dict. If a list of postprocessors is provided, applies each one in
             order to any parsed filename. May be used with a trivial 'conventions'
             to implement more complicated parsing.
        """
        self.file_in_class = file_in_class
        
        # Need defaults without a class-level File to refer to
        if file_in_class is None:
            self.conventions = None
            self.save = True
            self.postprocessor = None
            
        # Overrides
        if conventions is not None:
            self.conventions = conventions
        if save is not None:
            self.save = save
        if postprocessor is not None:
            self.postprocessor = postprocessor

    # Cast lists of conventions to immutable tuples of conventions, so that users can't do
    # conventions.append() / etc., which would change the class-level defaults in a case like
    #   instance.loadg.conventions.append("...")
    # for which the obvious behavior is changing the instance-level conventions.
    @property
    def conventions(self):
        return self._conventions
    @conventions.setter
    def conventions(self, value):
        if hasattr(value, '__iter__'):
            value = tuple(value) 
        self._conventions = value
    
    
    def __getattr__(self, name):
        # Only called as a last resort, if there is no attribute present with name
        # Allows for class-level defaults with instance-level overrides:
        # Until overridden, any attribute of FileInterface will default to that of File
        
        assert self.file_in_class is not None,\
            "No attribute {0} found in FileInterface, and not linked to a class-level File.".format(name)
        return self.file_in_class._getattr_override(name, self)
            
        
        
class FileInstanceInterface(FileInterface):
    """Interface for a class instance to a File in the class definition."""
    
    def __init__(self, task_instance, file_in_class=None, conventions=None, save=None, postprocessor=None):
        """
        Args (in addition to superclass):
            task_instance - The instance of the class containing file_in_class
             that this instance will be an interface for.
        """
        super(FileInstanceInterface, self).__init__(file_in_class=file_in_class,
             conventions=conventions, save=save, postprocessor=postprocessor)
        self.task_instance = weakref.ref(task_instance) # weakref to prevent memory leak when calling dispatcher.get_all_tasks repeatedly
        
    def __str__(self):
        """Generates filename using the task instance and the (first) provided convention"""
        
        if hasattr(self, '_value_override'):
            return None if self._value_override is None else str(self._value_override)
    
        if self._string_must_be_override:
            return None # InputFiles: Would have already returned override, so nothing to return
        else:
            return self.render()
    
    
    def render(self):
        convention = self.conventions
        if hasattr(convention, '__iter__'):
            convention = convention[0]
        
        assert isinstance(convention, basestring)
        
        task_instance = self.task_instance() # dereference weakref
        try:
            return convention.format(**task_instance.to_dict())
        except KeyError as e:
            raise KeyError("Unable to find key '{0}' while rendering convention '{1}'".format(e.args[0], convention))
    
    
    def __json__(self):
        """For use with taxi.LocalEncoder, which calls __json__ to get a JSON-serializable
        version of an object when __json__ is present.  Returns a string if the file
        is to be saved or loaded, or None otherwise."""
        if not (self.save or self.load):
            return None
        else:
            return str(self)
    
    
    def parse_params(self, fn=None):
        """Parse parameters out of the filename fn.  If fn is not specified, parse str(self)."""
        return parse_filename(str(self) if fn is None else fn, conventions=self.conventions, postprocessor=self.postprocessor)


class FileSubclassInterface(FileInterface):
    """Interface for a subclass of a class containing a File."""
    def __init__(self, subclass, file_in_class=None, conventions=None, save=None, postprocessor=None):
        super(FileSubclassInterface, self).__init__(file_in_class=file_in_class,
             conventions=conventions, save=save, postprocessor=postprocessor)
        self.subclass = subclass   

    def parse_params(self, fn):
        return parse_filename(fn, conventions=self.conventions, postprocessor=self.postprocessor)



class File(object):
    """Smart file name with file naming conventions. Takes specified file naming conventions
    and uses them to render a filename for each task. Can also parse parameters out
    of filenames using the same conventions.  Uses "parse" for inverse "...".format().
    
    Hierarchical overrides: if any attribute of file is changed in the File instance
    in the class, overrides for all subclasses and instances of the class; if any
    attribute is set for a subclass, overrides for all further subclasses and instances
    of that class; if an set for an instance, overrides for that instance only.
    """
    def __init__(self, conventions, save=True, postprocessor=None):
        self.conventions = conventions
        self.save = save
        self.load = False
        self.postprocessor = postprocessor
        
        self.instance_interfaces = {}
        self.subclass_interfaces = {}
        self.known_subclasses = []
        
        self._string_must_be_override = False
        
        
    # Cast lists of conventions to immutable tuples of conventions, so that users can't do
    # conventions.append() / etc., which would change the class-level defaults in a case like
    #   instance.loadg.conventions.append("...")
    # for which the obvious behavior is changing the instance-level conventions.
    @property
    def conventions(self):
        return self._conventions
    @conventions.setter
    def conventions(self, value):
        if hasattr(value, '__iter__'):
            value = tuple(value) 
        self._conventions = value
        
        
    def _getattr_override(self, name, interface):
        
        if isinstance(interface, FileInstanceInterface):
            relevant_class = interface.task_instance().__class__ # call to dereference weakref
        elif isinstance(interface, FileSubclassInterface):
            relevant_class = interface.subclass
            
        relevant_subclass = None
        for sc in self.known_subclasses:
            if issubclass(relevant_class, sc):
                # issubclass(a,a) = True; if a subclass is going to default to
                # something higher up in the class hierarchy, need to not find
                # that same subclass.
                if not (relevant_class is sc and isinstance(interface, FileSubclassInterface)):
                    relevant_subclass = sc
                    break
            
        if relevant_subclass is None:
            return getattr(self, name)
        else:
            # Block infinite recursion -- don't keep asking subclass interface for
            # an attribute it doesn't have (Note hasattr not affected by override)
            if hasattr(self.subclass_interfaces[id(relevant_subclass)], name):
                return getattr(self.subclass_interfaces[id(relevant_subclass)], name)
            else:
                return getattr(self, name)
        
    
    def _sort_known_subclasses(self):
        self.known_subclasses = sorted(self.known_subclasses, cmp=_subclass_cmp)
    
    
    def _make_new_instance_interface(self, inst):
        # Use weakref to allow FileInstanceInterfaces to be garbage collected
        # when their associated tasks fall out of scope
        new_interface = FileInstanceInterface(task_instance=inst, file_in_class=self)
        self.instance_interfaces[id(inst)] = weakref.ref(new_interface)
        # Reference interface from task to prevent interface from being garbage
        # collected until task is garbage collected
        if not hasattr(inst, '_file_interfaces'):
            inst._file_interfaces = []
        inst._file_interfaces.append(new_interface)
        return new_interface
    
    def _get_instance_interface(self, inst):
        # If no interface exists with the appropriate object id, make a new one
        if not self.instance_interfaces.has_key(id(inst)):
            return self._make_new_instance_interface(inst)
        
        interface = self.instance_interfaces[id(inst)]() # call dereferences weakref
        if interface is None:
            # We're asking for an interface, but the weakref has died -- this occurs
            # only when a new object is made with the same ID as an old object that
            # has been garbage collected. Need a new interface.
            return self._make_new_instance_interface(inst)
        else:
            return interface # Found the interface we wanted
    
        
    # Use descriptor technology to make a "property factory" -- return the
    # class-level defaults when an instance of that class
    # hasn't had anything (e.g. conventions, save?, value) set (overridden)
    def __get__(self, inst, objtype=None):
        # Access to class-level defaults
        if inst is None:
            # Subclasses can be overridden
            if not self.subclass_interfaces.has_key(id(objtype)):
                self.subclass_interfaces[id(objtype)] = FileSubclassInterface(subclass=objtype, file_in_class=self)
                self.known_subclasses.append(objtype)
                self._sort_known_subclasses()
            interface = self.subclass_interfaces[id(objtype)]
            
        else:        
            # Instances can be overridden
            interface = self._get_instance_interface(inst)
        
        if hasattr(interface, '_value_override'):
            return interface._value_override
        else:
            return interface
        
                
    def __set__(self, inst, value):
        """If a File is set for an instance, it overrides the value for that instance."""
        # Avoid unpredictable behavior -- cast to strings
        # Designed to catch the case of accidentally trying to set the Interface for one
        # instance to the Interface for another instance
        if value is not None:
            value = str(value)
            
        interface = self._get_instance_interface(inst)
        interface._value_override = value # call dereferences weakref
        
        
    def __delete__(self, inst):
        """Removes overrides. If a File has a _value_override set for an instance,
        clears it (i.e., if a.fout was set to some specific string, will restore
        dynamical rendering with file naming conventions.)
        If _value_override not set, then removes the deleted interface, clearing
        all instance-level overrides."""
        interface = self.instance_interfaces[id(inst)]() # Call dereferences weakref
        if hasattr(interface, '_value_override'):
            del interface._value_override
        else:
            self.instance_interfaces.pop(id(inst))
    

class InputFile(File):
    """Subclass to specify File properties that are exclusively input files. Provides
    additional safety against accidental deletions; when set to a certain value
    input files will, by default, automatically parse parameters out of the specified
    filename and load them in to the object where the filename was set.
    """
    
    def __init__(self, conventions, auto_parse=True, save=False, **kwargs):
        assert save == False, "InputFiles cannot be saved"
        super(InputFile, self).__init__(conventions, save=False, **kwargs)
        self.load = True
        self.auto_parse = auto_parse
        self._string_must_be_override = True
        
    def __set__(self, inst, value):
        """Whenever the value for an InputFile is set, parameters are automatically
        parsed out using the provided conventions and loaded in to the relevant instance."""
        super(InputFile, self).__set__(inst, value) # Store in _value_override
        
        if inst is None:
            return # Nowhere to load parameters in to for class-level string loading
        if value is None:
            return # Can't parse None
        
        if _IS_RUNTIME:
            return # Never auto-parse at runtime
        
        interface = self.instance_interfaces[id(inst)]() # Call derefences weakref
        if not self._getattr_override('auto_parse', interface):
            return # automatic parsing and loading can be disabled
        parsed = interface.parse_params()
        
        if parsed is None:
            return # Parsing unsuccessful, can't load
        
        # Parsing successful, load in to instance
        for k, v in parsed.items():
            setattr(inst, k, v)
        
        

def should_save_file(fn):
    """Convenience function for File properties in Runners.
    Returns True if file should be saved, i.e.,
        - fn is an instance of FileInstanceInterface and fn.save
        - fn is a string and not ''
    """
    if fn is None:
        return False
    if isinstance(fn, basestring):
        return len(fn) > 0
    if isinstance(fn, FileInstanceInterface):
        return fn.save
    raise Exception("What type of filename is {0}?".format(fn))


def should_load_file(fn):
    """Convenience function for File properties in Runners.
    Returns True if file should be saved, i.e.,
        - fn is an instmethodsance of FileInstanceInterface
        - fn is a string and not ''
    """
    if fn is None:
        return False
    if isinstance(fn, basestring):
        return len(fn) > 0
    if isinstance(fn, FileInstanceInterface):
        return fn.load
    raise Exception("What type of filename is {0}?".format(fn))


def file_attributes_for_task(task):
    """Finds the name of all File attributes in the class of which task is a member.
    Returns a list of attribute names (strings)."""
    return [k for (k,v) in task.__class__.__dict__.items() if isinstance(v, File)]


def output_file_attributes_for_task(task):
    """Finds the name of all InputFile attributes in the class of which task is a member.
    Returns a list of attribute names (strings)."""
    ofas = []
    for k in dir(task.__class__):
        v = getattr(task.__class__, k)
        if isinstance(v, FileInterface) and isinstance(v.file_in_class, File) and not isinstance(v.file_in_class, InputFile):
            ofas.append(k)
    return ofas


#class A(object):
#    fout = File(conventions="{prefix}_{traj:d}")
#    def to_dict(self):
#        d = self.__dict__
#        d['fout'] = self.fout
#        return d
#    def __init__(self, prefix, traj):
#        self.prefix = prefix
#        self.traj = traj
#        
#class B(A):
#    pass
#class C(B):
#    pass
#
#a = A('hmc', 12)
#b = A('hmc', 13)
#print a.fout
#print b.fout
#a.fout.conventions="{prefix}__{traj:d}"
#print a.fout
#print b.fout
#A.fout.conventions="{prefix}____{traj:d}"
#print a.fout
#print b.fout
#a.fout = None
#print a.fout
#print b.fout
#a.fout = 'asdf____12'
#print a.fout # Back to default 'conventions'
#print b.fout
#c = B('hmc', 14)
#print c.fout
#B.fout.conventions = "{prefix}__{traj:d}"
#print c.fout

#class D(object):
#    loadg = InputFile(conventions="{prefix}_{traj:d}")
#    def to_dict(self):
#        d = self.__dict__
#        d['loadg'] = self.loadg
#        return d
#    def __init__(self, prefix, traj):
#        self.prefix = prefix
#        self.traj = traj
#        
#d = D('hmc', 77)
#print d.loadg
