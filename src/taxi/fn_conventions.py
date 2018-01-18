#!/usr/bin/env python2
"""
Base classes for modularized naming conventions.
"""

from taxi import all_subclasses_of
from taxi.tasks import Task

def _is_dictlike(x):
    return isinstance(x, dict) or hasattr(x, "__getitem__")

class FileNameConvention(object):
    def read(self, fn):
        """Takes a filename (string) and parses it in to a dictionary of parameters.
        
        Returns None if unable to parse.
        """
        raise NotImplementedError
        
        
#    def write_task(self, task):
#        """Takes a subclass of Task and produces a formatted filename string.
#        
#        Returns None if unable to write.
#        """
#        raise NotImplementedError
#        
#        
#    def write_dict(self, params):
#        """Takes a dictionary of parameters and produces a formatted filename string.
#        
#        Returns None if unable to write.
#        """
#        raise NotImplementedError
#        
#        
#    def write(self, task_or_params):
#        if _is_dictlike(task_or_params):
#            return self.write_dict(task_or_params)
#        elif isinstance(task_or_params, Task):
#            return self.write_task(task_or_params)
#        else:
#            raise NotImplementedError("FileNameConvention doesn't know how to write something that isn't a dict or a Task subclass")

    def write(self, params):
        """Takes a dictionary of parameters and produces a formatted filename string.
        
        Returns None if unable to write.
        """
        raise NotImplementedError
        
        
    def __call__(self, fn_or_params):
        if isinstance(fn_or_params, str):
            return self.read(fn_or_params)
        elif _is_dictlike(fn_or_params):
            return self.write(fn_or_params)
        else:
            raise TypeError("FileNameConvention(...) takes either a filename (str) or parameters (dict) or Task subclass.")



def parse_with_conventions(fn, conventions):
    """Convenience function to parse a filename with one or more conventions.
    
    Args:
        fn (str): A parsable filename.
        conventions: Either a FileNameConvention object or a list thereof.
    """
    if conventions is None:
        return None
    
    ## One FNC or a list of FNCs?  Get in list form.
    if hasattr(conventions, '__iter__'):
        fncs = conventions
    #elif isinstance(conventions, FileNameConvention):
    elif hasattr(conventions, 'read'):
        fncs = [conventions]
    else:
        raise ValueError("loadg_filename_convention must be either a list of FileNameConventions or a FileNameConvention")


    ## Try each FNC in the order provided.  Accept first one that works, or return None.
    parsed = None
    for fnc in fncs:
        ## Get an instance of FNCs that were provided abstractly
        if issubclass(fnc, FileNameConvention):
            fnc = fnc()

        assert isinstance(fnc, FileNameConvention), "Provided fnc {fnc} is a {ft}, not a FileNameConvention".format(fnc=fnc, ft=type(fnc))
        assert hasattr(fnc, 'read'), "{fnc} has no 'read' method implemented".format(fnc=fnc)

        try:
            parsed = fnc.read(fn)
        except:
            continue
        if parsed is not None:
            break
    return parsed



def all_conventions_in(search_inside):
    fnc_subclasses = all_subclasses_of(FileNameConvention)
    fncs = []
    for k, v in search_inside.__dict__.items():
        try:
            if fnc_subclasses.has_key(k) and fnc_subclasses[k] == v:
                fncs.append(v)
        except TypeError:
            continue # Sometimes 'in' doesn't work if object is not hashable...
    return fncs

## Global variable
#fn_conventions = []
#
#
#def register(x):
#    global fn_conventions # Technically unnecessary, but conceptually nice
#    assert isinstance(x, FileNameConvention)
#    fn_conventions.append(x)
#
#    
#def find_unregistered_conventions():
#    global fn_conventions # Technically unnecessary, but conceptually nice
#    all_conventions = all_subclasses_of(FileNameConvention)
#    for fnc in all_conventions:
#        if fnc not in fn_conventions:
#            fn_conventions.append(fnc)
#
#        
##def encode(x):
##    for fnc in fn_conventions:
##        try:
##            enc = fnc.write(x)
##        except:
##            continue
##        if enc is not None:
##            return enc
##    raise ValueError("No FileNameConvention available to encode {x}".format(x=x))
#
#
#def decode(fn):
#    for fnc in fn_conventions:
#        try:
#            dec = fnc.read(fn)
#        except:
#            continue
#        if dec is not None:
#            return dec
#    raise ValueError("No FileNameConvention available to decode '{x}'".format(x=fn))
