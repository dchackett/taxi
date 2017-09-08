#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 12 20:24:56 2017

@author: dchackett
"""

import os, sys

def sanitized_path(path):
    if path is None:
        return None

    return "".join([c for c in path if c.isalpha() or c.isdigit() or c==' ' or c=='-' or c=='_' or c=='.' or c=='/']).rstrip()

def expand_path(path):
    if path is None:
        return None
    return os.path.abspath(os.path.expanduser(sanitized_path(os.path.expandvars(path))))

def flush_output():
    sys.stdout.flush()
    sys.stderr.flush()

def ensure_path_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)
        
        
def copy_nested_list(L):
    if not isinstance(L, list):
        return L
    else:
        return [copy_nested_list(l) for l in L]
        
class work_in_dir:
    """Context manager for changing the current working directory
    https://stackoverflow.com/a/13197763
    """
    def __init__(self, newPath):
        self.newPath = expand_path(newPath)

    def __enter__(self):
        self.savedPath = os.getcwd()
        ensure_path_exists(self.newPath)
        os.chdir(self.newPath)

    def __exit__(self, etype, value, traceback):
        os.chdir(self.savedPath)
        

import traceback    
def print_traceback():
    flush_output()
    print traceback.format_exc()
    flush_output()
    
def all_subclasses_of(my_class):
    class_dict = {}
    valid_task_classes = [my_class] + my_class.__subclasses__()
    for valid_task_class in valid_task_classes:
        class_dict[valid_task_class.__name__] = valid_task_class
        valid_task_classes += valid_task_class.__subclasses__()
    return class_dict


def fixable_dynamic_attribute(private_name, dynamical_getter):
    def _setter(self, x):
        setattr(self, private_name, x)
    def _getter(self):
        if hasattr(self, private_name):
            return getattr(self, private_name)
        else:
            return dynamical_getter(self)
    return property(fget=_getter, fset=_setter)