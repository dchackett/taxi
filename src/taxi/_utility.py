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

def mkdir_p(path):
    if not os.path.exists(path):
        os.makedirs(path)
        
class work_in_dir:
    """Context manager for changing the current working directory
    https://stackoverflow.com/a/13197763
    """
    def __init__(self, newPath):
        self.newPath = expand_path(newPath)

    def __enter__(self):
        self.savedPath = os.getcwd()
        mkdir_p(self.newPath)
        os.chdir(self.newPath)

    def __exit__(self, etype, value, traceback):
        os.chdir(self.savedPath)
        

import traceback    
def print_traceback():
    flush_output()
    print traceback.format_exc()
    flush_output()