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

    return "".join([c for c in path if c.isalpha() or c.isdigit() or c==' ' or c=='_' or c=='.' or c=='/']).rstrip()

def flush_output():
    sys.stdout.flush()
    sys.stderr.flush()

def mkdir_p(path):
    if not os.path.exists(path):
        os.makedirs(path)