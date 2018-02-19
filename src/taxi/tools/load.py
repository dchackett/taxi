#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
"""

import os

from taxi.dispatcher import SQLiteDispatcher
from taxi.pool import SQLitePool

def dispatch(db_path):
    assert os.path.exists(db_path), 'Specified dispatch file {0} does not exist'.format(db_path)
    return SQLiteDispatcher(db_path=db_path)

def pool(db_path, pool_name=None):
    assert os.path.exists(db_path), 'Specified dispatch file {0} does not exist'.format(db_path)
    return SQLitePool(db_path, pool_name)

def dispatch_and_pool(*db_paths):
    """Loads Dispatch and Pool DBs.  Figures out which path is which by trial and error. Returns dispatch, pool.
    
    Doesn't work with pool DBs with more than one pool. Ignores any path after second specified.
    """
    
    if len(db_paths) == 1:
        d = None
        p = None
        
        try:
            d = dispatch(db_paths[0])
        except:
            d = None
            
        try:
            p = pool(db_paths[0])
        except:
            d = None
    
    elif len(db_paths) == 2:
        # Try first two
        try:
            d = dispatch(db_paths[0])
            p = pool(db_paths[1])
        except:
            d = dispatch(db_paths[1])
            p = pool(db_paths[0])
    
    elif len(db_paths) == 3:
        # Assume we're getting (dispatch_path) and (pool_path pool_name) in some order
        try:
            d = dispatch(db_paths[0])
            p = pool(db_paths[1], pool_name=db_paths[2])
        except:
            d = dispatch(db_paths[2])
            p = pool(db_paths[0], pool_name=db_paths[1])
        
    return d,p