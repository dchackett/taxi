#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
"""

from taxi.dispatcher import SQLiteDispatcher
from taxi.pool import SQLitePool

def dispatch(db_path):
    return SQLiteDispatcher(db_path=db_path)

def pool(db_path, pool_name=None):
    return SQLitePool(db_path, pool_name)