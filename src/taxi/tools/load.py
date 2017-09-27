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