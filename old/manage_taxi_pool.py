# -*- coding: utf-8 -*-
"""
Created on Fri Dec 16 16:47:03 2016

@author: Dan
"""
import os

def next_available_taxi_name_in_pool(pool_dir, taxi_tag, N):
    found = 0
    ii = 0
    while found < N:
        taxi_name = taxi_tag+str(ii)
        if not os.path.exists(os.path.abspath(pool_dir+'/'+taxi_name)):
            found += 1
            yield taxi_name
        ii += 1

## Given taxi_name, gives {pool_dir}/{taxi_name}
def log_dir_for_taxi(pool_dir, taxi_name):
    return os.path.abspath(pool_dir+'/'+taxi_name)