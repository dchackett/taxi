#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""Extremely hacky script that loads a convenient dispatch-editing environment in to global scope.
"""

import taxi.tools as tools
import taxi.mcmc

def _load_to_global_scope(dispatch_db_path):
    d = tools.load.dispatch(dispatch_db_path)
    globals()['d'] = d
    
    tb = d.get_task_blob(d)
    globals()['tb'] = tb
    
    globals()['tbf'] = [t for t in tb.values() if t.status == 'failed']
    globals()['tba'] = [t for t in tb.values() if t.status == 'active']

    tbcg = [t for t in tb.values() if isinstance(t, taxi.mcmc.ConfigGenerator)]
    globals()['tbcg'] = tbcg
    
    globals()['trees'] = d.find_branches(tb.values())
    globals()['cgtrees'] = d.find_branches(tbcg)

    tools.summary(d)