#!/usr/bin/env python2
"""
"""

import os

import taxi.mcmc

class PureGaugeFnConvention(taxi.mcmc.BasicMCMCFnConvention):
    def write(self, params):
        return "{prefix}_{Ns}_{Nt}_{beta}_{_traj}".format(prefix=self.prefix, 
                _traj=(params['traj'] if params.has_key('traj') else params['final_traj']), **params)
    
    def read(self, fn):
        words = os.path.basename(fn).split('_')
        assert len(words) == 5
        return {
            'prefix' : words[0],
            'Ns' : int(words[1]), 'Nt' : int(words[2]),
            'beta' : float(words[3]),
            'traj' : int(words[4])
        }