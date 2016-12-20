# -*- coding: utf-8 -*-
"""
Created on Fri Dec 16 15:36:57 2016

@author: Dan
"""

from tasks import specify_binary_paths, specify_dir_with_runner_scripts, specify_spectro_binary_path

def use_shared_binary_paths(screening=False, baryon=False, p_plus_a=False):
    print "WARNING: Only phi and hmc binaries on fnal presently."
    
    specify_binary_paths(
         #hmc_binary='/lqcdproj/multirep/bin/su4_mrep_hmc_bc1_mvapich_dh',
         hmc_binary='/lqcdproj/multirep/dhackett/bin/sun_mrep_hmc',
         phi_binary='',
         flow_binary='/lqcdproj/multirep/dhackett/bin/sun_mrep_phi'
    )
        
        