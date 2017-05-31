# -*- coding: utf-8 -*-
"""
Created on Thu Jan 12 10:23:57 2017

@author: Ethan
"""

from tasks import specify_binary_paths, specify_dir_with_runner_scripts, specify_spectro_binary_path

def use_ethan_binary_paths():
    print "WARNING: No binaries for thermo-related spectroscopy (screening masses, P+A) on Summit!"

    specify_binary_paths(
        hmc_binary='/home/etne1079/bin/summit/sun_mrep_hmc_icc_v8_dbl',
        phi_binary='/home/etne1079/bin/summit/sun_mrep_phi_icc_v8_dbl',
        flow_binary='/home/etne1079/bin/summit/su4_wf_icc'
    )

    # Standard spectro
    specify_spectro_binary_path('/home/etne1079/bin/summit/spectro/su4_f_clov_cg', irrep='f', p_plus_a=False, screening=False, do_baryons=False)
    specify_spectro_binary_path('/home/etne1079/bin/summit/spectro/su4_as2_clov_cg', irrep='as2', p_plus_a=False, screening=False, do_baryons=False)
    
    # Baryons
    specify_spectro_binary_path('/home/etne1079/bin/summit/spectro/su4_f_clov_cg_bar', irrep='f', p_plus_a=False, screening=False, do_baryons=False)
    specify_spectro_binary_path('/home/etne1079/bin/summit/spectro/su4_as2_clov_cg_bar', irrep='as2', p_plus_a=False, screening=False, do_baryons=False)