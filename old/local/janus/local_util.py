# -*- coding: utf-8 -*-
"""
Created on Fri Dec 16 15:36:57 2016

@author: Dan
"""

from tasks import specify_binary_paths, specify_dir_with_runner_scripts, specify_spectro_binary_path

def use_dan_binary_paths():
    print "WARNING: No binaries for screening mass baryon spectroscopy on Janus"
    print "WARNING: No non-screening P+A binaries on Janus"
    
    specify_binary_paths(
        hmc_binary='/projects/daha5747/bin/sun_mrep_hmc_gcc_cgtime',
        phi_binary='/projects/daha5747/bin/sun_mrep_phi_gcc_cgtime',
        flow_binary='/projects/daha5747/bin/su4_wf'
    )
    
    # Standard spectro
    specify_spectro_binary_path('/projects/daha5747/bin/su4_f_clov_cg',   irrep='f',   p_plus_a=False, screening=False, do_baryons=False)
    specify_spectro_binary_path('/projects/daha5747/bin/su4_as2_clov_cg', irrep='as2', p_plus_a=False, screening=False, do_baryons=False)
    
    # Baryons
    specify_spectro_binary_path('/projects/daha5747/bin/su4_f_clov_cg_bar',   irrep='f',   p_plus_a=False, screening=False, do_baryons=True)
    specify_spectro_binary_path('/projects/daha5747/bin/su4_as2_clov_cg_bar', irrep='as2', p_plus_a=False, screening=False, do_baryons=True)
    
    # Screening
    specify_spectro_binary_path('/projects/daha5747/bin/su4_f_clov_cg_s',   irrep='f',   p_plus_a=False, screening=True, do_baryons=False)
    specify_spectro_binary_path('/projects/daha5747/bin/su4_as2_clov_cg_s', irrep='as2', p_plus_a=False, screening=True, do_baryons=False)

    # P+A
    specify_spectro_binary_path('/projects/daha5747/bin/su4_f_clov_cg_pa',   irrep='f',   p_plus_a=True, screening=False, do_baryons=False)
    specify_spectro_binary_path('/projects/daha5747/bin/su4_as2_clov_cg_pa', irrep='as2', p_plus_a=True, screening=False, do_baryons=False)

    # Screening P+A
    specify_spectro_binary_path('/projects/daha5747/bin/su4_f_clov_cg_s_pa',   irrep='f',   p_plus_a=True, screening=True, do_baryons=False)
    specify_spectro_binary_path('/projects/daha5747/bin/su4_as2_clov_cg_s_pa', irrep='as2', p_plus_a=True, screening=True, do_baryons=False)
