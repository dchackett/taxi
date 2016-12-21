# -*- coding: utf-8 -*-
"""
Created on Fri Dec 16 15:36:57 2016

@author: Dan
"""

from tasks import specify_binary_paths, specify_dir_with_runner_scripts, specify_spectro_binary_path

def use_shared_binary_paths(screening=False, baryon=False, p_plus_a=False):
    
    specify_binary_paths(
         hmc_binary='/lqcdproj/multirep/dhackett/bin/sun_mrep_hmc',
         phi_binary='/lqcdproj/multirep/dhackett/bin/sun_mrep_phi',
         flow_binary='/lqcdproj/multirep/dchackett/bin/su4_wf'
    )
        
    # Standard spectro
    specify_spectro_binary_path('/lqcdproj/multirep/dchackett/bin/su4_f_clov_cg',   irrep='f',   p_plus_a=False, screening=False, do_baryons=False)
    specify_spectro_binary_path('/lqcdproj/multirep/dchackett/bin/su4_as2_clov_cg', irrep='as2', p_plus_a=False, screening=False, do_baryons=False)
    
    # P+A
    specify_spectro_binary_path('/lqcdproj/multirep/dchackett/bin/su4_f_clov_cg_pa',   irrep='f',   p_plus_a=True, screening=False, do_baryons=False)
    specify_spectro_binary_path('/lqcdproj/multirep/dchackett/bin/su4_as2_clov_cg_pa', irrep='as2', p_plus_a=True, screening=False, do_baryons=False)
    
    # Screening
    specify_spectro_binary_path('/lqcdproj/multirep/dchackett/bin/su4_f_clov_cg_s',   irrep='f',   p_plus_a=False, screening=True, do_baryons=False)
    specify_spectro_binary_path('/lqcdproj/multirep/dchackett/bin/su4_as2_clov_cg_s', irrep='as2', p_plus_a=False, screening=True, do_baryons=False)
                
    # Screening P+A
    specify_spectro_binary_path('/lqcdproj/multirep/dchackett/bin/su4_f_clov_cg_s_pa',   irrep='f',   p_plus_a=True, screening=True, do_baryons=False)
    specify_spectro_binary_path('/lqcdproj/multirep/dchackett/bin/su4_as2_clov_cg_s_pa', irrep='as2', p_plus_a=True, screening=True, do_baryons=False)