# -*- coding: utf-8 -*-
"""
Created on Fri Dec 16 15:36:57 2016

@author: Dan
"""

from tasks import specify_binary_paths, specify_dir_with_runner_scripts, specify_spectro_binary_path

def use_dan_binary_paths():
    print "WARNING: No binaries for baryon spectroscopy on CU"
    print "WARNING: No phi binary exists on CU presently"
    
    specify_binary_paths(
         hmc_binary='/nfs/beowulf03/dchackett/mrep/bin/su4_mrep_hmc',
         phi_binary='',
         flow_binary='/nfs/beowulf03/dchackett/mrep/bin/su4_wf_mpi'
    )
    
    # Standard spectro
    specify_spectro_binary_path('/nfs/beowulf03/dchackett/mrep/bin/su4_f_clov_cg',   irrep='f',   p_plus_a=False, screening=False, do_baryons=False)
    specify_spectro_binary_path('/nfs/beowulf03/dchackett/mrep/bin/su4_as2_clov_cg', irrep='as2', p_plus_a=False, screening=False, do_baryons=False)
                
    # P+A
    specify_spectro_binary_path('/nfs/beowulf03/dchackett/mrep/bin/su4_f_clov_cg_pa',   irrep='f',   p_plus_a=True, screening=False, do_baryons=False)
    specify_spectro_binary_path('/nfs/beowulf03/dchackett/mrep/bin/su4_as2_clov_cg_pa', irrep='as2', p_plus_a=True, screening=False, do_baryons=False)

    # Screening P+A
    specify_spectro_binary_path('/nfs/beowulf03/dchackett/mrep/bin/su4_f_clov_cg_s_pa',   irrep='f',   p_plus_a=True, screening=True, do_baryons=False)
    specify_spectro_binary_path('/nfs/beowulf03/dchackett/mrep/bin/su4_as2_clov_cg_s_pa', irrep='as2', p_plus_a=True, screening=True, do_baryons=False)
        
        
