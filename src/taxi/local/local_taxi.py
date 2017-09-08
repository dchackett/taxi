#!/usr/bin/env python

## Taxi info
mpirun_str = "mpirun -np {0:d} "
use_mpi = True


## Binary locations
flow_binary = '/lqcdproj/multirep/dhackett/bin/su4_wf'
multirep_hmc_binary = '/lqcdproj/multirep/dhackett/bin/sun_mrep_hmc'
multirep_phi_binary = '/lqcdproj/multirep/dhackett/bin/sun_mrep_phi'

# Spectroscopy binaries are special, because there are so many of them in MILC
# Multirep dictionary key format: (Nc, irrep, screening, p_plus_a, compute_baryons)
multirep_spectro_binaries = {
    (4, 'f', False, False, False) : '/lqcdproj/multirep/dhackett/bin/su4_f_clov_cg',
    (4, 'f', False, True, False) : '/lqcdproj/multirep/dhackett/bin/su4_f_clov_cg_pa',
    (4, 'f', True, False, False) : '/lqcdproj/multirep/dhackett/bin/su4_f_clov_cg_s',
    (4, 'f', True, True, False) : '/lqcdproj/multirep/dhackett/bin/su4_f_clov_cg_s_pa',
    (4, 'a2', False, False, False) : '/lqcdproj/multirep/dhackett/bin/su4_as2_clov_cg',
    (4, 'a2', False, True, False) : '/lqcdproj/multirep/dhackett/bin/su4_as2_clov_cg_pa',
    (4, 'a2', True, False, False) : '/lqcdproj/multirep/dhackett/bin/su4_as2_clov_cg_s',
    (4, 'a2', True, True, False) : '/lqcdproj/multirep/dhackett/bin/su4_as2_clov_cg_s_pa',
}