#!/usr/bin/env python

## Taxi info
mpirun_str = "/usr/local/mpich2-1.4.1p1/bin/mpirun -np {0:d} "
use_mpi = True


## Binary locations
flow_binary = '/nfs/beowulf03/dchackett/mrep/bin/su4_wf_mpi'
pure_gauge_ora_binary = '/nfs/beowulf03/dchackett/su4_pure_gauge/bin/su4_ora'
multirep_hmc_binary = '/nfs/beowulf03/dchackett/mrep/bin/su4_mrep_hmc'
multirep_phi_binary = '/nfs/beowulf03/dchackett/mrep/bin/su4_mrep_phi'

# Spectroscopy binaries are special, because there are so many of them in MILC
# Multirep dictionary key format: (Nc, irrep, screening, p_plus_a, compute_baryons)
multirep_spectro_binaries = {
    (4, 'f', False, False, False) : '/nfs/beowulf03/dchackett/mrep/bin/su4_f_clov_cg',
    (4, 'f', False, True, False) : '/nfs/beowulf03/dchackett/mrep/bin/su4_f_clov_cg_pa',
    (4, 'f', True, True, False) : '/nfs/beowulf03/dchackett/mrep/bin/su4_f_clov_cg_s_pa',
    (4, 'a2', False, False, False) : '/nfs/beowulf03/dchackett/mrep/bin/su4_as2_clov_cg',
    (4, 'a2', False, True, False) : '/nfs/beowulf03/dchackett/mrep/bin/su4_as2_clov_cg_pa',
    (4, 'a2', True, True, False) : '/nfs/beowulf03/dchackett/mrep/bin/su4_as2_clov_cg_s_pa',
}