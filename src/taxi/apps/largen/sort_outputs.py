#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import os

from taxi.mcmc import sort_outputs

from taxi.mcmc import ConfigGenerator
#from hmc_mrep_singlerep import SingleRepHMCTask # Don't need to make this fine of a distinction until we start running pure gauge tasks
from flow import FlowTask
from spectro import SpectroTask#, PhysicalSpectroTask


def largen_output_type_subpath(task, ofa):
    if isinstance(task, ConfigGenerator) and (ofa == 'saveg' or ofa == 'saveg_meta'):
        return 'gauge'
    if ofa == 'savep':
        return 'prop'
    if ofa == 'fout':
        if isinstance(task, ConfigGenerator):
            out_type = 'hmc'
        elif isinstance(task, FlowTask):
            out_type = 'flow'
        elif isinstance(task, SpectroTask): # PhysicalSpectroTask is a subclass
            out_type = 'spec{irrep_fnc}'.format(irrep_fnc=task.irrep_fnc)
        return os.path.join('data', out_type)
    return None
        

def largen_ensemble_subpath(task):
    # This is unnecessarily complicated, but the second line should be gracefully expandable to deal with multirep theories
    theory_subpath = ["SU{Nc}".format(Nc=task.Nc)]
    theory_subpath += ["N{irrep_fnc}{Nf}".format(irrep_fnc=task.irrep_fnc, Nf=task.Nf)]
    theory_subpath = '_'.join(theory_subpath)
    
    volume_subpath = "{Ns}x{Nt}".format(Ns=task.Ns, Nt=task.Nt)
    params_subpath = "{beta}_{kappa}".format(beta=task.beta, kappa=task.kappa)
    
    return os.path.join(theory_subpath, volume_subpath, params_subpath)


def sort_largen_outputs(task_pool, dest_root):
    return sort_outputs(task_pool, dest_root=dest_root,
                        output_type_subpath=largen_output_type_subpath,
                        ensemble_subpath=largen_ensemble_subpath)
    