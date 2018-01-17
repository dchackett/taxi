#!/usr/bin/env python2
"""
"""

import os

import taxi.mcmc
from taxi.jobs import Copy

from taxi.file import File, InputFile


def _convention_metafactory(fmt, postprocessor=None):
    if not hasattr(fmt, '__iter__'):
        fmt = [fmt]
            
    def _factory(prefix=None, input_file=False):        
        conventions = [ff.format(prefix=prefix) for ff in fmt]        
        file_kwargs = dict(conventions=conventions, postprocessor=postprocessor)        
        return InputFile(**file_kwargs) if input_file else File(**file_kwargs)
    
    return _factory

pure_gauge_fmt = '{prefix}_{{Ns:d}}_{{Nt:d}}_{{beta:g}}_{{label}}_{{traj:d}}'
pure_gauge = _convention_metafactory(pure_gauge_fmt)

pure_gauge_spectro_fmt = '{prefix}_{{irrep}}_r{{r0:g}}_{{Ns:d}}_{{Nt:d}}_{{beta:g}}_{{kappa:g}}_{{label}}_{{traj:d}}'
pure_gauge_spectro = _convention_metafactory(pure_gauge_spectro_fmt)

mrep_dh_fmt = '{prefix}_{{Ns:d}}_{{Nt:d}}_{{beta:g}}_{{k4:g}}_{{k6:g}}_{{label}}_{{traj:d}}'
mrep_dh = _convention_metafactory(mrep_dh_fmt)

mrep_dh_spectro_fmt = [
        '{prefix}_{{irrep}}_r{{r0:g}}_{{Ns:d}}_{{Nt:d}}_{{beta:g}}_{{k4:g}}_{{k6:g}}_{{label}}_{{traj:d}}',
        '{prefix}_{{irrep}}_{{Ns:d}}_{{Nt:d}}_{{beta:g}}_{{k4:g}}_{{k6:g}}_{{label}}_{{traj:d}}',
    ]
mrep_dh_spectro = _convention_metafactory(mrep_dh_spectro_fmt)
        

def copy_jobs_for_multirep_outputs(job_pool, out_dir, gauge_dir):
    out_dir = taxi.expand_path(out_dir)
    gauge_dir = taxi.expand_path(gauge_dir)
    
    copy_jobs = []
    for task in job_pool:
        if not isinstance(task, taxi.mcmc.MCMC):
            continue
        volume_subpath = "{ns}x{nt}/".format(ns=task.Ns, nt=task.Nt)
        param_subpath = "{b}/{k4}_{k6}/".format(b=task.beta, k4=task.k4, k6=task.k6)
        if isinstance(task, taxi.mcmc.ConfigGenerator):
            if getattr(task, 'saveg', None) is not None:
                saveg_dest = os.path.join(gauge_dir, volume_subpath, param_subpath, task.saveg)
                
                new_copy_job = Copy(src=task.saveg, dest=saveg_dest)
                new_copy_job.depends_on = [task]
                copy_jobs.append(new_copy_job)
        
        if getattr(task, 'fout', None) is not None:
            words = os.path.basename(task.fout).split('_')
            
            type_subpath = ''
            if words[0] == 'hmc':
                type_subpath = 'hmc/'
            elif 'spec' in words[0]:
                if words[1] in ['f', 'F', '4']:
                    type_subpath = 'spec4'
                elif words[1] in ['a2', 'A2', 'as2', 'AS2', '6', 'as', 'AS']:
                    type_subpath = 'spec6'
                    
                if words[2].startswith('r'):
                    type_subpath += '_{0:g}'.format(float(words[2][len('r'):]))
                    # i.e., spec4_r6. {:g} formatting 
                    
            elif 'flow' in words[0]:
                type_subpath = 'flow'
            
            fout_dest = os.path.join(out_dir, volume_subpath, type_subpath, param_subpath, os.path.basename(task.fout))
            new_copy_job = Copy(src=task.fout, dest=fout_dest)
            new_copy_job.depends_on = [task]
            copy_jobs.append(new_copy_job)
    
    return copy_jobs



def _wijay_volume_postprocessor(parsed):
    assert parsed.has_key('Ns') and parsed.has_key('Nt'), str(parsed)
    vol_str = str(parsed['Ns']) + str(parsed['Nt'])
    assert len(vol_str) == 4, "wijay format doesn't know what to do with '{0}'; len({{Ns}}{{Nt}}) != 4".format(vol_str)
    parsed['Ns'] = int(vol_str[:2])
    parsed['Nt'] = int(vol_str[2:])
    return parsed

        
mrep_wijay_gauge_fmt = 'cfg_{{Ns:d}}{{Nt:d}}_b{{beta:g}}_kf{{k4:g}}_kas{{k6:g}}_{{label}}_{{traj:d}}'
mrep_wijay_gauge = _convention_metafactory(mrep_wijay_gauge_fmt, postprocessor=_wijay_volume_postprocessor)

mrep_wijay_spectro_fmt = 'outCt_{{Ns:d}}{{Nt:d}}_b{{beta:g}}_kf{{k4:g}}_kas{{k6:g}}_{{irrep}}_{{r0:g}}gf_{{traj:d}}'
mrep_wijay_spectro = _convention_metafactory(mrep_wijay_spectro_fmt, postprocessor=_wijay_volume_postprocessor)

mrep_wijay_prop_fmt = 'prop_{{Ns:d}}{{Nt:d}}_b{{beta:g}}_kf{{k4:g}}_kas{{k6:g}}_{{irrep}}_{{r0:g}}gf_{{traj:d}}'
mrep_wijay_prop = _convention_metafactory(mrep_wijay_prop_fmt, postprocessor=_wijay_volume_postprocessor)

mrep_wijay_modes_overlap_output_fmt = 'outOverlapModes_{{Ns:d}}{{Nt:d}}_b{{beta:g}}_kf{{k4:g}}_kas{{k6:g}}_{{traj:d}}'
mrep_wijay_modes_overlap_output = _convention_metafactory(mrep_wijay_modes_overlap_output_fmt, postprocessor=_wijay_volume_postprocessor)

mrep_wijay_gauge_landau_fmt = 'cfgLandau_{{Ns:d}}{{Nt:d}}_b{{beta:g}}_kf{{k4:g}}_kas{{k6:g}}_{{traj:d}}'
mrep_wijay_gauge_landau = _convention_metafactory(mrep_wijay_gauge_landau_fmt, postprocessor=_wijay_volume_postprocessor)

mrep_wijay_h0_fmt = 'h0_{{Ns:d}}{{Nt:d}}_b{{beta:g}}_kf{{k4:g}}_kas{{k6:g}}_{{traj:d}}'
mrep_wijay_h0 = _convention_metafactory(mrep_wijay_h0_fmt, postprocessor=_wijay_volume_postprocessor)

mrep_wijay_hov_fmt = 'hov_{{Ns:d}}{{Nt:d}}_b{{beta:g}}_kf{{k4:g}}_kas{{k6:g}}_{{traj:d}}'
mrep_wijay_hov = _convention_metafactory(mrep_wijay_hov_fmt, postprocessor=_wijay_volume_postprocessor)

mrep_wijay_prop_overlap_fmt = 'propOverlap_{{Ns:d}}{{Nt:d}}_b{{beta:g}}_kf{{k4:g}}_kas{{k6:g}}_{{irrep}}_{{r0:g}}_{{traj:d}}'
mrep_wijay_prop_overlap = _convention_metafactory(mrep_wijay_prop_overlap_fmt, postprocessor=_wijay_volume_postprocessor)

mrep_wijay_prop_overlap_output_fmt = 'outPropOverlap_{{Ns:d}}{{Nt:d}}_b{{beta:g}}_kf{{k4:g}}_kas{{k6:g}}_{{irrep}}_{{r0:g}}_{{traj:d}}'
mrep_wijay_prop_overlap_output = _convention_metafactory(mrep_wijay_prop_overlap_output_fmt, postprocessor=_wijay_volume_postprocessor)




            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
