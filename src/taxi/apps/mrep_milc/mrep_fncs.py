#!/usr/bin/env python2
"""
"""

import os

import taxi.mcmc
from taxi.jobs import Copy

## File naming conventions for pure gauge theory
class PureGaugeFnConvention(taxi.mcmc.BasicMCMCFnConvention):
    def write(self, params):
        return "{prefix}_{Ns}_{Nt}_{beta}_{label}_{_traj}".format(prefix=self.prefix, 
                _traj=(params['traj'] if params.has_key('traj') else params['final_traj']), **params)
    
    def read(self, fn):
        words = os.path.basename(fn).split('_')
        assert len(words) == 6
        return {
            'prefix' : words[0],
            'Ns' : int(words[1]), 'Nt' : int(words[2]),
            'beta' : float(words[3]),
            'label' : float(words[5]),
            'traj' : int(words[5])
        }


class PureGaugeSpectroFnConvention(taxi.mcmc.BasicMCMCFnConvention):
    def write(self, params):
        # xspec... for screening, tspec... for time-direction; ...spec for normal APBC spectro, ...specpa for P+A spectro
        prefix = ('x' if params['screening'] else 't') + 'spec' + ('pa' if params['p_plus_a'] else '')
        
        return "{prefix}_{irrep}_r{r0}_{Ns}_{Nt}_{beta}_{kappa}_{label}_{_traj}".format(
                prefix=prefix,
                _traj=(params['traj'] if params.has_key('traj') else params['final_traj']),
                **params)
        
    def read(self, fn):
        words = os.path.basename(fn).split('_')
        assert len(words) == 9
        assert 'spec' in words[0]
        return {
            'file_prefix' : words[0],
            'p_plus_a' : words[0].endswith('pa'),
            'screening' : words[0].startswith('x'),
            'irrep' : words[1],
            'r0' : float(words[2][1:]),
            'Ns' : int(words[3]), 'Nt' : int(words[4]),
            'beta' : float(words[5]),
            'kappa' : float(words[6]),
            'label' : words[7],
            'traj' : int(words[8])
        }
        
        
        
## File naming conventions for the SU(4) 2xF 2xA_2 multirep theory
class MrepFnConvention(taxi.mcmc.BasicMCMCFnConvention):
    def write(self, params):
        # Assume each kappa=0 if not specified
        k4 = params.get('k4', 0)
        k6 = params.get('k6', 0)
        
        return "{prefix}_{Ns}_{Nt}_{beta}_{_k4}_{_k6}_{label}_{_traj}".format(
                prefix=self.prefix, _k4=k4, _k6=k6,
                _traj=(params['traj'] if params.has_key('traj') else params['final_traj']),
                **params)
    
    def read(self, fn):
        words = os.path.basename(fn).split('_')
        assert len(words) == 8
        return {
            'file_prefix' : words[0],
            'Ns' : int(words[1]), 'Nt' : int(words[2]),
            'beta' : float(words[3]),
            'k4' : float(words[4]),
            'k6' : float(words[5]),
            'label' : words[6],
            'traj' : int(words[7])
        }
        
        
class MrepSpectroFnConvention(taxi.mcmc.BasicMCMCFnConvention):
    def write(self, params):
        # xspec... for screening, tspec... for time-direction; ...spec for normal APBC spectro, ...specpa for P+A spectro
        prefix = ('x' if params['screening'] else 't') + 'spec' + ('pa' if params['p_plus_a'] else '')
        
        # Assume each kappa=0 if not specified
        k4 = params.get('k4', 0)
        k6 = params.get('k6', 0)
        
        return "{prefix}_{irrep}_r{r0}_{Ns}_{Nt}_{beta}_{_k4}_{_k6}_{label}_{_traj}".format(
                prefix=prefix, _k4=k4, _k6=k6,
                _traj=(params['traj'] if params.has_key('traj') else params['final_traj']),
                **params)
        
    def read(self, fn):
        words = os.path.basename(fn).split('_')
        assert len(words) == 10
        assert 'spec' in words[0]
        return {
            'file_prefix' : words[0],
            'p_plus_a' : words[0].endswith('pa'),
            'screening' : words[0].startswith('x'),
            'irrep' : words[1],
            'r0' : float(words[2][1:]),
            'Ns' : int(words[3]), 'Nt' : int(words[4]),
            'beta' : float(words[5]),
            'k4' : float(words[6]), 'k6' : float(words[7]),
            'label' : words[8],
            'traj' : int(words[9])
        }

class MrepSpectroWijayFnConvention(taxi.mcmc.BasicMCMCFnConvention):
    """ Multirep spectroscopy filename conventions used by wijay """
    def write(self, params):
        # Assume each kappa=0 if not specified
        params['file_prefix'] = 'outCt'
        params['k4'] = params.get('k4',0)
        params['k6'] = params.get('k4',0)        
        return "{file_prefix}_{Ns}{Nt}_b{beta}_kf{k4}_kas{k6}_{irrep}_{r0}gf_{traj}".\
            format(**params)
    def read(self, fn, delim='_'):
        words = os.path.basename(fn).split(delim)
        assert (len(words) == 8),\
            "Error: Does filename satisfy wijay conventions?"
        assert words[0] == 'outCt',\
            "Error: Does filename refer to a correlation function output file?"
        return {
            'file_prefix' : words[0],
            'Ns' : words[1][:2],
            'Nt' : words[1][2:],
            'beta' : float(words[2][1:]),
            'k4' : float(words[3][2:]),
            'k6' : float(words[4][3:]),
            'irrep' : words[5],
            'r0' : float(words[6].strip('gf')),
            'traj' : int(words[7]),
        }
        
class MrepGaugeWijayFnConvention(taxi.mcmc.BasicMCMCFnConvention):
    """ Multirep gauge binary filename conventions used by wijay """
    def write(self, params):
        # Assume each kappa=0 if not specified
        params['k4'] = params.get('k4',0)
        params['k6'] = params.get('k4',0)        
        return "{file_prefix}_{Ns}{Nt}_b{beta}_kf{k4}_kas{k6}_{traj}".\
            format(**params)
    def read(self, fn, delim='_'):
        words = os.path.basename(fn).split(delim)
        assert (len(words) == 6),\
            "Error: Does filename satisfy wijay conventions?"
        assert (words[0] == 'cfg'),\
            "Error: Does filename refer to a gauge binary file?"
        return {
            'file_prefix' : words[0],
            'Ns' : words[1][:2],
            'Nt' : words[1][2:],
            'beta' : float(words[2][1:]),
            'k4' : float(words[3][2:]),
            'k6' : float(words[4][3:]),
            'traj' : int(words[5]),
        }
        
class MrepPropWijayFnConvention(taxi.mcmc.BasicMCMCFnConvention):
    """ Multirep propagator binary filename conventions used by wijay """
    def write(self, params):
        # Assume each kappa=0 if not specified
        params['file_prefix'] = 'prop'        
        params['k4'] = params.get('k4',0)
        params['k6'] = params.get('k4',0)        
        return "{file_prefix}_{Ns}{Nt}_b{beta}_kf{k4}_kas{k6}_{irrep}_{r0}gf_{traj}".\
            format(**params)
    def read(self, fn, delim='_'):
        words = os.path.basename(fn).split(delim)
        assert (len(words) == 8),\
            "Error: Does filename satisfy wijay conventions?"
        assert (words[0] == 'prop'),\
            "Error: Does filename refer to a propagator binary file?"
        return {
            'file_prefix' : words[0],
            'Ns' : words[1][:2],
            'Nt' : words[1][2:],
            'beta' : float(words[2][1:]),
            'k4' : float(words[3][2:]),
            'k6' : float(words[4][3:]),
            'irrep' : words[5],
            'r0' : float(words[6].strip('gf')),
            'traj' : int(words[7]),
        }

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
                    type_subpath += '_{0:g}'.format(words[2])
                    # i.e., spec4_r6. {:g} formatting 
                    
            elif 'flow' in words[0]:
                type_subpath = 'flow'
            
            fout_dest = os.path.join(out_dir, volume_subpath, type_subpath, param_subpath, os.path.basename(task.fout))
            new_copy_job = Copy(src=task.fout, dest=fout_dest)
            new_copy_job.depends_on = [task]
            copy_jobs.append(new_copy_job)
    
    return copy_jobs
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
