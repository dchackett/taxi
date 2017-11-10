#!/usr/bin/env python

"""
Created on Fri Nov 10 09:17:54 2017
Taxi classes for running "chimera baryons" with fermions in multiple representations.
@author: wijay
"""


from taxi.mcmc import MCMC

import taxi.fn_conventions
import mrep_fncs

chimera_input_template = """
prompt 0

nx {Ns}
ny {Ns}
nz {Ns}
nt {Nt}

nprops 2

propid f
irrep fund
kappa {k4}
csw 1.0
r0 {r0_4}
{load_prop_f}
forget_wprop

propid as2
irrep asym
kappa {k6}
csw 1.0
r0 {r0_6}
{load_prop_as}
forget_wprop
EOF"""

class ChimeraTask(MCMC):
    fout_filename_prefix = 'outCt'
    fout_filename_convention = mrep_fncs.MrepSpectroWijayFnConvention
    loadp_filename_convention = mrep_fncs.MrepPropWijayFnConvention
    output_file_attributes = ['fout']
    binary = '/nfs/beowulf03/wijay/mrep/bin/ci_chimera_baryon_spec_only'
    
    def __init__(self,
                 # Application-specific required arguments
                 loadp_f, loadp_as,
                 # Override ConfigMeasurement defaults
                 req_time=60*60, # 1 hour 
                 # Application-specific defaults
                 Ns=None, Nt=None, k4=None, k6=None,
                 r0_4=None, r0_6=None,
                 # Arguments to pass along to superclass
                 **kwargs):
        """
        Computes chimera baryon spectroscopy using saved propagator files.
        Args:
            loadp_f: str, full path to the fundamental propagator binary file
            loadp_as: str, full path to the sextet propagator binary file
            req_time: int, the requested time for the job in seconds, \
                default is 1 hour
            Ns: int, spatial lattice size, default is None \
                (parses from input filename)
            Nt: int, temporal lattice size, default is None \
                (parses from input filename)
            k4: float, fundamental hopping parameter, default is None \
                (parses from input filename)
            k6: float, sextet hopping parameter, default is None \
                (parses from input filename)
            r0_4: float, fundamental sink smearing radius, default is None \
                (matches to the smearing radius of the source, read from the 
                propagator input filename)
            r0_6: float, sextet sink smearing radius, default is None \
                (matches to the smearing radius of the source, read from the 
                propagator input filename)
        Returns:
            ChimeraTask instance
        """        
        
        super(ChimeraTask, self).__init__(req_time=req_time, **kwargs)
        
        # Propagator saving/loading
        self.loadp_f = loadp_f
        self.loadp_as = loadp_as

        params_f  = self.parse_params_from_loadp(loadp_f)
        params_as = self.parse_params_from_loadp(loadp_as)

        for key in ['beta','Ns','Nt','k4','k6']:
            assert params_f[key] == params_as[key],\
                "Error: mismatched values for {key} between the props".format(key=key)

        self.beta = params_f['beta']                
        self.k4 = params_f['k4']
        self.k6 = params_f['k6']
        self.Ns = params_f['Ns']
        self.Nt = params_f['Nt']

        self.r0_4 = params_f['r0']
        self.r0_6 = params_as['r0']
        self.irrep = 'mixed' # chimeras are mixed-rep objects

        # Override values parsed from input filenames
        if Ns is not None:
            self.Ns = Ns
        if Nt is not None:
            self.Nt = Nt
        if k4 is not None:
            self.k4 = k4
        if k6 is not None:
            self.k6 = k6
        if r0_4 is not None:
            self.r0_4 = r0_4
        if r0_6 is not None:
            self.r0_6 = r0_4
    
    def build_input_string(self):
        input_str = super(ChimeraTask, self).build_input_string()
        input_dict = self.to_dict()
        input_dict['load_prop_f'] = 'reload_serial_wprop {loadp_f}'.format(loadp=self.loadp_f)
        input_dict['load_prop_as'] = 'reload_serial_wprop {loadp_as}'.format(loadp=self.loadp_as)

        return input_str + chimera_input_template.format(**input_dict)


    def verify_output(self):
        super(ChimeraTask, self).verify_output()
    
        with open(self.fout) as f:
            found_running_completed = False
            for line in f:
                found_running_completed |= ("get_i(0): EOF on input" in line)                
            if not found_running_completed:
                raise RuntimeError("Spectro ok check fails: running did not complete in " + self.fout)
    
    def parse_params_from_loadp(self, fn):
        if self.loadp_filename_convention is None:
            return None
        parsed = taxi.fn_conventions.parse_with_conventions(fn=fn, conventions=self.loadp_filename_convention)
        if parsed is None:
            raise ValueError("Specified filename convention(s) {fnc} cannot parse filename {fn}".format(fnc=self.loadp_filename_convention, fn=fn))
        assert parsed.has_key('traj'), "FileNameConvention must return a key 'traj' when processing a configuration file name"
        return parsed





