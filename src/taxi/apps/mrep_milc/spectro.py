#!/usr/bin/env python

import os

from taxi import expand_path
from taxi import fixable_dynamic_attribute
from taxi.mcmc import ConfigMeasurement
import taxi.local.local_taxi as local_taxi

import taxi.fn_conventions
import mrep_fncs

## local_taxi should specify:
# - "spectro_binary"


spectro_input_template = """
prompt 0

nx {Ns}
ny {Ns}
nz {Ns}
nt {Nt}

number_of_kappas 1

kappa {kappa}
clov_c 1.0
u0 1.0
max_cg_iterations {maxcgiter}
max_cg_restarts 10
error_for_propagator {cgtol}
{source_type}
r0 {r0}

reload_serial {loadg}
coulomb_gauge_fix
forget
{load_prop}
{save_prop}        
serial_scratch_wprop w.scr
EOF
"""


# Synonyms for relevant irreps in SU4
SU4_F_irrep_names = [4, '4', 'f', 'F']
SU4_A2_irrep_names = [6, '6', 'a2', 'A2', 'as2', 'AS2', 'as', 'AS']


class SpectroTask(ConfigMeasurement):
    fout_filename_prefix = 'spec'
    fout_filename_convention = mrep_fncs.PureGaugeSpectroFnConvention
    loadg_filename_convention = taxi.fn_conventions.all_conventions_in(mrep_fncs) # Convention: do input/loading FNCs as lists for user-friendliness
    savep_filename_prefix = 'prop'
    savep_filename_convention = mrep_fncs.PureGaugeSpectroFnConvention
    
    output_file_attributes = ['fout', 'saveg', 'savep'] # savep in addition to usual MCMC fout and saveg
    
    def __init__(self,
                 # Application-specific required arguments
                 r0, irrep,
                 # Override ConfigMeasurement defaults
                 req_time=600, 
                 # Application-specific defaults
                 source_type='gaussian', cgtol=1e-9, maxcgiter=500, loadp=None, savep=None,
                 p_plus_a=False, screening=False, compute_baryons=False,
                 Nc=4,
                 # Overrides
                 Ns=None, Nt=None,
                 kappa=None,
                 # Arguments to pass along to superclass
                 **kwargs):
        """Measure spectroscopy on a correlation function.
        
        If starter is a filename, attempts to read parameters from filename. If
        starter is a ConfigGenerator, steals parameters from the GaugeGenerator.
        
        Args:
            starter: A filename or a ConfigGenerator.
            Ns: Number of lattice points in spatial direction. If provided, overrides
                whatever was found in the ConfigGenerator or filename.
            Nt: Number of lattice points in temporal direction. If provided, overrides
                whatever was found in the ConfigGenerator or filename.
            irrep: Irrep of fermion to compute correlators for
            p_plus_a: Apply Periodic+Antiperiodic boundary conditions trick? (Requires binary specified)
            screening: Compute screening mass (spatial-direction) correlation functions? (Requires binary specified)
            compute_baryons: Compute baryon correlators? (Requires binary specified)
            Nc: Number of colors (Default 4 for TACO multirep study) (Requires binary specified)
        """
        
        super(SpectroTask, self).__init__(req_time=req_time, **kwargs)

        # Physical parameters
        self.r0 = r0
        self.irrep = irrep
        self.source_type = source_type
        self.cgtol = cgtol
        self.maxcgiter = maxcgiter
        
        # Propagator saving/loading
        self.loadp = loadp
        self.savep = savep
        
        # Physical Flags (used to determine binary)
        self.p_plus_a = p_plus_a
        self.screening = screening
        self.compute_baryons = compute_baryons
        self.Nc = Nc
        
        # Override parameters read out from a filename or stolen from a ConfigGenerator
        if Ns is not None:
            self.Ns = Ns
        if Nt is not None:
            self.Nt = Nt
            
        # Plug in appropriate kappa from filename or ConfigGenerator, or override if provided
        if kappa is None:
            # Not overridden, figure out what kappa to use
            if self.irrep in SU4_F_irrep_names:
                kappa = self.k4
            elif self.irrep in SU4_A2_irrep_names:
                kappa = self.k6
        assert kappa is not None, "Kappa not found or specified for multirep spectroscopy task"
        self.kappa = kappa 
        
    
    def build_input_string(self):
        input_str = super(SpectroTask, self).build_input_string()
        
        input_dict = self.to_dict()
        
        if self.loadp is None:
            input_dict['load_prop'] = 'fresh_wprop'
        else:
            input_dict['load_prop'] = 'reload_serial_wprop {loadp}'.format(loadp=self.loadp)
            
        if self.savep is None:
            input_dict['save_prop'] = 'forget_wprop'
        else:
            input_dict['save_prop'] = 'save_serial_wprop {savep}'.format(savep=self.savep)

        return input_str + spectro_input_template.format(**input_dict)


    def verify_output(self):
        super(SpectroTask, self).verify_output()
    
        # If this job should save a propagator file, that file must exist
        if getattr(self, 'savep', None) is not None and (not os.path.exists(self.savep)):
            print "Spectro ok check fails: Propagator file {0} doesn't exist.".format(self.savep)
            raise RuntimeError

        # Check for well-formed output file
        # Trailing space avoids catching the error_something parameter input
        with open(self.fout) as f:
            found_end_of_header = False
            found_running_completed = False
            for line in f:
                if "error " in line.lower():
                    raise RuntimeError("Spectro ok check fails: Error detected in " + self.fout)
                found_running_completed |= ("RUNNING COMPLETED" in line)
                found_end_of_header |= ("END OF HEADER" in line)
                
            if not found_end_of_header:
                raise RuntimeError("Spectro ok check fails: did not find end of header in " + self.fout)
            if not found_running_completed:
                raise RuntimeError("Spectro ok check fails: running did not complete in " + self.fout)


    ## Spectroscopy typically has many different binaries.  Use a fixable property to specify which one.
    def _dynamic_get_binary(self):
        if self.irrep in SU4_F_irrep_names:
            irrep = 'f'
        elif self.irrep in SU4_A2_irrep_names:
            irrep = 'a2'
        else:
            raise NotImplementedError("SpectroTask only knows about F, A2 irreps, not {r}".format(r=self.irrep))
            
        key_tuple = (self.Nc, irrep, self.screening, self.p_plus_a, self.compute_baryons)
        
        try:
            return local_taxi.multirep_spectro_binaries[key_tuple]
        except KeyError:
            raise NotImplementedError("Missing binary for (Nc, irrep, screening?, p+a?, compute_baryons?)="+str(key_tuple))
    binary = fixable_dynamic_attribute(private_name='_binary', dynamical_getter=_dynamic_get_binary)

    ## Spectroscopy-specific output: Saved final propagator file
    def _dynamic_get_savep(self):
        return self.savep_filename_convention(prefix=self.savep_filename_prefix).write(self.to_dict())
    savep = taxi.fixable_dynamic_attribute(private_name='_savep', dynamical_getter=_dynamic_get_savep)
