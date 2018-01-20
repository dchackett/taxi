#!/usr/bin/env python
from taxi import fixable_dynamic_attribute
from taxi.mcmc import ConfigMeasurement
from taxi.file import File, InputFile, should_save_file, should_load_file

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

## Binaries
# Spectroscopy binaries are special, because there are so many of them in MILC
# Multirep dictionary key format: (Nc, irrep, screening, p_plus_a, compute_baryons)
multirep_spectro_binaries = {
    (4, 'f', False, False, False) : '/nfs/beowulf03/dchackett/mrep/bin/su4_f_clov_cg',
    (4, 'f', False, True, False) : '/nfs/beowulf03/dchackett/mrep/bin/su4_f_clov_cg_pa',
    (4, 'f', True, True, False) : '/nfs/beowulf03/dchackett/mrep/bin/su4_f_clov_cg_s_pa',
    (4, 'a2', False, False, False) : '/nfs/beowulf03/dchackett/mrep/bin/su4_as2_clov_cg',
    (4, 'a2', False, True, False) : '/nfs/beowulf03/dchackett/mrep/bin/su4_as2_clov_cg_pa',
    (4, 'a2', True, True, False) : '/nfs/beowulf03/dchackett/mrep/bin/su4_as2_clov_cg_s_pa',
    # Baryon binaries 
    (4, 'a2', False, False, True) : '/nfs/beowulf03/wijay/mrep/bin/su4_as2_clov_cg_bar',
    (4, 'f',  False, False, True) : '/nfs/beowulf03/wijay/mrep/bin/su4_f_clov_cg_bar',
}



class SpectroTask(ConfigMeasurement):
    ## File naming conventions
    loadg = InputFile('{loadg_prefix}_{Nt:d}_{beta:g}_{k4:g}_{k6:g}_{label}_{traj:d}')
    fout = File('{fout_prefix}_{irrep}_r{r0:g}_{Ns:d}_{Nt:d}_{beta:g}_{k4:g}_{k6:g}_{label}_{traj:d}')
    savep = File('{savep_prefix}_{irrep}_r{r0:g}_{Ns:d}_{Nt:d}_{beta:g}_{k4:g}_{k6:g}_{label}_{traj:d}')
    saveg = None
    
    # Dynamical behavior for e.g. "tspec" vs "xspecpa" vs ...
    def _get_fout_prefix(self):
        return ('x' if self.screening else 't') + 'spec' + ('pa' if self.p_plus_a else '')
    def _get_savep_prefix(self):
        return ('x' if self.screening else 't') + 'prop' + ('pa' if self.p_plus_a else '')
    fout_prefix = fixable_dynamic_attribute('_fout_prefix', _get_fout_prefix)
    savep_prefix = fixable_dynamic_attribute('_savep_prefix', _get_savep_prefix)
    
    
    def __init__(self,
                 # Application-specific required arguments
                 r0, irrep,
                 # Override ConfigMeasurement defaults
                 req_time=600, 
                 # Application-specific defaults
                 source_type='gaussian', cgtol=1e-9, maxcgiter=500, loadp=None,
                 save_propagator=False,
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
        self.savep.save = save_propagator
        
        
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
        
        if should_load_file(self.loadp):
            input_dict['load_prop'] = 'reload_serial_wprop {loadp}'.format(loadp=self.loadp)
        else:
            input_dict['load_prop'] = 'fresh_wprop'
            
        if should_save_file(self.savep):
            input_dict['save_prop'] = 'save_serial_wprop {savep}'.format(savep=self.savep)
        else:
            input_dict['save_prop'] = 'forget_wprop'

        return input_str + spectro_input_template.format(**input_dict)


    def verify_output(self):
        super(SpectroTask, self).verify_output()
    
        # If this task should save a propagator file, that file must exist
        if should_save_file(self.savep):
            print "Spectro ok check fails: Propagator file {0} doesn't exist.".format(self.savep)
            raise RuntimeError

        # Check for well-formed output file
        # Trailing space avoids catching the error_something parameter input
        with open(str(self.fout)) as f:
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
            return multirep_spectro_binaries[key_tuple]
        except KeyError:
            raise NotImplementedError("Missing binary for (Nc, irrep, screening?, p+a?, compute_baryons?)="+str(key_tuple))
    binary = fixable_dynamic_attribute(private_name='_binary', dynamical_getter=_dynamic_get_binary)

