#!/usr/bin/env python
from taxi import fixable_dynamic_attribute
from taxi.binary_menu import BinaryMenu, binary_from_binary_menu
from taxi.mcmc import ConfigMeasurement
from taxi.file import File, InputFile, should_save_file, should_load_file

import conventions

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


class SpectroTask(ConfigMeasurement, conventions.LargeN):
    ## File naming conventions
    loadg = InputFile(conventions.loadg_convention)
    saveg = None # Never want to save spectroscopy outputs
    fout = File(conventions.spectro_fout_convention) # fout_filename_prefix provided (or dynamically generated in sublcass)
    loadp = InputFile(conventions.loadp_convention)
    savep = File(conventions.savep_convention) # savep_filename_prefix provided (or dynamically generated in subclass)
    
    # Required params, checked to be present and not None at dispatch compile time
    _required_params = ['binary', 'Ns', 'Nt', 'kappa', 'r0', 'fout_prefix'] + ['Nc', 'beta', 'Nf', 'irrep_fnc', 'label']
    
    def __init__(self,
                 # Application-specific required arguments
                 kappa=None, r0=None, binary=None, fout_prefix=None,
                 # Override ConfigMeasurement defaults
                 req_time=600, 
                 # Application-specific defaults
                 source_type='gaussian', cgtol=1e-9, maxcgiter=500, loadp=None,
                 save_propagator=False, savep_prefix=None,
                 # Arguments to pass along to superclass
                 **kwargs):
        """Measure correlation functions on a stored gauge configuration.
        
        If measure_on is a filename, attempts to read parameters from filename. If
        measure_on is a ConfigGenerator, steals parameters from the GaugeGenerator.
        
        Args:
            measure_on: A filename or a ConfigGenerator.
            kappa: Either a numerical value to use for kappa, or the name of an attribute
                of this class to find kappa in (after it has been stolen from either
                a filename or a ConfigGenerator, e.g., "k4", "k6").
            r0 (float): Smearing radius
            binary: Spectroscopy binary. Force specification because physical flags
                (e.g., irrep, screening mass?, use p+a trick?, compute baryons?)
                are handled by having many different binaries, versus passing flags
                to one binary.
            fout_prefix: Prefix to spectroscopy filename, like "tspec" or "xspecpa".
                Force specification for same reason as binary.
        
            Ns: Number of lattice points in spatial direction. If provided, overrides
                whatever was found in the ConfigGenerator or filename.
            Nt: Number of lattice points in temporal direction. If provided, overrides
                whatever was found in the ConfigGenerator or filename.
        Filename-only args:
            Nc, beta, Nf, irrep, label
        """
        
        super(SpectroTask, self).__init__(req_time=req_time, **kwargs)

        # Physical parameters
        self.r0 = r0
        self.source_type = source_type
        self.cgtol = cgtol
        self.maxcgiter = maxcgiter
        
        self.binary = binary
        
        # Propagator saving/loading
        self.loadp = loadp
        self.savep.save = save_propagator
        
        # Filenames
        if save_propagator:
            assert savep_prefix is not None, "Must specify savep_prefix to save propagator"
            self.savep_filename_prefix = savep_prefix
        assert fout_prefix is not None, "Must specify fout_prefix for file names, e.g. tspec"
        self.fout_filename_prefix = fout_prefix
            
        # Semi-smart, semi-hackish kappa loading
        if kappa is not None:
            if isinstance(kappa, basestring):
                # Assume kappa is a parameter to be stolen
                try:
                    self.kappa = getattr(self, kappa)
                except AttributeError:
                    raise AttributeError("Specified kappa='{0}', but no attribute with that name is present in {1}".format(kappa, self))
            else:
                # Just take whatever value is provided
                self.kappa = kappa
        
        assert self.kappa > 0 # Idiot check
        
    
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



class PhysicalSpectroTask(SpectroTask):
    # Dynamical behavior for filenames e.g. "tspec" vs "xspecpa" vs ...
    def _get_fout_filename_prefix(self):
        return ('x' if self.screening else 't') + 'spec' + ('pa' if self.p_plus_a else '')
    fout_filename_prefix = fixable_dynamic_attribute('_fout_filename_prefix', _get_fout_filename_prefix)
    
    def _get_savep_filename_prefix(self):
        return ('x' if self.screening else 't') + 'prop' + ('pa' if self.p_plus_a else '')
    savep_filename_prefix = fixable_dynamic_attribute('_savep_filename_prefix', _get_savep_filename_prefix)
        
    ## Spectroscopy typically has many different binaries.  Use a fixable property to specify which one.
    binary_menu = BinaryMenu() # Load with binaries in run-spec script
    binary = binary_from_binary_menu(binary_menu, key_attr_names=['Nc', 'irrep', 'screening', 'p_plus_a', 'compute_baryons'])
    
    # Required params, checked to be present and not None at dispatch compile time
    _required_params = ['binary', 'Ns', 'Nt', 'irrep', 'kappa', 'r0', 'fout_prefix'] + ['Nc', 'beta', 'Nf', 'label']
    
    def __init__(self,
                 # Application-specific required arguments
                 r0=None, irrep=None,
                 # Override ConfigMeasurement defaults
                 req_time=600, 
                 # Application-specific defaults
                 source_type='gaussian', cgtol=1e-9, maxcgiter=500, loadp=None,
                 save_propagator=False,
                 p_plus_a=False, screening=False, compute_baryons=False,
                 # Overrides
                 kappa=None,
                 # Arguments to pass along to superclass
                 **kwargs):
        """Measure correlation functions on a stored gauge configuration.
        
        If measure_on is a filename, attempts to read parameters from filename. If
        measure_on is a ConfigGenerator, steals parameters from the GaugeGenerator.
        
        Instead of having to specify the binary and output file prefixes, these
        are dynamically determined from physical flags. Must have binaries specified
        in self.binary_menu for the keys (Nc, irrep, screening, p_plus_a, compute_baryons).
        
        Args:
            measure_on: A filename or a ConfigGenerator.
            Nc: Number of colors SU(Nc). Used to determine which binary to run, and
                for file names.
            Ns: Number of lattice points in spatial direction. If provided, overrides
                whatever was found in the ConfigGenerator or filename.
            Nt: Number of lattice points in temporal direction. If provided, overrides
                whatever was found in the ConfigGenerator or filename.
            irrep: Irrep of fermion to compute correlators for
            p_plus_a: Apply Periodic+Antiperiodic boundary conditions trick? (Requires binary specified)
            screening: Compute screening mass (spatial-direction) correlation functions? (Requires binary specified)
            compute_baryons: Compute baryon correlators? (Requires binary specified)
            Nc: Number of colors (Default 4 for TACO multirep study) (Requires binary specified)
        Filename args:
            Nc, Ns, Nt, Nf, beta, kappa, Nf
        """
        
        # HACK: We don't want to call the SpectroTask constructor, we just want
        # the other function overrides. So, skip that constructor by just calling
        # SpectroTask's superconstructor
        super(SpectroTask, self).__init__(req_time=req_time, **kwargs)
        
        # Physical parameters
        self.r0 = r0
        self.source_type = source_type
        self.cgtol = cgtol
        self.maxcgiter = maxcgiter
        
        # Physical Flags (used to determine binary)
        self.p_plus_a = p_plus_a
        self.screening = screening
        self.compute_baryons = compute_baryons
        
        # Propagator saving/loading
        self.loadp = loadp
        self.savep.save = save_propagator
                
        # Override parameters read out from a filename or stolen from a ConfigGenerator            
        # Semi-smart, semi-hackish kappa loading
        if kappa is not None:
            if isinstance(kappa, basestring):
                # Assume kappa is a parameter to be stolen
                try:
                    self.kappa = getattr(self, kappa)
                except AttributeError:
                    raise AttributeError("Specified kappa='{0}', but no attribute with that name is present in {1}".format(kappa, self))
            else:
                # Just take whatever value is provided
                self.kappa = kappa
        
        assert self.kappa > 0 # Idiot check

