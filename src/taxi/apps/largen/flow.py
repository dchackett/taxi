#!/usr/bin/env python
from taxi.mcmc import ConfigMeasurement
from taxi.file import File, InputFile
from taxi.binary_menu import BinaryMenu, binary_from_binary_menu

import conventions


flow_input_template = """
prompt 0
    
nx {Ns}
ny {Ns}
nz {Ns}
nt {Nt}
    
epsilon {epsilon}
tmax {tmax}
minE {minE}
mindE {mindE}
{xi_str}
    
reload_serial {loadg}
forget

EOF
"""


class FlowTask(ConfigMeasurement, conventions.LargeN):
    ## File naming conventions
    loadg = InputFile(conventions.loadg_convention)
    fout = File(conventions.fout_convention)
    fout_filename_prefix = conventions.flow_fout_filename_prefix
    saveg = None
    
    binary_menu = BinaryMenu() # Load with binaries in run-spec scripts
    binary = binary_from_binary_menu(binary_menu, key_attr_names=['Nc'])
    
    # Required params, checked to be present and not None at dispatch compile time
    _required_params = ['binary', 'Ns', 'Nt', 'Nc', 'tmax', 'epsilon'] + ['beta', 'kappa', 'irrep_fnc', 'Nf', 'label']
    
    def __init__(self,
                 # Application-specific required arguments
                 tmax=None,
                 # Override ConfigMeasurement defaults
                 req_time=600, 
                 # Application-specific defaults
                 minE=0, mindE=0.0, epsilon=0.01, xi=None,
                 # Arguments to pass along to superclass
                 **kwargs):
        """Perform the Wilson Flow on a gauge configuration.
        
        If measure_on is a filename, attempts to read parameters from filename. If
        measure_on is a ConfigGenerator, steals parameters from the GaugeGenerator.
        
        Instead of having to specify the binary and output file prefixes, these
        are dynamically determined from physical flags. Must have binaries specified
        in self.binary_menu for the key Nc.
        
        Args:
            measure_on: A filename or a ConfigGenerator.
            Nc: Number of colors SU(Nc). Used for binary selection.
            Ns: Number of lattice points in spatial direction. If provided, overrides
                whatever was found in the ConfigGenerator or filename.
            Nt: Number of lattice points in temporal direction. If provided, overrides
                whatever was found in the ConfigGenerator or filename.
            epsilon: Step size for numerical integration of flow
            xi: Optional argument for anisotropic flow
            tmax: Terminate flow at t=tmax.  If tmax=0, uses adaptive flow stopping.
            minE: Terminate flow when <t^2 E(t)> >= minE
            mindE: Terminate flow when <t d/dt t^2 E(t)> >= mindE
        Filename-only args:
            beta, kappa, irrep, Nf, label
        """
        
        super(FlowTask, self).__init__(req_time=req_time, **kwargs)

        # Physical parameters
        self.tmax = tmax
        self.minE = minE
        self.mindE = mindE
        self.epsilon = epsilon
        self.xi = xi
            
        # Don't run trivial flows
        if tmax == 0:
            assert minE != 0 or mindE != 0, \
                "If tmax is 0, must set adaptive flow parameters minE and mindE or flow will be trivial"
        
    
    def build_input_string(self):
        input_str = super(FlowTask, self).build_input_string()
        
        input_dict = self.to_dict()
        
        if self.xi is not None:
            input_dict['xi_str'] = 'xi {xi}'.format(xi=self.xi)
        else:
            input_dict['xi_str'] = ''

        return input_str + flow_input_template.format(**input_dict)
    

