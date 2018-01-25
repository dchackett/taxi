#!/usr/bin/env python
from taxi.mcmc import ConfigMeasurement
from taxi.file import File, InputFile

## local_taxi should specify:
# - "flow_binary"


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


class FlowTask(ConfigMeasurement):
    ## File naming conventions
    loadg = InputFile('{loadg_filename_prefix}_{Nt:d}_{beta:g}_{k4:g}_{k6:g}_{label}_{traj:d}')
    fout = File('{fout_filename_prefix}_{Ns:d}_{Nt:d}_{beta:g}_{k4:g}_{k6:g}_{label}_{traj:d}')
    fout_filename_prefix = 'flow'
    saveg = None
    
    binary = None # Specify this in run-specification scripts
    
    def __init__(self,
                 # Application-specific required arguments
                 tmax,
                 # Override ConfigMeasurement defaults
                 req_time=600, 
                 # Application-specific defaults
                 minE=0, mindE=0.0, epsilon=0.01, xi=None,
                 # Overrides
                 Ns=None, Nt=None,
                 # Arguments to pass along to superclass
                 **kwargs):
        """Perform the Wilson Flow on a gauge configuration.
        
        If starter is a filename, attempts to read parameters from filename. If
        starter is a ConfigGenerator, steals parameters from the GaugeGenerator.
        
        Args:
            starter: A filename or a ConfigGenerator.
            Ns: Number of lattice points in spatial direction. If provided, overrides
                whatever was found in the ConfigGenerator or filename.
            Nt: Number of lattice points in temporal direction. If provided, overrides
                whatever was found in the ConfigGenerator or filename.
            epsilon: Step size for numerical integration of flow
            tmax: Terminate flow at t=tmax.  If tmax=0, uses adaptive flow stopping.
            minE: Terminate flow when <t^2 E(t)> >= minE
            mindE: Terminate flow when <t d/dt t^2 E(t)> >= mindE
        """
        
        super(FlowTask, self).__init__(req_time=req_time, **kwargs)

        # Physical parameters
        self.tmax = tmax
        self.minE = minE
        self.mindE = mindE
        self.epsilon = epsilon
        self.xi = xi
        
        # Override parameters read out from a filename or stolen from a ConfigGenerator
        if Ns is not None:
            self.Ns = Ns
        if Nt is not None:
            self.Nt = Nt

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
    

