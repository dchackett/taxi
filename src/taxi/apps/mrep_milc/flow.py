#!/usr/bin/env python

from taxi.mcmc import ConfigMeasurement
import taxi.local.local_taxi as local_taxi

import taxi.fn_conventions
import mrep_fncs

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
    
reload_serial {loadg}
forget

EOF
"""

class FlowJob(ConfigMeasurement):
    fout_filename_prefix = 'flow'
    fout_filename_convention = mrep_fncs.PureGaugeFnConvention
    loadg_filename_convention = taxi.fn_conventions.all_conventions_in(mrep_fncs) # Convention: do input/loading FNCs as lists for user-friendliness
    
    def __init__(self,
                 # Application-specific required arguments
                 tmax,
                 # Override ConfigMeasurement defaults
                 req_time=600, 
                 # Application-specific defaults
                 minE=0, mindE=0.0, epsilon=0.1,
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
        
        super(FlowJob, self).__init__(req_time=req_time, **kwargs)

        # Physical parameters
        self.tmax = tmax
        self.minE = minE
        self.mindE = mindE
        self.epsilon = epsilon
        
        # Override parameters read out from a filename or stolen from a ConfigGenerator
        if Ns is not None:
            self.Ns = Ns
        if Nt is not None:
            self.Nt = Nt

        # Don't run trivial flows
        if tmax == 0:
            assert minE != 0 or mindE != 0, \
                "If tmax is 0, must set adaptive flow parameters minE and mindE or flow will be trivial"

        self.binary = local_taxi.flow_binary
        
    
    def build_input_string(self):
        input_str = super(FlowJob, self).build_input_string()
        
        input_dict = self.to_dict()

        return input_str + flow_input_template.format(**input_dict)

    def verify_output(self):
        pass
    

