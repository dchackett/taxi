#!/usr/bin/env python
from taxi.mcmc import ConfigMeasurement
from taxi.file import File, InputFile

## local_taxi should specify:
# - "flow_binary"


hvy_qpot_input_template = """
prompt 0
    
nx {Ns}
ny {Ns}
nz {Ns}
nt {Nt}
    
off_axis_flag 1
    
reload_serial {loadg}
forget

EOF
"""


class StaticPotentialTask(ConfigMeasurement):
    ## File naming conventions
    loadg = InputFile('{loadg_filename_prefix}_{Ns:d}_{Nt:d}_{beta:g}_{k4:g}_{k6:g}_{label}_{traj:d}')
    fout = File('{fout_filename_prefix}_{Ns:d}_{Nt:d}_{beta:g}_{k4:g}_{k6:g}_{label}_{traj:d}')
    fout_filename_prefix = 'qpot'
    saveg = None
    
    binary = None # Specify this in run-specification scripts
    
    def __init__(self,
                 # Override ConfigMeasurement defaults
                 req_time=600, 
                 # Overrides
                 Ns=None, Nt=None,
                 # Arguments to pass along to superclass
                 **kwargs):
        """Measure static quark potential on a gauge configuration.
        
        If measure_on is a filename, attempts to read parameters from filename. If
        measure_on is a ConfigGenerator, steals parameters from the GaugeGenerator.
        
        Args:
            measure_on: A filename or a ConfigGenerator.
            Ns: Number of lattice points in spatial direction. If provided, overrides
                whatever was found in the ConfigGenerator or filename.
            Nt: Number of lattice points in temporal direction. If provided, overrides
                whatever was found in the ConfigGenerator or filename.
        """
        
        super(StaticPotentialTask, self).__init__(req_time=req_time, **kwargs)
        
        # Override parameters read out from a filename or stolen from a ConfigGenerator
        if Ns is not None:
            self.Ns = Ns
        if Nt is not None:
            self.Nt = Nt
        
    
    def build_input_string(self):
        input_str = super(StaticPotentialTask, self).build_input_string()
        
        input_dict = self.to_dict()

        return input_str + hvy_qpot_input_template.format(**input_dict)
    

