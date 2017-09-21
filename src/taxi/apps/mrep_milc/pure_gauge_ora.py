#!/usr/bin/env python

from taxi.mcmc import ConfigGenerator
import taxi.local.local_taxi as local_taxi

from mrep_fncs import PureGaugeFnConvention


## local_taxi should specify:
# - "pure_gauge_ora_binary"

# Structure of input file - see build_input_string() below
pure_gauge_ora_input_template = """
prompt 0
nx {Ns}
ny {Ns}
nz {Ns}
nt {Nt}

iseed {seed}

warms {warms}
trajecs {n_traj}
traj_between_meas {tpm}

beta {beta}
steps_per_trajectory {nsteps}
qhb_steps {qhb_steps}

{load_gauge}
no_gauge_fix
{save_gauge}

EOF
"""



class PureGaugeORAJob(ConfigGenerator):
    saveg_filename_prefix = 'cfgPG'
    saveg_filename_convention = PureGaugeFnConvention
    fout_filename_prefix = 'ora'
    fout_filename_convention = PureGaugeFnConvention
    loadg_filename_convention = [PureGaugeFnConvention] # Convention: do input/loading FNCs as lists for user-friendliness
    
    binary = local_taxi.pure_gauge_ora_binary
    
    def __init__(self,
                 # Application-specific required arguments
                 Ns, Nt, beta, label,
                 # Application-specific defaults
                 warms=0, nsteps=4, qhb_steps=1, tpm=10,
                 # Override ConfigGenerator defaults (Must pass to superconstructor manually)
                 req_time=600, n_traj=100,
                 # Arguments to pass along to superclass
                 **kwargs):
        """Run a MILC pure-gauge ORA binary.
        
        Args (superclass):
            seed (required): Seed for random number generator
            starter (required): None, a filename, or a ConfigGenerator.
            start_traj (required if starter is a filename): Trajectory number of
                provided starter configuration.
            ntraj: Number of trajectories to run (after warmup) in total
        Args (this application):
            Ns (int): Number of lattice points in spatial direction
            Nt (int):  Number of lattice points in temporal direction
            beta:  Lattice beta parameter
            nsteps (int): Number of overrelaxation steps per trajectory
            qhb_steps (int): Number of heatbath steps per trajectory
            tpm (int): Trajectories per measurement (1=every traj, 2=every other traj, etc.)
            warms (int): Number of warmup trajectories to run
            label (str): Text label for the ensemble
        """
        super(PureGaugeORAJob, self).__init__(req_time=req_time, n_traj=n_traj, **kwargs)

        self.Ns = Ns
        self.Nt = Nt
        self.beta = beta
        self.label = label
        
        self.warms = warms
        self.nsteps = nsteps
        self.qhb_steps = qhb_steps
        self.tpm = tpm
        

    def build_input_string(self):
        input_str = super(PureGaugeORAJob, self).build_input_string()
        
        input_dict = self.to_dict()
        
        # Loadg/saveg logic
        if self.loadg is None:
            input_dict['load_gauge'] = 'fresh'
        else:
            input_dict['load_gauge'] = 'reload_serial {0}'.format(self.loadg)

        if self.saveg is None:
            input_dict['save_gauge'] = 'forget'
        else:
            input_dict['save_gauge'] = 'save_serial {0}'.format(self.saveg)

        input_str += pure_gauge_ora_input_template.format(**input_dict)

        return input_str
    

    def verify_output(self):
        ## In the future, we can define custom exceptions to distinguish the below errors, if needed
        super(PureGaugeORAJob, self).verify_output()

        # Check for errors
        # Trailing space avoids catching the error_something parameter input
        with open(self.fout) as f:
            for line in f:
                if "error " in line:
                    raise RuntimeError("ORA ok check fails: Error detected in " + self.fout)

        # Check that the appropriate number of GMESes are present
        count_gmes = 0
        count_exit = 0
        with open(self.fout) as f:
            for line in f:
                if line.startswith("GMES"):
                    count_gmes += 1
                elif line.startswith("exit: "):
                    count_exit += 1     
                    
        expect_gmes = self.n_traj / self.tpm
        
        if count_gmes < expect_gmes:
            raise RuntimeError("HMC ok check fails: Not enough GMES in " + self.fout + " %d/%d"%(count_gmes, expect_gmes))
            
        if count_exit < 1:
            raise RuntimeError("HMC ok check fails: No exit in " + self.fout)
     
