#!/usr/bin/env python

import task_runners
import jobs

import os, sys
import platform

import local_taxi

## local_taxi should specify:
# - "pure_gauge_ora_binary"

pure_gauge_ora_arg_list = [
    'Ns',           ## Number of lattice points in spatial direction
    'Nt',           ## Number of lattice points in temporal direction
    'beta',         ## Lattice beta parameter
    'nsteps',       ## Number of overrelaxation steps per trajectory
    'qhb_steps',    ## Number of heatbath steps per trajectory
    'tpm',          ## Trajectories per measurement (1=every traj, 2=every other traj, etc.)
    'ntraj',        ## Number of trajectories to run (after warmup) in total
    'warms',        ## Number of warmup trajectories to run
    'seed',         ## Seed for random number generator
    'loadg',        ## Path of gauge configuration to load from (None for fresh start.)
    'saveg',        ## Path of gauge configuration to save to
    'fout',         ## Path of filename to write Monte Carlo output to
]

pure_gauge_ora_input_template = """
prompt 0
nx {Ns}
ny {Ns}
nz {Ns}
nt {Nt}

iseed {seed}

warms {warms}
trajecs {ntraj}
traj_between_meas {tpm}

beta {beta}
steps_per_trajectory {nsteps}
qhb_steps {qhb_steps}

{load_gauge}
no_gauge_fix
{save_gauge}

EOF
"""

class PureGaugeORAJob(jobs.Job):
    def __init__(self, req_time=0, **kwargs):
        super(PureGaugeORAJob, self).__init__(req_time=req_time, **kwargs)

    def compile(self):
        super(PureGaugeORAJob, self).compile()

        self.compiled.update({
            'task_type': 'PureGaugeORARunner',
            'task_args': { k: getattr(self, k, default=None) for k in pure_gauge_ora_arg_list },
        })


class PureGaugeORARunner(task_runners.TaskRunner):
    def __init__(self, **kwargs):
        self.binary = local_taxi.pure_gauge_ora_binary

        for arg in pure_gauge_ora_arg_list:
            setattr(self, arg, kwargs[arg])

        ## Sanitize all paths
        self.loadg = task_runners.sanitized_path(kwargs['loadg'])
        self.saveg = task_runners.sanitized_path(kwargs['saveg'])
        self.fout = task_runners.sanitized_path(kwargs['fout'])

    def to_dict(self):
        return { k: getattr(self, k, default=None) for k in pure_gauge_ora_arg_list }

    def build_input_string(self):
        input_dict = self.to_dict()
        if self.loadg is None:
            input_dict['load_gauge'] = 'fresh'
        else:
            input_dict['load_gauge'] = 'reload_serial {}'.format(self.loadg)

        if self.saveg is None:
            input_dict['save_gauge'] = 'forget'
        else:
            input_dict['save_gauge'] = 'save_serial {}'.format(self.saveg)

        input_str = pure_gauge_ora_input_template.format(**input_dict)

        return input_str


        

    def verify_output(self):
        ## In the future, we can define custom exceptions to distinguish the below errors, if needed

        # Gauge file must exist
        if (self.saveg != None) and (not os.path.exists(self.saveg)):
            print "ORA ok check fails: Gauge file {} doesn't exist.".format(self.saveg)
            raise RuntimeError

        # Output file must exist
        if not os.path.exists(self.fout):
            print "ORA ok check fails: Output file {} doesn't exist.".format(self.fout)
            raise RuntimeError

        # Check for errors
        # Trailing space avoids catching the error_something parameter input
        with open(self.fout) as f:
            for line in f:
                if "error " in line:
                    print "ORA ok check fails: Error detected in " + self.fout
                    raise RuntimeError

        # Check that the appropriate number of GMESes are present
        count_gmes = 0
        count_exit = 0
        with open(self.fout) as f:
            for line in f:
                if line.startswith("GMES"):
                    count_gmes += 1
                elif line.startswith("exit: "):
                    count_exit += 1     
                    
        if count_gmes < self.ntraj:
            print "HMC ok check fails: Not enough GMES in " + self.fout + " %d/%d"%(self.ntraj, count_gmes)
            raise RuntimeError
            
        if count_exit < 1:
            print "HMC ok check fails: No exit in " + self.fout
            raise RuntimeError
            
        return        


    def execute(self):
        super(PureGaugeORARunner, self).execute()
        self.verify_output()
