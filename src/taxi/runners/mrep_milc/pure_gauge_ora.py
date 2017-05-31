#!/usr/bin/env python

import taxi.jobs
import taxi.local.local_taxi as local_taxi
import taxi.runners.flow as flow

import os, sys
import platform

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
    'loadg',        ## Path of gauge configuration to load from (or None for fresh start.)
    'saveg',        ## Path of gauge configuration to save to
    'fout',         ## Path of filename to write Monte Carlo output to
    'label',        ## Text label for the ensemble
    'start_traj',   ## Trajectory number for starting config
]

# Structure of input file - see build_input_string() below
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

def pure_gauge_ora_ens_name(Ns, Nt, beta, label):
    return "{Ns}_{Nt}_{beta}_{label}".format(Ns, Nt, beta, label)


class PureGaugeORAJob(taxi.jobs.Job):
    def __init__(self, req_time=0, starter=None, **kwargs):
        super(PureGaugeORAJob, self).__init__(req_time=req_time, **kwargs)

        for arg in pure_gauge_ora_arg_list:
            # Some arguments are not intended to be passed to the constructor (or are optional)
            if arg in ('loadg', 'saveg', 'fout', 'start_traj'):
                continue
            try:
                setattr(self, arg, kwargs[arg])
            except:
                raise AttributeError("Missing PureGaugeORAJob argument: {}".format(arg))

        # Starter input verification
        if starter is None:
            self.loadg = None
            self.start_traj = 0
        elif isinstance(starter, PureGaugeORAJob):
            self.loadg = starter.saveg
            self.start_traj = starter.final_traj
        elif isinstance(starter, str):
            assert os.path.exists(starter)
            self.loadg = starter
            try:
                self.start_traj = kwargs['start_traj']
            except:
                raise AttributeError("Must specify start_traj with starter as file path!")
        else:
            raise TypeError("Inappropriate starter type: {}".format(type(starter)))

        self.final_traj = self.start_traj + self.ntraj

        self.ensemble_name = pure_gauge_ora_ens_name(self.Ns, self.Nt, self.beta, self.label)
        self.saveg = "Gauge_" + self.ensemble_name + "_{}".format(self.final_traj)
        self.fout = "out_" + self.ensemble_name + "_{}".format(self.final_traj)

    def compile(self):
        super(PureGaugeORAJob, self).compile()

        self.compiled.update({
            'task_type': 'PureGaugeORARunner',
            'task_args': { k: getattr(self, k, default=None) for k in pure_gauge_ora_arg_list },
        })


class PureGaugeORARunner(taxi.jobs.TaskRunner):
    def __init__(self, **kwargs):
        self.binary = local_taxi.pure_gauge_ora_binary

        for arg in pure_gauge_ora_arg_list:
            setattr(self, arg, kwargs.get(arg, None))

        ## Sanitize all paths
        self.loadg = taxi.jobs.sanitized_path(kwargs['loadg'])
        self.saveg = taxi.jobs.sanitized_path(kwargs['saveg'])
        self.fout = taxi.jobs.sanitized_path(kwargs['fout'])

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
        if self.use_mpi:
            exec_str = local_taxi.mpirun_str.format(self.cores)
        else:
            exec_str = "./"

        input_str = self.build_input_string()

        exec_str += self.binary + " << EOF >> {fout}\n".format(fout=self.fout)
        exec_str += input_str

        os.system(exec_str)

        self.verify_output()


## Helper functions
### Pure gauge stream-maker convenience functions
def make_ora_job_stream(Ns, Nt, beta,
                        N_configs,
                        starter, req_time,
                        nsteps=4, qhb_steps=1,
                        start_count=0, N_traj=100, warms=0,
                        label='1',
                        streamseed=None, seeds=None,
                        ora_class=PureGaugeORAJob):

    assert issubclass(ora_class, PureGaugeORAJob), "hmc_class must be an PureGaugeORAJob or a subclass thereof, not {ora}".format(ora=str(ora_class))
    
    # Randomly generate a different seed for each hmc run
    if streamseed is None:
        streamseed = hash((Ns, Nt, beta, label))
    seed(streamseed%10000)
    
    ora_stream = []
    for cc, count in enumerate(range(start_count, start_count+N_configs)):
        if seeds is None:        
            job_seed = randint(0, 9999)
        else:
            job_seed = seeds[cc]
            
        new_job = ora_class(Ns=Ns, Nt=Nt, beta=beta,
                         label=label, count=count, req_time=req_time, N_traj=N_traj,
                         nsteps=nsteps, qhb_steps=qhb_steps, warms=warms,
                         starter=starter, seed=job_seed)
        
        if isinstance(starter, PureGaugeORAJob):
            new_job.depends_on.append(starter)
        starter = new_job
        ora_stream.append(new_job)
        
        warms = 0 # Only do warmups at beginning of stream
        
    # Let the first job know it's the beginning of a new branch/sub-trunk
    ora_stream[0].branch_root = True
        
    return ora_stream        


def flow_jobs_for_ora_jobs(ora_stream, req_time, tmax, minE=0, mindE=0, epsilon=.01, start_at_count=10):
    flow_jobs = []
    for ora_job in ora_stream:
        if not isinstance(ora_job, PureGaugeORAJob):
            continue
        if ora_job.count >= start_at_count:
            new_job = flow.FileFlowJob(ora_job.saveg, req_time=req_time,
                        tmax=tmax, minE=minE, mindE=mindE, epsilon=epsilon)
            new_job.depends_on = [ora_job]
            flow_jobs.append(new_job)
    return flow_jobs