#!/usr/bin/env python
from taxi import fixable_dynamic_attribute
from taxi.mcmc import ConfigGenerator
from taxi.file import File, InputFile, should_save_file, should_load_file

# Structure of input file - see build_input_string() below
hmc_input_template = """
prompt 0
    
nx {Ns}
ny {Ns}
nz {Ns}
nt {Nt}
    
iseed {seed}

## gauge action params
beta {beta}
gammarat {gammarat}
    
## fermion observables
max_cg_iterations {maxcgobs}
max_cg_restarts 10
error_per_site {cgtol}
    
## dynamical fermions
nkappas 2
npseudo {npseudo}
nlevels {nlevels}

## fermion actions
kpid kap_f         # identifier
kappa {k4}         # hopping parameter
csw 1.0            # clover coefficient
#mu 0.0             # chemical potential
nf 2               # how many Dirac flavors
irrep fund
    
kpid kap_as        # identifier
kappa {k6}         # hopping parameter
csw 1.0            # clover coefficient
#mu 0.0             # chemical potential
nf 2               # how many Dirac flavors
irrep asym
    
## pf actions
pfid onem_f        # a unique name, for identification in the outfile
type onemass       # pseudofermion action types: onemass twomass rhmc
kpid kap_f         # identifier for the fermion action
multip 1           # how many copies of this pseudofermion action will exist
level 1            # which MD update level
shift1 {shift}     # for simulating det(M^dag M + shift1^2)
iters {maxcgpf}    # CG iterations
rstrt 10           # CG restarts
resid {cgtol}       # CG stopping condition

{L2_pf_F}

pfid onem_as       # a unique name, for identification in the outfile
type onemass       # pseudofermion action types: onemass twomass rhmc
kpid kap_as        # identifier for the fermion action
multip 1           # how many copies of this pseudofermion action will exist
level 1            # which MD update level
shift1 {shift}     # for simulating det(M^dag M + shift1^2)
iters {maxcgpf}    # CG iterations
rstrt 10           # CG restarts
resid {cgtol}      # CG stopping condition

{L2_pf_A2}

## update params
warms {warms}
trajecs {n_traj}
traj_length {trajL}
traj_between_meas {tpm}
    
nstep {nsteps1}
{_nsteps2}
nstep_gauge {nsteps_gauge}
ntraj_safe {n_safe}
nstep_safe {nsteps_safe}

## load/save
{load_gauge}
{save_gauge}

EOF
"""

# These blocks get inserted if Hasenbusch preconditioning is used (needs a second set of pseudofermions)
L2_pf_A2_template = """pfid twom_f        # a unique name, for identification in the outfile
type twomass       # pseudofermion action types: onemass twomass rhmc
kpid kap_f         # identifier for the fermion action
multip 1           # how many copies of this pseudofermion action will exist
level 2            # which MD update level
shift1 .0          # simulates det(M^dag M + shift1^2)/det(M^dag M + shift2^2)
shift2 {shift}
iters {maxcgpf}    # CG iterations
rstrt 10           # CG restarts
resid {cgtol}      # CG stopping condition"""


L2_pf_F_template = """pfid twom_as       # a unique name, for identification in the outfile
type twomass       # pseudofermion action types: onemass twomass rhmc
kpid kap_as        # identifier for the fermion action
multip 1           # how many copies of this pseudofermion action will exist
level 2            # which MD update level
shift1 .0          # simulates det(M^dag M + shift1^2)/det(M^dag M + shift2^2)
shift2 {shift}
iters {maxcgpf}    # CG iterations
rstrt 10           # CG restarts
resid {cgtol}      # CG stopping condition"""


class MultirepHMCTask(ConfigGenerator):
    loadg = InputFile('{loadg_filename_prefix}_{Ns:d}_{Nt:d}_{beta:g}_{k4:g}_{k6:g}_{label}_{traj:d}')
    fout = File('{fout_filename_prefix}_{Ns:d}_{Nt:d}_{beta:g}_{k4:g}_{k6:g}_{label}_{traj:d}')
    fout_filename_prefix = 'hmc'
    saveg = File('{saveg_filename_prefix}_{Ns:d}_{Nt:d}_{beta:g}_{k4:g}_{k6:g}_{label}_{traj:d}')
    saveg_filename_prefix = 'cfg'
    
    binary = None # Specify this in run-specification scripts
    
    def __init__(self,
                 # Application-specific required arguments
                 Ns, Nt, beta, k4, k6, label, nsteps1,
                 # Application-specific defaults
                 gammarat=125., trajL=1.0,
                 warms=0, nsteps_gauge=6, n_safe=5, safe_factor=4, tpm=1,
                 nsteps2=None, shift=0.2, # Hasenbuch preconditioning
                 minAR=4,
                 maxcgobs=500, maxcgpf=500, cgtol=1e-6,
                 # Override ConfigGenerator defaults (Must pass to superconstructor manually)
                 req_time=600, n_traj=10,
                 # Arguments to pass along to superclass
                 **kwargs):
        """Run a MILC pure-gauge ORA binary.
        
        Args (superclass):
            seed (required): Seed for random number generator
            starter (required): None, a filename, or a ConfigGenerator.
            start_traj (required if starter is a filename): Trajectory number of
                provided starter configuration.
            n_traj (int): Number of trajectories to run (after warmup) in total
        Args (this application):
            Ns (int): Number of lattice points in spatial direction
            Nt (int):  Number of lattice points in temporal direction
            beta:  Lattice beta parameter
            gammarat: NDS action parameter. beta/gamma = gammarat, so gamma = beta/gammarat.
            k4: Kappa parameter for fundamental irrep fermions
            k6: Kappa paarameter for two-index antisymmetric irrep fermions
            label (str): Text label for the ensemble
            
            nsteps1 (int): Number of outmost-layer steps in integration. Number 
              of steps between full D^-2 evals where we evaluate (D^2 + m^2 / D^2)^-1 instead.
            nsteps2 (int): No Hasenbuch if not provided. Number of gauge steps between (D^2 + m^2 / D^2)^-1 evaluations.
            nsteps_gauge (int): Number of innermost (gauge-only) integration steps.
            shift: Fake m to use in D^2 + m^2. Ignored (set to zero) if nsteps2 is not provided.
            trajL: Length of HMC trajectory (always 1).
            
            n_safe (int): Run a safe trajectory once every n_safe trajectories (c.f. tpm)
            safe_factor (int): Safe trajectories have safe_factor*nsteps1 many outermost steps
            tpm (int): Trajectories per measurement (1=every traj, 2=every other traj, etc.)
            warms (int): Number of warmup trajectories to run
            minAR (int): Minimum number of accepts for task to succeed
            
            maxcgobs (int): Maximum number of CG iterations to run for fermion observables.
            maxcgpf (int): Maximum number of CG iterations to run for pseudofermions.
            cgtol: Maximum CG error per site for pseudofermions and fermion observables.

            
        """
        super(MultirepHMCTask, self).__init__(req_time=req_time, n_traj=n_traj, **kwargs)

        self.Ns = Ns
        self.Nt = Nt
        self.beta = beta
        self.gammarat = gammarat
        self.k4 = k4
        self.k6 = k6
        self.label = label
        
        self.nsteps1 = nsteps1
        self.nsteps2 = nsteps2
        self.nsteps_gauge = nsteps_gauge
        if nsteps2 is None:
            shift = 0 # No shift if not using Hasenbuch
        self.shift = shift
        self.trajL = trajL
        
        self.n_safe = n_safe
        self.safe_factor = safe_factor
        self.tpm = tpm
        self.warms = warms
        self.minAR = minAR
        
        self.maxcgobs = maxcgobs
        self.maxcgpf = maxcgpf
        self.cgtol = cgtol
        
        
    # Overridable dynamic attribute for nsteps_safe, uses nsteps1*safe_factor
    def _dynamic_get_nsteps_safe(self):
        return self.nsteps1 * self.safe_factor
    nsteps_safe = fixable_dynamic_attribute(private_name='_nsteps_safe', dynamical_getter=_dynamic_get_nsteps_safe)
        
    
    def build_input_string(self):
        input_str = super(MultirepHMCTask, self).build_input_string()
        
        input_dict = self.to_dict()
        
        # Saveg/loadg logic
        if should_load_file(self.loadg):
            input_dict['load_gauge'] = 'reload_serial {0}'.format(self.loadg)
        else:
            input_dict['load_gauge'] = 'fresh'
            
        if should_save_file(self.saveg):
            input_dict['save_gauge'] = 'save_serial {0}'.format(self.saveg)
        else:
            input_dict['save_gauge'] = 'forget'

        # Hasenbuch logic
        if self.nsteps2 is None:
            input_dict['L2_pf_F'] = ''
            input_dict['L2_pf_A2'] = ''
            input_dict['npseudo'] = 2
            input_dict['nlevels'] = 1
            input_dict['_nsteps2'] = ''.format(n2=self.nsteps2)
        else:
            input_dict['L2_pf_F'] = L2_pf_F_template.format(**input_dict)
            input_dict['L2_pf_A2'] = L2_pf_A2_template.format(**input_dict)
            input_dict['npseudo'] = 4
            input_dict['nlevels'] = 2
            input_dict['_nsteps2'] = 'nstep {n2}'.format(n2=self.nsteps2)
            
        input_str += hmc_input_template.format(**input_dict)

        return input_str
    

    def verify_output(self):
        ## In the future, we can define custom exceptions to distinguish the below errors, if needed
        super(MultirepHMCTask, self).verify_output()

        # Check for errors
        # Trailing space avoids catching the error_something parameter input
        with open(str(self.fout)) as f:
            for line in f:
                if "error " in line:
                    raise RuntimeError("HMC ok check fails: Error detected in " + self.fout)

        # Check that the appropriate number of GMESes are present
        count_gmes = 0
        count_traj = 0
        count_exit = 0
        count_accept = 0
        with open(str(self.fout)) as f:
            for line in f:
                if line.startswith("GMES"):
                    count_gmes += 1
                elif line.startswith("exit: "):
                    count_exit += 1     
                elif line.startswith("ACCEPT") or line.startswith("SAFE_ACCEPT"):
                    count_traj += 1
                    count_accept += 1
                elif line.startswith("REJECT") or line.startswith("SAFE_REJECT"):
                    count_traj += 1
                    
        if (count_traj < self.n_traj) and (count_gmes < self.n_traj):
            raise RuntimeError("HMC ok check fails: Not enough GMES or ACCEPT/REJECT in " + self.fout +\
                               " %d/%d, %d/%d"%(self.n_traj,count_traj,self.n_traj,count_gmes))
            
        if count_exit < 1:
            raise RuntimeError("HMC ok check fails: No exit in " + self.fout)
    
        if count_accept < self.minAR:
            raise RuntimeError("HMC ok check fails: %d acceptances < specified minimum %d"%(count_accept, self.minAR))
            
