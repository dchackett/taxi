#!/usr/bin/env python
from taxi import fixable_dynamic_attribute
from taxi.binary_menu import BinaryMenu, binary_from_binary_menu
from taxi.mcmc import ConfigGenerator
from taxi.file import File, InputFile, should_save_file, should_load_file

import conventions

# Structure of input file - see build_input_string() below
hmc_input_template = """
prompt 0

nflavors 2
nx {Ns}
ny {Ns}
nz {Ns}
nt {Nt}    
iseed {seed}
warms {warms}
trajecs {n_traj}
traj_length {trajL}
number_of_PF {npseudo}
nstep {nsteps1}
nstep {_nsteps2}
nstep_gauge {nsteps_gauge}
ntraj_safe {n_safe}
nstep_safe {nsteps_safe}
traj_between_meas {tpm}
    
beta {beta}
kappa {kappa}
{_shift}
clov_c 1.0
u0 1

max_cg_iterations {maxcgobs}
max_cg_restarts 10
error_per_site {cgtol}
error_for_propagator {cgtol}
    
{load_gauge}
{save_gauge}

EOF
"""



class LargeNHMCTask(ConfigGenerator, conventions.LargeN):
    loadg = InputFile(conventions.loadg_convention)
    
    fout = File(conventions.fout_convention)
    fout_filename_prefix = conventions.hmc_fout_filename_prefix
    
    saveg = File(conventions.saveg_convention)
    saveg_meta = File(conventions.saveg_convention+'.info') # Gauge metainfo file -- want to copy this along with gauge files
    saveg_filename_prefix = conventions.saveg_filename_prefix
    
    _binary_menu = BinaryMenu(default_dict={'enable_metropolis' : True, 'irrep' : 'f'}) # Load with binaries in run-spec scripts
    binary = binary_from_binary_menu(_binary_menu, key_attr_names=['Nc', 'irrep', 'enable_metropolis'])
    
    # Required params, checked to be present and not None at dispatch compile time
    _required_params = ['binary', 'Nc', 'Ns', 'Nt', 'beta', 'kappa', 'irrep', 'label', 'nsteps1']
    
    def __init__(self,
                 # Application-specific required arguments
                 Nc=None, Ns=None, Nt=None, beta=None, kappa=None, irrep=None, label=None, nsteps1=None,
                 # Application-specific defaults
                 trajL=1.0,
                 warms=0, nsteps_gauge=6, n_safe=5, safe_factor=4, tpm=1,
                 nsteps2=None, shift=0.2, # Hasenbuch preconditioning
                 enable_metropolis=True, minAR=4,
                 maxcgobs=500, maxcgpf=500, cgtol=1e-6,
                 # Override ConfigGenerator defaults (Must pass to superconstructor manually)
                 req_time=600, n_traj=10,
                 # Arguments to pass along to superclass
                 **kwargs):
        """Run the LargeN MILC HMC binary, which only runs Nf=2.
        Naming conventions intended for large N thermodynamics study.
        
        Instead of having to specify the binary and output file prefixes, these
        are dynamically determined from physical flags. Must have binaries specified
        in self.binary_menu for the key Nc, irrep, and enable_metropolis.
        
        Args (superclass):
            seed (required): Seed for random number generator
            starter (required): None, a filename, or a ConfigGenerator.
            start_traj (required if starter is a filename): Trajectory number of
                provided starter configuration.
            n_traj (int): Number of trajectories to run (after warmup) in total
        Args (this application):
            Nc (int): Number of colors SU(Nc). Used to decipher numerically-labeled irreps, for filenames, and for binary selection.
            Ns (int): Number of lattice points in spatial direction
            Nt (int): Number of lattice points in temporal direction
            beta:  Lattice beta parameter
            kappa: Kappa parameter for fermions
            irrep: Irrep of fermion for binary selection ('f', 'as2'; also supports 'g', 's2', but must override default naming conventions!)
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
        super(LargeNHMCTask, self).__init__(req_time=req_time, n_traj=n_traj, **kwargs)

        self.Nc = Nc
        self.Nf = 2 # Hardcoded, necessary for file naming conventions
        self.Ns = Ns
        self.Nt = Nt
        self.beta = beta
        self.label = label
        
        # Kappa and irrep logic
        self.kappa = kappa
        self.irrep = irrep
        
        self.nsteps1 = nsteps1
        self.nsteps2 = nsteps2
        self.nsteps_gauge = nsteps_gauge
        self.shift = shift
        if nsteps2 is None:
            shift = 0 # No shift if not using Hasenbuch
        self.trajL = trajL
        
        self.n_safe = n_safe
        self.safe_factor = safe_factor
        self.tpm = tpm
        self.warms = warms
        self.minAR = minAR
        self.enable_metropolis = enable_metropolis
        
        self.maxcgobs = maxcgobs
        self.maxcgpf = maxcgpf
        self.cgtol = cgtol
        
    
    # Overridable dynamic attribute for nsteps_safe, uses nsteps1*safe_factor
    def _dynamic_get_nsteps_safe(self):
        return self.nsteps1 * self.safe_factor
    nsteps_safe = fixable_dynamic_attribute(private_name='_nsteps_safe', dynamical_getter=_dynamic_get_nsteps_safe)
        
    
    def build_input_string(self):
        input_str = super(LargeNHMCTask, self).build_input_string()
        
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
            input_dict['npseudo'] = 1
            input_dict['_nsteps2'] = '1 # ignored for number_of_PF=1'.format(n2=self.nsteps2)
            input_dict['_shift'] = ''
        else:
            input_dict['npseudo'] = 2
            input_dict['_nsteps2'] = '{n2}'.format(n2=self.nsteps2)
            input_dict['_shift'] = 'shift {s}'.format(s=self.shift)
            
        input_str += hmc_input_template.format(**input_dict)

        return input_str
    

    def verify_output(self):
        ## In the future, we can define custom exceptions to distinguish the below errors, if needed
        super(LargeNHMCTask, self).verify_output()

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
        unitarity_violation = False
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
                elif line.startswith("Unitarity problem on node"):
                    unitarity_violation = True
                    
        if unitarity_violation:
            raise RuntimeError("HMC ok check fails: unitarity violation detected")
            
        if count_gmes < self.n_traj:
            raise RuntimeError("HMC ok check fails: Not enough GMES in " + self.fout +\
                               " %d/%d, %d/%d"%(self.n_traj,count_traj,self.n_traj,count_gmes))
        
        if count_exit < 1:
            raise RuntimeError("HMC ok check fails: No exit in " + self.fout)
    
        # If metropolis not enabled, then no accepts/rejects to count
        if self.enable_metropolis and count_traj < self.n_traj:
            raise RuntimeError("HMC ok check fails: Not enough ACCEPT/REJECT in " + self.fout +\
                               " %d/%d, %d/%d"%(self.n_traj,count_traj,self.n_traj,count_gmes))

        if self.enable_metropolis and count_accept < self.minAR:
            raise RuntimeError("HMC ok check fails: %d acceptances < specified minimum %d"%(count_accept, self.minAR))
            
