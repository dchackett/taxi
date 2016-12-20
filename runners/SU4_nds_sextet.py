#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Call a multirep MILC binary to run a simulation using the provided parameters.

This script is specifically written to call a binary to run Hybrid-Monte Carlo\
simulations of SU(4) theory with the NDS (dislocation suppressing) action.

Hasenbusch preconditioning enabled, here. (Which seems to be about a factor of\
two slower for the multirep theory).
"""

# Exit codes
ERR_NONE = 0
ERR_OUTPUT_DNE = 1
ERR_BAD_OUTPUT = 2
ERR_LOAD_GAUGEFILE_DNE = 3
ERR_SAVE_GAUGEFILE_DNE = 4
ERR_SAVE_GAUGEFILE_ALREADY_EXISTS = 5
ERR_FOUT_ALREADY_EXISTS = 6

import os, sys
import platform
#import resource ### DEBUG

# Argparse not around until Python 2.7; bring our own argparse
if sys.version[:3] != "2.7":
    sys.path.insert(0, "/path/to/argparse.py")
import argparse

### Read in command line arguments
parser = argparse.ArgumentParser(description="Run a Hybrid Monte Carlo simulation in SU(4) with fundamental and as2 fermions.")

parser.add_argument('--cpus',   type=int,   default=-1,     help='Number of CPUs to tell mpirun about.')
parser.add_argument('--binary', type=str,   required=True,  help='Path + filename to binary to run.')

parser.add_argument('--seed',   type=int,   required=True,  help='Seed for random number generator.')

parser.add_argument('--warms',  type=int,   default=0,   help='Number of trajectories to run in the beginning to warm up/burn in.')
parser.add_argument('--ntraj',  type=int,   default=10,  help='Number of trajectories to run (after warmup), total.')
parser.add_argument('--nsafe',  type=int,   default=5,  help='Run a safe trajectory every N trajectories.')
parser.add_argument('--tpm',    type=int,   default=1,   help='Trajectories per measurement. 1 = measure every trajec, 2 = every other, ...')
parser.add_argument('--trajL',  type=float, default=1.0, help='Length of HMC trajectory (always 1).')

parser.add_argument('--nsteps1',  type=int,   required=True,  help='Number of steps between full D^-2 evals where we evaluate (D^2 + m^2 / D^2)^-1 instead.')
parser.add_argument('--nsteps2',  type=int,   default=None,   help='No Hasenbuch if not provided. Number of gauge steps between (D^2 + m^2 / D^2)^-1 evaluations.')
parser.add_argument('--nstepsg',  type=int,   default=6,      help='Number of gauge steps to run.')
parser.add_argument('--shift',    type=float, default=0.,     help='Fake m to use in D^2 + m^2.')

parser.add_argument('--maxcgobs',  type=int,   default=500,  help='Maximum number of CG iterations to run for fermion observables.')
parser.add_argument('--maxcgpf',   type=int,   default=500,  help='Maximum number of CG iterations to run for pseudofermions.')

parser.add_argument('--Ns',       type=int,   required=True,  help='Number of lattice points in spatial direction.')
parser.add_argument('--Nt',       type=int,   required=True,  help='Number of lattice points in temporal direction.')
parser.add_argument('--beta',     type=float, required=True,  help='Lattice beta parameter.')
parser.add_argument('--k4',       type=float, required=True,  help='(IGNORED, left for convenience) Kappa parameter for fundamental fermions.')
parser.add_argument('--k6',       type=float, required=True,  help='Kappa parameter for 2-index antisymmetric fermions.')
parser.add_argument('--gammarat', type=float, default =125.,  help='NDS action parameter. beta/gamma = gammarat, so gamma = beta/gammarat.')

parser.add_argument('--fout',   type=str,   required=True,  help='Path + filename to write output to.')

parser.add_argument('--loadg',  type=str,   default=None,   help='Path + filename of gauge configuration to load and start from.  Fresh start if not provided.')
parser.add_argument('--saveg',  type=str,   required=True,  help='Path + filename to save final gauge configuration to.')

parg = parser.parse_args(sys.argv[1:])

   
### Function to produce input string for HMC
### Beware: long here-docs follow
def build_hmc_input_string(Ns, Nt, beta, gammarat, k4, k6,
                           seed, warms, ntraj, trajL, tpm, nsafe,
                           nsteps1, nsteps2, nstepsg, shift,
                           maxcgobs, maxcgpf,
                           loadg, saveg, **kwargs):
    output_str = \
"""
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
error_per_site 1.0e-06
    
## dynamical fermions
nkappas 1""".format(Ns=Ns, Nt=Nt, seed=seed, beta=beta, gammarat=gammarat, maxcgobs=maxcgobs)

    if nsteps2 is None:
        # No Hasenbuch
        shift = 0 # Force this to avoid another conditional lower down
        output_str += \
"""
npseudo 1
nlevels 1
"""
    else:
        # Hasenbuch preconditioning enabled, one level
        output_str += \
"""
npseudo 2
nlevels 2
"""
    
    output_str += \
""" 
kpid kap_as        # identifier
kappa {k6}         # hopping parameter
csw 1.0            # clover coefficient
#mu 0.0             # chemical potential
nf 2               # how many Dirac flavors
irrep asym
    
## pf actions
pfid onem_as       # a unique name, for identification in the outfile
type onemass       # pseudofermion action types: onemass twomass rhmc
kpid kap_as        # identifier for the fermion action
multip 1           # how many copies of this pseudofermion action will exist
level 1            # which MD update level
shift1 {shift}     # for simulating det(M^dag M + shift1^2)
iters {maxcgpf}    # CG iterations
rstrt 10           # CG restarts
resid 1.0e-6       # CG stopping condition
""".format(k6=k6, shift=shift, maxcgpf=maxcgpf)

    if nsteps2 is not None:
        # Second pseudofermion level for Hasenbuch
        output_str += \
"""
pfid twom_as       # a unique name, for identification in the outfile
type twomass       # pseudofermion action types: onemass twomass rhmc
kpid kap_as        # identifier for the fermion action
multip 1           # how many copies of this pseudofermion action will exist
level 2            # which MD update level
shift1 .0          # simulates det(M^dag M + shift1^2)/det(M^dag M + shift2^2)
shift2 {shift}
iters {maxcgpf}    # CG iterations
rstrt 10           # CG restarts
resid 1.0e-6       # CG stopping condition
""".format(shift=shift, maxcgpf=maxcgpf)

    output_str += \
"""
## update params
warms {warms}
trajecs {ntraj}
traj_length {trajL}
traj_between_meas {tpm}
    
nstep {nsteps1}
""".format(warms=warms, ntraj=ntraj, trajL=trajL, tpm=tpm, nsteps1=nsteps1)

    if nsteps2 is not None:
        output_str += "nstep {nsteps2}\n".format(nsteps2=nsteps2)
    
    output_str += \
"""nstep_gauge {nstepsg}
ntraj_safe {nsafe}
nstep_safe {nsteps_safe}

## load/save
""".format(nstepsg=nstepsg, nsafe=nsafe, nsteps_safe = nsteps1*2)

    if loadg is None: 
        output_str += "fresh   # all-identity start\n"
    else:
        output_str += "reload_serial {loadg}   # gauge load\n".format(loadg=loadg)
    
    output_str += "save_serial {saveg}   # gauge save\n".format(saveg=saveg)
    
    output_str += \
"""
EOF
"""
    
    return output_str


## Function to check HMC output ran okay
def hmc_check_ok(saveg, fout, ntraj):
    # Gauge file must exist
    if not os.path.exists(saveg):
        print "HMC ok check fails: Gauge file %s doesn't exist."%(saveg)
        return ERR_SAVE_GAUGEFILE_DNE
        
    # Output file must exist
    if not os.path.exists(fout):
        print "HMC ok check fails: Outfile %s doesn't exist."%(fout)
        return ERR_OUTPUT_DNE
    
    # Check for errors
    # Trailing space avoids catching the error_something parameter input
    with open(fout) as f:
        for line in f:
            if "error " in line:
                print "HMC ok check fails: Error detected in " + fout
                return ERR_BAD_OUTPUT
                
    # Check that the appropriate number of GMESes are present
    count_gmes = 0
    count_traj = 0
    count_exit = 0
    with open(fout) as f:
        for line in f:
            if line.startswith("GMES"):
                count_gmes += 1
            elif line.startswith("exit: "):
                count_exit += 1     
            elif line.startswith("ACCEPT") or line.startswith("SAFE_ACCEPT") \
               or line.startswith("REJECT") or line.startswith("SAFE_REJECT"):
                count_traj += 1
                
    if count_traj < ntraj:
        print "HMC ok check fails: Not enough GMES in " + fout + " %d/%d"%(ntraj,count_traj)
        return ERR_BAD_OUTPUT
        
    if count_exit < 1:
        print "HMC ok check fails: No exit in " + fout
        return ERR_BAD_OUTPUT
        
    return ERR_NONE
    

def mkdir_p(path):
    path = os.path.abspath(path)
    if not os.path.exists(path):
        os.makedirs(path)
            
### Body

# Never clobber a gauge config
# Check if job already done
if os.path.exists(parg.saveg):
    if not os.path.exists(parg.fout):
        print "FATAL: Saved gauge file {saveg} already exists, output {fout} does not!".format(saveg=parg.saveg, fout=parg.fout)
        sys.exit(ERR_SAVE_GAUGEFILE_ALREADY_EXISTS)
    else:
        print "Saved gauge file {saveg} and output {fout} already exist, checking if ok and exiting.".format(saveg=parg.saveg, fout=parg.fout)
        sys.exit(hmc_check_ok(saveg=parg.saveg, fout=parg.fout, ntraj=parg.ntraj))
else:
    if os.path.exists(parg.fout):
        print "FATAL: Output {fout} exists, saved gauge file {saveg} does not.  Multiple taxis tried this job?".format(saveg=parg.saveg, fout=parg.fout)
        sys.exit(ERR_FOUT_ALREADY_EXISTS)
        
# Check load gauge file exists
if parg.loadg is not None and not os.path.exists(parg.loadg):
    print "FATAL: Gauge file {loadg} does not exist".format(loadg=parg.loadg)
    sys.exit(ERR_LOAD_GAUGEFILE_DNE)
    
input_str = build_hmc_input_string(**vars(parg))

# Diagnostic output header, environment prep
print platform.node() #os.system("hostname")
os.system("date")
#resource.setrlimit(resource.RLIMIT_CORE, (0,-1)) #os.system("csh -c 'limit core 0'")
print "mynode", platform.node().split('.')[0] #os.system("set mynode = ` hostname -s `; echo mynode $mynode")
print "output " + parg.fout
print "running", parg.cpus, "nodes"
print "current working directory " + os.getcwd()
os.system("date > " + parg.fout) # Creates output file, puts date in

# Make sure output dirs exist
mkdir_p(os.path.split(parg.fout)[0])
mkdir_p(os.path.split(parg.saveg)[0])

# Call mpi
#call_str = "/usr/local/mpich2-1.4.1p1/bin/mpirun -np %d "%parg.cpus   # CU cluster
#call_str = "mpirun -np %d "%parg.cpus   # Janus
call_str = "$MPI_BINARY -np %d "%parg.cpus
call_str += parg.binary + " << EOF >> " + parg.fout + "\n" + input_str

# Run
os.system(call_str)
#print call_str

# Final diagnostic output
os.system("date >> " + parg.fout)
sys.exit(hmc_check_ok(saveg=parg.saveg, fout=parg.fout, ntraj=parg.ntraj))
