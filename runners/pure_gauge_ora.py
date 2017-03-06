#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Call the MILC overrelaxed-and-heatbath pure gauge binary.
"""

# Exit codes
ERR_NONE = 0
ERR_OUTPUT_DNE = 1
ERR_BAD_OUTPUT = 2
ERR_LOAD_GAUGEFILE_DNE = 3
ERR_SAVE_GAUGEFILE_DNE = 4
ERR_SAVE_GAUGEFILE_ALREADY_EXISTS = 5
ERR_FOUT_ALREADY_EXISTS = 6
ERR_BINARY_DNE = 8

import os, sys
import platform
#import resource ### DEBUG

# Argparse not around until Python 2.7; bring our own argparse
if sys.version[:3] != "2.7":
    sys.path.insert(0, "/path/to/argparse.py")
import argparse

### Read in command line arguments
parser = argparse.ArgumentParser(description="Call the MILC ORA/HB binary to run a pure-gauge simulation.")

parser.add_argument('--cpus',   type=int,   default=1,     help='Number of CPUs to tell mpirun about.')
parser.add_argument('--binary', type=str,   required=True,  help='Path + filename to binary to run.')

parser.add_argument('--seed',   type=int,   required=True,  help='Seed for random number generator.')

parser.add_argument('--warms',  type=int,   default=0,   help='Number of trajectories to run in the beginning to warm up/burn in.')
parser.add_argument('--ntraj',  type=int,   default=100,  help='Number of trajectories to run (after warmup), total.')
parser.add_argument('--tpm',    type=int,   default=1,   help='Trajectories per measurement. 1 = measure every trajec, 2 = every other, ...')

parser.add_argument('--nsteps',    type=int,   default=4,  help='Number of overrelaxation steps per trajectory.')
parser.add_argument('--qhb_steps', type=int,   default=1,      help='Number of heat bath steps per trajectory.')


parser.add_argument('--Ns',       type=int,   required=True,  help='Number of lattice points in spatial direction.')
parser.add_argument('--Nt',       type=int,   required=True,  help='Number of lattice points in temporal direction.')
parser.add_argument('--beta',     type=float, required=True,  help='Lattice beta parameter.')

parser.add_argument('--fout',   type=str,   required=True,  help='Path + filename to write output to.')

parser.add_argument('--loadg',  type=str,   default=None,   help='Path + filename of gauge configuration to load and start from.  Fresh start if not provided.')
parser.add_argument('--saveg',  type=str,   required=True,  help='Path + filename to save final gauge configuration to.')

parg = parser.parse_args(sys.argv[1:])

   
### Function to produce input string for HMC
### Beware: long here-docs follow
def build_ora_input_string(Ns, Nt, beta,
                           seed, warms, ntraj, tpm,
                           nsteps, qhb_steps,
                           loadg, saveg, **kwargs):
    output_str = \
"""prompt 0
nx {Ns}
ny {Ns}
nz {Ns}
nt {Ns}

iseed {seed}

warms {warms}
trajecs {ntraj}
traj_between_meas {tpm}

beta {beta}
steps_per_trajectory {nsteps}
qhb_steps {qhb_steps}""".format(Ns=Ns, Nt=Nt, beta=beta, seed=seed,
          warms=warms, ntraj=ntraj, tpm=tpm, nsteps=nsteps, qhb_steps=qhb_steps)

    if loadg is None: 
        output_str += "fresh\n"
    else:
        output_str += "reload_serial {loadg}\n".format(loadg=loadg)

    output_str += "no_gauge_fix\n"
	
    if saveg is not None:
        output_str += "save_serial {saveg}\n".format(saveg=saveg)
    else:
        output_str += "forget\n"
    
    output_str += """
EOF
"""
    return output_str


## Function to check HMC output ran okay
def ora_check_ok(saveg, fout, ntraj):
    # Gauge file must exist
    if not os.path.exists(saveg):
        print "ORA ok check fails: Gauge file %s doesn't exist."%(saveg)
        return ERR_SAVE_GAUGEFILE_DNE
        
    # Output file must exist
    if not os.path.exists(fout):
        print "ORA ok check fails: Outfile %s doesn't exist."%(fout)
        return ERR_OUTPUT_DNE
    
    # Check for errors
    # Trailing space avoids catching the error_something parameter input
    with open(fout) as f:
        for line in f:
            if "error " in line:
                print "ORA ok check fails: Error detected in " + fout
                return ERR_BAD_OUTPUT
                
    # Check that the appropriate number of GMESes are present
    count_gmes = 0
    count_exit = 0
    with open(fout) as f:
        for line in f:
            if line.startswith("GMES"):
                count_gmes += 1
            elif line.startswith("exit: "):
                count_exit += 1     
                
    if count_gmes < ntraj:
        print "HMC ok check fails: Not enough GMES in " + fout + " %d/%d"%(ntraj, count_gmes)
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
            
## Check binary exists
if not os.path.exists(parg.binary):
    print "FATAL: Binary {binary} does not exist".format(binary=parg.binary)
    sys.exit(ERR_BINARY_DNE)

# Check load gauge file exists
if parg.loadg is not None and not os.path.exists(parg.loadg):
    print "FATAL: Gauge file {loadg} does not exist".format(loadg=parg.loadg)
    sys.exit(ERR_LOAD_GAUGEFILE_DNE)

# Never clobber a gauge config
# Check if job already done
if not os.path.exists(parg.fout):   
    if not os.path.exists(parg.saveg):
        # Desired case -- neither fout nor saveg exist already
        # Create output file as soon as we know it doesn't exist as a "file lock" on this task
        os.system("date > " + parg.fout) # Creates output file, puts date in    
    else:
        print "FATAL: Saved gauge file {saveg} already exists, output {fout} does not!".format(saveg=parg.saveg, fout=parg.fout)
        sys.exit(ERR_SAVE_GAUGEFILE_ALREADY_EXISTS)        
else:
    if os.path.exists(parg.saveg):
        print "Saved gauge file {saveg} and output {fout} already exist, checking if ok and exiting.".format(saveg=parg.saveg, fout=parg.fout)
        sys.exit(ora_check_ok(saveg=parg.saveg, fout=parg.fout, ntraj=parg.ntraj))
    else:
        print "FATAL: Output {fout} exists, saved gauge file {saveg} does not.  Multiple taxis tried this job?".format(saveg=parg.saveg, fout=parg.fout)
        sys.exit(ERR_FOUT_ALREADY_EXISTS)
    
input_str = build_ora_input_string(**vars(parg))

# Diagnostic output header, environment prep
print platform.node() #os.system("hostname")
os.system("date")
#resource.setrlimit(resource.RLIMIT_CORE, (0,-1)) #os.system("csh -c 'limit core 0'")
print "mynode", platform.node().split('.')[0] #os.system("set mynode = ` hostname -s `; echo mynode $mynode")
print "output " + parg.fout
print "running", parg.cpus, "nodes"
print "current working directory " + os.getcwd()

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
sys.exit(ora_check_ok(saveg=parg.saveg, fout=parg.fout, ntraj=parg.ntraj))
