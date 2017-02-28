#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Call a MILC spectroscopy binary to run a simulation using the provided parameters.

This script is specifically written to call the usual single-rep binaries.
"""

# Exit codes
ERR_NONE = 0
ERR_OUTPUT_DNE = 1
ERR_BAD_OUTPUT = 2
ERR_GAUGEFILE_DNE = 3
ERR_FOUT_ALREADY_EXISTS = 4
ERR_KAPPA_ZERO = 5
ERR_BINARY_DNE = 6

import os, sys
import platform
#import resource ### DEBUG

# Argparse not around until Python 2.7; bring our own argparse
if sys.version[:3] != "2.7":
    sys.path.insert(0, "/path/to/argparse.py")
import argparse

### Read in command line arguments
parser = argparse.ArgumentParser(description="Calculate spectroscopy from some gauge configuration.")
parser.add_argument('--cpus',   type=int,   required=True,  help='Number of CPUs to tell mpirun about.')
parser.add_argument('--binary', type=str,   required=True,  help='Path + filename of binary to run.')

parser.add_argument('--Nt',     type=int,   required=True,  help='Number of lattice points in temporal direction.')
parser.add_argument('--Ns',     type=int,   required=True,  help='Number of lattice points in spatial direction.')
parser.add_argument('--kappa',  type=float, required=True,  help='Kappa parameter.')
parser.add_argument('--r0',     type=float, required=True,  help='Source/sink smearing radius parameter.  If 0, uses "point" instead of "gaussian".')

parser.add_argument('--maxcgiter',  type=int, default=500,   help='Maximum number of CG iterations')

parser.add_argument('--loadg',  type=str,   required=True,  help='Path + filename of gauge configuration to load and start from.')
parser.add_argument('--fout',   type=str,   required=True,  help='Path + filename to write out file to.')

parser.add_argument('--loadp',  type=str,   default=None,   help='Path + filename of a saved propagator state to load start from.')
parser.add_argument('--savep',  type=str,   default=None,   help='Path + filename to save final propagator state to.')

parg = parser.parse_args(sys.argv[1:])
 
 
### Function to build input string for spectroscopy binary
### Beware, long here-docs to follow
def build_spectro_input_string(Ns, Nt, kappa, r0, maxcgiter,
                               loadg, loadp, savep, **kwargs):
    output_str = \
"""
prompt 0

nx {Ns}
ny {Ns}
nz {Ns}
nt {Nt}

number_of_kappas 1

kappa {kappa}
clov_c 1.0
u0 1.0
max_cg_iterations {maxcgiter}
max_cg_restarts 10
error_for_propagator 1e-06
""".format(Ns=Ns, Nt=Nt, kappa=kappa, maxcgiter=maxcgiter)

    if r0 == 0:
        output_str += "point\n"
    else:
        output_str += "gaussian\n"

    output_str += \
"""r0 {r0}

reload_serial {loadg}
coulomb_gauge_fix
forget
""".format(r0=r0, loadg=loadg)

    if loadp is not None:
        output_str += "reload_serial_wprop {loadp}\n".format(loadp=loadp)
    else:
        output_str += "fresh_wprop\n"
        
    if savep is not None:
        output_str += "save_serial_wprop {savep}\n".format(savep=savep)
    else:
        output_str += "forget_wprop\n"
        
    output_str += \
"""
serial_scratch_wprop w.scr
EOF
"""
    
    return output_str
  
  
### Function to check that spectroscopy output exists, looks okay
### Return values meant to be passed to sys.exit()
def spectro_check_ok(outfilename):
    # Output file must exist
    if not os.path.exists(outfilename):
        print "Spectro ok check fails: Outfile %s doesn't exist."%(outfilename)
        return ERR_OUTPUT_DNE
    
    # Check for well-formed file
    # Trailing space avoids catching the error_something parameter input
    with open(outfilename) as f:
        found_end_of_header = False
        found_running_completed = False
        for line in f:
            if "error " in line.lower():
                print "Spectro ok check fails: Error detected in " + outfilename
                return ERR_BAD_OUTPUT
            found_running_completed |= ("RUNNING COMPLETED" in line)
            found_end_of_header |= ("END OF HEADER" in line)
            
        if not found_end_of_header:
            print "Spectro ok check fails: did not find end of header in " + outfilename
            return ERR_BAD_OUTPUT
        if not found_running_completed:
            print "Spectro ok check fails: running did not complete in " + outfilename
            return ERR_BAD_OUTPUT
    
    return ERR_NONE
    
    
def mkdir_p(path):
    path = os.path.abspath(path)
    if not os.path.exists(path):
        os.makedirs(path)

   
   
### Body -- Set up the environment to run the spectroscopy binary, and then run it

## Check binary exists
if not os.path.exists(parg.binary):
    print "FATAL: Binary {binary} does not exist".format(binary=parg.binary)
    sys.exit(ERR_BINARY_DNE)
    
## Fail if we accidentally try to run with zero kappa
if parg.kappa == 0:
    print "FATAL: Tried to run spectroscopy with kappa=0"
    sys.exit(ERR_KAPPA_ZERO)

## Check load gauge file exists
if not os.path.exists(parg.loadg):
    print "FATAL: Gauge file {loadg} does not exist".format(loadg=parg.loadg)
    sys.exit(ERR_GAUGEFILE_DNE)
    
## Don't clobber output file (as a file-locking redundancy to DB serialization)
if not os.path.exists(parg.fout):
    # Create output file ASAP to prevent serialization mistakes
    os.system("date > " + parg.fout) # Creates output file, puts date in
else:
    print "Output file {fout} already exists, checking if okay and exiting".format(fout=parg.fout)
    sys.exit(spectro_check_ok(parg.fout))

input_str = build_spectro_input_string(**vars(parg))

# Diagnostic header output, environment prep
print platform.node() # == os.system("hostname")
os.system("date") # instead of using python datetime to make sure formatting is consistent
#resource.setrlimit(resource.RLIMIT_CORE, (0,-1)) # os.system("csh -c 'limit core 0'")
print "mynode", platform.node().split('.')[0] # os.system("set mynode = ` hostname -s `; echo mynode $mynode")
print "output " + parg.fout
print "running", parg.cpus, "nodes"
print "current working directory " + os.getcwd()

# Make sure output dir, propagator save dir exists
mkdir_p(os.path.split(parg.fout)[0])
if parg.savep is not None:
    mkdir_p(os.path.split(parg.savep)[0])

# Build MPI call string
# call_str = "/usr/local/mpich2-1.4.1p1/bin/mpirun -np %d "%parg.cpus   # CU cluster
# call_str = "mpirun -np %d "%parg.cpus   # Janus
call_str = "$MPI_BINARY -np %d "%parg.cpus
call_str += parg.binary + " << EOF >> " + parg.fout + "\n" + input_str

# Run
os.system(call_str)
#print call_str

# Final diagnostic output
os.system("date >> " + parg.fout)
sys.exit(spectro_check_ok(parg.fout))
