#!/nfs/beowulf03/dchackett/anaconda/bin/python
# -*- coding: utf-8 -*-
"""
Script to run a Wilson Flow binary on some gauge configuration.
"""

# Exit codes
ERR_NONE = 0
ERR_OUTPUT_DNE = 1
ERR_BAD_OUTPUT = 2
ERR_GAUGEFILE_DNE = 3
ERR_FOUT_ALREADY_EXISTS = 4

import os, sys
import platform
#import resource ### DEBUG

# Argparse not around until Python 2.7; bring our own argparse
if sys.version[:3] != "2.7":
    sys.path.insert(0, "/path/to/argparse.py")
import argparse

#### Read in command line arguments
parser = argparse.ArgumentParser(description="Do Wilson flow on some saved lattice.")

parser.add_argument('--cpus',   type=int,   required=True,  help='Number of CPUs to tell mpirun about.')
parser.add_argument('--binary', type=str,   required=True,  help='Path + filename of binary to run.')

parser.add_argument('--Nt',     type=int,   required=True,  help='Number of lattice points in temporal direction.')
parser.add_argument('--Ns',     type=int,   required=True,  help='Number of lattice points in spatial direction.')
parser.add_argument('--epsilon',type=float, default=0.01,   help='Epsilon parameter (i.e., flow timestep size).')
parser.add_argument('--tmax',   type=float, default=0,      help='Maximum flow time. Intelligent conditional stopping for tmax=0.')
parser.add_argument('--minE',   type=float, default=0,      help='Minimum t^2<E> to stop flowing. Ignored if tmax!=0.')
parser.add_argument('--mindE',  type=float, default=0,      help='Minimum t d/dt t^2<E> to stop flowing. Ignored if tmax!=0.')

parser.add_argument('--loadg',  type=str,   required=True,  help='Path + filename of gauge configuration to load and start from.')
parser.add_argument('--fout',   type=str,   required=True,  help='Path + filename to write out file to.')

parg = parser.parse_args(sys.argv[1:])
 
 
### Build input string to pass to Wilson Flow binary
### Beware, long here-docs follow
def build_flow_input_string(Ns, Nt, epsilon, tmax, minE, mindE, loadg, **kwargs):
    output_str = \
"""
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
""".format(Ns=Ns, Nt=Nt, epsilon=epsilon, tmax=tmax, minE=minE, mindE=mindE, loadg=loadg)
    
    return output_str


### Function to check that flow output exists, looks okay
### Return values meant to be passed to sys.exit()
def flow_check_ok(outfilename):
    # Output file must exist
    if not os.path.exists(outfilename):
        print "Flow check ok fails: Outfile %s doesn't exist."%(outfilename)
        return ERR_OUTPUT_DNE
    
    # Check for well-formed file
    with open(outfilename) as f:
        found_running_completed = False
        for line in f:
            # Trailing space avoids catching the error_something parameter input
            if "error " in line.lower():
                print "Flow check ok fails: Error detected in " + outfilename
                return ERR_BAD_OUTPUT
            found_running_completed |= ("RUNNING COMPLETED" in line)
            
        if not found_running_completed:
            print "Flow check ok fails: running did not complete in " + outfilename
            return ERR_BAD_OUTPUT
    
    return ERR_NONE
    
    
def mkdir_p(path):
    path = os.path.abspath(path)
    if not os.path.exists(path):
        os.makedirs(path)

   
   
### Body -- Set up environment for flow binary call, then call it

## Check load gauge file exists
if not os.path.exists(parg.loadg):
    print "FATAL: Gauge file {loadg} does not exist".format(loadg=parg.loadg)
    sys.exit(ERR_GAUGEFILE_DNE)
    
## Don't clobber output file (as a file-locking redundancy to DB serialization)
if not os.path.exists(parg.fout):
    # Create output file ASAP to prevent serialization mistakes
    os.system("date > " + parg.fout) # Creates output file, puts date in
else:
    print "FATAL: Output file {fout} already exists".format(fout=parg.fout)
    sys.exit(ERR_FOUT_ALREADY_EXISTS)

input_str = build_flow_input_string(**vars(parg))

# Diagnostic output header, environment prep
print platform.node() #os.system("hostname")
os.system("date")
#resource.setrlimit(resource.RLIMIT_CORE, (0,-1)) #os.system("csh -c 'limit core 0'")
print "mynode", platform.node().split('.')[0] #os.system("set mynode = ` hostname -s `; echo mynode $mynode")
print "output " + parg.fout
print "running", parg.cpus, "nodes"
print "current working directory " + os.getcwd()

# Make sure output dir exists
mkdir_p(os.path.split(parg.fout)[0])

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
sys.exit(flow_check_ok(parg.fout))
