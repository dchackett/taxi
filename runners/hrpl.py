#!/nfs/beowulf03/dchackett/anaconda/bin/python
# -*- coding: utf-8 -*-
"""
Script to run the Higher-Rep Polyakov Loop binary on some gauge configuration.
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
parser = argparse.ArgumentParser(description="Measure higher-rep Polyakov loops on some saved lattice.")

parser.add_argument('--cpus',   type=int,   required=True,  help='Number of CPUs to tell mpirun about.')
parser.add_argument('--binary', type=str,   required=True,  help='Path + filename of binary to run.')

parser.add_argument('--Nt',     type=int,   required=True,  help='Number of lattice points in temporal direction.')
parser.add_argument('--Ns',     type=int,   required=True,  help='Number of lattice points in spatial direction.')

parser.add_argument('--loadg',  type=str,   required=True,  help='Path + filename of gauge configuration to load and start from.')
parser.add_argument('--fout',   type=str,   required=True,  help='Path + filename to write out file to.')

parg = parser.parse_args(sys.argv[1:])
 
 
### Build input string to pass to Higher-Rep Polyakov Loop binary
### Beware, long here-docs follow
def build_hrpl_input_string(Ns, Nt, loadg, **kwargs):
    output_str = \
"""
prompt 0
    
nx {Ns}
ny {Ns}
nz {Ns}
nt {Nt}
    
reload_serial {loadg}

EOF
""".format(Ns=Ns, Nt=Nt, loadg=loadg)
    
    return output_str


### Function to check that flow output exists, looks okay
### Return values meant to be passed to sys.exit()
def hrpl_check_ok(outfilename):
    # Output file must exist
    if not os.path.exists(outfilename):
        print "HRPL check ok fails: Outfile %s doesn't exist."%(outfilename)
        return ERR_OUTPUT_DNE
    
    # Check for well-formed file
    with open(outfilename) as f:
        found_hrpls = []
        for line in f:
            # Trailing space avoids catching the error_something parameter input
            if "error " in line.lower():
                print "HRPL check ok fails: Error detected in " + outfilename
                return ERR_BAD_OUTPUT
            if line.startswith("HR"):
                found_hrpls.append(line.split()[0])
        
        for demand_hrpl in ['HR_PLPT_F', 'HR_PLPT_S2', 'HR_PLPT_A2', 'HR_PLPT_G']:
            if demand_hrpl not in found_hrpls:
                print "HRPL check ok fails: Missing loop " + demand_hrpl
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

input_str = build_hrpl_input_string(**vars(parg))

# Diagnostic output header, environment prep
print platform.node() #os.system("hostname")
os.system("date")
#resource.setrlimit(resource.RLIMIT_CORE, (0,-1)) #os.system("csh -c 'limit core 0'")
print "mynode", platform.node().split('.')[0] #os.system("set mynode = ` hostname -s `; echo mynode $mynode")
print "output " + parg.fout
print "running", parg.cpus, "nodes"
print "current working directory " + os.getcwd()
os.system("date > " + parg.fout) # Creates output file, puts date in

# Make sure output dir exists
mkdir_p(os.path.split(parg.fout)[0])

# Call mpi
# call_str = "/usr/local/mpich2-1.4.1p1/bin/mpirun -np %d "%parg.cpus   # CU cluster
# call_str = "mpirun -np %d "%parg.cpus   # Janus
call_str = "$MPI_BINARY -np %d "%parg.cpus
call_str += parg.binary + " << EOF >> " + parg.fout + "\n" + input_str

# Run
os.system(call_str)
#print call_str

# Final diagnostic output
os.system("date >> " + parg.fout)
sys.exit(hrpl_check_ok(parg.fout))
