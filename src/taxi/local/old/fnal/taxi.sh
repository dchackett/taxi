#!/bin/bash

#    Remarks on options:
#       -S Chooses the shell
#       -j Joins output and error files 
#       -m Chooses messaging options. "n" disables email messaging.
#       -A Chooses the allocation for the submission
#       -v Passes the folowing environmental variables to the parallel jobs
#       -q Run on bc

#PBS -S /bin/bash
#PBS -j oe
#PBS -m n
#PBS -v PATH,LD_LIBRARY_PATH,PV_NCPUS,PV_LOGIN,PV_LOGIN_PORT
#PBS -A multirep
#PBS -q bc

TaxiInstallDir="/nfs/beowulf03/dchackett/taxi/"

# Put taxi install dir in PYTHONPATH if not there already
if ! echo $PYTHONPATH | grep -q $TaxiInstallDir; then
    export PYTHONPATH="${PYTHONPATH}:${TaxiInstallDir}"
fi

# Pick the correct mpirun binary (Prevents needing different runner scripts for CU/Janus/FNAL/etc)
export MPI_BINARY='/usr/local/mvapich/bin/mpirun'

# Figure out where this script is
ThisScript="${TaxiInstallDir}/taxi.sh"

# # These should work, but they don't
# nNodes=$PBS_NUM_NODES
# nCores=$PBS_NP

# From USQCD batch submission example (and auto-pbs):
# determine number of cores per host
coresPerNode=`cat /proc/cpuinfo | grep -c processor`
# count the number of nodes listed in PBS_NODEFILE
nNodes=$[`cat ${PBS_NODEFILE} | wc --lines`]
(( nNodes= 0 + nNodes ))
(( nCores = nNodes * coresPerNode ))
echo "NODEFILE nNodes=$nNodes ($nCores cores)"

# Call python script
python ${TaxiInstallDir}/taxi.py --nodes $nNodes --cpus $nCores --name $PBS_JOBNAME --shell $ThisScript $*
