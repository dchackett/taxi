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
#PBS -v LD_LIBRARY_PATH,PV_NCPUS,PV_LOGIN,PV_LOGIN_PORT
#PBS -q bc

# HACKED-IN: Load up virtualenv
source $TAXI_PYENV/bin/activate

# TODO: Below block currently unused
# From USQCD batch submission example (and auto-pbs):
# determine number of cores per host
coresPerNode=`cat /proc/cpuinfo | grep -c processor`
# count the number of nodes listed in PBS_NODEFILE
nNodes=$[`cat ${PBS_NODEFILE} | wc --lines`]
(( nNodes= 0 + nNodes ))
(( nCores = nNodes * coresPerNode ))
echo "NODEFILE nNodes=$nNodes ($nCores cores)"

# "which" that doesn't care about executability
run_taxi_path=$(find ${PATH//:/ } -maxdepth 1 -name run_taxi.py -print -quit)

python $run_taxi_path $@