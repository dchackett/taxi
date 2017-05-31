#!/bin/bash

TaxiInstallDir="/nfs/beowulf03/dchackett/taxi/"

# Put taxi install dir in PYTHONPATH if not there already
if ! echo $PYTHONPATH | grep -q $TaxiInstallDir; then
    export PYTHONPATH="${PYTHONPATH}:${TaxiInstallDir}"
fi

# Pick the correct mpirun binary (Prevents needing different runner scripts for CU/Janus/etc)
#alias mpirun=/usr/local/mpich2-1.4.1p1/bin/mpirun
export MPI_BINARY='/usr/local/mpich2-1.4.1p1/bin/mpirun'

# Figure out where this script is
ThisScript="${TaxiInstallDir}/taxi.sh"

# Call python script.  Hardcode 1 node == 8 cores.
python ${TaxiInstallDir}/taxi.py --nodes 1 --cpus 8 --name $JOB_NAME --shell $ThisScript $*
