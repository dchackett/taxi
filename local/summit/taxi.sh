#!/bin/bash

#SBATCH -p shas

TaxiInstallDir='/projects/etne1079/taxi/'

# Put taxi install dir in PYTHONPATH if not there already
if ! echo $PYTHONPATH | grep -q $TaxiInstallDir; then
    export PYTHONPATH="${PYTHONPATH}:${TaxiInstallDir}"
fi

## Determine number of nodes, number of cpus
nNodes=$SLURM_JOB_NUM_NODES
(( nNodes= 0 + nNodes ))
coresPerNode=$SLURM_CPUS_ON_NODE
(( nCores = nNodes * coresPerNode ))

## Need to know where this script is to resubmit it
ThisScript="${TaxiInstallDir}/taxi.sh"

## Settings for Intel MPI - hopefully these will become generic when Summit enters production
export MPI_BINARY='mpirun -genv I_MPI_FABRIC=shm:tmi -genv I_MPI_TMI_PROVIDER=psm2'

python ${TaxiInstallDir}/taxi.py --nodes $nNodes --cpus $nCores --name $SLURM_JOB_NAME --shell $ThisScript $*
