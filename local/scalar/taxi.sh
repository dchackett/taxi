#!/bin/bash

TaxiInstallDir='/home/eneil/python/taxi'

# Put taxi install dir in PYTHONPATH if not there already
if ! echo $PYTHONPATH | grep -q $TaxiInstallDir; then
    export PYTHONPATH="${PYTHONPATH}:${TaxiInstallDir}"
fi

python ${TaxiInstallDir}/run_taxi.py $@