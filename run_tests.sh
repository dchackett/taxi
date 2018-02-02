#!/bin/bash

# Install as user if not in a virtualenv
if [[ "$VIRTUAL_ENV" == "" ]; then
    MY_USER_FLAG="--user" # No virtualenv active, install as user
else
    MY_USER_FLAG="" # virtualenv active, do not install as user
fi

# Check if tox is installed before trying to run tests; install if not
if ! pip list | tail -n +1 | grep -q tox; then
    pip install tox $MY_USER_FLAG
fi

# Clean up working dir
rm -r build bin dist test_run
python setup.py localize --machine=scalar
python setup.py install $MY_USER_FLAG
tox

