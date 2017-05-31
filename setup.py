#!/usr/bin/env python

import distutils.cmd
import distutils.log
import setuptools

import os
import shutil
import errno
    
machine_list = ['scalar', 'cu_hep']

class LocalizeCommand(distutils.cmd.Command):
    """Custom command that localizes Taxi install for a particular machine."""

    description = 'Custom command that localizes Taxi install for a particular machine.'
    user_options = [
        ('machine=', None, 'Machine label to localize for.'),
    ]

    def initialize_options(self):
        """Set default values for options."""
        self.machine = 'scalar'

    def finalize_options(self):
        """Post-processing."""
        try:
            assert self.machine in machine_list
        except AssertionError:
            print "Error: invalid machine choice '{}'".format(self.machine)
            print "Valid options:"
            for m in machine_list:
                print "- {}".format(m)

            raise SystemExit("Exiting...")

    def run(self):
        machine_files = [
            'local_queue.py',
            'local_taxi.py',
            'taxi.sh',
        ]
        if (self.machine == 'scalar'):
            machine_files.append('sqlite_queue_runner.py')

        if not os.path.exists('bin'):
            os.makedirs('bin')

        for f in machine_files:
            src = 'src/taxi/local/{}/{}'.format(self.machine, f)
            if f == 'taxi.sh':
                dst = 'bin/taxi.sh'
            else:
                dst = 'src/taxi/local/{}'.format(f)

            self.announce(
                src + '-->' + dst,
                level=distutils.log.INFO
            )

            shutil.copy(src, dst)

        shutil.copy('src/taxi/run_taxi.py', 'bin/run_taxi.py')

        self.announce(
            'Symlinked local files for machine {}.'.format(self.machine),
            level=distutils.log.INFO
        )

setuptools.setup(
    cmdclass={
        'localize': LocalizeCommand,
    }
)
