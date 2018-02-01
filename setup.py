#!/usr/bin/env python

import distutils.cmd
import distutils.log
import setuptools

import os
import shutil
import errno
    
machine_list = ['scalar', 'cu_hep', 'fnal']

def symlink_with_overwrite(src, dst):
    assert os.path.exists(src), src
    if os.path.lexists(dst):
        os.remove(dst)
    os.symlink(os.path.abspath(src), dst)
    

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
            print "Error: invalid machine choice '{0}'".format(self.machine)
            print "Valid options:"
            for m in machine_list:
                print "- {}".format(m)

            raise SystemExit("Exiting...")

    def run(self):
        machine_files = [
            'local_queue.py',
            'local_taxi.py'
        ]
        if (self.machine == 'scalar'):
            machine_files.append('sqlite_queue_runner.py')

        if not os.path.exists('bin'):
            os.makedirs('bin')

        for f in machine_files:
            src = 'src/taxi/local/{0}/{1}'.format(self.machine, f)
            dst = 'src/taxi/local/{0}'.format(f)

            self.announce(
                src + '-->' + dst,
                level=distutils.log.INFO
            )

            shutil.copy(src, dst)
            #symlink_with_overwrite(src, dst)

        self.announce(
            'src/taxi/run_taxi.py --> bin/run_taxi.py',
            level=distutils.log.INFO
        )
        #shutil.copy('src/taxi/run_taxi.py', 'bin/run_taxi.py')
        symlink_with_overwrite('src/taxi/run_taxi.py', 'bin/run_taxi.py')

        self.announce(
            'src/taxi/local/%s/taxi.sh --> bin/taxi.sh'%self.machine,
            level=distutils.log.INFO
        )
        #shutil.copy('src/taxi/local/%s/taxi.sh'%self.machine, 'bin/taxi.sh')
        symlink_with_overwrite('src/taxi/local/%s/taxi.sh'%self.machine, 'bin/taxi.sh')

        self.announce(
            'Symlinked local files for machine {0}.'.format(self.machine),
            level=distutils.log.INFO
        )

setuptools.setup(
    name='taxi',
    version='0.2.0',

    cmdclass={
        'localize': LocalizeCommand,
    },

    # Find package in src/taxi
    package_dir={'':'src'},
    packages=setuptools.find_packages('src'),

    # Dependencies
    install_requires=[
        'argparse', 'parse'
    ],
    
    # Make taxi.sh and run_taxi.py available to run everywhere
    scripts=['bin/taxi.sh', 'bin/run_taxi.py',
             'src/taxi/tools/taxi-summary',
             'src/taxi/tools/taxi-edit',
             'src/taxi/tools/taxi-unabandon',
             'src/taxi/tools/taxi-rollback',
             'src/taxi/tools/taxi-spawn-idle-taxis']
)
