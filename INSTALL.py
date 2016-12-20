# -*- coding: utf-8 -*-
"""
Created on Fri Dec 16 16:08:27 2016

@author: Dan
"""

import os
import sys
import argparse

### Parse command line arguments
parser = argparse.ArgumentParser(description="Workflow Installer")

parser.add_argument('--machine', type=str, required=True, help='What machine are we installing on?  Implemented options: cu, janus, fnal')
parser.add_argument('--path', type=str, default='.', help='Folder where library is (by default, the folder where this file sits); probably never change this.')

print sys.argv
parg = parser.parse_args(sys.argv[1:]) # Call looks like "python install.py ...args..."

### Get install arguments in convenient form
machine = parg.machine.lower()
machine_subdir = machine # trivial for now, but could be convenient
install_dir = os.path.abspath(parg.path)


### Implementation check
if machine not in ['cu', 'janus', 'fnal']:
    raise Exception("Machine '{machine}' not implemented.".format(machine=parg.machine))
    

### Make soft links to appropriate localized files
def make_soft_link(target, link):
    target = os.path.abspath(target)
    link = os.path.abspath(link)
    if os.path.exists(link):
        os.unlink(link) # os.symlink won't clobber existing links
    os.symlink(target, link)

make_soft_link(target=install_dir+'/local/'+machine_subdir+'/local_taxi.py', link=install_dir+'/local_taxi.py')
make_soft_link(target=install_dir+'/local/'+machine_subdir+'/local_util.py', link=install_dir+'/local_util.py')
make_soft_link(target=install_dir+'/local/'+machine_subdir+'/taxi.sh', link=install_dir+'/taxi.sh')


### Edit taxi.sh for TaxiInstallDir
with open(install_dir+'/taxi.sh', 'r') as f:
    file_lines = f.readlines()
    
found = False
for ll, line in enumerate(file_lines):
    if line.startswith("TaxiInstallDir="):
        found = True
        break
    
# Put correct path in contents and overwrite
file_lines[ll] = "TaxiInstallDir='{tid}'\n".format(tid=install_dir)
with open(install_dir+'/taxi.sh', 'w') as f:
    f.writelines(file_lines)

