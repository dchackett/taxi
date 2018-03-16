#!/usr/bin/env python2
# -*- coding: utf-8 -*-


## File name conventions
base = 'SU{Nc:d}N{irrep_fnc}{Nf:d}_{Ns:d}x{Nt:d}_b{beta:g}_k{irrep_fnc}{kappa:g}_{label}_{traj:d}'
spectro_base = '{irrep}_r{r0:g}_' + base

fout_convention = '{fout_filename_prefix}_' + base
hmc_fout_filename_prefix = 'hmc'
flow_fout_filename_prefix = 'flow'
qpot_fout_filename_prefix = 'qpot'

spectro_fout_convention = '{fout_filename_prefix}_' + spectro_base
# fout_filename_prefix for spectro outputs is dynamically generated

loadg_convention   = '{loadg_filename_prefix}_' + base
# Don't need a filename prefix for InputFiles

saveg_convention   = '{saveg_filename_prefix}_' + base
saveg_filename_prefix = 'cfg'

loadp_convention = '{loadp_filename_prefix}_' + spectro_base
# Don't need a filename prefix for InputFiles

savep_convention = '{savep_filename_prefix}_' + spectro_base
savep_filename_prefix = 'prop'


## Convenient to work with irreps in standard format
# Synonyms for irrep names
F_irrep_names = ['f', 'fund', 'fundamental']
A2_irrep_names = ['a2', 'as2', 'as', 'antisymmetric']
G_irrep_names = ['g', 'adjt', 'adjoint']
S2_irrep_names = ['s', 's2', 'symmetric']
irrep_names = {'f' : F_irrep_names, 'a2' : A2_irrep_names, 'g' : G_irrep_names, 's2' : S2_irrep_names}

# MILC conventions for each irrep
milc_irrep_names = {'f' : 'fund', 'a2' : 'asym', 's2' : 'symm', 'g' : 'adjt'}

# Irrep names for filenames
fnc_irrep_names = {'f' : 'f', 'a2' : 'as', 's2' : 's', 'g' : 'g'}
invert_fnc_irrep_names = {v:k for (k,v) in fnc_irrep_names.items()}

# Irreps from dimensions
def dim_F(Nc):
    return Nc
def dim_A2(Nc):
    return 0.5 * Nc * (Nc - 1)
def dim_S2(Nc):
    return 0.5 * Nc * (Nc + 1)
def dim_G(Nc):
    return Nc**2 - 1
def irrep_from_dim(dim_irrep, Nc):
    irrep_dims = {dim_F(Nc) : 'f', dim_A2(Nc) : 'a2', dim_S2(Nc) : 's2', dim_G(Nc) : 'g'}
    if not irrep_dims.has_key(dim_irrep):
        raise ValueError("dim(irrep)={} not in dimensions for common irreps in SU({}): {}".format(dim_irrep, Nc, irrep_dims))
    return irrep_dims(dim_irrep)

# Convenience function
def conventionalized_irrep(irrep, Nc=None):
    try:
        irrep = int(irrep)
        if Nc is None:
            raise ValueError("Must provide Nc to decipher irrep labeled as dim(irrep)={}".format(irrep))
        return irrep_from_dim(irrep, Nc)
    except ValueError:
        pass
    
    for k,v in irrep_names.items():
        if irrep.lower() in v:
            return k
    raise ValueError("Don't know what irrep {r} indicates".format(r=irrep))
    
    
class LargeN(object):
    # Dynamic translation between conventions allows users to change irrep after instantiation
    @property
    def irrep_milc(self):
        return milc_irrep_names[self.irrep]
    @property
    def irrep_fnc(self):
        return fnc_irrep_names[self.irrep]
    
    # Transparent conventionalized irrep names
    @property
    def irrep(self):
        if getattr(self, 'Nc', None) is None:
            raise Exception("Must set Nc before possible to render a conventionalized irrep!")
        return conventionalized_irrep(self._irrep, Nc=self.Nc)
    @irrep.setter
    def irrep(self, new_irrep):
        self._irrep = new_irrep