#!/usr/bin/env python
"""
Created on Tue Oct 24 16:47:17 2017
Taxi classes for running overlap propagators in the Landau gauge.

For more context and explanation about what the MILC source code is does and \
what the parameters mean, see the long README from Tom located in:

    /nfs/hepusers/users/wijay/MilcI9red/arb_overlap/README

@author: wijay
"""

import os

#from taxi import expand_path
#from taxi import fixable_dynamic_attribute
from taxi.mcmc import ConfigMeasurement
#import taxi.local.local_taxi as local_taxi

import taxi.fn_conventions
import mrep_fncs

overlap_template = """
prompt 0

nx {Ns}
ny {Ns}
nz {Ns}
nt {Nt}

iseed {seed}
beta {beta}
u0 {u0}
number_of_masses {number_of_masses}
{m0}
R0 {R0}
scalez {scalez}
prec_sign {prec_sign}
zolo_min {zolo_min}
zolo_max {zolo_max}
inner_cg_iterations {inner_cg_iters}
inner_residue {inner_residue}
inner_residue_h {inner_residue_h}
Number_of_inner_eigenvals {n_inner_eigenvals}
Number_of_h0_eigenvals {n_h0_eigenvals}
Number_of_hov_eigenvals {n_hov_eigenvals}
Max_Rayleigh_iters {max_rayleigh_iters}
Max_r0_iters {max_r0_iters}
Restart_Rayleigh {restart_rayleigh}
Kalkreuter_iters {kalkreuter_iters}
eigenvec_quality {eigenvec_quality}
eigenval_tol_low {eigenval_tol_low}
error_decr_low {error_decr_low}
eigenval_tol_high {eigenval_tol_high}
error_decr_high {error_decr_high}
max_cg_iterations {max_cg_iters}
max_cg_restarts {max_cg_restarts}
{error_for_propagator}
{source_type}
r0 {r0}
{load_gauge}
topology {topology}
{gauge_fix}
{save_gauge} 
{load_prop}
{save_prop}
{load_h0}
{save_h0}
{load_hov}
{save_hov}
EOF
"""

# Synonyms for relevant irreps in SU4
SU4_F_irrep_names = [4, '4', 'f', 'F']
SU4_A2_irrep_names = [6, '6', 'a2', 'A2', 'as2', 'AS2', 'as', 'AS']

class OverlapTask(ConfigMeasurement):
    
    def __init__(self,
                 number_of_masses, m0, error_for_propagator,
                 source_type, r0,
                 n_inner_eigenvals, n_h0_eigenvals, n_hov_eigenvals,
                 prec_sign, zolo_min, zolo_max, 
                 inner_cg_iters, inner_residue, inner_residue_h,                                 
                 max_rayleigh_iters, max_r0_iters, restart_rayleigh, 
                 kalkreuter_iters, eigenvec_quality,
                 eigenval_tol_low, error_decr_low,
                 eigenval_tol_high, error_decr_high,
                 req_time, **kwargs):
                
        super(OverlapTask, self).__init__(req_time=req_time, **kwargs)
                
        self.number_of_masses = number_of_masses
        self.m0 = m0
        self.error_for_propagator = error_for_propagator
        self.source_type = source_type
        self.r0 = r0

        ######################################################
        # Hard-coded values for multimass conjugate gradient #
        ######################################################

        self.scalez = 1.0                  # Additional rescaling of dslash
        self.max_cg_iters = 500            # Used if doing CG, else not used
        self.max_cg_restarts = 1           # Used if doing CG, else not used
        
        ####################################
        # Specifying number of eigenvalues #
        ####################################
        
        assert (n_h0_eigenvals == 2*n_hov_eigenvals),\
            "Error: Must have h_h0_eigenvals == 2*n_hov_eigenvals"
        self.n_inner_eigenvals = n_inner_eigenvals
        self.n_h0_eigenvals = n_h0_eigenvals
        self.n_hov_eigenvals = n_hov_eigenvals

        ######################################
        # Params for Zolotarov step function #
        ######################################

        self.prec_sign = prec_sign
        self.zolo_min = zolo_min
        self.zolo_max = zolo_max
        self.inner_cg_iters = inner_cg_iters
        self.inner_residue = inner_residue
        self.inner_residue_h = inner_residue_h
        
        ################################
        # Params for eigenvalue finder #
        ################################
        
        self.max_rayleigh_iters = max_rayleigh_iters
        self.max_r0_iters = max_r0_iters 
        self.restart_rayleigh = restart_rayleigh
        self.kalkreuter_iters = kalkreuter_iters
        self.eigenvec_quality = eigenvec_quality
        self.eigenval_tol_low = eigenval_tol_low
        self.error_decr_low = error_decr_low
        self.eigenval_tol_high = eigenval_tol_high
        self.error_decr_high = error_decr_high
        
    def construct_seed(self):
        """ Construct a reaonable seed for the random number generator. """        
        self.seed = int("{0}{1}{2}{3}".format(
                        self.Ns,
                        self.Nt,
                        int((self.beta*100)//2),
                        self.loadg.split('_')[-1]))
        
    def reformat_args_for_input(self):
        """
        Reformat the arguments 'm0' and 'error_for_propagator'. These input lines \
        depend on the value of 'number_of_masses'; there should be one line for \
        each mass.
        """
        # Make sure the m0 and error_for_propagator are lists
        try:
            len(self.m0)
        except TypeError:
            self.m0 = [self.m0]
        try:  
            len(self.error_for_propagator)
        except:
            self.error_for_propagator = [self.error_for_propagator]

        # Do lists have the correct length for the specified number of masses?
        assert ((len(self.m0) == self.number_of_masses) and
               (len(self.error_for_propagator) == self.number_of_masses)),\
            "Error: 'number_of_masses' = {0}. Must specify {0} values for 'm0' and 'error_for_propagator'".\
            format(self.number_of_masses)

        # Reformat for input file with one entry per line
        self.m0 = '\n'.join(['m0 '+str(val) for val in self.m0])        
        self.error_for_propagator =\
            '\n'.join(['error_for_propagator '+str(val) for val in self.error_for_propagator])        

    def build_input_string(self):
        input_str = super(OverlapTask, self).build_input_string()
        
        input_dict = self.to_dict()

        ## Gauge I/O
        if self.loadg is None:
            input_dict['load_gauge'] = 'fresh'
        else:
            input_dict['load_gauge'] = 'reload_serial {loadg}'.format(loadg=self.loadg)
            
        if self.saveg is None:
            input_dict['save_gauge'] = 'forget'
        else:
            input_dict['save_gauge'] = 'save_serial {saveg}'.format(saveg=self.saveg)

        ## Gauge fixing
        if self.gauge_fix is None:
            input_dict['gauge_fix'] = 'no_gauge_fix'
        elif 'landau' in self.gauge_fix:
            input_dict['gauge_fix'] = 'landau_gauge_fix'
        elif 'coulomb' in self.gauge_fix:
            input_dict['gauge_fix'] = 'coulomb_gauge_fix'       
        else:
            msg = "Error: invalid gauge_fixing command '{0}' encountered.".format(self.gauge_fix)
            raise RuntimeError(msg)

        ## Prop I/O
        if self.loadp is None:
            input_dict['load_prop'] = 'fresh_wprop'
        else:
            input_dict['load_prop'] = 'reload_serial_wprop {loadp}'.format(loadp=self.loadp)
            
        if self.savep is None:
            input_dict['save_prop'] = 'forget_wprop'
        else:
            input_dict['save_prop'] = 'save_serial_wprop {savep}'.format(savep=self.savep)

        ## h0 modes I/O
        if self.load_h0 is None:
            input_dict['load_h0'] = 'fresh_hr0_modes'
        else:
            # Leading i for 'input'
            input_dict['load_h0'] = 'iserial_hr0_modes {load_h0}'.format(load_h0=self.load_h0)

        if self.save_h0 is None:
            input_dict['save_h0'] = 'forget_hr0_modes'
        else:
            input_dict['save_h0'] = 'serial_hr0_modes {save_h0}'.format(save_h0=self.save_h0)
            
        ## hov modes I/O
        if self.load_hov is None:
            input_dict['load_hov'] = 'fresh_hov_modes'
        else:
            # Leading i for 'input'
            input_dict['load_hov'] = 'iserial_hov_modes {load_hov}'.format(load_hov=self.load_hov)          

        if self.save_hov is None:
            input_dict['save_hov'] = 'forget_hov_modes'
        else:
            input_dict['save_hov'] = 'serial_hov_modes {save_hov}'.format(save_hov=self.save_hov)       

        return input_str + overlap_template.format(**input_dict)

class OverlapPreconditionTask(OverlapTask):

    fout_prefix = 'outModes'
    saveg_prefix = 'cfgLandau'
    saveh0_prefix = 'h0ModesOverlap'
    savehov_prefix = 'hovModesOverlap'

    fout_filename_convention = mrep_fncs.MrepModesOverlapOutputWijayFnConvention
    saveg_filename_convention = mrep_fncs.MrepGaugeLandauWijayFnConvention
    saveh0_filename = mrep_fncs.MrepH0WijayFnConvention
    savehov_filename_convention = mrep_fncs.MrepHovWijayFnConvention
    # Convention: do input/loading FNCs as lists for user-friendliness
    loadg_filename_convention = taxi.fn_conventions.all_conventions_in(mrep_fncs)
    
    output_file_attributes = ['fout', 'saveg', 'saveh0', 'savehov']
        
    binary = '/nfs/beowulf03/wijay/mrep/bin/su3_ov_eig_cg_f_hyp'
    
    def __init__(self,
                 ## I/O
                 loadg,
                 saveg,
                 save_h0,
                 save_hov,
                 ## For taxi
                 req_time=600,
                 ## Override autodetection from loadg
                 beta=None,
                 Ns=None, 
                 Nt=None, 
                 gauge_fix='landau_gauge_fix',
                 error_for_propagator=1e-6,
                 ## Numbers of eigenvalues
                 n_inner_eigenvals=12, 
                 n_h0_eigenvals=16, 
                 n_hov_eigenvals=8, 
                 ## Zolotarov 
                 prec_sign=1e-7, 
                 zolo_min=0.01, 
                 zolo_max=2.7, 
                 inner_cg_iters=1000, 
                 inner_residue=1e-7, 
                 inner_residue_h=1e-7, 
                 ## Eigenvalue finder
                 max_rayleigh_iters=100, 
                 max_r0_iters=100, 
                 restart_rayleigh=10, 
                 kalkreuter_iters=10, 
                 eigenvec_quality=1.0e-4,
                 eigenval_tol_low=1.0e-6,
                 error_decr_low=0.3,
                 eigenval_tol_high=1.0e-7,
                 error_decr_high=0.3,
                 ## Topology
                 topology=200, 
                 **kwargs ## Should inlcude 'measure_on' for ConfigMeasurement
                 ):
        """
        Computes exact eigenvalues of <?> and <?> to 'precondition' the
        computation of overlap propagators.
        
        Because the binary itself requires many input parameters, this class has \
        many parameters. Mostly we try to use sensible default values.
        
        Args:
            # Required arguments            
            req_time: int, the requested time (in seconds) for the job
            beta: float, the inverse gauge coupling
            loadg: str, the full path to the gauge file to load
            saveg: str, the full path for saving the gauge file. 
            save_h0: str, the full path for saving eigenvalues of h(0)
            save_hov: str, the full path for saving eigenvalues of ??
            gauge_fix: str, the gauge to which to fix. Default is 'landau'. \
                If None, will not fix the gauge.

            # Optional arguments (those with defaults)
            n_inner_eigenvals: int, number of eigenvalues of h(-R0) to compute, \
                where R0 is the GW radius. Default is 12.
            n_h0_eigenvals: int, number of eigenvalues of h(0) to compute. Must \
                be 2 x n_hov_eigenvals below. Default is 16.
            n_hov_eigenvals: int, number of eigenvaluess of H(0) to compute. Must \
                be 1/2 x n_h0_eigenvals above. Default is 8.
            
            prec_sign: float, desired accuracy for Zolotarov step function. \
                Default is 1e-7
            zolo_min: float, minimum range for computing Zolotarov step function. \
                Default is 0.01
            zolo_max: float, maximum range for computing Zolotarov step function. \
                Default is 2.7
            inner_cg_iterations: int, number of iterations for the conjugate \
                gradient used to compute the Zolotarov step function. Default is \
                1000
            inner_residue: float, the conjugate gradient residue used to compute \
                the Zolotarov step function. Default is 1e-7. The code holds this \
                value as 'resid_inner_save'
            inner_residue_h: float, used in build_hr0_field2.c. Only used if 
                precflag=HIGHP is set. Default is 1e-7.
            
            max_rayleigh_iters: int,??. Default is 100.
            max_r0_iters: int, ??. Default is 100.
            restart_rayleigh: int, ??. Default is 10.
            kalkreuter_iters: int, ??. Default is 10.
            eigenvec_quality: float, ??. Default is 1.0e-4.
            eigenval_tol_low: float, ??. Default is 1.0e-6.
            error_decr_low: float, ??. Default is 0.3.
            eigenval_tol_high: float, ??. Default is1.0e-7.
            error_decr_high: float, ??. Default is 0.3.

            topology: int, how to handle the topolgy. This feature is used more \
                in the dynamical overlap code, which keeps track of topolgy.
                If = -100 uses current topology.
                If <= -100, reads topology from gauge info file. If >= 100, allocates
                space for Nvecs_hov+abs(current_topology)/100 + 2 H(0) modes.
                If < 100, Nvecs_hov+abs(current_topology)+2 modes/
                If >= 100, find current topology before start of trajectory.

            kwargs: keyword arguments to pass to super. Should include the \
                keyword 'measure_on' for ConfigMeasurement. If 'measure_on' is \
                a filename, the code attemps to detect beta, Ns, and NT from 
                the filename.
                TODO: Implement support for starting with a ConfigGenerator.
        Returns: 
            OverlapPreconditionTask instance
        """
        # No multimass CG when computing eigenmodes
        number_of_masses = 1
        m0 = 0.0
        # Hard-coded point source to make it clear no smearing occurs
        source_type= 'point' # Not used for eigenmode preconditioning
        r0 = 0.0             # Not used for eigenmode preconditioning
        
        OverlapTask.__init__(self,\
            number_of_masses, m0, error_for_propagator,\
            source_type, r0,
            n_inner_eigenvals, n_h0_eigenvals, n_hov_eigenvals,\
            prec_sign, zolo_min, zolo_max,\
            inner_cg_iters, inner_residue, inner_residue_h,\
            max_rayleigh_iters, max_r0_iters, restart_rayleigh,\
            kalkreuter_iters, eigenvec_quality,\
            eigenval_tol_low, error_decr_low,\
            eigenval_tol_high, error_decr_high,\
            req_time, **kwargs)
        
        ## Override parameters read out from a filename or stolen from a ConfigGenerator
        if beta is not None:
            self.beta = beta
        if Ns is not None:
            self.Ns = Ns
        if Nt is not None:
            self.Nt = Nt

        assert gauge_fix is not None,\
            "Error: Should gauge fix to landau gauge for OverlapPreconditioning"
        self.gauge_fix = gauge_fix 

        #######################
        # Hard-coded settings #
        #######################

        self.u0 = 1.0             # tadpole improvement term from MILC. Default is 1.0
        self.number_of_masses = 1 # Always 1 for eigenvalue preconditioning
        self.m0 = 0.0             # The overlap mass. 0.0 for eigenvalue preconditioning
        self.R0 = 1.2             # The GW radius; present action uses 1.2
        self.topology = 200

        ############
        # File I/O #
        ############

        assert loadg is not None,\
            "Error: Must load gauge config for OverlapPreconditioning."
        self.loadg = loadg
        assert saveg is not None,\
            "Error: Must save gauge config after gauge fixing for OverlapPreconditioning."
        self.saveg = saveg
        
        self.loadp = None # Never load a propagator when preconditioning to compute a propagator
        self.savep = None # Never save a propagator when preconditioning to compute a propagator
        self.load_h0 = None  # Never load eigenvalues; they're the end goal here!
        self.load_hov = None # Never load eigenvalues; they're the end goal here!
        assert save_h0 is not None,\
            "Error: Please specify save_h0 when computing eigenvalues."
        self.save_h0 = save_h0
        assert save_hov is not None,\
            "Error: Please specify save_hov when computing eigenvalues."        
        self.save_hov = save_hov

        OverlapTask.construct_seed(self)
        OverlapTask.reformat_args_for_input(self)

        
    def verify_output(self):
        super(OverlapPreconditionTask, self).verify_output()
    
        # Confirm presence of saved gauge file
        if (not os.path.exists(self.saveg)):
            msg = "OverlapPrecondition verification failure: missing saved gauge config {0}".format(self.saveg)
            raise RuntimeError(msg)

        # Confirm presence of saved hov file
        if (not os.path.exists(self.save_hov)):
            msg = "OverlapPrecondition verification failure: missing saved hov file {0}".format(self.save_hov)
            raise RuntimeError(msg)

        # Confirm presence of saved h0 file
        if (not os.path.exists(self.save_h0)):
            msg = "OverlapPrecondition verification failure: missing saved h0 file {0}".format(self.save_h0)
            raise RuntimeError(msg)

        # Check for well-formed output file
        # Trailing space avoids catching the error_something parameter input
        with open(self.fout) as f:
            found_end_of_reading = False
            found_running_completed = False            
            for line in f:
                if "error " in line.lower():
                    raise RuntimeError("Spectro ok check fails: Error detected in " + self.fout)
                found_running_completed |= ("RUNNING COMPLETED" in line)
                found_end_of_reading |= ("Time to check unitarity" in line)
                
            if not found_end_of_reading:
                raise RuntimeError("OverlapPrecondition verification failure: did not find end of reading in " + self.fout)
            if not found_running_completed:
                raise RuntimeError("OverlapPrecondition verification failure: running did not complete in " + self.fout)
    
    ## Spectroscopy typically has many different binaries.  Use a fixable property to specify which one.
#    def _dynamic_get_binary(self):
#        if self.irrep in SU4_F_irrep_names:
#            irrep = 'f'
#        elif self.irrep in SU4_A2_irrep_names:
#            irrep = 'a2'
#        else:
#            raise NotImplementedError("SpectroTask only knows about F, A2 irreps, not {r}".format(r=self.irrep))
#            
#        key_tuple = (self.Nc, irrep, self.screening, self.p_plus_a, self.compute_baryons)
#        
#        try:
#            return local_taxi.multirep_spectro_binaries[key_tuple]
#        except KeyError:
#            raise NotImplementedError("Missing binary for (Nc, irrep, screening?, p+a?, compute_baryons?)="+str(key_tuple))
#    binary = fixable_dynamic_attribute(private_name='_binary', dynamical_getter=_dynamic_get_binary)

class OverlapPropagatorTask(OverlapTask):

    loadho_prefix = 'h0'
    loadhov_prefix = 'hov'    
    fout_prefix = 'outProp'
    savep_prefix = 'prop'

    loadh0_filename_convention = mrep_fncs.MrepH0WijayFnConvention
    loadhov_filename_convention = mrep_fncs.MrepHovWijayFnConvention
    fout_filename_convention = mrep_fncs.MrepPropOverlapOutputWijayFnConvention
    savep_filename_convention = mrep_fncs.MrepPropOverlapWijayFnConvention    
    # Convention: do input/loading FNCs as lists for user-friendliness
    loadg_filename_convention = taxi.fn_conventions.all_conventions_in(mrep_fncs)
    
    output_file_attributes = ['fout', 'savep', 'loadg', 'loadh0', 'loadhov']

    binary = '/nfs/beowulf03/wijay/mrep/bin/su3_ov_eig_cg_multi'

    def __init__(self,
                 ## I/O
                 loadg,
                 savep,
                 load_h0,
                 load_hov,
                 m0=[0.0],
                 error_for_propagator=[1e-6],
                 source_type='point',
                 r0=0.0,
                 ## For taxi
                 req_time=600,
                 ## Override autodetection from loadg
                 beta=None,
                 Ns=None, 
                 Nt=None, 
                 ## Numbers of eigenvalues
                 n_inner_eigenvals=10, 
                 n_h0_eigenvals=16, 
                 n_hov_eigenvals=8, 
                 ## Zolotarov 
                 prec_sign=1e-7, 
                 zolo_min=0.01, 
                 zolo_max=2.7, 
                 inner_cg_iters=1000, 
                 inner_residue=1e-7, 
                 inner_residue_h=1e-7, 
                 ## Eigenvalue finder
                 max_rayleigh_iters=0, 
                 max_r0_iters=0, 
                 restart_rayleigh=10, 
                 kalkreuter_iters=10, 
                 eigenvec_quality=1.0e-4,
                 eigenval_tol_low=1.0e-6,
                 error_decr_low=0.3,
                 eigenval_tol_high=1.0e-7,
                 error_decr_high=0.3,
                 ## Topology
                 topology=200, 
                 **kwargs ## Should inlcude 'measure_on' for ConfigMeasurement
                 ):
        """ doc string here """
        number_of_masses = len(m0)
        if len(error_for_propagator) != number_of_masses:
            assert len(error_for_propagator) == 1,\
                "Error: poorly formatted 'error_for_propagator'; check the length of the list."
            error_for_propagator = [error_for_propagator[0] for _ in m0]
            
        OverlapTask.__init__(self,\
            number_of_masses, m0, error_for_propagator,\
            source_type, r0,\
            n_inner_eigenvals, n_h0_eigenvals, n_hov_eigenvals,\
            prec_sign, zolo_min, zolo_max,\
            inner_cg_iters, inner_residue, inner_residue_h,\
            max_rayleigh_iters, max_r0_iters, restart_rayleigh,\
            kalkreuter_iters, eigenvec_quality,\
            eigenval_tol_low, error_decr_low,\
            eigenval_tol_high, error_decr_high,\
            req_time, **kwargs)

        ## Override parameters read out from a filename or stolen from a ConfigGenerator
        if beta is not None:
            self.beta = beta
        if Ns is not None:
            self.Ns = Ns
        if Nt is not None:
            self.Nt = Nt

        #######################
        # Hard-coded settings #
        #######################

        self.gauge_fix = None # Never fix the gauge, assumed to be done already       
        self.saveg = None     # Never save gauge config, only read
        self.save_h0 = None   # Never save h0 modes, only ready
        self.save_hov = None  # Never save hov modes, only read
        self.loadp = None     # Never load a propagator when preconditioning to compute a propagator

        self.u0 = 1.0         # tadpole improvement term from MILC. Default is 1.0
        self.R0 = 1.2         # The GW radius; present action uses 1.2
        self.topology = 200

        ############
        # File I/O #
        ############

        assert loadg is not None,\
            "Error: Must load gauge config for OverlapPropagator"
        self.loadg = loadg

        assert savep is not None,\
            "Error: Saving a propagator is the goal of OverlapPropagator"
        self.savep = savep
        
        assert load_h0 is not None,\
            "Error: OverlapPropagator assumes h0 eigenvalues have been computed. Please load them."
        self.load_h0 = load_h0
        
        assert load_hov is not None,\
            "Error: OverlapPropagator assumes hov eigenvalues have been computed. Please load them."
        self.load_hov = load_hov

        OverlapTask.construct_seed(self)
        OverlapTask.reformat_args_for_input(self)
        
        
    def verify_output(self):
        super(OverlapPropagatorTask, self).verify_output()
    
        # Confirm presence of saved gauge file
        if (not os.path.exists(self.savep)):
            msg = "OverlapPropagator verification failure: missing saved propagator {0}".format(self.savep)
            raise RuntimeError(msg)

        # Check for well-formed output file
        # Trailing space avoids catching the error_something parameter input
        with open(self.fout) as f:
            found_end_of_reading = False
            found_running_completed = False            
            for line in f:
                if "error " in line.lower():
                    raise RuntimeError("Spectro ok check fails: Error detected in " + self.fout)
                found_running_completed |= ("RUNNING COMPLETED" in line)
                found_end_of_reading |= ("Time to check unitarity" in line)
                
            if not found_end_of_reading:
                raise RuntimeError("OverlapPropagator verification failure: did not find end of reading in " + self.fout)
            if not found_running_completed:
                raise RuntimeError("OverlapPropagator verification failure: running did not complete in " + self.fout)