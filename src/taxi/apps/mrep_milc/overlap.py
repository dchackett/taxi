#!/usr/bin/env python
"""
Created on Tue Oct 24 16:47:17 2017
Taxi classes for running overlap propagators in the Landau gauge.

Below are the contents of a long README file that accompanied the original code.

#####################
# Begin long README #
#####################

This subdirectory has code to compute eigenmodes and propagators for my implementation
of overlap fermions.

The action is nearly that of
%\cite{DeGrand:2000tf}
\bibitem{DeGrand:2000tf} 
  T.~A.~DeGrand [MILC Collaboration],
  %``A Variant approach to the overlap action,''
  Phys.\ Rev.\ D {\bf 63}, 034503 (2000)
  [hep-lat/0007046].

For differences, see Sec 2B of

  T.~A.~DeGrand and S.~Schaefer,
  %``Physics issues in simulations with dynamical overlap fermions,''
  Phys.\ Rev.\ D {\bf 71}, 034507 (2005)
  [hep-lat/0412005].

In a sentence, the later paper introduces a kernel action where the neighbor terms are all
projectors, (1+/- gamma \cdot n) where n is a unit vector in the direction. This is what is in the present
code.


My S-parameter paper is
  T.~DeGrand,
  %``Oblique correction in a walking lattice theory,''
  arXiv:1006.3777 [hep-lat].


Executables which can be made in this directory are

 	su3_ov_eig_cg_f_hyp::eigenmodes--one chirality of H(0). This is the workhorse program,
  	it finds (and can save) eigenmodes of the kernel h(-r0) and then finds eigenmodes of h(0)
  	to be used in eigenmodes of H(0) the massless Hermitian Dirac operator

	su3_ov_eig_cg_f_hyp_per::periodic b.c. version of the above (used for P+A propagators)


 
 	su3_ov_eig_cg_multi:: Multimass CG: computes propagators (for spectroscopy, etc). Uses eigenmodes
 	created by su3_ov_eig_cg_f to percondition. Save propagators to run clover_invert/su3_spec_only on them

	su3_ov_eig_cg_multi_per: periodic b.c. version of the above, for P+A propagators

Much other ``legacy code'' exists, for example, for direct computation of psibar-psi with noisy
sources. But I didn't include it to simplify matters

The workhorse outer conjugate gradient code is multi_cg.c, which uses multi_cg_iter.c
to iterate. It does a multimass CG.

Here are input files. 
(Some lines are kept from earlier versions or from other projects, for nostalgia,
or to simplify setup_p_cl.c)Actually, the two input files hae identical lines, just some of them
are no used by each individual code.

First, an input for eigenmode finding

prompt 0
nx 4
ny 4
nz 4
nt 4
iseed 3737
 
beta 7.3
u0 1.0
number_of_masses 1
m0 0.0
R0 1.2 
scalez 1.0
prec_sign 1.e-7
zolo_min 0.02
zolo_max 2.7
inner_cg_iterations 1000
inner_residue 1.e-7
inner_residue_h 1.e-7
Number_of_inner_eigenvals 4  <- number of h(-r0) eigenmodes
Number_of_h0_eigenvals 8 <- number of h(0) modes, should be greater than 2 times next line
Number_of_hov_eigenvals 4 <- number of H(0) modes desired
Max_Rayleigh_iters 1000 <-now, parameters for the eigen finder
Max_r0_iters 100
Restart_Rayleigh 10
Kalkreuter_iters 10
eigenvec_quality 1.2e-4
eigenval_tol_low 1.0e-6
error_decr_low .3
eigenval_tol_high 1.0e-7
error_decr_high .3
max_cg_iterations 100 <-used if you are doing CG, otherwise un-used
max_cg_restarts 1
error_for_propagator  1.e-5
point
r0 2.0
reload_serial c4444.d
topology 100  <-see below
no_gauge_fix
forget
fresh_wprop ,_ not used for eigenmodes
forget_wprop
fresh_hr0_modes <- also iserial_hr0_modes
serial_hr0_modes hr0  <- save eigenmodes of h(-r0)
fresh_hov_modes  <- also iserial_hov_modes
serial_hov_modes hov <-save eigenmodes of H(0)


And now for propagators
prompt 0
nx 4
ny 4
nz 4
nt 4
iseed 3737
 
beta 7.3
u0 1.0
number_of_masses 4
m0 0.01
m0 0.02
m0 0.05
m0 0.1
R0 1.2
scalez 1.0
prec_sign 1.e-7
zolo_min 0.02
zolo_max 2.7
inner_cg_iterations 1000
inner_residue 1.e-7
inner_residue_h 1.e-7
Number_of_inner_eigenvals 4
Number_of_h0_eigenvals 8
Number_of_hov_eigenvals 4
Max_Rayleigh_iters 1000

Max_r0_iters 0  <- the propagator code can compute h(-r0) eigenmodes, but we read them in
Restart_Rayleigh 10
Kalkreuter_iters 10
eigenvec_quality 1.2e-4  <- check in build_hr0 of <phi|(h-lambda)|phi>
eigenval_tol_low 1.0e-6
error_decr_low .3
eigenval_tol_high 1.0e-7
error_decr_high .3
max_cg_iterations 100
max_cg_restarts 1
error_for_propagator  1.e-5
error_for_propagator  1.e-5
error_for_propagator  1.e-5
error_for_propagator  1.e-5
point
r0 2.0
reload_serial c4444.d
topology 100
no_gauge_fix
forget
fresh_wprop
save_serial_wprop www
iserial_hr0_modes hr0
forget_hr0_modes
iserial_hov_modes hov
forget_hov_modes



Parameters:


beta, u0: u0 is tadpole inprovement term per Milc. I always use 1.0
m0 -- the overlap mass. m0=0.0 for eigenvalues
number_of_masses: 1 for eigenmodes, the number of multimass masses for the inverter.
You need a mass and an accuracy for each mass,
separate entries per mass for error_for_propagator 

R0: GW radius.The present action uses R0=1.2
scalez: additional rescaling of dslash (COULD BE REMOVED, it exists only in
 congrad_multi_field.c and hdelta0_field.c step_hmc_hb.c lattice.h params.h. It is set to 1.0
for the present kernel.) If you really wanted to do Wilson kernel action in the overlap, you'd
have to play with these terms.

prec_sign: desired Zoloratov accuracy, for input range zolo_min to zolo_max. zolo_min is 
stored and reused as zolo_min_save and the variable zolo_min is altered repeatedly by
 the code, in congrad_multi_field.c. Using eigenmodes to deflate makes for needing
a smaller range for the step function.

inner_cg_iterations/maxcg_inner max cg's for step function

inner_residue/resid_inner. Usual inner CG residue. Input value saved permanently as
 resid_inner_save. MINN/MOUT flags tune it.

resid_inner is used for H(0)^2 eigenmodes if MINNI flag is used, but this choice
is dangerous in practice, code is fragile and convergence can be degraded.
See eigen_stuff.c

inner_residue_h/resid_inner_h:  In  build_hr0_field2.c used if precflag= HIGHP



error_decr's are parameters in eigen_stuff for convergence of Kalkreuter. Not clear
both are needed. HIGHP flags which one is  used.

max_cg_restarts/nrestart: UNUSED. Legacy code from clover fermions. You can't restart a 
multimass CG, anyway. You could restart a single mass one (and single mass CG code exists,
somewhere.)

error_for_fermforce/resid[i] used in congrad_multi_field for outer CG residue

error_for_fermaction/resid2[i]/resid_acc[i] Doesn't seem to be used (probably was
 used in  dynamical overlap)


gaussian--source type, unused in eigenmode code, but used for inverter
r0 3.0  par_buf.wqs.r0 also unused by eigenmode code

topology -100: current_topology  if <= -100 read topology from gauge info file. If >= 100,
allocate space for Nvecs_hov+abs(current_topology)/100+2 H(0) modes (if topology < 100,
Nvecs_hov+abs(current_topology)+2 modes) . If >=100, find current topology before start of
trajectory. This is used more in the dynamical overlap code, which keeps track of topology.

Defines:

EIG  --create space for, and use, eigenmodes of h(-r0)
EIGO --create space for, and use, eigenmodes of H(0)
INV -  reate some variables for inverter--not used except in lattice.h
   note H0INV is used to save the inverse of H(m)^2, incase you want to restart the CG 
FIELD - use fieldwise variables (nearly our standard, now)
RANDOM   - used in setup and .h's to create space for random number stuff:
 setup_p_cl.c defines.h lattice.h params.h



MINN --in congrad_multi_field.c,
adaptively adjust inner CG accuracy to produce desired step fn accuracy
#ifdef MINN
    if(do_minn ==1){
        dest_norm=0.0;
        FORALLSITES(i,s){
            dest_norm += (double)magsq_wvec((wilson_vector *)F_PT(s,dest));
        }
        g_doublesum( &dest_norm );

        /* you can always increase resid_inner--up to a point! */
        if(test_epsilon < resid_inner && resid_inner_run < 0.02 ) resid_inner_run *= 1.2;
        /* but it should not shrink too small */
        if((resid_inner_run >= resid_inner) &&(test_epsilon > resid_inner))
            resid_inner_run /= 1.2;
    }
#endif
 

List of c routines

build_h0.c  --find eigenmodes of h(0), to ``seed'' the start of build_hov.c
build_hov.c -- find eigenmodes of H(0) the massless overlap operator
build_hr0_field2.c -- find eigenmodes of h(-r0)
build_lowest_chi.c  --figure out which chirality sector has zero modes
build_params_0.166.c
congrad_multi_field.c -- multimass CG for commputing overlap operator
control_f.c
copy_fields.c
delta0y.c -- the kernel Dirac operator
eigen_stuff.c  -- Milc Kalkreuter routine
eigen_stuff_JD.c -- wrapper for Primme
f_mu_nu1.c Compute F_{mu,nu} used in the clover fermion action
gauge_info.c
grsource.c
hdelta0_field.c D^\dagger D for kernel or for overlap D
hoverlap.c dest= gamma_5 D * src
io_modes.c
jacobi_eig.c
make_clov_field.c  -- modified version used in overlap action -- see comment
mult_ldu2.c  -- modified version used in overlap action
multi_cg.c ``outer'' CG for massive overlap operator, with deflation
multi_cg_iter.c iteration routines for above
myclock.c
overlap_info.c  -- for reading/writing lattices
path.c -- arbitrry path walker
print_var.c -- for writing out ascii things over the lattice
readinfo.c
setup_links.c --set up links for the kernel action
setup_offset.c --set up gathers for the kernel action
setup_p_cl.c  -- the usual ``setup.c''
vectorh.c  -- some more vector routines used in eigensolver
wp_grow_pl_field_pf.c  --routines for the projector trick (1\pm gamma \cdot n)
wp_grow_pl_field_pf_l.c
wp_shrink_pl_field_pf.c
wp_shrink_pl_field_pf_l.c

Output of su3_ov_eig_cg_f_hyp
First, code finds eigenmodes of h(-r0), actually it finds the lowest eigenmodes of h(-r0)^2 and then 
diagonalizes h(-r0) in this basis. See build_hr0_field2.c
F3HSQ  -- eigenvalues of h(-r0)^2
F3MEX  --eigenvalue of h(-r0)
F3MES

Then it finds eigenmodes of h(0) to seed the modes of H(0). See build_h0.c.

Next, build_lowest_chi.c figures out which chirality sector has any zero modes, by 
constructing one state in each chirality sector
and comparing.

Finally, eigenmodes of the overlap, build_hov.c. This starts with eigenmodes of h(0), 
projected onto the lower-eigenvalue sector of chirality.
F3HO2V eigenvalues of H(0)^2
Then the modes of H(0) are either the eigenmodes of H90)^2 (if they have zero eigenvalue) or they are
 mixtures of degenerate opposite chirality modes of H(0)^2. F3OGH02X2 gives thee eigenmodes.


Output of su3_ov_eig_cg_multi is much more standard. At the start, if eigenmodes of h(-r0) are read in (to precondition the 
overlap operator) the code checks their quality to warn against errors. See the F3MES line.
CG printe residue every 5 CG steps (can be made bigger).

###################################
To compute Pi_LR, routines in MilcI4/clover_invert are brought to clover_invert

#       "su3_spec_only_ptp_pi"  flavor-diagonal mesons  no CG at all
#             point-to-point correlators transformed to k-space and projected into
#             Pi_L and Pi_T
#
su3_spec_only_ptp_pi::
        make -f ${MAKEFILE} target "MYTARGET= $@" \
        "DEFINES=  ${DEFINES} -DBI  -DPIMUNU" \
        "ADDHEADERS = addsite_clov_bi.h " \
        "EXTRA_OBJECTS= control_spec_only_pi.o  w_meson_pi.o print_var.o "

And, this is from MilcI7/clover_invert
su3_spec_only::
        make -f ${MAKEFILE} target "MYTARGET= $@" \
        "DEFINES=  ${DEFINES} -DBI  " \
        "ADDHEADERS = addsite_clov_bi.h " \
        "EXTRA_OBJECTS= control_spec_only.o "

It just reads in  a set of propagators and measures meson propagators off them.
(note the BARYON flag, used to call baryon routines).

#########################################################################

NEED TO UPDATE THIS!

Calculation of Pi_LR plus some analysis in
/axp/aurinko/wrk1/ltm/degrand/version6/clover_dynamical/Run_1616NPptp
or Run_1212NPptp

The actual overlap propagators are computed in 
/axp/aurinko/wrk1/ltm/degrand/version6/ov_hmc/Run_1616Q/
or
/axp/aurinko/wrk1/ltm/degrand/version6/clover_dynamical/Run_1212NP,
see
do_many_1212_ov_modesl
do_many_1212_ov_props
and the usual cascade of shell scripts.


Other useful things:
Around July 28 2014 I discovered that teh code was computing AS2 links incorrectly. The flag
REALITY_CHECK corrects this error. (It may get incorporated
into an upgrade of the code but for now, the routine sits in control_f.c and
control_f_rebuild.c

su3_ov_eig_cg_f_hyp_rebuild -- code to generate eigenmodes, but using stored ones as seeds.

###################
# End long README #
###################

@author: wijay
"""

import os

from taxi import expand_path
from taxi import fixable_dynamic_attribute
from taxi.mcmc import ConfigMeasurement
import taxi.local.local_taxi as local_taxi

import taxi.fn_conventions

overlap_precondition_template = """
prompt 0

nx {Ns}
ny {Ns}
nz {Ns}
nt {Nt}

iseed {seed}
beta {beta}
u0 1.0
number_of_masses 1
m0 0.0
R0 1.2
scalez 1.0
prec_sign 1.0e-7
zolo_min 0.01
zolo_max 2.7
inner_cg_iterations 500
inner_residue 1.e-6
inner_residue_h 1.e-6
Number_of_inner_eigenvals 12
Number_of_h0_eigenvals 0
Number_of_hov_eigenvals 0
Max_Rayleigh_iters 100
Max_r0_iters 100
Restart_Rayleigh 10
Kalkreuter_iters 10
eigenvec_quality 1.e-4
eigenval_tol_low 1.0e-6
error_decr_low .3
eigenval_tol_high 1.0e-6
error_decr_high .3
max_cg_iterations 500
max_cg_restarts 1
error_for_propagator  1.e-5
gaussian
r0 3.0
{load_gauge} -- e.g., reload_serial <fname>
topology -100
landau_gauge_fix
{save_gauge} -- e.g., save_serial <fname>
fresh_wprop 
forget_wprop 
fresh_hr0_modes
{save_hr0_modes} -- e.g., serial_hr0_modes <fname>
fresh_hov_modes
{save_hov_modes} -- serial_hov_modes $hov
LimitStr

"""

overlap_inversion_input_template = """
#TODO: add content
"""

# Synonyms for relevant irreps in SU4
SU4_F_irrep_names = [4, '4', 'f', 'F']
SU4_A2_irrep_names = [6, '6', 'a2', 'A2', 'as2', 'AS2', 'as', 'AS']

class OverlapPreconditionTask(ConfigMeasurement):

    fout_prefix = 'outModes'
    saveg_prefix = 'cfg'
    saveh0_prefix = 'h0ModesOverlap'
    savehov_prefix = 'hovModesOverlap'
    #TODO: Set file name conventions
    fout_filename_convention = None
    saveg_filename_convention = None
    saveh0_filename_convention = None
    savehov_filename_convention = None    
    # Convention: do input/loading FNCs as lists for user-friendliness
    loadg_filename_convention = taxi.fn_conventions.all_conventions_in(mrep_fncs)
    
    output_file_attributes = ['fout', 'saveg', 'saveh0', 'savehov']
        
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
                 gauge_fix='landau',
                 ## Numbers of eigenvalues
                 n_inner_eigenvals=12, 
                 n_h0_eigenvals=16, 
                 n_hov_eivenvals=8, 
                 ## Zolotarov 
                 prec_sign=1e-7, 
                 zolo_min=0.01, 
                 zolo_max=2.7, 
                 inner_cg_iterations=1000, 
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
            ??
        """
        super(OverlapPreconditionTask, self).__init__(req_time=req_time, **kwargs)

        ## Override parameters read out from a filename or stolen from a ConfigGenerator
        if beta is not None:
            self.beta = beta
        if Ns is not None:
            self.Ns = Ns
        if Nt is not None:
            self.Nt = Nt

        self.gauge_fix = gauge_fix # 'landau', usually

        ##############################################################
        # Hard-coded settings specific to eigenvalue preconditioning #
        ##############################################################

        self.u0 = 1.0 # tadpole improvement term from MILC. Default is 1.0
        self.number_of_masses = 1 # Always 1 for eigenvalue preconditioning
        self.m0 = 0.0 # The overlap mass. 0.0 for eigenvalue preconditioning
        self.R0 = 1.2 # The GW radius; present action uses 1.2

        ############
        # File I/O #
        ############

        self.loadg = loadg
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

        ####################################
        # Specifying number of eigenvalues #
        ####################################
        
        assert (n_h0_eigenvals == 2*n_hov_eivenvals),\
            "Error: Must have h_h0_eivenvals == 2*n_hov_eigenvals"
        self.n_inner_eigenvals = n_inner_eigenvals
        self.n_h0_eigenvals = n_h0_eigenvals
        self.n_hov_eivenvals = n_hov_eivenvals

        ######################################
        # Params for Zolotarov step function #
        ######################################

        self.prec_sign = prec_sign
        self.zolo_min = zolo_min
        self.inner_cg_iterations = inner_cg_iterations
        self.inner_residue = inner_residue
        self.inner_residue_h = inner_residue_h

        
        ################################
        # Params for eigenvalue finder #
        ################################
        
        self.max_rayleigh_iters = max_rayleigh_iters
        self.max_r0_iters=max_r0_iters =max_r0_iters=max_r0_iters
        self.restart_rayleigh = restart_rayleigh
        self.kalkreuter_iters = kalkreuter_iters
        self.eigenvec_quality = eigenvec_quality
        self.eigenval_tol_low = eigenval_tol_low
        self.error_decr_low = error_decr_low
        self.eigenval_tol_high = eigenval_tol_high
        self.error_decr_high = error_decr_high

        ## Topology
        self.topology = topology
        
        
        #####################################################################
        # Legacy parameters required by the binary but (probably?) not used #
        #####################################################################
            
        self.scalez = 1.0                  # Additional rescaling of dslash
        self.max_cg_iterations = 500       # Used if doing CG, else not used
        self.max_cg_restarts = 1           # Used if doing CG, else not used
        self.error_for_propagator = 1.e-5  # Used if doing CG, else not used
        self.source_type = 'point'         # Used if doing CG, else not used
        self.r0 = 0.0                      # Used if doing CG, else not used

    def build_input_string(self):
        input_str = super(OverlapPreconditionTask, self).build_input_string()
        
        input_dict = self.to_dict()
        ## Gauge I/O
        if self.loadg is None:
            input_dict['load_gauge'] = 'fresh'
        else:
            input_dict['load_gauge'] = 'load_serial {loadg}'.format(loadg=self.loadg)
            
        if self.saveg is None:
            input_dict['save_gauge'] = 'forget'
        else:
            input_dict['save_gauge'] = 'save_serial {saveg}'.format(saveg=self.saveg)
        
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
           input_dict['load_h0'] = 'fresh'
        else:
           input_dict['load_h0'] = 'iserial_hr0_modes {loadh0}'.format(loadh0=self.loadh0)          

        if self.save_h0 is None:
            input_dict['save_h0'] = 'forget_hr0_modes'
        else:
            input_dict['save_h0'] = 'save_serial_hr0_modes'         
            
        ## hov modes I/O
        if self.load_hov is None:
           input_dict['load_hov'] = 'fresh'
        else:
           input_dict['load_hov'] = 'iserial_hov_modes {loadhov}'.format(loadh0=self.loadh0)          

        if self.save_hov is None:
            input_dict['save_hov'] = 'forget_hov_modes'
        else:
            input_dict['save_hov'] = 'save_serial_hov_modes'         

        return input_str + spectro_input_template.format(**input_dict)


    def verify_output(self):
        super(SpectroTask, self).verify_output()
    
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
                
            if not found_end_of_header:
                raise RuntimeError("OverlapPrecondition verification failure: did not find end of reading in " + self.fout)
            if not found_running_completed:
                raise RuntimeError("OverlapPrecondition verification failure: running did not complete in " + self.fout)


    ## Spectroscopy typically has many different binaries.  Use a fixable property to specify which one.
    def _dynamic_get_binary(self):
        if self.irrep in SU4_F_irrep_names:
            irrep = 'f'
        elif self.irrep in SU4_A2_irrep_names:
            irrep = 'a2'
        else:
            raise NotImplementedError("SpectroTask only knows about F, A2 irreps, not {r}".format(r=self.irrep))
            
        key_tuple = (self.Nc, irrep, self.screening, self.p_plus_a, self.compute_baryons)
        
        try:
            return local_taxi.multirep_spectro_binaries[key_tuple]
        except KeyError:
            raise NotImplementedError("Missing binary for (Nc, irrep, screening?, p+a?, compute_baryons?)="+str(key_tuple))
    binary = fixable_dynamic_attribute(private_name='_binary', dynamical_getter=_dynamic_get_binary)


