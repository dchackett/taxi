#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 12 20:45:17 2017

@author: dchackett
"""

from random import seed, randint

import os

import jobs
from taxi import sanitized_path

import taxi.fn_conventions

privileged_param_names = [
    'fout',  # Output file for inline measurements and diagnostic outputs
    'saveg', # File to store configuration in at end of binary
    'loadg', # File to load configuration from at beginning of run
    'binary', # Binary to run
    'seed',   # Seed for binary
    
    # Specific to ConfigGenerator
    'start_traj', # Index of starting trajectory of MCMC run
    'n_traj', # Number of MCMC trajectories to run
    'final_traj', # Index of trajectory MCMC run will end on
]


class BasicMCMCFnConvention(taxi.fn_conventions.FileNameConvention):
    def __init__(self, prefix=None, *args, **kwargs):
        self.prefix = prefix
        
    def write(self, params):
        return "{prefix}_{traj}".format(prefix=self.prefix, traj=params['final_traj'])
    
    def read(self, fn):
        words = os.path.basename(fn).split('_')
        return {
            'prefix' : words[0],
            'traj' : words[-1]
        }
        


class MCMC(jobs.Runner):
    ## Modular file naming convention defaults
    saveg_filename_prefix = 'cfg'
    saveg_filename_convention = BasicMCMCFnConvention
    fout_filename_convention = BasicMCMCFnConvention
    loadg_filename_convention = BasicMCMCFnConvention
    
    
    def build_input_string(self):
        """Take a heredoc, terminating in EOF, as input.  Reroute output to output
        file self.fout
        """
        input_str = super(MCMC, self).build_input_string().strip()
        input_str += "<< EOF >> " + self.fout + "\n"
        return input_str
    
    
    ## Common "start from"/"measure on" behavior (load params from config_geneartor or filename)
    def start_from_config_generator(self, config_generator):
        """Steal parameters from specified config_generator.
        
        Stores privileged MCMC attribute names like fout, saveg, loadg (listed in
        mcmc.privileged_param_names) as 'cg_...' to not clobber anything important.
        """
        assert isinstance(config_generator, ConfigGenerator)
        
        cg_dict = config_generator.to_dict()
        
        # Remove metainfo from cg_dict
        for special_key in jobs.special_keys:
            cg_dict.pop(special_key, None)
        
        # Rename certain dangerously-named parameters like '...'->'cg_...'
        for fix_param in privileged_param_names:
            if cg_dict.has_key(fix_param):
                cg_dict['cg_' + fix_param] = cg_dict.pop(fix_param)
                
        # Load parameters from CG in to this object
        for k, v in cg_dict.items():
            setattr(self, k, v)
            
        # Load the output configuration of this ConfigGenerator
        self.loadg = config_generator.saveg
        
        # Need the config generator to run first
        self.depends_on.append(config_generator)
            
            
    def start_from_config_file(self, loadg):
        loadg = sanitized_path(loadg)
        
        # Measure on a stored gauge file
        assert os.path.exists(loadg), "Need a valid path to a configuration; {s} does not exist".format(s=loadg)
        self.loadg = loadg
        
        # Steal parameters from parsed filename
        self.__dict__.update(**self.parse_params_from_loadg(self.loadg))
            
    
    ## Modular file name conventions
    # Input: Loaded gauge file
    def parse_params_from_loadg(self, fn):
        parsed = taxi.fn_conventions.parse_with_conventions(fn=fn, conventions=self.loadg_filename_convention)        
        if parsed is None:
            raise ValueError("Specified filename convention(s) {fnc} cannot parse filename {fn}".format(fnc=self.loadg_filename_convention, fn=fn))
        assert parsed.has_key('traj'), "FileNameConvention must return a key 'traj' when processing a configuration file name"
        return parsed
    
    # Output: Saved final configuration file       
    def _dynamic_get_saveg(self):
        return self.saveg_filename_convention(prefix=self.saveg_filename_prefix).write(self.to_dict())        
    saveg = taxi.fixable_dynamic_attribute(private_name='_saveg', dynamical_getter=_dynamic_get_saveg)
    
    # Output: Binary diagnostic and inline measurement outputs
    def _dynamic_get_fout(self):
        return self.fout_filename_convention(prefix=self.fout_filename_prefix).write(self.to_dict())    
    fout = taxi.fixable_dynamic_attribute(private_name='_fout', dynamical_getter=_dynamic_get_fout)
    


class ConfigGenerator(MCMC):
    """Abstract superclass of tasks that run some external binary that
    generates a (gauge) field configuration."""
    
    fout_filename_prefix = 'mcmc'
    
    def __init__(self, seed, n_traj=1,
                 starter=None,
                 start_traj=None, **kwargs):
        
        super(ConfigGenerator, self).__init__(**kwargs)
        
        self.n_traj = n_traj
        self.seed = seed
        
        ## Flexible seed configuration behavior
        if starter is None:
            # Fresh start
            self.loadg = None
            self.start_traj = (0 if start_traj is None else start_traj)
        
        elif isinstance(starter, ConfigGenerator):
            # Start where another ConfigGenerator leaves off
            self.start_from_config_generator(starter) # Steals parameters, adds to dependencies, etc
            self.start_traj = starter.final_traj
        
        elif isinstance(starter, str):
            # Start from a saved file -- try to steal parameters from filename
            self.start_from_config_file(starter)
            self.start_traj = self.__dict__.pop('traj', None) # traj should have been parsed out
            
            # Figure out starting trajectory -- user provided (overrides), or use file name conventions
            if start_traj is not None:
                self.start_traj = start_traj
            else:
                raise NotImplementedError("Don't know how to parse start_traj out of config filename yet.")
                
        else:
            raise TypeError("Don't know what to do with starter type: {s}".format(s=type(starter)))

        self.final_traj = self.start_traj + self.n_traj
        
    
                
        
class ConfigMeasurement(MCMC):
    """Abstract superclass of tasks that run some external binary that performs
    a measurement on a stored (gauge) field configuration."""
    
    fout_filename_prefix = 'meas'
        
    def __init__(self, measure_on, **kwargs):
        
        super(ConfigMeasurement, self).__init__(**kwargs)
        
        ## Flexible behavior for finding config to measure on
        if isinstance(measure_on, str):
            # Measure on some existing configuration file
            self.start_from_config_file(measure_on)
            
        elif isinstance(measure_on, ConfigGenerator):
            # Measure on the config file saved by a ConfigGenerator task
            self.start_from_config_generator(measure_on) # Steal parameters from the ConfigGenerator
            self.traj = measure_on.final_traj

            
                


### Stream functions
def make_config_generator_stream(config_generator_class, N,
                                 starter=None, # default: fresh start
                                 streamseed=None, seeds=None, # One of these must be provided
                                 **kwargs):
    """Assembles a stream of sequential ConfigGenerators, each picking up where the last left off.
    
    Args:
        config_generator_class: A subclass of ConfigGenerator.
        N (int): Number of ConfigGenerators to build the stream out of.
        starter: Where to start the stream.  Passed to the first ConfigGenerator
            in the stream, whose default behavior is: if None, then fresh start.  If a string, then
            load the config stored in the file with that filename.  If a ConfigGenerator,
            then start the stream on the saved output configuration of that ConfigGenerator.
        streamseed: A metaseed, used to seed an RNG that then produces N many seeds,
            which are passed to the ConfigGenerators in the stream as seeds. Either
            streamseed or seeds must be provided.  If seeds is provided, overrides
            streamseed.
        seeds: An ordered list of seeds to be fed to each ConfigGenerator in the
            stream.  Must provide at least N seeds.  If more than N are provided,
            ignores the unneeded seeds at the end of the list.  Either streamseed
            or seeds must be provided.  If seeds is provided, overrides streamseed.
        **kwargs: Arguments to pass along to each ConfigGenerator in the stream unmodified.
    
    Returns:
        List of ConfigGenerator tasks.
    """

    assert issubclass(config_generator_class, ConfigGenerator), \
        "config_generator_class must be a subclass of ConfigGenerator, not {c}".format(c=str(config_generator_class))
    
    assert streamseed is not None or seeds is not None, \
        "Must provide either a stream metaseed 'streamseed' or a seed for each ConfigGenerator 'seed=[...]'"
    if seeds is not None:
        assert len(seeds) >= N, "Must provide enough seeds for every ConfigGenerator"
        
    # Make our own seeds from streamseed, unless we've been manually provided seeds
    if seeds is None:
        # Randomly (with seed) generate a different seed for each MCMC run
        seed(streamseed%10000)      
        seeds = [randint(0, 9999) for nn in range(N)]
    
    ## Assemble stream
    stream = []
    for cc in range(N):
        # Make new job and add it to list
        new_job = config_generator_class(starter=starter, seed=seeds[cc], **kwargs)
        stream.append(new_job)
        
        # Next ConfigGenerator will depend on this ConfigGenerator
        starter = new_job
        
    # Let the first job know it's the beginning of a new branch/sub-trunk
    stream[0].branch_root = True
        
    return stream  


def measure_on_config_generators(config_measurement_class, measure_on,
                                 start_at_traj=0,
                                 **kwargs):
    """Assembles a pool of ConfigMeasurement tasks, applied to the ConfigGenerators
    in the provided task pool config_generators.
    
    Args:
        config_measurement_class: A subclass of ConfigMeasurement.
        measure_on: A pool of Tasks.  Tasks in this pool that are not
            subclasses of ConfigGenerator will be silently ignored.
        start_at_traj: Don't make a measurement on a ConfigGenerator cg unless
            cg.final_traj >= start_at_traj.  Intended to be used to conveniently
            avoid running measurements on not-yet-equilibrated field configurations.
        **kwargs: Arguments to be passed along to each ConfigMeasurement.
    """
    
    assert issubclass(config_measurement_class, ConfigMeasurement), \
        "config_measurement_class must be a subclass of ConfigMeasurement, not {c}".format(c=str(config_measurement_class))
    
    measurements = []
    for cg in measure_on:
        if not isinstance(cg, ConfigGenerator):
            continue # Instead of raising, just skip; allows this function to operate on entire pools conveniently
        
        if cg.final_traj < start_at_traj:
            continue
        
        measurements.append(config_measurement_class(measure_on=cg, **kwargs))
        
    return measurements


def measure_on_files(config_measurement_class, filenames, **kwargs):
    """Assembles a pool of ConfigMeasurement tasks, applied to saved configurations
    in the provided pool of filenames in filenames.
    
    Args:
        config_measurement_class: A subclass of ConfigMeasurement.
        filenames: A list of filenames.  Filenames that point to nonexistent files
            will be ignored without Exception, but a complaint will be printed.
        **kwargs: Arguments to be passed along to each ConfigMeasurement.
    """
    
    assert issubclass(config_measurement_class, ConfigMeasurement), \
        "config_measurement_class must be a subclass of ConfigMeasurement, not {c}".format(c=str(config_measurement_class))
    
    measurements = []
    for fn in filenames:
        
        if not os.path.exists(sanitized_path(fn)):
            print "Can't apply measurement {m} to {fn}: file does not exist.".format(m=str(config_measurement_class, fn=fn))
            continue # Instead of raising, just skip; more convenient
        
        measurements.append(config_measurement_class(measure_on=fn, **kwargs))
        
    return measurements