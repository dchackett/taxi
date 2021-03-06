#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 12 20:45:17 2017

@author: dchackett
"""

from random import seed, randint

import os

import tasks
from taxi import sanitized_path, expand_path
from taxi.file import File, InputFile, FileInterface, should_save_file, should_load_file

        


class MCMC(tasks.Runner):
    """Abstract superclass for Runners that run binaries from typical MCMC suites
    (e.g., MILC)."""
    
    ## Modular file naming conventions
    # Output files
    saveg_filename_prefix = 'cfg'
    saveg = File(conventions="{saveg_filename_prefix}_{traj:d}", save=True)
    fout_filename_prefix = 'fout'
    fout = File(conventions="{fout_filename_prefix}_{traj:d}", save=True)
    # Input files -- Won't be tracked by rollbacker, automatically parse and load parameters
    loadg = InputFile(conventions="{loadg_filename_prefix}_{traj:d}", save=False)
    
 
    def __init__(self, save_config=True, **kwargs):
        super(MCMC, self).__init__(**kwargs)
        
        if self.saveg is not None:
            self.saveg.save = save_config
    
    
    def build_input_string(self):
        """Take a heredoc, terminating in EOF, as input.  Reroute output to output
        file self.fout
        """
        input_str = super(MCMC, self).build_input_string().strip()
        input_str += "<< EOF >> {0}\n".format(self.fout)
        return input_str
    
    
    ## Common "start from"/"measure on" behavior (load params from config_generator or filename)
    def start_from_config_generator(self, config_generator):
        """Steal parameters from specified config_generator.
        
        Stores privileged MCMC attribute names like fout, saveg, loadg (listed in
        mcmc.privileged_param_names) as 'cg_...' to not clobber anything important.
        """
        assert isinstance(config_generator, ConfigGenerator)
        
        cg_dict = config_generator.to_dict()
        
        # Remove metainfo from cg_dict
        for special_key in tasks.special_keys:
            cg_dict.pop(special_key, None)
        
        # Rename certain dangerously-named parameters like '...'->'cg_...'
        for fix_param in privileged_param_names:
            if cg_dict.has_key(fix_param):
                cg_dict['cg_' + fix_param] = cg_dict.pop(fix_param)
                
        # Load parameters from CG in to this object
        for k, v in cg_dict.items():
            if getattr(self, k, None) is None: # Don't override anything already present
                setattr(self, k, v)
        
        # Need the config generator to run first
        self.depends_on.append(config_generator)
        
        # Set loadg without parsing
        if isinstance(self.loadg, basestring):
            del self.loadg # Remove any override that is present
        self.loadg._value_override = config_generator.saveg
            
            
    def start_from_config_file(self, loadg, delay_fn_exists_check=True):
        """Sets up MCMC task to run the specified config file loadg.  Uses file
        naming conventions (see taxi.file) to parse parameters from the filename.
        Specified config file must exist at time of task specification unless
        self.delay_fn_exists_check.
        """
        assert loadg is not None
        loadg = sanitized_path(str(loadg))
        
        if not delay_fn_exists_check:
            assert os.path.exists(loadg), "Need a valid path to a configuration; {s} does not exist".format(s=loadg)
        
        self.loadg = loadg # Automatically parses out parameters and loads them in to the object

            
    def execute(self, cores=None):
        """Versus Runner, checks to make sure loadg exists before running.
        """
        assert not should_load_file(self.loadg) or os.path.exists(str(self.loadg)),\
            "Error: file {loadg} does not exist.".format(loadg=self.loadg)
   
        super(MCMC, self).execute(cores=cores)
        
    
    
    ## Standard output verification
    def verify_output(self):
        """Checks to make sure that output file fout and output gauge configuration
        saveg exist after running.
        """
        super(MCMC, self).verify_output()
        
        ## In the future, we can define custom exceptions to distinguish the below errors, if needed
        
        # If this task should save a gauge file, that gauge file must exist
        if should_save_file(self.saveg) and not os.path.exists(str(self.saveg)):
            print "MCMC ok check fails: Config file {0} doesn't exist.".format(self.saveg)
            raise RuntimeError
            
        # If this task should save an output file, that output file must exist
        if should_save_file(self.fout) and not os.path.exists(str(self.fout)):
            print "MCMC ok check fails: Output file {0} doesn't exist.".format(self.fout)
            raise RuntimeError
            

    
    

class ConfigGenerator(MCMC):
    """Abstract superclass of tasks that run some external binary that
    generates a (gauge) field configuration."""
    
    fout_filename_prefix = 'mcmc'
    
    def __init__(self, seed, n_traj=1,
                 starter=None,
                 start_traj=None, **kwargs):
        """Specify a task that will call a binary that generates configurations.
        
        Args:
            seed - Random seed; no default to prevent accidentally repeating computations.
            n_traj - Number of trajectories/sweeps/etc. for the binary to run.
            start_traj - What trajectory to start counting at? If not specified,
             determined based on starter: if fresh start, initial traj is traj 0;
             if starting from a ConfigGenerator, initial trja is final_traj for
             that ConfigGenerator; if starting from a file, initial traj is parsed
             out of the filename (must be returned as 'traj').
            starter - Specifies where to get the starting configuration for the
            ConfigGenerator.  If None, fresh start; if a ConfigGenerator, start
            from the field output by that ConfigGenerator, stealing physical parameters
            from that ConfigGenerator (to continue an MCMC sequence); if a stored
            config file, start from that field, stealing physical parameters from
            the filename as parsed by file naming conventions (see taxi.file).
        """
        super(ConfigGenerator, self).__init__(**kwargs)
        
        self.n_traj = n_traj
        self.seed = seed
        
        self.trunk = True # ConfigGenerators are "trunk" tasks
        
        ## Flexible seed configuration behavior
        if starter is None:
            # Fresh start
            self.loadg = None
            self.start_traj = (0 if start_traj is None else start_traj)
        
        elif isinstance(starter, ConfigGenerator):
            # Start where another ConfigGenerator leaves off
            self.start_from_config_generator(starter) # Steals parameters, adds to dependencies, etc
            self.start_traj = (starter.final_traj if start_traj is None else start_traj)
        
        elif isinstance(starter, str):
            # Start from a saved file -- try to steal parameters from filename
            self.start_from_config_file(starter)
            
            # Figure out starting trajectory -- user provided (overrides), or use file name conventions
            if start_traj is not None:
                self.start_traj = start_traj
            
            assert getattr(self, 'start_traj', None) is not None,\
                "Wasn't able to parse 'traj' for 'start_traj' out of config filename '{0}'.".format(starter)
                
        else:
            raise TypeError("Don't know what to do with starter type: {s}".format(s=type(starter)))

    @property
    def final_traj(self):
        return self.start_traj + self.n_traj
    @final_traj.setter
    def final_traj(self, value):
        assert value >= self.start_traj
        self.n_traj = value - self.start_traj
    
    # For compatibility with file naming conventions
    # WARNING: May lead to unpredictable behavior
    @property
    def traj(self):
        return self.final_traj
    @traj.setter
    def traj(self, value):
        self.start_traj = value

## Need to be able to steal physical parameters from ConfigGenerator
## However, we don't want any of ConfigGenerator's logistical parameters (e.g. fout, seed, trunk?, ...)
## to overwrite the logistical parameters in the ConfigGenerator/ConfigMeasurement doing the stealing
## Need a list of parameters to be careful with. Rather than maintaining a list by hand, just
## make an instance of the ConfigGenerator abstract class and see what attributes it has
privileged_param_names = ConfigGenerator(seed=0).to_dict().keys()

        
class ConfigMeasurement(MCMC):
    """Abstract superclass of tasks that run some external binary that performs
    a measurement on a stored (gauge) field configuration."""
    
    fout_filename_prefix = 'meas'
        
    def __init__(self, measure_on, save_config=False, delay_fn_exists_check=True, **kwargs):
        """Specify a task to perform a measurement on a field configuration.
        
        Args:
            measure_on - Where to find the field configuration to perform the
             measurement on. If a ConfigGenerator or ConfigMeasurement, perform
             the measurement on the output configuration, stealing physical parameters
             from the specified object; if a configuration file, perform the measurement
             on that file, stealing physical parameters from the specified filename
             using file naming conventions (see taxi.file). Compare with starter
             for ConfigGenerator.__init__.
            delay_fn_exists_check - If measure_on is a file, usually check at time
             of task specification that the file exists. However, sometimes, might
             want to delay checking until time of executing. If True, don't check
             that input file exists until executing the binary.
        """
        
        super(ConfigMeasurement, self).__init__(save_config=save_config, **kwargs)
        
        ## Flexible behavior for finding config to measure on
        if isinstance(measure_on, str):
            # Measure on some existing configuration file
            self.start_from_config_file(measure_on,
                                        delay_fn_exists_check=delay_fn_exists_check)
            
        elif isinstance(measure_on, ConfigGenerator):
            # Measure on the config file saved by a ConfigGenerator task
            self.start_from_config_generator(measure_on) # Steal parameters from the ConfigGenerator
            self.traj = measure_on.final_traj    


### Convenience functions to make streams of ConfigGenerators
def make_config_generator_stream(config_generator_class, N,
                                 starter=None, # default: fresh start
                                 streamseed=None, seeds=None, # One of these must be provided
                                 start_traj=0,
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
        start_traj: Which trajectory the stream of ConfigGenerators will begin
            counting at.  The provided trajectory number is the trajectory number
            of the configuration specified in 'starter', e.g., with the default
            of 0 and starter=None for a fresh start, the all-identity fresh starting
            configuration is configuration number 0. If None is provided, will
            take starting traj from the ConfigGenerator or parse it out of the
            filename according to provided conventions for loadg.
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
        
    # For first task in stream, restart trajectory counting (don't just continue from last stream)
    # Unless start_traj is None, in which case let the ConfigGenerator figure out start_traj from starter
    if start_traj is not None:
        kwargs['start_traj'] = start_traj
        
    # If provided a file naming convention handle, render to string
    if isinstance(starter, File) or isinstance(starter, FileInterface):
        starter = str(starter)
    
    ## Assemble stream
    stream = []
    for cc in range(N):
        # Make new task and add it to list
        new_task = config_generator_class(starter=starter, seed=seeds[cc], **kwargs)
        stream.append(new_task)
        
        # Next ConfigGenerator will depend on this ConfigGenerator
        starter = new_task
        
        # After first task, don't want to reset start_traj each time
        kwargs.pop('start_traj', None)
        
    # Let the first task know it's the beginning of a new branch/sub-trunk
    stream[0].branch_root = True
        
    return stream  


def extend_ensemble(config_generator_class, N,
                     starter,
                     streamseed=None, seeds=None, # One of these must be provided
                     **kwargs):
    """Wrapper function for make_config_generator_stream to extend an existing
    ensemble of gauge configurations. Assembles a stream of sequential ConfigGenerators,
    each picking up where the last left off.
    
    Args:
        config_generator_class: A subclass of ConfigGenerator.
        N (int): Number of ConfigGenerators to build the stream out of.
        starter: Where to start the stream.  Passed to the first ConfigGenerator
            in the stream, whose default behavior is: if a string, then
            load the config stored in the file with that filename.  If a ConfigGenerator,
            then start the stream on the saved output configuration of that ConfigGenerator.
            Must not be None, doesn't make sense to extend a fresh-start ensemble.
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

    assert starter is not None, "Cannot extend a fresh ensemble"
        
    # TODO: Check **kwargs versus parsed params to make sure physical parameters aren't
    # being overridden (?)
    
    # Passing None to start_traj lets config_generator_class get its start_traj
    # from starter, instead of resetting it
    return make_config_generator_stream(
        config_generator_class=config_generator_class,
        N=N, starter=starter, start_traj=None,
        streamseed=streamseed, seeds=seeds, **kwargs
    )
    
    

### Convenience functions to run "measurement" binaries on gauge configurations
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
            print "Can't apply measurement {m} to {fn}: file does not exist.".format(m=str(config_measurement_class), fn=fn)
            continue # Instead of raising, just skip; more convenient
        
        measurements.append(config_measurement_class(measure_on=fn, **kwargs))
        
    return measurements
