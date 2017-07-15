#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 12 20:45:17 2017

@author: dchackett
"""

from random import seed, randint

import os

import jobs
from _utility import sanitized_path


class ConfigGenerator(jobs.Runner):
    """Abstract superclass of tasks that run some external binary that
    generates a (gauge) field configuration."""

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
            self.start_traj = 0
        
        elif isinstance(starter, ConfigGenerator):
            # Start where another ConfigGenerator left off
            self.loadg = starter.saveg
            self.start_traj = starter.final_traj
        
        elif isinstance(starter, str):
            # Start from a file
            starter = sanitized_path(starter)
            assert os.path.exists(starter), \
                "ConfigGenerator must be provided a valid path to a starter; {s} does not exist".format(s=starter)
            self.loadg = starter
            
            # Figure out starting trajectory -- user provided (overrides), or use file name conventions
            if start_traj is not None:
                self.start_traj = start_traj
            else:
                raise NotImplementedError("Don't know how to parse start_traj out of config filename yet.")
                
        else:
            raise TypeError("Don't know what to do with starter type: {s}".format(s=type(starter)))

        self.final_traj = self.start_traj + self.n_traj

        # TODO: Use modularized file name conventions...
        self.saveg = "cfg_%d"%self.final_traj
        
    def build_input_string(self):
        input_str = super(ConfigGenerator, self).build_input_string().strip()
        input_str += "<< EOF >> " + self.fout + "\n"
        return input_str
        
        
        
        
        
class ConfigMeasurement(jobs.Runner):
    """Abstract superclass of tasks that run some external binary that performs
    a measurement on a stored (gauge) field configuration."""
    
    def __init__(self, measure_on, **kwargs):
        
        super(ConfigMeasurement, self).__init__(**kwargs)
        
        ## Flexible behavior for finding config to measure on
        if isinstance(measure_on, str):
            # Measure on a stored gauge file
            measure_on = sanitized_path(measure_on)
            assert os.path.exists(measure_on), \
                "ConfigMeasurement must be provided a valid path to a configuration; {s} does not exist".format(s=measure_on)
            self.loadg = measure_on
        
        elif isinstance(measure_on, ConfigGenerator):
            # Measure on the config file saved by a ConfigGenerator task
            # Steal parameters from the ConfigGenerator
            self.steal_params_from_config_generator(measure_on)
            self.loadg = measure_on.saveg
            self.depends_on.append(measure_on) # Depend on the ConfigGenerator
            
            
    def build_input_string(self):
        input_str = super(ConfigMeasurement, self).build_input_string().strip()
        input_str += "<< EOF >> " + self.fout + "\n"
        return input_str
        
    
    def steal_params_from_config_generator(self, config_generator):
        assert isinstance(config_generator, ConfigGenerator)
        
        cg_dict = config_generator.to_dict()
        
        # Remove metainfo from cg_dict
        for special_key in jobs.special_keys:
            cg_dict.pop(special_key, None)
        
        # Rename certain dangerously-named parameters like '...'->'cg_...'
        for fix_param in ['fout', 'saveg', 'loadg', 'binary', 'seed']:
            cg_dict['cg_' + fix_param] = cg_dict.pop(fix_param, None)        
        cg_dict['traj'] = cg_dict.pop('final_traj')
        
        # Load parameters from CG in to this object
        for k, v in cg_dict.items():
            setattr(self, k, v)
        
        

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
        # Randomly generate a different seed for each hmc run
        seed(streamseed%10000)      
        seeds = [randint(0, 9999) for nn in range(N)]
    
    ## Assemble stream
    stream = []
    for cc in range(N):
        # Make new job and add it to list
        new_job = config_generator_class(starter=starter, seed=seeds[cc], **kwargs)
        stream.append(new_job)
        
        # ConfigGenerators in a stream depend on the previous ConfigGenerator
        # If the stream is forked off another stream, first ConfigGenerator in
        #  stream must depend on forked-off-of ConfigGenerator
        if isinstance(starter, ConfigGenerator):
            new_job.depends_on.append(starter)
        
        # Next ConfigGenerator will depend on this ConfigGenerator
        starter = new_job
        
    # Let the first job know it's the beginning of a new branch/sub-trunk
    stream[0].branch_root = True
        
    return stream  


def measure_on_config_generators(config_measurement_class, config_generators,
                                 start_at_traj=0,
                                 config_generator_filter=lambda g: True,
                                 **kwargs):
    """Assembles a pool of ConfigMeasurement tasks, applied to the ConfigGenerators
    in the provided task pool config_generators.
    
    Args:
        config_measurement_class: A subclass of ConfigMeasurement.
        config_generators: A pool of Tasks.  Tasks in this pool that are not
            subclasses of ConfigGenerator will be silently ignored.
        start_at_traj: Don't make a measurement on a ConfigGenerator cg unless
            cg.final_traj >= start_at_traj.  Intended to be used to conveniently
            avoid running measurements on not-yet-equilibrated field configurations.
        config_generator_filter: Function that takes a ConfigGenerator and returns
            True if it should be measured upon, and False if it should not.  For
            more intelligent filtering than provided by "start_at_traj".
        **kwargs: Arguments to be passed along to each ConfigMeasurement.
    """
    
    assert issubclass(config_measurement_class, ConfigMeasurement), \
        "config_measurement_class must be a subclass of ConfigMeasurement, not {c}".format(c=str(config_measurement_class))
    
    measurements = []
    for cg in config_generators:
        if not isinstance(cg, ConfigGenerator):
            continue # Instead of raising, just skip; allows this function to operate on entire pools conveniently
        
        if cg.final_traj < start_at_traj:
            continue
        
        if not config_generator_filter(cg):
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