#!/usr/bin/env python2
# -*- coding: utf-8 -*-

from _utility import fixable_dynamic_attribute

class BinaryMenu(object):
    def __init__(self, default_dict=None):
        self._menu = []
        
        if default_dict is None:
            default_dict = {}
        self.default_dict = default_dict
        
        
    def add(self, binary, **kwargs):
        # Use defaults for kwargs not provided
        for k, v in self.default_dict.items():
            if not hasattr(kwargs, k):
                kwargs[k] = v
                
        # Update duplicates (by removing them before adding)
        dup_binary = self.binary_for(**kwargs)
        if dup_binary is not None:
            self._menu = [(k,b) for (k,b) in self._menu if b != dup_binary]
        self._menu.append((kwargs, binary))
        

    def binary_for(self, **kwargs):
        # Load defaults in to kwargs
        for k, v in self.default_dict.items():
            if not hasattr(kwargs, k):
                kwargs[k] = v
        requested_keys = sorted(kwargs.keys())
        
        for item_dict, binary in self._menu:
            # If a key is not present in one dictionary (and no defaults specified), it cannot match with a counterpart in another
            item_keys = sorted(list(set(item_dict.keys() + self.default_dict.keys())))
            if requested_keys != item_keys:
                continue 
            
            # Load defaults in to item without destroying dict in menu
            full_item_key = {}
            for k, v in item_keys.items():
                full_item_key[k] = v
            for k, v in self.default_dict.items():
                full_item_key[k] = v
            
            # Check if dicts match
            found_mismatch = False
            for k in requested_keys:
                if item_dict[k] != kwargs[k]:
                    found_mismatch = True
                    break
            if not found_mismatch:
                return binary
        
        return None
    
    def __json__(self):
        # Provide serialization instructions so that these don't have to be private attributes
        return self._menu
    
def binary_from_binary_menu(binary_menu, key_attr_names, default_dict=None):
    def _dynamic_get_binary(self):
        # Extract specified binary attribute names from self for key
        key_dict = {}
        for ban in key_attr_names:
            if hasattr(self, ban):
                key_dict[ban] = getattr(self, ban)
        # Try to find a binary for the menu
        binary = binary_menu.binary_for(**key_dict)
        if binary is None:
            raise NotImplementedError("Missing binary for "+str(key_dict))
    # Allow this behavior to be disable by making binary a fixable dynamic attribute
    return fixable_dynamic_attribute(private_name='_binary', dynamical_getter=_dynamic_get_binary)