import inspect
from typing import List, Optional

from common.logger import *
from common.logger import BatchLogger



class Container:
    def __init__(self, logger : Optional[BatchLogger] = None, register_logger : bool = True):
        self._registry = {}
        self._singletons = {}
        self._logger = logger
        
        if register_logger and logger:
            self.register(
                dependency_type = BatchLogger, 
                implementation = logger, 
                is_singleton = True)


    def register(self, dependency_type, implementation = None, is_singleton = False):
        if not implementation:
            implementation = dependency_type

        if dependency_type in self._registry: return

        is_implementation_instance = isinstance(implementation, dependency_type)

        if not is_implementation_instance:
            for base in inspect.getmro(implementation):
                if base not in (object, dependency_type):
                    self._registry[base] = implementation
        else:
            if (self._logger is not None):
                implementation = trace(logger = self._logger)(implementation)                
            
        self._registry[dependency_type] = implementation
        
        if (is_singleton):
            if not is_implementation_instance:
                self._singletons[dependency_type] = None
            else:
                self._singletons[dependency_type] = implementation


    def resolve(self, dependency_type):
        if dependency_type not in self._registry:
            raise ValueError(f"Dependency '{dependency_type}' not registered")
        implementation = self._registry[dependency_type]
        if (isinstance(implementation, dependency_type)):
            return implementation
        constructor_signature = inspect.signature(implementation.__init__)
        constructor_params = constructor_signature.parameters.values()

        dependencies = [
            self.resolve(param.annotation)
            for param in constructor_params
            if param.annotation is not inspect.Parameter.empty
        ]

        if (dependency_type in self._singletons):
            if (self._singletons[dependency_type] is not None):
                return self._singletons[dependency_type]

            if (self._logger is not None):
                self._singletons[dependency_type] = trace(logger = self._logger)(implementation(*dependencies))
            else: 
                self._singletons[dependency_type] = implementation(*dependencies)

            return self._singletons[dependency_type]
        
        if (self._logger is not None):
            return trace(logger = self._logger)(implementation(*dependencies))

        return implementation(*dependencies)
    
    

