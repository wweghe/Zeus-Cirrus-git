from typing import Any, List, Dict, Type
import json
from types import SimpleNamespace

import common.constants as constants

from domain.state import SharedStateProxy
from domain.state import SharedStateKeyEnum

from repositories.solution_repository import SolutionRepository
from repositories.object_registration_repository import ObjectRegistrationRepository


class ObjectRegistrationService:
    
    def __init__(self, 
                 object_reg_repository : ObjectRegistrationRepository,
                 shared_state :  SharedStateProxy
                ) -> None:

        if (object_reg_repository is None): raise ValueError(f"object_reg_repository cannot be empty")
        if (shared_state is None): raise ValueError(f"shared_state cannot be empty")

        self._object_reg_repository = object_reg_repository
        self._state = shared_state

    
    _object_reg_repository : ObjectRegistrationRepository = None
    _state : SharedStateProxy = None


    def get_field_definitions(self, rest_path : str):

        registration = self.get_object_registration(rest_path)
        
        return registration.fieldDefinitions
    

    def get_classification(self, rest_path : str):
        
        registration = self.get_object_registration(rest_path)
        
        if (hasattr(registration, "classification")):
            return registration.classification

        return None
    

    def get_object_registration(self, 
                                rest_path: str, 
                                throw_error_if_not_found : bool = True) -> SimpleNamespace:

        if rest_path is None or len(str(rest_path)) == 0: raise ValueError(f"rest_path cannot be empty")
        object_registration : SimpleNamespace = None
        self._state.lock()

        try:
            registrations : Dict[str, SimpleNamespace] = self._state.get(SharedStateKeyEnum.OBJECT_REGISTRATIONS, {})

            object_registration = registrations.get(rest_path, None)
            if (object_registration is None):
                reg_list = self._object_reg_repository.get_by_filter(
                    start = 0,
                    limit = 1,
                    filter = f"eq(restPath,'{rest_path}')"
                )
                if (reg_list is None or len(reg_list) == 0):
                    if throw_error_if_not_found:
                        raise RuntimeError(f"Object registration for rest path '{rest_path}' was not found.")
                else:
                    object_registration = reg_list[0]
                    registrations.update({ rest_path: object_registration })
                    self._state.update(SharedStateKeyEnum.OBJECT_REGISTRATIONS, registrations)
        finally:
            self._state.unlock()

        if (object_registration is None and throw_error_if_not_found): 
            raise RuntimeError(f"Loading object registration for rest path '{rest_path}' failed")
            
        return object_registration
    

    def get_all_object_registration_rest_paths(self) -> List[str]:

        rest_paths : List[str] = None
        self._state.lock()

        try:
            rest_paths = self._state.get(SharedStateKeyEnum.OBJECT_REST_PATHS, None)

            if (rest_paths is None):
                object_registrations = self._object_reg_repository.get_by_filter(
                    start = 0,
                    limit = constants.FETCH_OBJECTS_LIMIT_COUNT,
                    filter = "",
                    fields = ["restPath"]
                )
                rest_paths = [reg.restPath for reg in object_registrations]
                self._state.update(SharedStateKeyEnum.OBJECT_REST_PATHS, rest_paths)
        finally:
            self._state.unlock()

        if (rest_paths is None): 
            raise RuntimeError(f"Loading object registration rest paths failed")

        return rest_paths
    

