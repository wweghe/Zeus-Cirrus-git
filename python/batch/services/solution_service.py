from typing import Any, List, Dict, Type
import json
from types import SimpleNamespace

from common.logger import BatchLogger
from common.errors import *

from domain.state import SharedStateProxy
from domain.state import SharedStateKeyEnum
from domain.launch_arguments import LaunchArguments

from repositories.solution_repository import SolutionRepository
from repositories.object_registration_repository import ObjectRegistrationRepository
from repositories.user_repository import UserRepository


class SolutionService:

    _solution_repository : SolutionRepository = None
    # state data
    _solution_details : Any = None
    _object_registration_rest_paths : List[str] = None
    _current_user : Any = None
    _state : SharedStateProxy = None
    _batch_logger : BatchLogger = None
    _launch_args : LaunchArguments = None


    def __init__(self, 
                 solution_repository : SolutionRepository, 
                 object_reg_repository : ObjectRegistrationRepository,
                 user_repository : UserRepository,
                 shared_state :  SharedStateProxy,
                 launch_args : LaunchArguments
                ) -> None:

        if (solution_repository is None): raise ValueError(f"solution_repository cannot be empty")
        if (object_reg_repository is None): raise ValueError(f"object_reg_repository cannot be empty")
        if (shared_state is None): raise ValueError(f"shared_state cannot be empty")
        if (launch_args is None): raise ValueError(f"launch_args cannot be empty")
        if (user_repository is None): raise ValueError(f"user_repository cannot be empty")

        self._solution_repository = solution_repository
        self._object_reg_repository = object_reg_repository
        self._user_repository = user_repository
        self._state = shared_state
        self._launch_args = launch_args


    # private members
    def __get_solution_details(self, reload : bool = False) -> Any:

        solution_details = None
        self._state.lock()
        
        try:
            solution_details = self._state.get(SharedStateKeyEnum.SOLUTION_DETAILS, None)

            if (reload or solution_details is None): 
                solution = self._solution_repository.get_by_short_name(short_name = self._launch_args.solution)
                if (solution is not None):
                    solution_details, _ = self._solution_repository.get_by_key(key = solution.key)
                    self._state.update(SharedStateKeyEnum.SOLUTION_DETAILS, solution_details)
        finally:
            self._state.unlock()

        if (solution_details is None): 
            raise RuntimeError(f"Loading solution '{self._launch_args.solution}' details failed")

        return solution_details
    

    def __get_application_property(self, 
                                   property_name : str, 
                                   return_type : Type = str):
        solution_details = self.__get_solution_details()
        prop = getattr(solution_details.ui.application.configurationProperties, property_name, None)
        if prop is None:
            raise ConfigurationPropertyNotFoundError(property_name)
        has_value = (getattr(prop, "value", None) is not None and len(str(prop.value)) > 0)

        if (return_type in [object, SimpleNamespace]):
            if has_value:
                return json.loads(prop.value, object_hook = lambda d: SimpleNamespace(**d))
            return None
        # elif(return_type in [bool]):
        #     if has_value:
        #         return config_value
        #         # return not config_value.lower() in ["false", "0"]
        #     return None
        else:
            return prop.value
    

    # public members
    def get_analysis_run_code_library_default(self) -> SimpleNamespace:
        return self.__get_application_property(property_name = 'AnalysisRuns.codeLibrary.default',
                                               return_type = SimpleNamespace)
    

    def get_cycle_code_library_default(self) -> SimpleNamespace:
        return self.__get_application_property(property_name = 'Cycles.codeLibrary.default',
                                               return_type = SimpleNamespace)
    

    def get_analysis_run_config_set_default(self) -> SimpleNamespace:
        return self.__get_application_property(property_name = 'AnalysisRuns.configurationSet.default',
                                               return_type = SimpleNamespace)
    

    def get_cycle_config_set_default(self) -> SimpleNamespace:
        return self.__get_application_property(property_name = 'Cycles.configurationSet.default',
                                               return_type = SimpleNamespace)
    

    def get_cycle_init_task_name_default(self) -> str:
        return self.__get_application_property(property_name = 'Cycles.initTaskName.default')
    

    def get_cycle_run_script_transition_name(self) -> str:
        return self.__get_application_property(property_name = 'Cycles.workflowTransitions.validateParameters')
    

    def get_cycle_skip_transition_name(self) -> str:
        return self.__get_application_property(property_name = 'Cycles.workflowTransitions.skipTaskValue')
    

    def get_cycle_state_enabled(self) -> bool:
        return self.__get_application_property(property_name = 'Cycles.state.enabled', return_type = bool)
    

    def get_cycle_state_default(self) -> str:
        return self.__get_application_property(property_name = 'Cycles.state.default')
    

    def get_cycle_entity_role_enabled(self) -> bool:
        return self.__get_application_property(property_name = 'Cycles.entityRole.enabled', return_type = bool)
    

    def get_current_user(self, reload : bool = False) -> None:

        current_user = None
        self._state.lock()

        try:
            current_user : SimpleNamespace = self._state.get(SharedStateKeyEnum.CURRENT_USER, None)

            if (reload or current_user is None):
               current_user = self._user_repository.get_current_user()
               self._state.update(SharedStateKeyEnum.CURRENT_USER, current_user)
        finally:
            self._state.unlock()

        if (current_user is None): 
            raise RuntimeError(f"Loading current user failed")
            
        return current_user

