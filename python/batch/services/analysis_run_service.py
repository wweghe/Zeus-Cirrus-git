from typing import List, Any, Dict, Tuple
from types import SimpleNamespace
import uuid, collections
from datetime import datetime
import time
from string import Template

import common.constants as constants
from common.errors import *
import common.utils as utils

from domain.batch_config import BatchConfig
from domain.analysis_run_config import AnalysisRunConfig
from domain.state import ConfigStateProxy
from domain.identifier import Identifier
from domain.cirrus_object import CirrusObject
from domain.launch_arguments import LaunchArguments

from repositories.analysis_run_repository import AnalysisRunRepository
from repositories.link_type_repository import LinkTypeRepository
from repositories.code_library_repository import CodeLibraryRepository
from repositories.script_repository import ScriptRepository
from repositories.config_set_repository import ConfigSetRepository
from repositories.dimension_repository import DimensionRepository
from repositories.dimension_point_repository import DimensionPointRepository
from repositories.cycle_repository import CycleRepository

from services.solution_service import SolutionService
from services.object_registration_service import ObjectRegistrationService
from services.script_execution_service import ScriptExecutionService
from services.identifier_service import IdentifierService
from services.link_instance_service import LinkInstanceService


class AnalysisRunService:

    # private memebers
    __SCRIPT_STATUS_SUCCESS = "SUCCESS"
    __FIELD_SCRIPT_PARAMETERS = "scriptParameters"
    __FIELD_SCRIPT_PARAMETERS_UI = "scriptParametersUI"
    __CYCLE_FIELD_RUN_TYPE_CD : str = "runTypeCd"
    __CYCLE_FIELD_RUN_TYPE_CD_PROD : str = "PROD"
    __FIELD_STATUS_CD : str = "statusCd"

    __supported_link_types : Dict[str, SimpleNamespace] = None

    # protected members
    _batch_config : BatchConfig = None
    _analysis_run_repository : AnalysisRunRepository = None
    _link_type_repository : LinkTypeRepository = None
    _script_repository : ScriptRepository = None
    _code_library_repository : CodeLibraryRepository = None
    _config_set_repository : ConfigSetRepository = None
    _solution_service : SolutionService = None
    _object_registration_service : ObjectRegistrationService = None
    _script_execution_service : ScriptExecutionService = None
    _dimension_repository : DimensionRepository = None
    _dimension_point_repository : DimensionPointRepository = None
    _cycle_repository : CycleRepository = None
    _link_instance_service : LinkInstanceService = None


    def __init__(self, 
                 analysis_run_repository : AnalysisRunRepository,
                 link_type_repository : LinkTypeRepository,
                 code_library_repository: CodeLibraryRepository,
                 config_set_repository : ConfigSetRepository,
                 script_repository : ScriptRepository,
                 dimension_repository : DimensionRepository,
                 dimension_point_repository : DimensionPointRepository,
                 cycle_repository : CycleRepository,
                 solution_service : SolutionService,
                 object_registration_service : ObjectRegistrationService,
                 script_execution_service : ScriptExecutionService,
                 config_state : ConfigStateProxy,
                 identifier_service : IdentifierService,
                 link_instance_service : LinkInstanceService,
                 launch_args : LaunchArguments
                 ) -> None:
        
        if (analysis_run_repository is None): raise ValueError(f"analysis_run_repository cannot be empty")
        if (link_type_repository is None): raise ValueError(f"link_type_repository cannot be empty")
        if (solution_service is None): raise ValueError(f"solution_service cannot be empty")
        if (object_registration_service is None): raise ValueError(f"object_registration_service cannot be empty")
        if (code_library_repository is None): raise ValueError(f"code_library_repository cannot be empty")
        if (config_set_repository is None): raise ValueError(f"config_set_repository cannot be empty")
        if (script_repository is None): raise ValueError(f"script_repository cannot be empty")
        if (dimension_repository is None): raise ValueError(f"dimension_repository cannot be empty")
        if (dimension_point_repository is None): raise ValueError(f"dimension_point_repository cannot be empty")
        if (cycle_repository is None): raise ValueError(f"cycle_repository cannot be empty")
        if (script_execution_service is None): raise ValueError(f"script_execution_service cannot be empty")
        if (config_state is None): raise ValueError(f"config_state cannot be empty")
        if (identifier_service is None): raise ValueError(f"identifier_service cannot be empty")
        if (link_instance_service is None): raise ValueError(f"link_instance_service cannot be empty")
        if (launch_args is None): raise ValueError(f"launch_args cannot be empty")

        self._analysis_run_repository = analysis_run_repository
        self._link_type_repository = link_type_repository
        self._code_library_repository = code_library_repository
        self._config_set_repository = config_set_repository
        self._script_repository = script_repository
        self._dimension_repository = dimension_repository
        self._dimension_point_repository = dimension_point_repository
        self._solution_service = solution_service
        self._object_registration_service = object_registration_service
        self._script_execution_service = script_execution_service
        self._cycle_repository = cycle_repository
        self._batch_config = config_state.get()
        self._identifier_service = identifier_service
        self._link_instance_service = link_instance_service
        self._launch_args = launch_args

        if self._batch_config is None:
            raise ValueError(f"Batch run configuration is not available, perhaps it was not read or parsed properly.")


    def __get_object_link(self, 
                          link_type_identifier : Dict[str, Any], 
                          object_key: str,
                          link_obj_attr_name : str = "businessObject2"):

        link_type : object = self._link_type_repository.get_by_id(
            id = link_type_identifier["id"],
            ssc = link_type_identifier["ssc"])
        
        if (link_type is None): 
            raise CirrusObjectNotFoundError(object_type = "LinkType", 
                                            id = link_type_identifier["id"],
                                            ssc = link_type_identifier["ssc"],
                                            error = "Unable to set link to analysis_run object")
        
        link =  {
                    "sourceSystemCd": str(link_type.sourceSystemCd),
                    "linkType": str(link_type.key),
                    link_obj_attr_name: str(object_key),
                    "objectId": str(uuid.uuid4())
                }
        return utils.convert_dict_to_object(link)
    

    def __remove_object_link(self, 
                             link_type : SimpleNamespace,
                             object_links : List[Dict[str, Any]],
                             object_key : str = None,
                             link_obj_attr_name : str = "businessObject2"
                            ):
        if (object_links is None or len(object_links) == 0): return object_links
        
        if (link_type is None): 
            raise CirrusObjectNotFoundError(object_type = "LinkType", 
                                            id = link_type["id"],
                                            ssc = link_type["ssc"],
                                            error = "Unable to remove link from analysis_run object")
        if object_key is not None:
            idx_remove = (i for i, e in enumerate(object_links) \
                          if e.linkType == link_type.key and e[link_obj_attr_name] == object_key)
        else:
            idx_remove = (i for i, e in enumerate(object_links) \
                          if e.linkType == link_type.key)

        for idx in idx_remove:
            object_links.pop(idx)

        return object_links

    
    def __get_analysis_run_from_config(self, 
                                       config : AnalysisRunConfig,
                                       raise_error_if_not_exists : bool = True,
                                       with_etag : bool = False
                                      ) -> Tuple[CirrusObject, str]:
        if (config is None): raise ValueError(f"config cannot be empty")

        analysis_run : CirrusObject = self._analysis_run_repository.get_by_id(
            id = config.objectId, 
            ssc = config.sourceSystemCd,
            fields = ["key"])
        
        if (analysis_run is None and raise_error_if_not_exists):
            raise CirrusObjectNotFoundError(object_type = self._analysis_run_repository.get_object_type(),
                                            id = config.objectId,
                                            ssc = config.sourceSystemCd)
        if with_etag and analysis_run is not None:
            return self._analysis_run_repository.get_by_key(key = analysis_run.key)
        
        return analysis_run, None


    def __get_default_library(self):
        config = self._solution_service.get_analysis_run_code_library_default()
        
        if (config is not None):
            return self._code_library_repository.get_by_id(
                id = config.objectId, 
                ssc = config.sourceSystemCd
                )
        
        return None
    

    def __get_default_config_set(self) -> CirrusObject:
        config = self._solution_service.get_analysis_run_config_set_default()
        
        if (config is not None):
            return self._config_set_repository.get_by_id(
                id = config.objectId, 
                ssc = config.sourceSystemCd
                )
        
        return None


    def __get_dependent_code_libraries_as_object_links(self,
                                                       primary_code_library: object
                                                      ) -> List[SimpleNamespace]:
        object_links = []

        dependent_code_libraries : List[CirrusObject] = self._code_library_repository.get_by_has_object_link_to(
                link_type_id = constants.LINK_TYPE_CODE_LIBRARY_DEPENDS_ON["id"],
                link_type_ssc = constants.LINK_TYPE_CODE_LIBRARY_DEPENDS_ON["ssc"],
                object_key = primary_code_library.key,
                link_side = 1
            )
            
        if (dependent_code_libraries is not None):
            for lib in dependent_code_libraries:
                link = self.__get_object_link(
                    link_type_identifier = constants.LINK_TYPE_ANALYSIS_RUN_CODE_LIBRARY_DEPENDENTS,
                    object_key = lib.key)
                object_links.append(link)
        return object_links
    

    def __get_job_owner_as_object_links(self) -> List[Dict[str,Any]]:

        object_links : List[Dict[str, Any]] = []
        user = self._solution_service.get_current_user()
        link = self.__get_object_link(
            link_type_identifier = constants.LINK_TYPE_ANALYSIS_RUN_JOB_OWNER,
            object_key = user.id,
            link_obj_attr_name = "user2")
        object_links.append(link)        

        return object_links
    

    def __update_object_links_from_cycle(self, 
                                         cycle_link : SimpleNamespace,
                                         object_links_existing : List[Dict[str, Any]] = None
                                        ) -> List[Dict[str, Any]]:
        
        object_links : List[Dict[str, Any]] = object_links_existing or []
        if (cycle_link is None): return object_links

        analysisRun_cycle_identifier = self._identifier_service.create_by_key_value(
            constants.LINK_TYPE_ANALYSIS_RUN_CYCLE)
        analysisRun_cycle = self.__get_supported_link_types().get(
            analysisRun_cycle_identifier.get_key(), None)
        
        analysisRun_library_identifier = self._identifier_service.create_by_key_value(
            constants.LINK_TYPE_ANALYSIS_RUN_CODE_LIBRARY)
        analysisRun_codeLibrary = self.__get_supported_link_types().get(
            analysisRun_library_identifier.get_key(), None)
        
        analysisRun_dependents_identifier = self._identifier_service.create_by_key_value(
            constants.LINK_TYPE_ANALYSIS_RUN_CODE_LIBRARY_DEPENDENTS)
        analysisRun_codeLibrary_dependents = self.__get_supported_link_types().get(
            analysisRun_dependents_identifier.get_key(), None)

        analysisRun_configSet_identifier = self._identifier_service.create_by_key_value(
            constants.LINK_TYPE_ANALYSIS_RUN_CONFIGURATION_SET)
        analysisRun_configSet = self.__get_supported_link_types().get(
            analysisRun_configSet_identifier.get_key(), None)

        # link cycle
        cycle = self._cycle_repository.get_by_id(
            id = cycle_link.objectId,
            ssc = cycle_link.sourceSystemCd,
            fields = ["key"])
        if (cycle is None):
            raise CirrusObjectNotFoundError(object_type = self._cycle_repository.get_object_type(), 
                                            id = cycle_link.objectId,
                                            ssc = cycle_link.sourceSystemCd,
                                            error = "Unable to link cycle to analysis_run")
        
        self.__remove_object_link(analysisRun_cycle, object_links)

        link = self.__get_object_link(
            link_type_identifier = constants.LINK_TYPE_ANALYSIS_RUN_CYCLE, 
            object_key = cycle.key)

        object_links.append(link)

        # link libraries from cycle
        libs = self._code_library_repository.get_by_has_object_link_to(
            link_type_id = constants.LINK_TYPE_CYCLE_CODE_LIBRARY["id"],
            link_type_ssc = constants.LINK_TYPE_CYCLE_CODE_LIBRARY["ssc"],
            object_key = cycle.key,
            link_side = 2)
        primary_code_library = next(iter(libs), None) # libs[0] if libs else None

        if (primary_code_library is not None):
            
            self.__remove_object_link(analysisRun_codeLibrary, object_links)

            link = self.__get_object_link(
                link_type_identifier = constants.LINK_TYPE_ANALYSIS_RUN_CODE_LIBRARY, 
                object_key = primary_code_library.key)
            object_links.append(link)

            dependent_code_libraries = self.__get_dependent_code_libraries_as_object_links(
                primary_code_library)
            
            if len(dependent_code_libraries or []) > 0:
                
                self.__remove_object_link(analysisRun_codeLibrary_dependents, object_links)

                object_links += dependent_code_libraries

        # link configuration sets from cycle
        sets : List[CirrusObject] = self._config_set_repository.get_by_has_object_link_to(
            link_type_id = constants.LINK_TYPE_CYCLE_CONFIGURATION_SET["id"],
            link_type_ssc = constants.LINK_TYPE_CYCLE_CONFIGURATION_SET["ssc"],
            object_key = cycle.key,
            link_side = 2)
        config_set = next(iter(sets), None) # sets[0] if sets else None
        
        if (config_set is not None):
            
            self.__remove_object_link(analysisRun_configSet, object_links)

            link = self.__get_object_link(
                link_type_identifier = constants.LINK_TYPE_ANALYSIS_RUN_CONFIGURATION_SET, 
                object_key = config_set.key)
            object_links.append(link)
        

        return object_links
    

    def __update_script_object_links(self,
                                     script_link : SimpleNamespace,
                                     object_links_existing : List[Dict[str, Any]] = []
                                    ) -> List[Dict[str,Any]]:

        object_links : List[Dict[str, Any]] = object_links_existing or []
        if (script_link is None): return object_links

        analysisRun_script_identifier = self._identifier_service.create_by_key_value(
            constants.LINK_TYPE_ANALYSIS_RUN_SCRIPT)
        analysisRun_script = self.__get_supported_link_types().get(
            analysisRun_script_identifier.get_key(), None)

        script = self._script_repository.get_by_id(
                id = script_link.objectId,
                ssc = script_link.sourceSystemCd)
        if (script is None): 
            raise CirrusObjectNotFoundError(object_type = self._script_repository.get_object_type(), 
                                            id = script_link.objectId,
                                            ssc = script_link.sourceSystemCd,
                                            error = "Unable to set script to analysis_run object")

        self.__remove_object_link(
            link_type = analysisRun_script,
            object_links = object_links_existing)
        
        link = self.__get_object_link(
            link_type_identifier = constants.LINK_TYPE_ANALYSIS_RUN_SCRIPT,
            object_key = script.key)
        object_links.append(link)

        return object_links


    def __update_code_libraries_object_links(self, 
                                             codeLibrary_link : SimpleNamespace,
                                             set_default_if_empty : bool = True,
                                             object_links_existing : List[Dict[str, Any]] = []
                                            ) -> List[Dict[str, Any]]:
        object_links : List[Dict[str, Any]] = object_links_existing or []
        primary_code_library : CirrusObject = None

        analysisRun_library_identifier = self._identifier_service.create_by_key_value(
            constants.LINK_TYPE_ANALYSIS_RUN_CODE_LIBRARY)
        analysisRun_codeLibrary = self.__get_supported_link_types().get(
            analysisRun_library_identifier.get_key(), None)
        
        analysisRun_dependents_identifier = self._identifier_service.create_by_key_value(
            constants.LINK_TYPE_ANALYSIS_RUN_CODE_LIBRARY_DEPENDENTS)
        analysisRun_codeLibrary_dependents = self.__get_supported_link_types().get(
            analysisRun_dependents_identifier.get_key(), None)

        if (codeLibrary_link is None and set_default_if_empty):
            primary_code_library = self.__get_default_library()
        elif (codeLibrary_link is not None):
            primary_code_library = self._code_library_repository.get_by_id(
                id = codeLibrary_link.objectId,
                ssc = codeLibrary_link.sourceSystemCd
                )
            if (primary_code_library is None): 
                raise CirrusObjectNotFoundError(object_type = self._code_library_repository.get_object_type(), 
                                                id = codeLibrary_link.objectId,
                                                ssc = codeLibrary_link.sourceSystemCd,
                                                error = "Unable to set primary library to analysis_run object")
            
        if (primary_code_library is not None):

            self.__remove_object_link(
                link_type = analysisRun_codeLibrary,
                object_links = object_links_existing)
            
            link = self.__get_object_link(
                link_type_identifier = constants.LINK_TYPE_ANALYSIS_RUN_CODE_LIBRARY,
                object_key = primary_code_library.key)
            object_links.append(link)

            dependent_code_libraries = self.__get_dependent_code_libraries_as_object_links(
                primary_code_library)
            if len(dependent_code_libraries or []) > 0:
                
                self.__remove_object_link(analysisRun_codeLibrary_dependents, object_links)
                object_links += dependent_code_libraries
            
        return object_links
    

    def __update_config_set_object_links(self, 
                                         configuration_set_link : SimpleNamespace,
                                         set_default_if_empty : bool = True,
                                         object_links_existing : List[Dict[str, Any]] = []
                                        ) -> List[Dict[str, Any]]:
        object_links : List[Dict[str, Any]] = object_links_existing or []
        config_set : CirrusObject = None

        analysisRun_configSet_identifier = self._identifier_service.create_by_key_value(
            constants.LINK_TYPE_ANALYSIS_RUN_CONFIGURATION_SET)
        analysisRun_configSet = self.__get_supported_link_types().get(
            analysisRun_configSet_identifier.get_key(), None)

        if (configuration_set_link is None and set_default_if_empty):
            config_set = self.__get_default_config_set()
        elif (configuration_set_link is not None):
            config_set = self._config_set_repository.get_by_id(
                id = configuration_set_link.objectId,
                ssc = configuration_set_link.sourceSystemCd
                )
            if (config_set is None): 
                raise CirrusObjectNotFoundError(object_type = self._config_set_repository.get_object_type(), 
                                                id = configuration_set_link.objectId,
                                                ssc = configuration_set_link.sourceSystemCd,
                                                error = "Unable to set configuration set to analysis_run object")
            
        if (config_set is not None):
            self.__remove_object_link(
                link_type = analysisRun_configSet,
                object_links = object_links_existing)

            link = self.__get_object_link(
                link_type_identifier = constants.LINK_TYPE_ANALYSIS_RUN_CONFIGURATION_SET,
                object_key = config_set.key)
            object_links.append(link)
            
        return object_links


    def __get_supported_link_types(self) -> Dict[str, SimpleNamespace]:
        
        if (self.__supported_link_types is None):

            analysis_run_registration = self._object_registration_service.get_object_registration(
                rest_path = self._analysis_run_repository.get_rest_path())
            supported_link_types = self._link_type_repository.get_all_by_object_type_key(
                object_type_key = analysis_run_registration.key)
            self.__supported_link_types = dict([(Identifier(link_type.objectId, link_type.sourceSystemCd).get_key(), link_type) \
                                                for link_type in supported_link_types])
        
        return self.__supported_link_types
    

    def __create_links(self,
                       analysis_run_config : AnalysisRunConfig,
                       link_type_identifier_key : str,
                       set_default_if_empty : bool = True,
                       object_links_existing : List[Dict[str, Any]] = []
                      ) -> List[Dict[str, Any]]:
        link_type_identifier = self._identifier_service.create_by_key(link_type_identifier_key)        
        link_type_match = self.__get_supported_link_types().get(link_type_identifier_key, None)
        if (link_type_match is None): return object_links_existing or []

        result = object_links_existing or []

        code_library_identifier = self._identifier_service.create_by_key_value(constants.LINK_TYPE_ANALYSIS_RUN_CODE_LIBRARY)
        config_set_identifier = self._identifier_service.create_by_key_value(constants.LINK_TYPE_ANALYSIS_RUN_CONFIGURATION_SET)
        script_identifier = self._identifier_service.create_by_key_value(constants.LINK_TYPE_ANALYSIS_RUN_SCRIPT)
        cycle_identifier = self._identifier_service.create_by_key_value(constants.LINK_TYPE_ANALYSIS_RUN_CYCLE)

        has_analysisRun_cycle = analysis_run_config.get_link_by_identifier(cycle_identifier) is not None

        if ((self._identifier_service.compare(link_type_identifier, code_library_identifier) 
            or link_type_identifier is None)
            and not has_analysisRun_cycle):

            result = self.__update_code_libraries_object_links(
                codeLibrary_link = analysis_run_config.get_link_by_identifier(code_library_identifier),
                set_default_if_empty = set_default_if_empty,
                object_links_existing = object_links_existing)
        
        if ((self._identifier_service.compare(link_type_identifier, config_set_identifier) 
            or link_type_identifier is None)
            and not has_analysisRun_cycle):

            result = self.__update_config_set_object_links(
                configuration_set_link = analysis_run_config.get_link_by_identifier(config_set_identifier),
                set_default_if_empty = set_default_if_empty,
                object_links_existing = object_links_existing)
        
        if (self._identifier_service.compare(link_type_identifier, script_identifier) 
            or link_type_identifier is None):

            result = self.__update_script_object_links(
                script_link = analysis_run_config.get_link_by_identifier(script_identifier),
                object_links_existing = object_links_existing)
        
        if (self._identifier_service.compare(link_type_identifier, cycle_identifier) 
            or link_type_identifier is None):

            result = self.__update_object_links_from_cycle(
                cycle_link = analysis_run_config.get_link_by_identifier(cycle_identifier),
                object_links_existing = object_links_existing)
                
        
        return result
    
    
    def __prepare_script_parameters(self, 
                                    analysis_run_config : AnalysisRunConfig,
                                    enforce : bool
                                   ) -> Tuple[Dict[str, Any], Dict[str, Any], bool]:
        """
            Resolves script parameters to backend and front-end formats and indicates if values should be saved.
            The function will use scriptParameters/scriptParametersUI field if these fields were defined and not None in the analysis_runs configuration, both fields needs to be provided.
            Otherwise, the function will try to resolve parameters from the analysis_run_script_parameters configuration, in this case analysisRun_script relationship field must be provided.
            If enforce flag is True, script needs to be specified in the configuration analysisRun_script relationship field, that is required for script execution (RUN action).
            If enforce flag is False, script is not mandatory and no parameters will be resolved / returned. 
            Flag 'enforce' has no impact on scriptParameters/scriptParametersUI fields that were defined and not None on the analysis_runs configration.

            Parameters:
                - analysis_run_config (AnalysisRunConfig): analysis run configuration
                - enforce (bool): enforces the script relationship (analysisRun_script) to be present in the configuration.

            Returns:
                Tuple with the following values:
                    - scriptParameters: resolved parameters in the backend format.
                    - scriptParametersUI: resolved parameters in the UI format.
                    - save_enabled: flag that indicates if the values should be saved, 
                        for example, when scriptParameters/scriptParametersUI were provided or it is a RUN action.
        """
        script_parameters = getattr(analysis_run_config, self.__FIELD_SCRIPT_PARAMETERS, None)
        script_parameters_ui = getattr(analysis_run_config, self.__FIELD_SCRIPT_PARAMETERS_UI, None)
        if script_parameters is not None or script_parameters_ui is not None: 
            if script_parameters is None or script_parameters_ui or None:
                raise ValueError(f"Configuration field '{self.__FIELD_SCRIPT_PARAMETERS if script_parameters is not None else self.__FIELD_SCRIPT_PARAMETERS_UI}' " \
                                 f"is not defined for analysis run '{analysis_run_config.objectId}:{analysis_run_config.sourceSystemCd}'.\n" \
                                 f"Both fields ('{self.__FIELD_SCRIPT_PARAMETERS_UI}', '{self.__FIELD_SCRIPT_PARAMETERS}') are required if set through '{constants.SHEET_NAME_ANALYSIS_RUNS}' configuration.")

            return script_parameters, script_parameters_ui, True
        
        analysisRun_script_identifier = self._identifier_service.create_by_key_value(constants.LINK_TYPE_ANALYSIS_RUN_SCRIPT)
        analysisRun_script = analysis_run_config.get_link_by_identifier(analysisRun_script_identifier)
        # assumption: a link to the script is required in the configuration
        if analysisRun_script is None:
            if enforce:
                raise ValueError(f"Script linked to analysis run '{analysis_run_config.objectId}:{analysis_run_config.sourceSystemCd}' was not found. " \
                                 f"Configuration for '{analysisRun_script_identifier.get_key()}' is empty.\n" \
                                 f"Script parameter resolution is not possible.")
            else:
                return None, None, False

        script = self._script_repository.get_by_id(
            id = analysisRun_script.objectId,
            ssc = analysisRun_script.sourceSystemCd)
            
        if script is None: 
            raise ValueError(f"Script '{analysisRun_script.objectId}:{analysisRun_script.sourceSystemCd}' " \
                             f"linked to analysis run '{analysis_run_config.objectId}:{analysis_run_config.sourceSystemCd}' was not found.\n" \
                             f"Script parameter resolution is not possible.")
        
        config_key : str = analysis_run_config.get_key()
        # this might not have any parameters that is acceptable for the enforce = True case
        parameter_list = self._batch_config.get_analysis_run_script_parameters_by_key(config_key)
        root_parameter_list = [p for p in parameter_list if p.parent_parameter is None or len(str(p.parent_parameter)) == 0] \
            if parameter_list is not None else None
        script_parameters, script_parameters_ui = self._script_execution_service.resolve_script_parameters(
            full_parameter_list = parameter_list,
            subset_parameter_list = root_parameter_list, 
            root_instance = script)
        
        return script_parameters, script_parameters_ui, True
    

    def __set_payload_field(self,
                            analysis_run : CirrusObject,
                            analysis_run_config : AnalysisRunConfig,
                            field : str
                           ) -> None:
        is_set : bool = False

        # add other handlers here

        if not is_set:
            analysis_run.set_field(field, getattr(analysis_run_config, field))

    
    def __validate(self,
                   analysis_run : CirrusObject):
        cycle_link_identifier = self._identifier_service.create_by_key_value(constants.LINK_TYPE_ANALYSIS_RUN_CYCLE)
        cycle = self._link_instance_service.get_linked_object(
            cirrus_object = analysis_run,
            link_type_identifier = cycle_link_identifier,
            repository = self._cycle_repository)
        if cycle is None:
            return
        
        is_prod = cycle.get_field(self.__CYCLE_FIELD_RUN_TYPE_CD, None) == self.__CYCLE_FIELD_RUN_TYPE_CD_PROD
        # prod script
        script_link_identifier = self._identifier_service.create_by_key_value(constants.LINK_TYPE_ANALYSIS_RUN_SCRIPT)
        script = self._link_instance_service.get_linked_object(
            cirrus_object = analysis_run,
            link_type_identifier = script_link_identifier,
            repository = self._script_repository)
        if script is not None:
            status_cd = script.get_field(self.__FIELD_STATUS_CD, None)
            if is_prod and status_cd != self.__CYCLE_FIELD_RUN_TYPE_CD_PROD:
                raise CycleError(f"Setting non-production script ({script.objectId}:{script.sourceSystemCd}) to Analysis Run with production Cycle ({cycle.objectId}:{cycle.sourceSystemCd}) is prohibited.")
            
        # prod code library
        code_library_link_identifier = self._identifier_service.create_by_key_value(constants.LINK_TYPE_CYCLE_CODE_LIBRARY)
        primary_code_library = self._link_instance_service.get_linked_object(
            cirrus_object = analysis_run,
            link_type_identifier = code_library_link_identifier,
            repository = self._code_library_repository)
        if primary_code_library is not None:
            status_cd = primary_code_library.get_field(self.__FIELD_STATUS_CD, None)
            if is_prod and status_cd != self.__CYCLE_FIELD_RUN_TYPE_CD_PROD:
                raise CycleError(f"Setting non-production code library ({primary_code_library.objectId}:{primary_code_library.sourceSystemCd}) to Analysis Run with production Cycle ({cycle.objectId}:{cycle.sourceSystemCd}) is prohibited.")

        # prod config set 
        config_set_link_identifier = self._identifier_service.create_by_key_value(constants.LINK_TYPE_CYCLE_CONFIGURATION_SET)
        config_set = self._link_instance_service.get_linked_object(cirrus_object = analysis_run,
                                 link_type_identifier = config_set_link_identifier,
                                 repository = self._config_set_repository)
        if config_set is not None:
            status_cd = config_set.get_field(self.__FIELD_STATUS_CD, None)
            if is_prod and status_cd != self.__CYCLE_FIELD_RUN_TYPE_CD_PROD:
                raise CycleError(f"Setting non-production configuration set ({config_set.objectId}:{config_set.sourceSystemCd}) to Analysis Run with production Cycle ({cycle.objectId}:{cycle.sourceSystemCd}) is prohibited.")
            

    def __create_save_payload(self,
                              analysis_run_config : AnalysisRunConfig,
                              analysis_run : CirrusObject,
                              change_reason : str = constants.CHANGE_REASON_DEFAULT,
                              set_default_links_if_empty : bool = False,
                              enforce_script_parameters : bool = False
                             ) -> CirrusObject:
        supported_links = self.__get_supported_link_types()
        field_definitions = self._object_registration_service.get_field_definitions(
            rest_path = self._analysis_run_repository.get_rest_path())
        # get available custom fields except for the scriptParameters and scriptParametersUI
        # fields scriptParameters and scriptParametersUI will be resolved and updated separately
        custom_field_names = [field.name for field in field_definitions 
                              if field.name not in [self.__FIELD_SCRIPT_PARAMETERS, self.__FIELD_SCRIPT_PARAMETERS_UI]]
        # NOTE: avoid using utils.get_attributes because runnable config has dynamic properties
        config_attributes = [attr for attr in vars(analysis_run_config) if not attr.startswith("_")]
        
        if analysis_run is None:
            analysis_run = self.__create_new_analysis_run()

        for attr in list(config_attributes):
            if analysis_run.is_field(attr, custom_field_names):
                self.__set_payload_field(analysis_run, analysis_run_config, attr)
            elif analysis_run.is_classification(attr):
                classification = self.__create_classifications(analysis_run_config)
                analysis_run.set_classification(classification)

        script_parameters, script_parameters_ui, save_enabled = self.__prepare_script_parameters(
            analysis_run_config, 
            enforce = enforce_script_parameters)
        if save_enabled:
            analysis_run.set_field(self.__FIELD_SCRIPT_PARAMETERS, script_parameters)
            analysis_run.set_field(self.__FIELD_SCRIPT_PARAMETERS_UI, script_parameters_ui)

        for link_identifier_key in supported_links:
            object_links = self.__create_links(
                analysis_run_config = analysis_run_config,
                link_type_identifier_key = link_identifier_key,
                set_default_if_empty = set_default_links_if_empty,
                object_links_existing = analysis_run.get_object_links().copy())
            
            analysis_run.set_object_links(object_links)
        
        self.__validate(analysis_run)

        analysis_run.remove_object_links_if_empty()
        analysis_run.remove_links()
        analysis_run.set_change_reason(change_reason)

        return analysis_run


    def __create_new_analysis_run(self, status_cd : str = "CREATED") -> CirrusObject:
        draft = {
                    "customFields": {
                        "statusCd": status_cd
                    },
                    "classification": [],
                    "fileAttachments": [],
                    "changeReason": constants.CHANGE_REASON_DEFAULT,
                    "objectId": "",
                    # "sourceSystemCd": constants.SOURCE_SYSTEM_CD_DEFAULT,
                    "name": "",
                    "objectLinks": [],
                    "createdInTag": self._launch_args.solution
                }
        
        return utils.convert_dict_to_cirrus_object(draft)
    

    def __create_classifications(self, analysis_run_config : AnalysisRunConfig):
        
        classification = self._object_registration_service.get_classification(
            rest_path = self._analysis_run_repository.get_rest_path())
        if (classification is None): return None
        context = list(vars(classification).keys())[0] # get the first attribute -> it should be the context
        result = { context: [] }

        if (not hasattr(analysis_run_config, "classification") 
            or analysis_run_config.classification is None):
            return result

        for item in analysis_run_config.classification:
            dimension = self._dimension_repository.get_by_id(id = item.namedTreeId, ssc = item.sourceSystemCd)
            if (dimension is None):
                CirrusObjectNotFoundError(object_type = "NamedTree", 
                                          id = item.namedTreeId, 
                                          ssc = item.sourceSystemCd, 
                                          error = "Unable to get dimension for Analysis Run")
            
            dimension_path = self._dimension_repository.get_path_by_path(
                dimension_key = dimension.key, 
                path = item.path)
            if (dimension_path is None):
                CirrusObjectNotFoundError(object_type = "NamedTreePath", 
                                          key = dimension.key, 
                                          error = f"Unable to get dimension path '{item.path}' for Analysis Run")
            points = self._dimension_point_repository.save_points(named_tree_path_keys = [dimension_path.key])
            result[context] += [point.key for point in points]

        return result

    
    # public members
    def delete(self, 
               analysis_run_config : AnalysisRunConfig,
               raise_error_if_not_exists : bool = False
              ) -> bool:
        if (analysis_run_config is None): raise ValueError(f"analysis_run_config cannot be empty")
        
        analysis_run, _ = self.__get_analysis_run_from_config(
            config = analysis_run_config, 
            raise_error_if_not_exists = raise_error_if_not_exists)

        if (analysis_run is not None):
            return self._analysis_run_repository.delete_by_key(
                key = analysis_run.key)

        return False
    

    def create(self, 
               analysis_run_config : AnalysisRunConfig,
               update_job_owner: bool = False,
               enforce_script_parameters : bool = False
              ) -> CirrusObject:
        if (analysis_run_config is None): raise ValueError(f"analysis_run_config cannot be empty")

        draft = self.__create_save_payload(analysis_run_config = analysis_run_config,
                                           analysis_run = None,
                                           set_default_links_if_empty = True,
                                           enforce_script_parameters = enforce_script_parameters)
        if (update_job_owner):
            object_links = self.__get_job_owner_as_object_links()
            draft.add_object_links(object_links)

        result, _ = self._analysis_run_repository.create(cirrus_object = draft)
        return result


    def update(self, 
               analysis_run_config : AnalysisRunConfig,
               analysis_run : CirrusObject = None,
               update_job_owner : bool = False,
               enforce_script_parameters : bool = False
              ) -> CirrusObject:
        if (analysis_run_config is None): raise ValueError(f"analysis_run_config cannot be empty")

        if analysis_run is None:
            analysis_run, _ = self.__get_analysis_run_from_config(
                config = analysis_run_config, 
                raise_error_if_not_exists = True)
        
        analysis_run, etag = self._analysis_run_repository.get_by_key(key = analysis_run.key)
        
        payload = self.__create_save_payload(
            analysis_run_config = analysis_run_config, 
            analysis_run = analysis_run, 
            enforce_script_parameters = enforce_script_parameters)
        if (update_job_owner):
            object_links = self.__get_job_owner_as_object_links()
            analysis_run.add_object_links(object_links)

        analysis_run_updated, _ = self._analysis_run_repository.update(cirrus_object = payload, etag = etag)

        return analysis_run_updated


    def run(self, 
            analysis_run_config : AnalysisRunConfig,
            create_if_not_exists : bool = True,
            wait : bool = True
           ) -> Tuple[bool, str]:
        if (analysis_run_config is None): raise ValueError(f"analysis_run_config cannot be empty")
        
        analysis_run, _ = self.__get_analysis_run_from_config(
                config = analysis_run_config, 
                raise_error_if_not_exists = False)
        if (analysis_run is None and create_if_not_exists):
            analysis_run = self.create(analysis_run_config,
                                       update_job_owner = True,
                                       enforce_script_parameters = True)
        else:
            analysis_run = self.update(analysis_run_config = analysis_run_config,
                                       analysis_run = analysis_run,
                                       update_job_owner = True, 
                                       enforce_script_parameters = True)
        script_parameters = SimpleNamespace()
        # make sure we do not pass None to script parameters that endpoint cannot consume
        analysis_run.set_field(self.__FIELD_SCRIPT_PARAMETERS, script_parameters)
        analysis_run.set_field(self.__FIELD_SCRIPT_PARAMETERS_UI, script_parameters)

        # execute script (will throw error in case if job is empty)
        job = self._script_execution_service.execute(object_key = analysis_run.key,
                                                     object_rest_path = self._analysis_run_repository.get_rest_path(),
                                                     parameters = script_parameters)
        if wait:
            return self._script_execution_service.wait(
                analysis_run_key = job.analysisRunID,
                sleep_in_sec = self._batch_config.general_config.script_wait_sleep or 10,
                timeout_in_sec = self._batch_config.general_config.script_wait_timeout)
        
        analysis_run, _ = self._analysis_run_repository.get_by_key(key = analysis_run.key, fields = [self.__FIELD_STATUS_CD])
        statusCd = CirrusObject(analysis_run).get_field(self.__FIELD_STATUS_CD, "")
        return  statusCd != self.__SCRIPT_STATUS_SUCCESS, statusCd
        
    

