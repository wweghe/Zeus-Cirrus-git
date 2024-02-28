from typing import List, Any, Dict, Tuple, Set
from types import SimpleNamespace
import uuid
from datetime import datetime
import time, copy

import common.constants as constants
from common.errors import *
import common.utils as utils
from common.logger import BatchLogger

from domain.batch_config import BatchConfig
from domain.cycle_config import CycleConfig
from domain.workflow_config import WorkflowConfig
from domain.diagram_node_status_enum import DiagramNodeStatusEnum
from domain.state import ConfigStateProxy
from domain.identifier import Identifier
from domain.batch_run_action_enum import BatchRunActionEnum
from domain.cirrus_object import CirrusObject
from domain.launch_arguments import LaunchArguments
from domain.batch_job_step import BatchJobStepStateEnum
from domain.base_config import BaseConfigRunnable
from domain.batch_run_action_enum import BatchRunActionEnum

from repositories.link_type_repository import LinkTypeRepository
from repositories.code_library_repository import CodeLibraryRepository
from repositories.script_repository import ScriptRepository
from repositories.config_set_repository import ConfigSetRepository
from repositories.dimension_repository import DimensionRepository
from repositories.dimension_point_repository import DimensionPointRepository
from repositories.cycle_repository import CycleRepository
from repositories.analysis_run_repository import AnalysisRunRepository
from repositories.workflow_template_repository import WorkflowTemplateRepository
from repositories.workflow_definition_repository import WorkflowDefinitionRepository
from repositories.cirrus_object_repository import CirrusObjectRepository

from services.solution_service import SolutionService
from services.object_registration_service import ObjectRegistrationService
from services.script_execution_service import ScriptExecutionService
from services.request_service import RequestService
from services.workflow_diagram_service import WorkflowDiagramService
from services.identifier_service import IdentifierService
from services.link_instance_service import LinkInstanceService
from services.batch_job_service import BatchJobService
from services.cirrus_object_service import CirrusObjectService
from services.runnable_service import RunnableService


class CycleService(RunnableService):

    __FIELD_NAME_SCRIPT_PARAMETERS : str = "currentTaskParameters"
    __FIELD_WORKFLOW_DIAGRAM : str = "wfDiagram"
    __FIELD_RUN_TYPE_CD : str = "runTypeCd"
    __FIELD_RUN_TYPE_CD_PROD : str = "PROD"
    __FIELD_STATUS_CD : str = "statusCd"
    __FIELD_ENTITY_ROLE : str = "entityRole"
    __FIELD_BATCH_JOB_ID : str = "batchJobId"
    __FIELD_WORKFLOW_DEFINITON_NAME = "wfDefinitionName"
    __FIELD_ENTITY_ROLE_BOTH : str = "BOTH"
    __supported_link_types : Dict[str, SimpleNamespace] = None

    # protected members
    _batch_config : BatchConfig = None
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
    _workflow_template_repository : WorkflowTemplateRepository = None
    _analysis_run_repository : AnalysisRunRepository = None
    _workflow_definition_repository : WorkflowDefinitionRepository = None
    _request_service : RequestService = None
    _diagram_service : WorkflowDiagramService = None
    _batch_logger : BatchLogger = None
    _link_instance_service : LinkInstanceService = None
    _launch_args : LaunchArguments = None
    _batch_job_service : BatchJobService = None
    _cirrus_object_service : CirrusObjectService = None


    def __init__(self, 
                 link_type_repository : LinkTypeRepository,
                 code_library_repository: CodeLibraryRepository,
                 config_set_repository : ConfigSetRepository,
                 script_repository : ScriptRepository,
                 dimension_repository : DimensionRepository,
                 dimension_point_repository : DimensionPointRepository,
                 cycle_repository : CycleRepository,
                 analysis_run_repository : AnalysisRunRepository,
                 workflow_template_repository : WorkflowTemplateRepository,
                 workflow_definition_repository : WorkflowDefinitionRepository,
                 solution_service : SolutionService,
                 object_registration_service : ObjectRegistrationService,
                 script_execution_service : ScriptExecutionService,
                 request_service : RequestService,
                 diagram_service : WorkflowDiagramService,
                 config_state : ConfigStateProxy,
                 identifier_service : IdentifierService,
                 batch_logger : BatchLogger,
                 link_instance_service : LinkInstanceService,
                 launch_args : LaunchArguments,
                 cirrus_object_service : CirrusObjectService,
                 batch_job_service : BatchJobService
                 ) -> None:
        
        super().__init__(
            batch_job_service = batch_job_service
        )

        if (link_type_repository is None): raise ValueError(f"link_type_repository cannot be empty")
        if (solution_service is None): raise ValueError(f"solution_service cannot be empty")
        if (object_registration_service is None): raise ValueError(f"object_registration_service cannot be empty")
        if (code_library_repository is None): raise ValueError(f"code_library_repository cannot be empty")
        if (config_set_repository is None): raise ValueError(f"config_set_repository cannot be empty")
        if (script_repository is None): raise ValueError(f"script_repository cannot be empty")
        if (dimension_repository is None): raise ValueError(f"dimension_repository cannot be empty")
        if (dimension_point_repository is None): raise ValueError(f"dimension_point_repository cannot be empty")
        if (cycle_repository is None): raise ValueError(f"cycle_repository cannot be empty")
        if (analysis_run_repository is None): raise ValueError(f"analysis_run_repository cannot be empty")
        if (workflow_template_repository is None): raise ValueError(f"workflow_template_repository cannot be empty")
        if (script_execution_service is None): raise ValueError(f"script_execution_service cannot be empty")
        if (workflow_definition_repository is None): raise ValueError(f"workflow_definition_repository cannot be empty")
        if (request_service is None): raise ValueError(f"request_service cannot be empty")
        if (diagram_service is None): raise ValueError(f"diagram_service cannot be empty")
        if (config_state is None): raise ValueError(f"config_state cannot be empty")
        if (identifier_service is None): raise ValueError(f"identifier_server cannot be empty")
        if (batch_logger is None): raise ValueError(f"batch_logger cannot be empty")
        if (link_instance_service is None): raise ValueError(f"link_instance_service cannot be empty")
        if (launch_args is None): raise ValueError(f"launch_args cannot be empty")
        if (cirrus_object_service is None): raise ValueError(f"cirrus_object_service cannot be empty")
        if (batch_job_service is None): raise ValueError(f"batch_job_service cannot be empty")

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
        self._workflow_template_repository = workflow_template_repository
        self._analysis_run_repository = analysis_run_repository
        self._workflow_definition_repository = workflow_definition_repository
        self._request_service = request_service
        self._diagram_service = diagram_service
        self._batch_config = config_state.get()
        self._identifier_service = identifier_service
        self._batch_logger = batch_logger
        self._link_instance_service = link_instance_service
        self._launch_args = launch_args
        self._cirrus_object_service = cirrus_object_service
        self._batch_job_service = batch_job_service

        if self._batch_config is None:
            raise ValueError(f"Batch run configuration is not available, perhaps it was not read or parsed properly.")


    # private members
    def __get_object_link(self, 
                          link_type : Dict[str, Any], 
                          object_key: str,
                          link_obj_attr_name : str = "businessObject2"):

        link_type : object = self._link_type_repository.get_by_id(
            id = link_type["id"],
            ssc = link_type["ssc"])
        
        if (link_type is None): 
            raise CirrusObjectNotFoundError(object_type = "LinkType", 
                                            id = link_type["id"],
                                            ssc = link_type["ssc"],
                                            error = "Unable to set link to cycle object")
        
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
            raise CirrusObjectNotFoundError(object_type = self._link_type_repository.get_rest_path(), 
                                            id = link_type.objectId,
                                            ssc = link_type.sourceSystemCd,
                                            error = "Unable to remove link from cycle object")
        if object_key is not None:
            idx_remove = (i for i, e in enumerate(object_links) \
                          if e.linkType == link_type.key and e[link_obj_attr_name] == object_key)
        else:
            idx_remove = (i for i, e in enumerate(object_links) \
                          if e.linkType == link_type.key)

        for idx in idx_remove:
            object_links.pop(idx)

        return object_links


    def __get_default_library(self):
        config = self._solution_service.get_cycle_code_library_default()
        
        if (config is not None):
            return self._code_library_repository.get_by_id(
                id = config.objectId, 
                ssc = config.sourceSystemCd
                )
        
        return None
    

    def __get_default_config_set(self):
        config = self._solution_service.get_cycle_config_set_default()
        
        if (config is not None):
            return self._config_set_repository.get_by_id(
                id = config.objectId, 
                ssc = config.sourceSystemCd
                )
        
        return None


    def __get_dependent_code_libraries_as_object_links(self,
                                                       primary_code_library: object
                                                      ) -> List[object]:
        object_links = []

        dependent_code_libraries = self._code_library_repository.get_by_has_object_link_to(
                link_type_id = constants.LINK_TYPE_CODE_LIBRARY_DEPENDS_ON["id"],
                link_type_ssc = constants.LINK_TYPE_CODE_LIBRARY_DEPENDS_ON["ssc"],
                object_key = primary_code_library.key,
                link_side = 1
            )
            
        if (dependent_code_libraries is not None):
            for lib in dependent_code_libraries:
                link = self.__get_object_link(link_type = constants.LINK_TYPE_CYCLE_CODE_LIBRARY_DEPENDENTS,
                                              object_key = lib.key)
                object_links.append(link)
        return object_links


    def __update_code_libraries_object_links(self, 
                                             codeLibrary_link : SimpleNamespace,
                                             set_default_if_empty : bool = True,
                                             object_links_existing : List[Dict[str, Any]] = []
                                            ) -> List[Dict[str, Any]]:
        object_links : List[Dict[str, Any]] = object_links_existing or []
        primary_code_library : object = None

        cycle_library_identifier = self._identifier_service.create_by_key_value(
            constants.LINK_TYPE_CYCLE_CODE_LIBRARY)
        cycle_codeLibrary = self.__get_supported_link_types().get(
            cycle_library_identifier.get_key(), None)
        
        cycle_dependents_identifier = self._identifier_service.create_by_key_value(
            constants.LINK_TYPE_CYCLE_CODE_LIBRARY_DEPENDENTS)
        cycle_codeLibrary_dependents = self.__get_supported_link_types().get(
            cycle_dependents_identifier.get_key(), None)

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
                                                error = "Unable to set primary library to cycle object")
            
        if (primary_code_library is not None):
            self.__remove_object_link(
                link_type = cycle_codeLibrary,
                object_links = object_links_existing)
            
            link = self.__get_object_link(link_type = constants.LINK_TYPE_CYCLE_CODE_LIBRARY,
                                          object_key = primary_code_library.key)
            object_links.append(link)

            dependent_code_libraries = self.__get_dependent_code_libraries_as_object_links(
                primary_code_library)
            if len(dependent_code_libraries or []) > 0:
                self.__remove_object_link(cycle_codeLibrary_dependents, object_links)
                object_links += dependent_code_libraries
            
        return object_links
    

    def __validate_workflow_prod_scripts(self,
                                         workflow_template : CirrusObject
                                        ) -> None:
        is_prod = not self._solution_service.get_cycle_state_enabled()
        if is_prod:
            workflow_scripts : List[CirrusObject] = self._script_repository.get_by_has_object_link_to(
                    link_type_id = constants.LINK_TYPE_WORKFLOW_TEMPLATE_SCRIPT["id"],
                    link_type_ssc = constants.LINK_TYPE_WORKFLOW_TEMPLATE_SCRIPT["ssc"],
                    object_key = workflow_template.key,
                    link_side = 2
                )
            if workflow_scripts is not None and len(workflow_scripts) > 0:
                for script in workflow_scripts:
                    statusCd = script.get_field(self.__FIELD_STATUS_CD, None)
                    if statusCd != self.__FIELD_RUN_TYPE_CD_PROD:
                        raise CycleError(f"Using workflow template ({workflow_template.objectId}:{workflow_template.sourceSystemCd}) with non-production scripts ({script.objectId}:{script.sourceSystemCd}) in production Cycle is prohibited.")
    

    def __set_default_fields(self,
                             cycle : CirrusObject,
                             cycle_config : CycleConfig,
                             classification_entity_role : str = None
                            ) -> CirrusObject:
        
        # run type cd
        runTypeCd_default : str = self._solution_service.get_cycle_state_default()
        cycle.set_field(
            name = self.__FIELD_RUN_TYPE_CD, 
            value = runTypeCd_default, 
            set_if_empty = True)

        # entity role
        if (self._solution_service.get_cycle_entity_role_enabled()):
            entity_role_default = classification_entity_role if classification_entity_role is not None \
                and classification_entity_role != self.__FIELD_ENTITY_ROLE_BOTH else None
            cycle_entity_role = getattr(cycle_config, self.__FIELD_ENTITY_ROLE, entity_role_default)
            cycle.set_field(
                name = self.__FIELD_ENTITY_ROLE,
                value = cycle_entity_role,
                set_if_empty = True)
            
        return cycle


    def __validate(self,
                   cycle : CirrusObject,
                   cycle_config : CycleConfig,
                   classification_entity_role : str = None
                  ) -> None:
        # runTypeCd
        cycle_runTypeCd = cycle.get_field(self.__FIELD_RUN_TYPE_CD, None)
        is_prod = not self._solution_service.get_cycle_state_enabled() or (cycle_runTypeCd == self.__FIELD_RUN_TYPE_CD_PROD)
        if is_prod and cycle_runTypeCd != self.__FIELD_RUN_TYPE_CD_PROD:
            raise CycleError(message = f"Setting Cycle {self.__FIELD_RUN_TYPE_CD} value to {cycle_runTypeCd} is prohibited in production environment.")

        # prod code library
        code_library_link_identifier = self._identifier_service.create_by_key_value(constants.LINK_TYPE_CYCLE_CODE_LIBRARY)
        primary_code_library = self._link_instance_service.get_linked_object(cirrus_object = cycle,
                                 link_type_identifier = code_library_link_identifier,
                                 repository = self._code_library_repository)
        if primary_code_library is not None:
            status_cd = primary_code_library.get_field(self.__FIELD_STATUS_CD, None)
            if is_prod and status_cd != self.__FIELD_RUN_TYPE_CD_PROD:
                raise CycleError(f"Setting non-production code library ({primary_code_library.objectId}:{primary_code_library.sourceSystemCd}) to production Cycle is prohibited.")

        # prod config set 
        config_set_link_identifier = self._identifier_service.create_by_key_value(constants.LINK_TYPE_CYCLE_CONFIGURATION_SET)
        config_set = self._link_instance_service.get_linked_object(cirrus_object = cycle,
                                 link_type_identifier = config_set_link_identifier,
                                 repository = self._config_set_repository)
        if config_set is not None:
            status_cd = config_set.get_field(self.__FIELD_STATUS_CD, None)
            if is_prod and status_cd != self.__FIELD_RUN_TYPE_CD_PROD:
                raise CycleError(f"Setting non-production configuration set ({config_set.objectId}:{config_set.sourceSystemCd}) to production Cycle in production environment is prohibited.")
    
        # prod workflow
        workflow_cycle_link_identifier = self._identifier_service.create_by_key_value(constants.LINK_TYPE_WORKFLOW_TEMPLATE_CYCLE)
        workflow_template = self._link_instance_service.get_linked_object(cirrus_object = cycle,
                                 link_type_identifier = workflow_cycle_link_identifier,
                                 link_obj_attr_name = "businessObject1",
                                 repository = self._workflow_template_repository)
        if workflow_template is not None:
            self.__validate_workflow_prod_scripts(workflow_template)
        # check workflow template for run
        is_required = cycle_config.get_action() == BatchRunActionEnum.RUN
        if is_required and workflow_template is None:
            raise CycleError(f"Cycle requires workflow template to perform '{cycle_config.get_action()}' action.")
        
        # entityRole
        cycle_entity_role = cycle.get_field(self.__FIELD_ENTITY_ROLE, None)
        if (self._solution_service.get_cycle_entity_role_enabled()):
            # entity role field is mandatory
            if (cycle_entity_role is None):
                raise CycleError(f"Cycle field '{self.__FIELD_ENTITY_ROLE}' is mandatory therefore cannot be empty.")
        
        if (classification_entity_role is not None and cycle_entity_role is not None \
            and classification_entity_role != self.__FIELD_ENTITY_ROLE_BOTH and cycle_entity_role != classification_entity_role):
            raise CycleError(f"Cycle {self.__FIELD_ENTITY_ROLE} field (current value is {repr(cycle_entity_role)}) " \
                             f"must match with entity classification value '{classification_entity_role}'.")
    
    
    def __update_workflow_object_links(self, 
                                       workflow_cycle_link : SimpleNamespace,
                                       object_links_existing : List[Dict[str, Any]] = []
                                      ) -> List[Dict[str, Any]]:
        object_links : List[Dict[str, Any]] = object_links_existing or []
        if (workflow_cycle_link is None): return object_links

        cycle_workflow_identifier = self._identifier_service.create_by_key_value(
            constants.LINK_TYPE_WORKFLOW_TEMPLATE_CYCLE)
        cycle_workflow = self.__get_supported_link_types().get(
            cycle_workflow_identifier.get_key(), None)

        workflow_template : CirrusObject = self._workflow_template_repository.get_by_id(
                id = workflow_cycle_link.objectId,
                ssc = workflow_cycle_link.sourceSystemCd
                )
        if (workflow_template is None): 
            raise CirrusObjectNotFoundError(object_type = self._workflow_template_repository.get_object_type(), 
                                            id = workflow_cycle_link.objectId,
                                            ssc = workflow_cycle_link.sourceSystemCd,
                                            error = "Unable to set workflow template to cycle object")

        if (workflow_template is not None):
            self.__remove_object_link(
                    link_type = cycle_workflow,
                    object_links = object_links_existing)
            link = self.__get_object_link(link_type = constants.LINK_TYPE_WORKFLOW_TEMPLATE_CYCLE,
                                        object_key = workflow_template.key,
                                        link_obj_attr_name = "businessObject1")
            object_links.append(link)
            
        return object_links
    

    def __update_config_set_object_links(self, 
                                         configurationSet_link : SimpleNamespace,
                                         set_default_if_empty : bool = True,
                                         object_links_existing : List[Dict[str, Any]] = []
                                        ) -> List[Dict[str, Any]]:
        object_links : List[Dict[str, Any]] = object_links_existing or []
        config_set : CirrusObject = None

        cycle_configSet_identifier = self._identifier_service.create_by_key_value(
            constants.LINK_TYPE_CYCLE_CONFIGURATION_SET)
        cycle_configSet = self.__get_supported_link_types().get(
            cycle_configSet_identifier.get_key(), None)

        if (configurationSet_link is None and set_default_if_empty):
            config_set = self.__get_default_config_set()
        elif (configurationSet_link is not None):
            config_set = self._config_set_repository.get_by_id(
                id = configurationSet_link.objectId,
                ssc = configurationSet_link.sourceSystemCd
                )
            if (config_set is None): 
                raise CirrusObjectNotFoundError(object_type = self._config_set_repository.get_object_type(), 
                                                id = configurationSet_link.objectId,
                                                ssc = configurationSet_link.sourceSystemCd,
                                                error = "Unable to set configuration set to cycle object")
            
        if (config_set is not None):
            self.__remove_object_link(
                link_type = cycle_configSet,
                object_links = object_links_existing)
            
            link = self.__get_object_link(link_type = constants.LINK_TYPE_CYCLE_CONFIGURATION_SET,
                                          object_key = config_set.key)
            object_links.append(link)
            
        return object_links


    def __get_supported_link_types(self) -> Dict[str, SimpleNamespace]:
        """
            Load all registered link types in the system into a dictionary, where 
            key is the link type identifier (id:ssc) and value is the link type instance.
        """
        if (self.__supported_link_types is None):

            cycle_registration = self._object_registration_service.get_object_registration(
                rest_path = self._cycle_repository.get_rest_path())
            
            supported_link_types = self._link_type_repository.get_all_by_object_type_key(
                object_type_key = cycle_registration.key)
            
            self.__supported_link_types = dict([(Identifier(link_type.objectId, link_type.sourceSystemCd).get_key(), link_type) \
                                                for link_type in supported_link_types])
        
        return self.__supported_link_types
    

    def __create_links(self,
                       cycle_config : CycleConfig,
                       link_type_identifier_key : str,
                       set_default_if_empty : bool = True,
                       object_links_existing : List[Dict[str, Any]] = []
                      ) -> List[Dict[str, Any]]:
        
        link_type_match = self.__get_supported_link_types().get(link_type_identifier_key, None)
        if (link_type_match is None): return object_links_existing or []

        result = object_links_existing or []

        link_type_identifier = self._identifier_service.create_by_key(link_type_identifier_key)
        code_library_identifier = self._identifier_service.create_by_key_value(constants.LINK_TYPE_CYCLE_CODE_LIBRARY)
        config_set_identifier = self._identifier_service.create_by_key_value(constants.LINK_TYPE_CYCLE_CONFIGURATION_SET)
        workflow_cycle_identifier = self._identifier_service.create_by_key_value(constants.LINK_TYPE_WORKFLOW_TEMPLATE_CYCLE)

        if (self._identifier_service.compare(link_type_identifier, code_library_identifier) 
            or link_type_identifier is None):
            
            result = self.__update_code_libraries_object_links(
                codeLibrary_link = cycle_config.get_link_by_identifier(code_library_identifier),
                set_default_if_empty = set_default_if_empty,
                object_links_existing = object_links_existing)
        
        if (self._identifier_service.compare(link_type_identifier, config_set_identifier) 
            or link_type_identifier is None):

            result = self.__update_config_set_object_links(
                configurationSet_link = cycle_config.get_link_by_identifier(config_set_identifier), 
                set_default_if_empty = set_default_if_empty,
                object_links_existing = object_links_existing)
            
        if (self._identifier_service.compare(link_type_identifier, workflow_cycle_identifier) 
            or link_type_identifier is None):

            result = self.__update_workflow_object_links(
                workflow_cycle_link = cycle_config.get_link_by_identifier(workflow_cycle_identifier),
                object_links_existing = object_links_existing)
        
        return result
    

    def __set_workflow_diagram(self,
                               cycle : CirrusObject,
                               cycle_config : CycleConfig
                              ) -> None:
        wfTemplate_cycle_identifier = self._identifier_service.create_by_key_value(constants.LINK_TYPE_WORKFLOW_TEMPLATE_CYCLE)
        wfTemplate_cycle = cycle_config.get_link_by_identifier(wfTemplate_cycle_identifier)
        if (wfTemplate_cycle is None): return

        workflow_template : CirrusObject = self._workflow_template_repository.get_by_id(
                id = wfTemplate_cycle.objectId,
                ssc = wfTemplate_cycle.sourceSystemCd,
                fields = [self.__FIELD_WORKFLOW_DIAGRAM]
                )
        if (workflow_template is None): 
            raise CirrusObjectNotFoundError(object_type = self._workflow_template_repository.get_object_type(), 
                                            id = wfTemplate_cycle.objectId,
                                            ssc = wfTemplate_cycle.sourceSystemCd,
                                            error = "Unable to set workflow diagram to cycle object")
        wfDiagram = workflow_template.get_field(self.__FIELD_WORKFLOW_DIAGRAM, None)
        if wfDiagram is None: 
            raise RuntimeError(f"Workflow diagram was not found in workflow template " \
                               f"(id: {workflow_template.objectId}, ssc: {workflow_template.sourceSystemCd}).")

        cycle.set_field(self.__FIELD_WORKFLOW_DIAGRAM, wfDiagram, set_if_empty = True)

    
    def __set_payload_run_type_cd(self, 
                               cycle : CirrusObject,
                               cycle_config : CycleConfig,
                               field : str
                              ) -> bool:
        if field is None or len(str(field)) == 0 or field != self.__FIELD_RUN_TYPE_CD:
            return False
        
        is_prod : bool = not self._solution_service.get_cycle_state_enabled()
        runTypeCd_default : str = self._solution_service.get_cycle_state_default()
        runTypeCd = str(getattr(cycle_config, self.__FIELD_RUN_TYPE_CD, runTypeCd_default)).upper()

        if hasattr(cycle_config, self.__FIELD_RUN_TYPE_CD):
            # check correctness
            if is_prod:
                if runTypeCd != self.__FIELD_RUN_TYPE_CD_PROD:
                    raise CycleError(message = f"Setting Cycle runTypeCd value to {runTypeCd} is prohibited in production environment.")
                cycle.set_field(self.__FIELD_RUN_TYPE_CD, self.__FIELD_RUN_TYPE_CD_PROD)
            else:
                cycle.set_field(self.__FIELD_RUN_TYPE_CD, runTypeCd)

            return True
            
        elif not hasattr(cycle_config, self.__FIELD_RUN_TYPE_CD):
            cycle.set_field(self.__FIELD_RUN_TYPE_CD, runTypeCd_default)

            return True
        
        return False
    

    def __set_payload_field(self,
                            cycle : CirrusObject,
                            cycle_config : CycleConfig,
                            field : str
                           ) -> None:
        is_set : bool = False

        if not is_set:
            is_set = self.__set_payload_run_type_cd(cycle, cycle_config, field)
        # add other handlers here

        if not is_set:
            cycle.set_field(field, getattr(cycle_config, field))


    def __create_save_payload(self,
                              cycle_config : CycleConfig,
                              cycle : CirrusObject,
                              change_reason : str = constants.CHANGE_REASON_DEFAULT,
                              set_default_links_if_empty : bool = False
                             ) -> CirrusObject:
        supported_links = self.__get_supported_link_types()
        field_definitions = self._object_registration_service.get_field_definitions(rest_path = self._cycle_repository.get_rest_path())
        custom_field_names = [field.name for field in field_definitions]
        config_attributes = [attr for attr in vars(cycle_config) if not attr.startswith("_")]
        classification_entity_role : str = None

        if cycle is None:
            cycle = self.__create_new_cycle()

        self.__set_workflow_diagram(cycle = cycle, cycle_config = cycle_config)

        for attr in list(config_attributes):
            if cycle.is_field(attr, custom_field_names):

                self.__set_payload_field(cycle, cycle_config, attr)

            elif cycle.is_classification(attr):
                classification, classification_entity_role = self.__create_classifications(cycle_config)
                cycle.set_classification(classification)
        
        for link_identifier_key in supported_links:
            object_links = self.__create_links(
                cycle_config = cycle_config,
                link_type_identifier_key = link_identifier_key,
                set_default_if_empty = set_default_links_if_empty,
                object_links_existing = cycle.get_object_links().copy())
            cycle.set_object_links(object_links)
        
        self.__set_default_fields(
            cycle = cycle,
            cycle_config = cycle_config,
            classification_entity_role = classification_entity_role)

        self.__validate(
            cycle = cycle, 
            cycle_config = cycle_config, 
            classification_entity_role = classification_entity_role)

        cycle.remove_object_links_if_empty()
        cycle.remove_links()
        cycle.set_change_reason(change_reason)
        if self._launch_args.job_id is not None:
            cycle.set_field(name = self.__FIELD_BATCH_JOB_ID, value = self._launch_args.job_id)

        return cycle


    def __create_new_cycle(self, status_cd : str = "CREATED") -> CirrusObject:
        user = self._solution_service.get_current_user()
        runType_default : str = self._solution_service.get_cycle_state_default()
        draft = {
                    "customFields": {
                        "statusCd": status_cd,
                        "cycleInitiatorUserId": user.id,
                        "runTypeCd": runType_default
                    },
                    "classification": [],
                    "fileAttachments": [],
                    "changeReason": constants.CHANGE_REASON_DEFAULT,
                    "objectId": "",
                    "name": "",
                    "objectLinks": [],
                    "createdInTag": self._launch_args.solution
                }
        
        #return utils.convert_dict_to_object(draft)
        return utils.convert_dict_to_cirrus_object(draft)
    

    def __create_classifications(self, 
                                 cycle_config : CycleConfig
                                ) -> Tuple[Dict[Any, List[str]], str]:
        
        classification = self._object_registration_service.get_classification(
            rest_path = self._cycle_repository.get_rest_path())
        if (classification is None): return None
        context = list(vars(classification).keys())[0] # get the first attribute -> it should be the context
        result = { context: [] }
        entity_role_result : str = None

        if (not hasattr(cycle_config, "classification") 
            or cycle_config.classification is None):
            return result

        for item in cycle_config.classification:
            dimension = self._dimension_repository.get_by_id(id = item.namedTreeId, ssc = item.sourceSystemCd)
            if (dimension is None):
                CirrusObjectNotFoundError(object_type = "NamedTree", 
                                          id = item.namedTreeId, 
                                          ssc = item.sourceSystemCd, 
                                          error = "Unable to get dimension for Cycle")
            
            dimension_path = self._dimension_repository.get_path_by_path(
                dimension_key = dimension.key, 
                path = item.path)
            if (dimension_path is None):
                CirrusObjectNotFoundError(object_type = "NamedTreePath", 
                                          key = dimension.key, 
                                          error = f"Unable to get dimension path '{item.path}' for Cycle")
            points = self._dimension_point_repository.save_points(
                named_tree_path_keys = [dimension_path.key])
            result[context] += [point.key for point in points]

            # check if 
            if (entity_role_result is None and dimension.objectId == "entity_id" \
                and hasattr(dimension_path, "customFields") and hasattr(dimension_path.customFields, "entityRole")):
                entity_role_result = dimension_path.customFields.entityRole

        return result, entity_role_result
    

    def __get_workflow_template_from_cycle(self, 
                                           cycle : CirrusObject,
                                           raise_error_if_not_exist : bool = True
                                          ) -> CirrusObject:
        templates = self._workflow_template_repository.get_by_has_object_link_to(
                link_type_id = constants.LINK_TYPE_WORKFLOW_TEMPLATE_CYCLE["id"],
                link_type_ssc = constants.LINK_TYPE_WORKFLOW_TEMPLATE_CYCLE["ssc"],
                link_side = 1,
                object_key = cycle.key
            )
        workflow_template : CirrusObject = next(iter(templates), None)

        if (workflow_template is None and raise_error_if_not_exist):
            raise CirrusObjectNotFoundError(object_type = self._workflow_template_repository.get_object_type(),
                                            linked_object_key = cycle.key,
                                            error = "Unable to get workflow template from cycle")
        
        return workflow_template
    

    def __get_cycle_from_config(self, 
                                cycle_config : CycleConfig,
                                raise_error_if_not_exists : bool = True,
                                with_etag : bool = False
                               ) -> Tuple[CirrusObject, str]:
        if (cycle_config is None): raise ValueError(f"cycle_config cannot be empty")

        cycle : CirrusObject = self._cycle_repository.get_by_id(
            id = cycle_config.objectId, 
            ssc = cycle_config.sourceSystemCd,
            fields = ["key"])
        
        if (cycle is None and raise_error_if_not_exists):
            raise CirrusObjectNotFoundError(object_type = self._cycle_repository.get_object_type(),
                                            id = cycle_config.objectId,
                                            ssc = cycle_config.sourceSystemCd)
        if with_etag and cycle is not None:
            return self._cycle_repository.get_by_key(key = cycle.key)
        
        return cycle, None
    

    def __prepare_script_parameters(self, 
                                    cycle_config : CycleConfig,
                                    script : CirrusObject,
                                    task_name : str,
                                    parameter_set : str
                                   ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        
        script_parameters = getattr(cycle_config, self.__FIELD_NAME_SCRIPT_PARAMETERS, None)
        if script_parameters is not None: return copy.deepcopy(script_parameters), script_parameters
        
        config_key : str = cycle_config.get_key()

        parameter_list = self._batch_config.get_cycle_script_parameters_by_key_task_name(
            config_key, task_name, parameter_set)
        root_parameter_list = [p for p in parameter_list if p.parent_parameter is None or len(str(p.parent_parameter)) == 0] \
            if parameter_list is not None else None
        return self._script_execution_service.resolve_script_parameters(
            full_parameter_list = parameter_list, 
            subset_parameter_list = root_parameter_list,
            root_instance = script)
    

    def __run_script(self, 
                     cycle_key : str,
                     script : CirrusObject,
                     script_parameters: Dict[str, Any],
                     script_parameters_ui: Dict[str, Any],
                     task_name : str
                    ) -> SimpleNamespace:
        if (cycle_key is None or len(str(cycle_key)) == 0): raise ValueError(f"cycle_key cannot be empty")
        if (script is None): raise ValueError(f"script cannot be empty") 
        if (task_name is None or len(str(task_name)) == 0): raise ValueError(f"task_name cannot be empty")
        
        cycle_recent, etag = self._cycle_repository.get_by_key(key = cycle_key, fields = [self.__FIELD_NAME_SCRIPT_PARAMETERS])
        if (cycle_recent is None):
            raise CirrusObjectNotFoundError(object_type = self._cycle_repository.get_object_type(),
                                            key = cycle_key,
                                            error = "Unable to get cycle to run script")
        
        cycle_recent.set_field(self.__FIELD_NAME_SCRIPT_PARAMETERS, SimpleNamespace())
        setattr(cycle_recent.customFields.currentTaskParameters, task_name, script_parameters_ui)

        cycle_updated, etag = self._cycle_repository.update(
            cirrus_object = cycle_recent, 
            etag = etag, 
            is_patch = True)
        
        job : SimpleNamespace

        try:
          job = self._script_execution_service.execute(
                object_key = cycle_updated.key,
                object_rest_path = self._cycle_repository.get_rest_path(),
                parameters = script_parameters,
                task_name = task_name)  
        except ScriptExecutionError as error:
            _, _ = self.__remove_job_and_update(
                cycle_key = cycle_key, 
                job = None,
                task_name = task_name,
                is_error = True)
            
            raise error from None

        cycle_updated, etag = self._cycle_repository.get_by_key(
            key = cycle_updated.key, 
            fields = [self.__FIELD_NAME_SCRIPT_PARAMETERS, self.__FIELD_WORKFLOW_DIAGRAM])
        # update workflow diagram
        cycle_updated.customFields.wfDiagram = self._diagram_service.update_current_tasks_status(
            diagram = cycle_updated.customFields.wfDiagram,
            task_name = task_name,
            status = DiagramNodeStatusEnum.RUNNING)
        # save job
        utils.setattr_if_not_exist(cycle_updated.customFields.currentTaskParameters, "__jobs__", [])
        cycle_updated.customFields.currentTaskParameters.__jobs__.append(job)

        cycle_updated, etag = self._cycle_repository.update(
            cirrus_object = cycle_updated, 
            etag = etag,
            is_patch = True)

        return job
    

    def __run_task(self, 
                   cycle_key : str,
                   workflow_template : CirrusObject,
                   task_name : str,
                   parameter_set : str,
                   cycle_config : CycleConfig,
                   is_init_task : bool = False
                  ) -> Tuple[CirrusObject, str]:
        if (cycle_key is None or len(str(cycle_key)) == 0): raise ValueError(f"cycle_key cannot be empty")
        if (task_name is None or len(str(task_name)) == 0): raise ValueError(f"task_name cannot be empty")
        if (cycle_config is None): raise ValueError(f"cycle_config cannot be empty")
        if (workflow_template is None): raise ValueError(f"workflow_template cannot be empty")

        if (workflow_template.customFields.wfTaskDetails is None):
            raise ValueError(f"Workflow template '{workflow_template.objectId}:{workflow_template.sourceSystemCd}' wfTaskDetails cannot be empty")
        
        task_detail = next((td for td in workflow_template.customFields.wfTaskDetails if td.name == task_name), None)
        if task_detail is None:
            raise RuntimeError(f"Failed to find init task details (name: '{task_name}') " \
                                f"in workflow template (key: {workflow_template.key}).")
        if (is_init_task and task_detail.script is None):
            raise RuntimeError(f"Script is not linked to init task (name: '{task_name}') " \
                                "of the workflow template (key: {workflow_template.key}).")
        
        if task_detail.script is None:
            return self._cycle_repository.get_by_key(key = cycle_key)

        script : CirrusObject = self._script_repository.get_by_id(
            id = task_detail.script.objectId, 
            ssc = task_detail.script.sourceSystemCd)
        if (script is None):
            raise CirrusObjectNotFoundError(object_type = self._script_repository.get_object_type(),
                                            id = task_detail.script.objectId,
                                            ssc = task_detail.script.sourceSystemCd,
                                            error = f"Unable to get script from init task (name: '{task_name}') " \
                                                f"of the workflow template (key: {workflow_template.key})")
        script_parameters, script_parameters_ui = self.__prepare_script_parameters(
            cycle_config, script, task_name, parameter_set)
        # if script submittion fails it will throw an error
        job = self.__run_script(cycle_key = cycle_key, 
                                script = script, 
                                script_parameters = script_parameters,
                                script_parameters_ui = script_parameters_ui,
                                task_name = task_name)

        cycle : CirrusObject = None
        etag : str = None
        is_error : bool = True
        batch_step_id = self._batch_job_service.get_step_id_by_config(config = cycle_config)
        try:
            self._script_execution_service.wait(
                analysis_run_key = job.analysisRunID,
                sleep_in_sec = self._batch_config.general_config.script_wait_sleep or 10,
                timeout_in_sec = self._batch_config.general_config.script_wait_timeout,
                raise_error_on_failure = True,
                raise_error_on_timeout = True,
                batch_step_id = batch_step_id)
            is_error = False
        finally:
            # make sure we clean our job
            cycle, etag = self.__remove_job_and_update(
                cycle_key = cycle_key, 
                job = job,
                task_name = task_name,
                is_error = is_error)
        
        return cycle, etag
    

    def __remove_job_and_update(self, 
                                cycle_key : str,
                                job : SimpleNamespace,
                                task_name : str,
                                is_error : bool = True
                               ) -> Tuple[CirrusObject, str]:
        cycle, etag = self._cycle_repository.get_by_key(
            key = cycle_key, 
            fields = [self.__FIELD_NAME_SCRIPT_PARAMETERS, self.__FIELD_WORKFLOW_DIAGRAM])

        if job is not None:
            jobs = getattr(cycle.customFields.currentTaskParameters, "__jobs__", [])
            idx_remove = (i for i, e in enumerate(jobs) if e.analysisRunID == job.analysisRunID)
            for idx in idx_remove:
                jobs.pop(idx)
            utils.setattr_if_not_exist(cycle.customFields.currentTaskParameters, "__jobs__", [])
            cycle.customFields.currentTaskParameters.__jobs__ = jobs

        utils.setattr_if_not_exist(cycle.customFields.currentTaskParameters, "__dummy__", str(uuid.uuid4()))
        # remove parameters by task_name
        delattr(cycle.customFields.currentTaskParameters, task_name)

        # update workflow diagram
        cycle.customFields.wfDiagram = self._diagram_service.update_current_tasks_status(
            diagram = cycle.customFields.wfDiagram,
            task_name = task_name,
            status = DiagramNodeStatusEnum.FAILED if is_error else DiagramNodeStatusEnum.COMPLETED
        )

        return self._cycle_repository.update(
            cirrus_object = cycle, 
            etag = etag,
            is_patch = True)
    
    
    def __start_workflow(self, 
                         cycle_config : CycleConfig,
                         cycle : CirrusObject,
                         workflow_template : CirrusObject,
                         workflow_definition: SimpleNamespace
                        ) -> Tuple[bool, CirrusObject, SimpleNamespace, str]:

        if workflow_template.get_field("initializeFlg", False):
            # run_task will throw an error in case of failed script exec submission
            init_task_name = self._solution_service.get_cycle_init_task_name_default()
            workflow_config = self._batch_config.get_uncompleted_cycle_workflow_by_task_name(
                config_key = cycle_config.get_key(),
                task_name = init_task_name)
            cycle_updated, etag = self.__run_task(
                cycle_key = cycle.key,
                workflow_template = workflow_template,
                task_name = init_task_name,
                parameter_set = workflow_config.parameter_set, # init task can have only onve value
                cycle_config = cycle_config,
                is_init_task = True)
        else: # no init task
            cycle_updated = copy.deepcopy(cycle)
        
        # definitions = self._workflow_definition_repository.get_by_filter(
        #     filter = f"eq(name,'{workflow_template.get_field('wfDefinitionName')}')")
        # if (definitions is None or len(definitions) == 0):
        #     raise RuntimeError(f"Workflow definition " \
        #                        f"'{workflow_template.get_field('wfDefinitionName')}' not found")
        # workflow_definition = self._workflow_definition_repository.get_by_name(
        #     name = workflow_template, 
        #     raise_error_if_not_exists = True)

        cycle_updated, etag = self._cycle_repository.start_workflow(
            cirrus_object_key = cycle_updated.key,
            workflow_definition_id = workflow_definition.id)
        batch_step_id = self._batch_job_service.get_step_id_by_config(config = cycle_config)

        if (not cycle_updated.has_workflow_tasks(workflow_definition_id = workflow_definition.id)):

            return self._cirrus_object_service.wait_for_workflow_tasks(
                cirrus_object_repository = self._cycle_repository,
                cirrus_object_key = cycle_updated.key, 
                workflow_definition = workflow_definition,
                timeout_in_sec = self._batch_config.general_config.workflow_wait_timeout or 10,
                sleep_in_sec = self._batch_config.general_config.workflow_wait_sleep or 5,
                raise_error_on_timeout = True,
                batch_step_id = batch_step_id)
        
        return True, cycle_updated, etag
        

    def __transition_to_workflow_task(self,
                                      cycle_key : str,
                                      transition_name : str,
                                      task : SimpleNamespace,
                                      workflow_definition : SimpleNamespace,
                                      batch_step_id : str = None
                                     ) -> Tuple[bool, CirrusObject, str]:
        if (transition_name is None and len(str(transition_name)) == 0): raise ValueError(f"transition_name cannot be empty")
        if (cycle_key is None): raise ValueError(f"cycle_key cannot be empty")
        if (task is None): raise ValueError(f"task cannot be empty")
        if (workflow_definition is None): raise ValueError(f"workflow_definition cannot be empty")

        skip_transition_name = self._solution_service.get_cycle_skip_transition_name()
        is_skip = transition_name == skip_transition_name
        workflow_task_status = DiagramNodeStatusEnum.SKIPPED if is_skip else DiagramNodeStatusEnum.COMPLETED
        cycle, etag = self._cycle_repository.get_by_key(
            key = cycle_key, 
            fields = ["objectId","sourceSystemCd","workflow",self.__FIELD_WORKFLOW_DIAGRAM])
        cycle : CirrusObject = cycle

        if (not cycle.has_workflow_tasks(workflow_definition_id = workflow_definition.id)):
            raise ValueError(f"Cycle '{cycle.objectId}:{cycle.sourceSystemCd}' workflow has no tasks or it has not started")

        # update workflow diagram task status
        cycle.customFields.wfDiagram = self._diagram_service.update_current_tasks_status(
            diagram = cycle.customFields.wfDiagram, 
            task_name = task.name,
            status = workflow_task_status)

        transitions = next((prompt.values for prompt in task.prompts if prompt.name == "CIRRUS_WORKFLOW_TRANSITIONS"), None)
        # check if transition exists for the cycle
        if (not transition_name in [t.name for t in transitions]):
            raise ValueError(f"Transition '{transition_name}' was not found for cycle '{cycle.objectId}:{cycle.sourceSystemCd}'.")
        workflow = \
            {
                "taskId": task.id,
                "variables": {
                    "CIRRUS_WORKFLOW_TRANSITIONS": transition_name
                }
            }
        cycle.workflow = workflow
        for attr in ["links", "mediaTypeVersion", "objectId", "sourceSystemCd"]:
            cycle.remove_field(attr)
        
        cycle_updated, etag = self._cycle_repository.update(
            cirrus_object = cycle, 
            etag = etag,
            is_patch = True)

        if (not cycle_updated.has_workflow_tasks(workflow_definition_id = workflow_definition.id)
            and cycle_updated.is_workflow_running(workflow_definition_id = workflow_definition.id)):
            
            return self._cirrus_object_service.wait_for_workflow_tasks(
                cirrus_object_repository = self._cycle_repository,
                cirrus_object_key = cycle_updated.key, 
                workflow_definition = workflow_definition,
                sleep_in_sec = self._batch_config.general_config.workflow_wait_sleep or 5,
                timeout_in_sec = self._batch_config.general_config.workflow_wait_timeout,
                raise_error_on_timeout = True,
                batch_step_id = batch_step_id)
        
        return True, cycle_updated, etag
    

    def __claim_task(self, 
                     cycle : CirrusObject,
                     tasks_to_run : List[SimpleNamespace]):
        if (tasks_to_run is None): raise ValueError(f"tasks_to_run cannot be empty")
        if (cycle is None): raise ValueError(f"cycle_config cannot be empty")

        for task in tasks_to_run:
            task_claimed = cycle.get_workflow_task_claimed(task_name = task.name)
            if (task_claimed is None):
                self._cycle_repository.claim_task(cirrus_object_key = cycle.key, task_id = task.id)

    
    def __get_tasks_to_run(self,
                           cycle : CirrusObject,
                           uncompleted_task_names : Set[str]
                          ) -> Tuple[bool, List[SimpleNamespace]]:
        result = []
        if uncompleted_task_names is not None:
            for task in cycle.workflow.tasks.items:
                if (not task.name in uncompleted_task_names): 
                    continue # there is no transition configuration for this task name
                result.append(task)
        
        return len(result) > 0, result
    

    def execute_action(self, config : BaseConfigRunnable, *args, **kwargs) -> Any:
        
        action = config.get_action()

        if (action == BatchRunActionEnum.DELETE):
            return self.delete(cycle_config = config)
        elif (action == BatchRunActionEnum.CREATE):
            return self.create(cycle_config = config)
        elif (action == BatchRunActionEnum.UPDATE):
            return self.update(cycle_config = config)
        elif (action == BatchRunActionEnum.RUN):
            return self.run(cycle_config = config)

        return super().execute_action(config, args, kwargs)
    

    def delete(self, 
               cycle_config : CycleConfig,
               raise_error_if_not_exists : bool = False
              ) -> bool:
        if (cycle_config is None): raise ValueError(f"cycle_config cannot be empty")
        
        self._batch_job_service.update_step(config = cycle_config)
        
        cycle, _ = self.__get_cycle_from_config(cycle_config = cycle_config, 
                                                raise_error_if_not_exists = raise_error_if_not_exists)

        if (cycle is not None):
            # cycle_run_type_cd = cycle.get_field(name = self.__FIELD_RUN_TYPE_CD)
            return self._cycle_repository.delete_by_key(
                key = cycle.key)

        return False
    

    def create(self, 
               cycle_config : CycleConfig
              ) -> CirrusObject:
        if (cycle_config is None): raise ValueError(f"cycle_config cannot be empty")

        self._batch_job_service.update_step(config = cycle_config)

        draft = self.__create_save_payload(cycle_config = cycle_config,
                                           cycle = None,
                                           set_default_links_if_empty = True)
        result, _ = self._cycle_repository.create(cirrus_object = draft)
        
        return result


    def update(self, 
               cycle_config : CycleConfig,
               cycle : object = None
              ) -> CirrusObject:
        if (cycle_config is None): raise ValueError(f"cycle_config cannot be empty")

        self._batch_job_service.update_step(config = cycle_config)

        etag = None
        if cycle is None:
            cycle, etag = self.__get_cycle_from_config(cycle_config, with_etag = True)
        else:
            cycle, etag = self._cycle_repository.get_by_key(key = cycle.key)

        if (cycle is None):
            raise CirrusObjectNotFoundError(object_type = self._cycle_repository.get_object_type(),
                                            id = cycle_config.objectId,
                                            ssc = constants.SOURCE_SYSTEM_CD_DEFAULT,
                                            error = "Unable to update cycle")
        workflow_template = self.__get_workflow_template_from_cycle(
            cycle = cycle, 
            raise_error_if_not_exist = False)
        if workflow_template is not None:
            workflow_definition = self._workflow_definition_repository.get_by_name(
                name = workflow_template.get_field(self.__FIELD_WORKFLOW_DEFINITON_NAME))
            if cycle.is_workflow_complete(workflow_definition_id = workflow_definition.id):
                raise CycleError(f"Cycle {cycle.objectId}:{cycle.sourceSystemCd} workflow is complete. Running and/or updating completed cycle is prohibited.")
        
        payload = self.__create_save_payload(cycle_config, cycle)
        cycle_updated, _ = self._cycle_repository.update(cirrus_object = payload, etag = etag)

        return cycle_updated
    

    def run(self, 
            cycle_config : CycleConfig,
            create_if_not_exists : bool = True):
        if (cycle_config is None): raise ValueError(f"cycle_config cannot be empty")

        self._batch_job_service.update_step(config = cycle_config)

        cycle, _ = self.__get_cycle_from_config(cycle_config, False, True)

        if (cycle is None and create_if_not_exists):
            cycle = self.create(cycle_config)
        else:
            cycle = self.update(cycle_config = cycle_config, cycle = cycle)

        run_script_transition_name = self._solution_service.get_cycle_run_script_transition_name()
        workflow_template = self.__get_workflow_template_from_cycle(
            cycle = cycle, 
            raise_error_if_not_exist = True)
        workflow_definition = self._workflow_definition_repository.get_by_name(
            name = workflow_template.get_field(self.__FIELD_WORKFLOW_DEFINITON_NAME),
            raise_error_if_not_exists = True)
        has_tasks = cycle.has_workflow_tasks(workflow_definition_id = workflow_definition.id)
        is_workflow_running = cycle.is_workflow_running(workflow_definition_id = workflow_definition.id)
        
        batch_step_id = self._batch_job_service.get_step_id_by_config(config = cycle_config)
        
        if (not is_workflow_running):
            has_tasks, cycle, etag = self.__start_workflow(
                cycle_config, 
                cycle, 
                workflow_template,
                workflow_definition)
            is_workflow_running = has_tasks

        while (has_tasks and is_workflow_running):
            uncompleted_task_names = self._batch_config.get_uncompleted_cycle_workflows_task_names(
                config_key = cycle_config.get_key())
            has_tasks, tasks_to_run = self.__get_tasks_to_run(
                cycle = cycle, 
                uncompleted_task_names = uncompleted_task_names)
            
            if has_tasks:
                self.__claim_task(
                    cycle = cycle, 
                    tasks_to_run = tasks_to_run)
                
                for task in tasks_to_run:

                    self._batch_job_service.is_cancelation_requested(batch_step_id)

                    workflow_config = self._batch_config.get_uncompleted_cycle_workflow_by_task_name(
                        config_key = cycle_config.get_key(), 
                        task_name = task.name)
                    transition_name = workflow_config.transition_name
                    has_error_transition = (workflow_config.error_transition_name is not None 
                        and len(str(workflow_config.error_transition_name)) > 0)
                    # check if transition is valid
                    active_transitions = cycle.get_workflow_task_transition_names(task_name = task.name)
                    if (transition_name not in active_transitions):
                        raise RuntimeError(f"Transition '{transition_name}' is not available for task '{task.name}'.")

                    if (transition_name == run_script_transition_name):
                        try:
                            self.__run_task(
                                cycle_key = cycle.key,
                                workflow_template = workflow_template,
                                task_name = task.name,
                                parameter_set = workflow_config.parameter_set,
                                cycle_config = cycle_config)
                        except ScriptExecutionError as e:
                            if has_error_transition:
                                transition_name = workflow_config.error_transition_name
                            else:
                                raise e
                    
                    if (transition_name not in active_transitions):
                        raise RuntimeError(f"Transition '{transition_name}' is not available for task '{task.name}'.")
                    
                    has_tasks, cycle, _ = self.__transition_to_workflow_task(
                        cycle_key = cycle.key, 
                        transition_name = transition_name,
                        task = task,
                        workflow_definition = workflow_definition,
                        batch_step_id = batch_step_id)
                    workflow_config.mark_as_processed()
            is_workflow_running = cycle.is_workflow_running(workflow_definition_id = workflow_definition.id)

