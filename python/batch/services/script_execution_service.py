from typing import List, Dict, Any, Tuple
from types import SimpleNamespace
from datetime import datetime
import time, copy

from common.errors import *
import common.constants as constants
import common.utils as utils

from domain.script_parameter_config import ScriptParameterConfig

from repositories.analysis_run_repository import AnalysisRunRepository
from repositories.cirrus_object_repository import CirrusObjectRepository

from services.request_service import RequestService
from services.solution_service import SolutionService
from services.repository_factory import RepositoryFactory
from services.batch_job_service import BatchJobService


class ScriptExecutionService:

    # private members
    __API_URL = "/riskCirrusCore/executeScript"
    __SCRIPT_STATUSES_IN_PROGRESS = ["CREATED", "PENDING", "RUNNING", "VALIDATING", "CANCELING"]
    __SCRIPT_STATUS_SUCCESS = "SUCCESS"
    __PARAMS_NESTED_KEYWORD = "Params"
    __PAGE_DEFINITION_FILED_PROP_NAME = "pageDefinitionField"
    __COMPONENT_NAME_PROP = "compName"
    __COMPONENT_TYPE_EBPWOS = "EBPWithObjectSelector"

    # protected members
    _request_service : RequestService = None
    _analysis_run_repository : AnalysisRunRepository = None
    _solution_service : SolutionService = None
    _repository_factory : RepositoryFactory = None
    _batch_job_service : BatchJobService = None


    def __init__(self, 
                 request_service : RequestService,
                 analysis_run_repository : AnalysisRunRepository,
                 solution_service : SolutionService,
                 repository_factory : RepositoryFactory,
                 batch_job_service : BatchJobService
                ) -> None:
        if (request_service is None): raise ValueError(f"request_service cannot be empty")
        if (analysis_run_repository is None): raise ValueError(f"analysis_run_repository cannot be empty")
        if (solution_service is None): raise ValueError(f"solution_service cannot be empty")
        if (repository_factory is None): raise ValueError(f"repository_factory cannot be empty")
        if (batch_job_service is None): raise ValueError(f"batch_job_service cannot be empty")

        self._request_service = request_service
        self._analysis_run_repository = analysis_run_repository
        self._solution_service = solution_service
        self._repository_factory = repository_factory
        self._batch_job_service = batch_job_service


    def __get_query_param_value_as_list(self, query, query_element : str, delimiter : str = ",", default_value = None) -> List[str]:
        element = getattr(query, query_element, "")
        return element.split(delimiter) if element is not None and len(str(element)) > 0 else default_value
    

    def _resolve_parameter_expression(self, 
                                      expression : SimpleNamespace, 
                                      context : SimpleNamespace = None):
        if (expression is None): raise ValueError(f"param cannot be empty")
        non_variables = ["result"]

        variables = [variable for variable in expression.__dict__.keys() if variable not in non_variables]
        if (len(variables) > 1):
            raise ValueError(f"Parameter expression should contain only one query variable. Variables provided: {variables}")
        elif (len(variables) == 0):
            raise ValueError(f"Parameter expression does not contain query variable")
        
        query_variable = variables[0]
        object_query = getattr(expression, query_variable)
        if not hasattr(object_query, "query"):
            raise ValueError(f"{query_variable}.query attribute was not found")
        query_variable_result = getattr(object_query, "result", None)
        if query_variable_result is None:
            raise ValueError(f"{query_variable}.result attribute was not found")
        is_nested_result = type(query_variable_result) is SimpleNamespace
        
        rest_path = getattr(object_query.query, "restPath", None)
        if (rest_path is None or len(str(rest_path)) == 0):
            raise ValueError(f"{query_variable}.query.restPath attribute was not found")
        repository = self._repository_factory.get_by_rest_path(rest_path)
        if (repository is None):
            raise NotImplementedError(f"{query_variable}.query.restPath {repr(rest_path)} is not supported")

        query_result_array : List[Any] = []
        query_result_single : Any = None
        is_single_result = hasattr(object_query.query, "key")
        fields = self.__get_query_param_value_as_list(query  = object_query.query, query_element = "fields")

        if is_single_result:
            key_value = getattr(object_query.query, "key", "")
            if (context is not None and len(str(key_value)) > 0):
                key_value = str(key_value).format(**context.__dict__)
            query_result_single, _ = repository.get_by_key(
                    key = key_value,
                    fields = fields)
        else:
            sortBy = self.__get_query_param_value_as_list(query  = object_query.query, query_element = "sortBy")
            filter = getattr(object_query.query, "filter", "")
            if (context is not None and len(str(filter)) > 0):
                filter = str(filter).format(**context.__dict__)
            
            query_result_array = repository.get_by_filter(
                start = getattr(object_query.query, "start", 0),
                limit = getattr(object_query.query, "limit", constants.FETCH_OBJECTS_LIMIT_COUNT),
                filter = filter,
                sort_by = sortBy,
                fields = fields)
            
        if query_result_single is None and query_result_array is None:
            raise ValueError(f"Variable '{query_variable}' query returned empty result.")
        
        # update context
        if (context is None):
            context = SimpleNamespace()
        if hasattr(context, query_variable):
            raise ValueError(f"Duplicate query variable '{query_variable}' found.")
        
        setattr(context, query_variable, 
                query_result_single if is_single_result else query_result_array)

        # resolve result value
        if is_nested_result:
            return self._resolve_parameter_expression(
                expression = query_variable_result, 
                context = context)
        else:
            return str(query_variable_result).format(**context.__dict__)
    

    def _resolve_parameters(self, 
                            parameter_list : List[ScriptParameterConfig]
                           ) -> Dict[str, Any]:
        
        if (parameter_list is None): return None

        script_parameters = {}
        for param in parameter_list:
            value = param.parameter_value
            expression = param.parameter_expression

            if (expression is not None and len(str(expression)) > 0):
                try:
                    resolved_value = self._resolve_parameter_expression(
                        expression = utils.convert_str_to_object(expression))
                except Exception as e:
                    raise ScriptParameterResolutionError(
                        parameter_name = param.parameter_name, 
                        expression = expression, 
                        error = e)
                # try to convert value to dictionary (object), 
                # if it fails it must be of a simple data type (str, int, ...)
                try:
                    value = utils.convert_str_to_dict(resolved_value)
                except:
                    value = resolved_value
            else:
                try:
                    value = utils.convert_str_to_dict(param.parameter_value)
                except:
                    value = param.parameter_value
            script_parameters.update({ param.parameter_name: value })
            
        return script_parameters
    

    def _find_parameter_in_script_structure(self, 
                                            page_element_structure : SimpleNamespace,
                                            parameter_name : str
                                           ) -> SimpleNamespace:
        elements = vars(page_element_structure)
        if parameter_name in elements: 
            return getattr(page_element_structure, parameter_name)
        
        for element in elements:    
            element_structure = getattr(page_element_structure, element, None)
            if element_structure is None: return None
            has_children = hasattr(element_structure, "__dict__")
            if has_children:
                result = self._find_parameter_in_script_structure(
                    page_element_structure = element_structure,
                    parameter_name = parameter_name)
                if result is not None:
                    return result
            
        return None

    def _get_parameter_page_definition_field(self, 
                                             script_layout : SimpleNamespace,
                                             parameter_name : str
                                            ) -> SimpleNamespace:
        if script_layout is None: return None
        if parameter_name is None or len(str(parameter_name)) == 0: raise ValueError(f"parameter_name cannot be empty")

        structure = script_layout.application.Home.structure
        parameter_definition = self._find_parameter_in_script_structure(
            structure, 
            parameter_name)
        return parameter_definition
    

    def _resolve_parameter_ui(self, 
                              parameter_name : str,
                              parameter_resolved : Dict[str, Any],
                              is_parent : bool,
                              parent_instance : SimpleNamespace,
                              parent_instance_field_parameters : str,
                              nested_level : int,
                            ) -> Tuple[Dict[str, Any], SimpleNamespace]:
        if parent_instance is None: raise ValueError(f"Parameter instance cannot be empty")
        if not "value" in parameter_resolved[parameter_name]:
            raise ParseError(f"Nested script parameter '{parameter_name}' does not contain 'value' attribute that is required for nested parameters.")
        if len(parameter_resolved[parameter_name]["value"]) != 1:
            raise ParseError(f"Nested script parameter '{parameter_name}' attribute 'value' contains {len(parameter_resolved[parameter_name]['value'])} elements. " \
                             "Only one value item should be defined.")
        
        value_first_element = parameter_resolved[parameter_name]["value"][0]
        
        instance : SimpleNamespace
        is_key_value = "key" in value_first_element
        rest_path = parameter_resolved[parameter_name]["restPath"]

        if rest_path is None:
            raise ValueError(f"Attribute 'restPath' was not resolved or not provided for parameter '{parameter_name}'.")
        
        repository = self._repository_factory.get_by_rest_path(rest_path)
        
        if repository is None:
            raise NotImplementedError(f"Repository for rest_path {repr(rest_path)} is not supported.")

        if is_key_value:
            instance, _ = repository.get_by_key(key = value_first_element["key"])
        else:
            instance = repository.get_by_id(
                id = value_first_element["objectId"],
                ssc = value_first_element["sourceSystemCd"])
        
        instance_identifier = f"key: {value_first_element['key']}" if is_key_value \
            else f"'{value_first_element['objectId']}:{value_first_element['sourceSystemCd']}'"
        if instance is None:
            raise ValueError(f"Failed to get instance for '{rest_path}' with identifier {instance_identifier}.")
        
        if not hasattr(parent_instance.customFields, parent_instance_field_parameters):
            raise ValueError(f"Field '{parent_instance_field_parameters}' that holds parameters definition in instance '{parent_instance.objectId}:{parent_instance.sourceSystemCd}' was not found.")
        parameter_definition = self._get_parameter_page_definition_field(
            script_layout = getattr(parent_instance.customFields, parent_instance_field_parameters, None), 
            parameter_name = parameter_name)
        
        if parameter_definition is None:
            raise ValueError(f"Script parameter '{parameter_name}' definition was not found in the instance '{parent_instance.objectId}:{parent_instance.sourceSystemCd}'.")
        
        parameter_component_name = getattr(parameter_definition, self.__COMPONENT_NAME_PROP, None)
        if parameter_component_name != self.__COMPONENT_TYPE_EBPWOS:
            raise ValueError(f"Script parameter '{parameter_name}' is flagged as parent parameter, but is not defined as '{self.__COMPONENT_TYPE_EBPWOS}' component in instance '{parent_instance.id}:{parent_instance.sourceSystemCd}'.")
        page_definition_field = getattr(parameter_definition.props, self.__PAGE_DEFINITION_FILED_PROP_NAME, None)
        if page_definition_field is None:
            raise ValueError(f"Attribute '{self.__PAGE_DEFINITION_FILED_PROP_NAME}' was not found in the parameter instance '{parent_instance.id}:{parent_instance.sourceSystemCd}'.")


        if not hasattr(parent_instance, "customFields") or not hasattr(parent_instance.customFields, page_definition_field):
            raise ValueError(f"Field '{page_definition_field}' in object instance of '{rest_path}' with identifier {instance_identifier} was not found.\n" \
                             f"Not possible to resolve ui parameter '{parameter_name}'.")
        parameters_layout = getattr(parent_instance.customFields, page_definition_field)

        parameter_ui : Dict[str, Any]= { parameter_name: {} }
        parameter_ui[parameter_name].update({ "pageData": parameters_layout })
        parameter_ui[parameter_name].update({ "type": parameter_component_name })
        resolved_copy = copy.deepcopy(parameter_resolved)
        if not is_parent:
            value_first_element_copy = resolved_copy[parameter_name]["value"][0]
            parameter_ui[parameter_name].update({ f"{parameter_name}{nested_level}": value_first_element_copy[self.__PARAMS_NESTED_KEYWORD] })
            value_first_element_copy.pop(self.__PARAMS_NESTED_KEYWORD)
        parameter_ui[parameter_name].update({ "objectSelectorVal": resolved_copy[parameter_name] })

        return parameter_ui, instance


    # public members
    def execute(self, 
                object_key : str,
                object_rest_path : str,
                compute_context_name : str = "",
                parameters : Dict[str, Any] = None,
                task_name : str = None
               ) -> SimpleNamespace:
        if (object_key is None): raise ValueError(f"request_service cannot be empty")
        if (object_rest_path is None): raise ValueError(f"request_service cannot be empty")

        query_params = \
            { 
                "computeContextName": str(compute_context_name),
                "objectKey": str(object_key),
                "objectRestPath": object_rest_path,
                "validateOnlyFlg": "false",
                "codeEndsWithAsyncFlg": "false",
            }
        if (task_name is not None):
            query_params.update({"userTaskName": task_name})
            
        result, resp = self._request_service.post(
            url = self.__API_URL,
            params = query_params,
            headers = { "content-type": "application/json" },
            payload = parameters
        )

        if (result is None):
            raise ScriptExecutionError(error = resp.text)
        
        return result
    

    def wait(self, 
             analysis_run_key : str, 
             sleep_in_sec : int = 10,
             timeout_in_sec : int = None,
             raise_error_on_timeout : bool = True,
             raise_error_on_failure : bool = True,
             batch_step_id : str = None
            ) -> Tuple[bool, str]:
        if (analysis_run_key is None): raise ValueError(f"analysis_run_key cannot be empty")
        if (timeout_in_sec is not None and timeout_in_sec < 0): raise ValueError(f"timeout_in_sec cannot be negative")

        start_dttm = datetime.now()
        seconds_passed = 0
        is_timeout = False
        is_complete = False
        
        while (not is_complete and not is_timeout):
            analysis_run, _ = self._analysis_run_repository.get_by_key(
                key = analysis_run_key, 
                fields = ["statusCd"])
            if (analysis_run is None):
                raise CirrusObjectNotFoundError(object_type = self._analysis_run_repository.get_object_type(),
                                                key = analysis_run_key,
                                                error = "Unable to get analysis run to wait script execution")
            
            self._batch_job_service.is_cancelation_requested(batch_step_id)

            if (timeout_in_sec is not None):
                seconds_passed = (datetime.now() - start_dttm).total_seconds()

            if (not str(analysis_run.customFields.statusCd).upper() in self.__SCRIPT_STATUSES_IN_PROGRESS):
                is_complete = True
            elif (timeout_in_sec is not None and seconds_passed > timeout_in_sec):
                is_timeout = True
            else:
                time.sleep(sleep_in_sec)
        
        if (raise_error_on_timeout and is_timeout):
            raise ScriptExecutionTimeoutError()

        if (raise_error_on_failure
            and analysis_run.customFields.statusCd != self.__SCRIPT_STATUS_SUCCESS):
            raise ScriptExecutionError(status = analysis_run.customFields.statusCd)
        
        return is_complete, analysis_run.customFields.statusCd
    

    def resolve_script_parameters(self, 
                                  full_parameter_list : List[ScriptParameterConfig],
                                  subset_parameter_list : List[ScriptParameterConfig],
                                  root_instance : SimpleNamespace,
                                  nested_level : int = 0
                                 ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        if (full_parameter_list is None or subset_parameter_list is None): return None, None

        parents = list([param for param in subset_parameter_list \
                    if param.parent_parameter is None or len(str(param.parent_parameter)) == 0])
        if len(parents) == len(full_parameter_list): 
            resolved_parameters = self._resolve_parameters(subset_parameter_list)
            # NOTE: make a copy, not to pass the same reference to the same structure
            return resolved_parameters, copy.deepcopy(resolved_parameters)
        
        resolved_result : Dict[str, Any] = {}
        resolved_ui_result : Dict[str, Any] = {}

        for parameter in subset_parameter_list:

            children = list([child for child in full_parameter_list if child.parent_parameter == parameter.parameter_name])
            is_parent = len(children) > 0
            resolved = self._resolve_parameters([parameter])
            resolved_ui = copy.deepcopy(resolved)

            if is_parent:
                resolved_ui, current_instance = self._resolve_parameter_ui(
                        parameter_name = parameter.parameter_name,
                        parameter_resolved = resolved,
                        is_parent = is_parent,
                        parent_instance = root_instance, 
                        parent_instance_field_parameters = parameter.parent_field_parameters,
                        nested_level = nested_level)
                
                children_resolved, children_resolved_ui = self.resolve_script_parameters(
                    full_parameter_list, children, current_instance, nested_level + 1)
                
                resolved_value = resolved[parameter.parameter_name]
                if not "value" in resolved_value: # resolved is expected to be a dictionary (result from self._resolve_parameters function)
                    raise ParseError(f"Script parameter '{parameter.parameter_name}' does not contain 'value' attribute that is required for nested parameters.")
                if len(resolved_value["value"]) == 0:
                    raise ParseError(f"Script parameter '{parameter.parameter_name}' attribute 'value' has empty array, at least one value is expected for nested parameters.")
            
                for value in resolved_value["value"]:
                    if not self.__PARAMS_NESTED_KEYWORD in value:
                        value[self.__PARAMS_NESTED_KEYWORD] = {}
                    value[self.__PARAMS_NESTED_KEYWORD].update(children_resolved)

                resolved_ui[parameter.parameter_name].update({ f"{parameter.parameter_name}{nested_level + 1}": children_resolved_ui })
                
            resolved_result.update(resolved)
            resolved_ui_result.update(resolved_ui)
        
        return resolved_result, resolved_ui_result

    