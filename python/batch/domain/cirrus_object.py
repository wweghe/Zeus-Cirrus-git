from typing import List, Any, Dict
from types import SimpleNamespace

import common.constants as constants
import common.utils as utils


class CirrusObject(SimpleNamespace):
    
    __CUSTOM_FIELDS_ATTR : str = "customFields"
    __OBJECT_LINKS_ATTR : str = "objectLinks"
    __LINKS_ATTR : str = "links"
    __CLASSIFICATION_ATTR : str = "classification"
    __CHANGE_REASON_ATTR : str = "changeReason"


    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)


    def __get_workflow_definition_by_id(self, workflow_definition_id : str) -> SimpleNamespace:
        
        if self.has_workflow(workflow_definition_id):
            return next((wd for wd in self.workflow.definitions if wd.id == workflow_definition_id), None)

        return None


    def is_field(self, name : str, custom_field_names : List[str]) -> bool:
        return (name in constants.CIRRUS_OBJECT_ROOT_PROPERTIES.keys() \
                or name in custom_field_names)
    

    def is_classification(self, name : str):
        return name.lower() == self.__CLASSIFICATION_ATTR


    def has_field(self,
                  name : str
                 ) -> bool:
        if hasattr(self, name):
            return True

        return hasattr(self, self.__CUSTOM_FIELDS_ATTR) and hasattr(self.customFields, name)


    def get_field(self, 
                  name : str, 
                  default_value : Any | None = None
                 ) -> Any:
        
        if (name in constants.CIRRUS_OBJECT_ROOT_PROPERTIES.keys()):
            return getattr(self, name, default_value)

        customFields = getattr(self, self.__CUSTOM_FIELDS_ATTR, {})
        return getattr(customFields, name, default_value)


    def set_field(self, 
                  name : str, 
                  value : Any | None = None,
                  set_if_empty : bool = False
                 ) -> None:
        if (name in constants.CIRRUS_OBJECT_ROOT_PROPERTIES.keys()):
            if (set_if_empty and self.get_field(name, None) is None) or not set_if_empty:
                setattr(self, name, value)
        else:
            if (set_if_empty and self.get_field(name, None) is None) or not set_if_empty:
                utils.setattr_if_not_exist(self, self.__CUSTOM_FIELDS_ATTR, SimpleNamespace())
                setattr(self.customFields, name, value)

    
    def remove_field(self,
                     name : str
                    ) -> None:
        utils.remove_attribute(self, name)

    
    def get_object_links(self, default_value = []) -> List:
        return getattr(self, self.__OBJECT_LINKS_ATTR, default_value)
    

    def set_object_links(self, value : List[Dict[str, Any]]):
        utils.setattr_if_not_exist(self, self.__OBJECT_LINKS_ATTR, [])
        setattr(self, self.__OBJECT_LINKS_ATTR, value)

    
    def add_object_links(self, value : List[Dict[str, Any]]):
        utils.setattr_if_not_exist(self, self.__OBJECT_LINKS_ATTR, [])
        self.objectLinks = self.get_object_links() + value


    def set_change_reason(self, value : str):
        setattr(self, self.__CHANGE_REASON_ATTR, value)


    def remove_links(self):
        utils.remove_attribute(self, self.__LINKS_ATTR)


    def remove_object_links_if_empty(self):
        if len(self.get_object_links()) == 0:
            utils.remove_attribute(self, self.__OBJECT_LINKS_ATTR)


    def set_classification(self, value : Dict[Any, List[str]]):
        utils.setattr_if_not_exist(self, self.__CLASSIFICATION_ATTR, SimpleNamespace())
        setattr(self, self.__CLASSIFICATION_ATTR, value)
    

    def has_workflow(self, workflow_definition_id : str = None) -> bool:
        
        has_definitions = hasattr(self, "workflow") \
            and hasattr(self.workflow, "definitions") \
                and self.workflow.definitions is not None \
                    and len(self.workflow.definitions) > 0
        if has_definitions and workflow_definition_id is not None:
            # has a specific defintion by id
            return next((wd for wd in self.workflow.definitions if wd.id == workflow_definition_id), None) is not None

        return has_definitions
    

    def has_workflow_tasks(self, workflow_definition_id : str = None) -> bool:
        
        return self.has_workflow(workflow_definition_id) \
            and hasattr(self.workflow, "tasks") \
            and hasattr(self.workflow.tasks, "items") \
                and self.workflow.tasks.items is not None \
                    and len(self.workflow.tasks.items) > 0
    

    def is_workflow_complete(self, workflow_definition_id : str = None) -> bool:
        
        if workflow_definition_id is not None:
            wd = self.__get_workflow_definition_by_id(workflow_definition_id)
            return getattr(wd, "complete", False) if wd is not None else False
        
        return self.has_workflow() \
            and getattr(self.workflow.definitions[0], "complete", False)
    

    def is_workflow_running(self, workflow_definition_id : str = None) -> bool:

        if workflow_definition_id is not None:
            wd = self.__get_workflow_definition_by_id(workflow_definition_id)
            return getattr(wd, "running", False) if wd is not None else False

        return self.has_workflow() \
            and getattr(self.workflow.definitions[0], "running", False)
    

    def get_workflow_task_claimed(self, 
                                  task_name : str,
                                  workflow_definition_id : str = None
                                 ) -> SimpleNamespace:
        if (task_name is None or len(str(task_name)) == 0): raise ValueError(f"cirrus_object cannot be empty")

        if (not self.has_workflow_tasks(workflow_definition_id)):
            raise ValueError(f"Workflow cirrus object '{self.objectId}:{self.sourceSystemCd}' " \
                             f"does not have tasks or workflow (definition id {workflow_definition_id}) has not started")

        task_claimed = [task for task in self.workflow.tasks.items \
                        if task.name == task_name and hasattr(task, "actualOwner")]
        if (len(task_claimed) > 0):
            return task_claimed[0]
        return None
    

    def get_workflow_task_transition_names(self, 
                                           task_name : str,
                                           workflow_definition_id : str = None
                                          ) -> List[str]:
        if (task_name is None or len(str(task_name)) == 0): raise ValueError(f"cirrus_object cannot be empty")

        if (not self.has_workflow_tasks(workflow_definition_id)):
            raise ValueError(f"Workflow cirrus object '{self.objectId}:{self.sourceSystemCd}' " \
                             f"does not have tasks or workflow (definition id {workflow_definition_id}) has not started")
        
        task = next((task for task in self.workflow.tasks.items if task.name == task_name), None)
        
        if task is not None:
            prompts = getattr(task, "prompts", [])
            transitions = next((p.values for p in prompts if p.variableName == 'CIRRUS_WORKFLOW_TRANSITIONS'), [])
            if (len(transitions) > 0):
                return [t.name for t in transitions]
        
        return []