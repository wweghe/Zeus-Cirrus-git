from typing import Dict, Tuple, List
import hashlib

import common.constants as constants
import common.utils as utils
from common.errors import ParseError

from domain.base_config import BaseConfigParameter


class ScriptParameterConfig(BaseConfigParameter):

    def __init__(self, 
                 rest_path : str,
                 object_type : str
                ) -> None:
        super().__init__(rest_path, object_type)


    task_name : str = None

    parameter_name : str = None
    parameter_value : str = None
    parameter_expression : str = None
    parent_parameter : str = None
    parent_field_parameters : str = "parameters"
    parameter_set : str = None


    def validate_headers(self, 
                         sheet_name : str,
                         column_fields_map : Dict[str, int], 
                         column_other_map : Dict[str, int],
                         throw_exception : bool = True
                        ) -> Tuple[bool, List[str]]:

        errors : List[str] = []
        attributes = list(utils.get_attributes(self))
        not_found = [attr for attr in attributes if attr not in column_fields_map]

        if constants.SHEET_NAME_ANALYSIS_RUN_SCRIPT_PARAMETERS == sheet_name:
            not_found = [col for col in not_found if col not in ["task_name", "parameter_set"]]

        for required_col in not_found:
            errors.append(f"Mandatory column '{required_col}' was not found in '{sheet_name}' worksheet.")

        if throw_exception and len(errors) > 0:
            raise ParseError(message = '\n'.join(errors))
        
        return len(errors) > 0, errors
    

    def get_key_unique(self) -> str:
        return hashlib.sha256(f"{self._rest_path}" \
                              f":{getattr(self, 'objectId')}" \
                              f":{getattr(self, 'sourceSystemCd')}" \
                              f":{self.get_action()}" \
                              f":{self.task_name}" \
                              f":{self.parameter_name}" \
                              f":{self.parent_parameter}" \
                              f":{self.parameter_set}".encode('utf-8')).hexdigest()
    

    def get_key_group(self):
        return hashlib.sha256(f"{self._rest_path}" \
                              f":{getattr(self, 'objectId')}" \
                              f":{getattr(self, 'sourceSystemCd')}" \
                              f":{self.get_action()}" \
                              f":{self.task_name}".encode('utf-8')).hexdigest()