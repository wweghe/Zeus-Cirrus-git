from typing import Dict, Tuple, List
import hashlib

from common.constants import *
import common.utils as utils
from common.errors import *

from domain.base_config import BaseConfigParameter


class WorkflowConfig(BaseConfigParameter):

    def __init__(self, 
                 rest_path : str,
                 object_type : str
                ) -> None:

        super().__init__(rest_path, object_type)


    __is_processed : bool = False


    task_name : str = None
    transition_name : str = None
    error_transition_name : str = None
    parameter_set : str = None
    iteration : str = None

    def validate_headers(self, 
                         sheet_name : str,
                         column_fields_map : Dict[str, int], 
                         column_other_map : Dict[str, int],
                         throw_exception : bool = True
                        ) -> Tuple[bool, List[str]]:

        errors : List[str] = []
        attributes = list(utils.get_attributes(self))
        not_found = [attr for attr in attributes if attr not in column_fields_map]

        for required_col in not_found:
            errors.append(f"Mandatory column '{required_col}' was not found in '{sheet_name}' worksheet.")

        if throw_exception and len(errors) > 0:
            raise ParseError('\n'.join(errors))
        
        return len(errors) > 0, errors
    

    def get_key_unique(self):
        return hashlib.sha256(f"{self._rest_path}:{getattr(self, 'objectId')}:{getattr(self, 'sourceSystemCd')}:{self._action}:{self.task_name}:{self.iteration or ''}".encode('utf-8')).hexdigest()
    

    def is_processed(self) -> bool:
        return self.__is_processed
    

    def mark_as_processed(self) -> None:
        self.__is_processed = True