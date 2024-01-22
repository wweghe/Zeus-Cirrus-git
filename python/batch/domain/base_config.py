from typing import Callable, Any, Dict, List, Tuple
from types import SimpleNamespace
import hashlib, json
from openpyxl.cell.cell import Cell

import common.constants as constants
import common.utils as utils
from  common.errors import *

from domain.batch_run_action_enum import BatchRunActionEnum
from domain.identifier import Identifier


class BaseConfig:

    _ACTION_COLUMN_NAME : str = "_action"


    def __init__(self, rest_path : str, object_type : str) -> None:
        
        if rest_path is None or len(str(rest_path)) == 0: 
            raise ValueError(f"rest_path cannot be empty")
        if object_type is None or len(str(object_type)) == 0: 
            raise ValueError(f"object_type cannot be empty")
        
        self._rest_path : str = rest_path
        self._object_type : str = object_type
        self._action : BatchRunActionEnum = BatchRunActionEnum.SKIP


    def __repr__(self) -> str:
        return json.dumps(self, default = lambda s: vars(s))


    def _set_action_field_from_excel_row(self,
                                         excel_row : Tuple[Cell, ...], 
                                         column_other_map : Dict[str, int],
                                         action_default : BatchRunActionEnum = BatchRunActionEnum.SKIP
                                        ):
        action_cell = excel_row[column_other_map[self._ACTION_COLUMN_NAME]]
        action = str(action_cell.value).upper() if action_cell.value is not None and len(str(action_cell.value)) > 0 else None
        allowed_actions = [e.value for e in BatchRunActionEnum]
        if (action is not None) and (not action in allowed_actions):
            row_idx = excel_row[column_other_map[self._ACTION_COLUMN_NAME]].row
            raise ParseError(f"Column value {repr(action)} is row {row_idx} is invalid.\n" \
                f"Column '{self._ACTION_COLUMN_NAME}' value must be empty or take one of the values: {','.join(allowed_actions)}",
                action_cell.parent.title,
                row_idx)
        self.set_action(BatchRunActionEnum(action) if action is not None else action_default)


    objectId : str = None
    sourceSystemCd : str = None


    def get_rest_path(self) -> str:
        return self._rest_path
    

    def get_object_type(self) -> str:
        return self._object_type
    
    
    def set_action(self, action : BatchRunActionEnum = BatchRunActionEnum.SKIP):
        self._action = BatchRunActionEnum.SKIP if action is None else action

    
    def get_action(self) -> BatchRunActionEnum: 
        return self._action
    

    def get_key(self) -> str:
        return hashlib.sha256(f"{self._rest_path}:{getattr(self, 'objectId')}:{getattr(self, 'sourceSystemCd')}:{self._action}".encode('utf-8')).hexdigest()


class BaseConfigRunnable(BaseConfig):

    _IS_PARALLEL_COLUMN_NAME : str = "_is_parallel"


    def __init__(self, 
                 rest_path : str, 
                 object_type : str,
                 ordinal_number : int = 0
                ) -> None:
        
        super().__init__(rest_path, object_type)
        self._is_parallel : bool = False
        self._links : Dict[str, SimpleNamespace] = {}
        self._ordinal_number = ordinal_number


    def _set_batch_fields_from_excel_row(self,
                                       excel_row : Tuple[Cell, ...],
                                       column_other_map : Dict[str, int],
                                       ):
        self._set_action_field_from_excel_row(excel_row, column_other_map)
        self._set_is_parallel_field_from_excel_row(excel_row, column_other_map)
    
    
    def _set_fields_from_excel_row(self,
                                   excel_row : Tuple[Cell, ...], 
                                   column_fields_map : Dict[str, int], 
                                   field_type_map : Dict[str, str]):
        for col in column_fields_map.keys():

            from_type = field_type_map[col]
            to_type = constants.CIRRUS_FIELD_TYPE_MAP[from_type]
            cell = excel_row[column_fields_map[col]]
            converted_value = None

            if cell.value is not None:
                row_idx = cell.row
                try:
                    converted_value = utils.convert_xlsx_config_value(
                        value = cell.value, 
                        to_type = to_type)
                except Exception as e:
                    raise ParseError(f"Column '{col}' value {repr(cell.value)} in row {row_idx} is invalid.\n" \
                        f"Failed to convert column '{col}' value from type '{from_type}' to {to_type}",
                        cell.parent.title,
                        row_idx) from None
                
            setattr(self, col, converted_value if converted_value is not None else None)


    def _set_classification_from_excel_row(self,
                                           excel_row : Tuple[Cell, ...],
                                           column_classification_idx : int):
        to_type = object
        cell = excel_row[column_classification_idx]
        converted_value = None
        if cell.value is not None:
            row_idx = cell.row
            try:
                converted_value = utils.convert_xlsx_config_value(
                    value = cell.value, 
                    to_type = to_type)
            except Exception as e:
                raise ParseError(f"Classification column value {repr(cell.value)} in row {row_idx} is invalid.\n" \
                    f"Failed to parse json for column ''",
                    cell.parent.title,
                    row_idx) from None
            
        setattr(self, constants.SHEET_COLUMN_CLASSIFICATION, 
                converted_value if converted_value is not None else None)


    def _set_links_from_excel_row(self,
                                  excel_row : Tuple[Cell, ...], 
                                  column_links_map : Dict[str, int]):
        for link_type_identifier_key in column_links_map.keys():
            to_type = object
            cell = excel_row[column_links_map[link_type_identifier_key]]
            converted_value = None

            if cell.value is not None:
                row_idx = cell.row
                try:
                    converted_value = utils.convert_xlsx_config_value(
                        value = cell.value, 
                        to_type = to_type)
                except Exception as e:
                    raise ParseError(f"Relationship column '{link_type_identifier_key}' value {repr(cell.value)} in row {row_idx} is invalid.\n" \
                        f"Failed to parse json for column '{link_type_identifier_key}'",
                        cell.parent.title,
                        row_idx) from None
            self.add_link_by_identifier_key(converted_value, link_type_identifier_key)
    

    def _set_is_parallel_field_from_excel_row(self,
                                              excel_row : Tuple[Cell, ...], 
                                              column_other_map : Dict[str, int],
                                              is_parallel_default : bool = False
                                             ):
        is_parallel_cell = excel_row[column_other_map[self._IS_PARALLEL_COLUMN_NAME]]
        is_parallel = is_parallel_cell.value
        if is_parallel is not None and len(str(is_parallel)) > 0:
            is_parallel_to_use = False
            row_idx = excel_row[column_other_map[self._IS_PARALLEL_COLUMN_NAME]].row
            try: 
                if (type(is_parallel) == bool):
                    is_parallel_to_use = is_parallel
                else:
                    is_parallel_to_use = json.loads(str(is_parallel).lower())
            except Exception as e:
                raise ParseError(f"Column value {repr(is_parallel)} in row {row_idx} is invalid.\n" \
                    f"Column '{self._IS_PARALLEL_COLUMN_NAME}' value must be of boolean type: True, False",
                    is_parallel_cell.parent.title,
                    row_idx) from None
            self.set_is_parallel(is_parallel_to_use if is_parallel is not None else is_parallel_default)


    def set_is_parallel(self, is_parallel : bool):
        self._is_parallel = is_parallel

    
    def get_is_parallel(self) -> bool: 
        return self._is_parallel
    

    def get_link_by_id_ssc(self, id : str, ssc : str = constants.SOURCE_SYSTEM_CD_DEFAULT) -> SimpleNamespace:
        return self._links.get(Identifier(id, ssc).get_key(), None)
    

    def get_link_by_identifier(self, identifier : Identifier) -> SimpleNamespace:
        return self._links.get(identifier.get_key(), None)
    

    def get_link_by_identifier_key(self, key : str) -> SimpleNamespace:
        return self._links.get(key, None)
    

    def add_link_by_identifier_key(self, link : SimpleNamespace, key : str):
        self._links.update({ key: link})


    def get_links(self) -> List[SimpleNamespace]:
        return list(self._links.values())
    

    def get_ordinal_number(self) -> int:
        return self._ordinal_number


class BaseConfigParameter(BaseConfig):

    def __init__(self, rest_path : str, object_type : str) -> None:
        super().__init__(rest_path, object_type)
        self._action : BatchRunActionEnum = BatchRunActionEnum.RUN


    def set_from_excel_row(self, 
                           excel_row : Tuple[Cell, ...], 
                           column_fields_map : Dict[str, int]
                           ):
        for col in column_fields_map.keys():
            cell = excel_row[column_fields_map[col]]
            to_type = type(getattr(self, col))
            converted_value = utils.convert_xlsx_config_value(
                value = cell.value, 
                to_type = to_type)
            setattr(self, col, converted_value)


        
