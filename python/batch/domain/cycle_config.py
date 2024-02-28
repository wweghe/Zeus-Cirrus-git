import hashlib
from typing import Callable, Tuple, List, Dict, Any, Optional
from datetime import date, datetime
from openpyxl.cell.cell import Cell

import common.constants as constants
from common.errors import *

from domain.base_config import BaseConfigRunnable


class CycleConfig(BaseConfigRunnable):

    def __init__(self, ordinal_number : int = 0) -> None:

        super().__init__(
            rest_path = constants.REST_PATH_CYCLE,
            object_type = constants.OBJECT_TYPE_CYCLE,
            ordinal_number = ordinal_number)
    

    def set_from_excel_row(self, 
                           excel_row : Tuple[Cell, ...], 
                           column_fields_map : Dict[str, int], 
                           field_type_map : Dict[str, str],
                           column_other_map : Dict[str, int],
                           column_links_map : Dict[str, int]
                           ):
        # batch specific fields
        self._set_batch_fields_from_excel_row(
            excel_row, column_other_map)
        # standard fields + customFields
        super()._set_fields_from_excel_row(excel_row, column_fields_map, field_type_map)
        # classification
        super()._set_classification_from_excel_row(
            excel_row,
            column_other_map[constants.SHEET_COLUMN_CLASSIFICATION])
        # relationships
        super()._set_links_from_excel_row(excel_row, column_links_map)


    def validate_headers(self, 
                         column_fields_map : Dict[str, int], 
                         column_other_map : Dict[str, int],
                         throw_exception : bool = True
                        ) -> Tuple[bool, List[str]]:

        errors : List[str] = []
        # action
        if self._ACTION_COLUMN_NAME not in column_other_map:
            errors.append(f"Mandatory column {repr(self._ACTION_COLUMN_NAME)} was not found in '{constants.SHEET_NAME_CYCLES}' worksheet.")
        # is_parallel
        if self._IS_PARALLEL_COLUMN_NAME not in column_other_map:
            errors.append(f"Mandatory column {repr(self._IS_PARALLEL_COLUMN_NAME)} was not found in '{constants.SHEET_NAME_CYCLES}' worksheet.")
        
        if throw_exception and len(errors) > 0:
            raise ParseError('\n'.join(errors))

        return len(errors) > 0, errors

