from types import SimpleNamespace
from typing import Dict, Tuple

import common.constants as constants

from services.request_service import RequestService
from repositories.cirrus_object_repository import CirrusObjectRepository


class CycleRepository(CirrusObjectRepository):

    # private members
    __REST_PATH = "cycles"
    __OBJECT_TYPE = constants.OBJECT_TYPE_CYCLE
    

    def __init__(self, request_service : RequestService) -> None:
        if (request_service is None): raise ValueError(f"request_service cannot be empty")
        
        super().__init__(request_service = request_service, 
                         rest_path = self.__REST_PATH,
                         object_type = self.__OBJECT_TYPE)
        
    

    



