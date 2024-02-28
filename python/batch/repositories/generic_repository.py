from types import SimpleNamespace
from typing import Any, List, Dict, Tuple
import time, copy
from datetime import datetime

from common.errors import *
import common.utils as utils
import common.constants as constants

from domain.cirrus_object import CirrusObject

from repositories.base_repository import BaseRepository

from services.request_service import RequestService


class GenericRepository(BaseRepository):

    __HEADERS_DEFAULT = \
        {
            "accept": constants.HEADERS_CONTENT_TYPE_JSON,
            "content-type": constants.HEADERS_CONTENT_TYPE_JSON
        }
    

    def __init__(self, 
                 request_service : RequestService, 
                 rest_path : str) -> None:
        if (request_service is None): raise ValueError(f"request_service cannot be empty")
        if (rest_path is None) or len(str(rest_path)) == 0: raise ValueError(f"rest_path cannot be empty")
        
        self._object_type = ""
        super().__init__(request_service, 
                         rest_path = "", 
                         base_url = rest_path, 
                         headers = self.__HEADERS_DEFAULT,
                         return_type = CirrusObject)


    # public members
    def get_object_type(self) -> str: 
        return self._object_type
    

    
        