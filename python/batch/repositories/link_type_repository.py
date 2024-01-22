from typing import Any, Dict, List, Tuple

import common.constants as constants

from services.request_service import RequestService
from repositories.base_repository import BaseRepository


class LinkTypeRepository(BaseRepository):

    # private members
    __REST_PATH = "linkTypes"
    __BASE_URL = "/riskCirrusObjects"
    

    def __init__(self, request_service : RequestService) -> None:
        if (request_service is None): raise ValueError(f"request_service cannot be empty")
        
        super().__init__(request_service = request_service, 
                         rest_path = self.__REST_PATH,
                         base_url = self.__BASE_URL)


    # public members
    def get_all_by_object_type_key(self, object_type_key: str) -> List[Any]:
        filter = f"or(eq(side1.typeKey,'{object_type_key}'),eq(side2.typeKey,'{object_type_key}'))"
        return self.get_by_filter(filter = filter, start = 0, limit = constants.FETCH_OBJECTS_LIMIT_COUNT)

