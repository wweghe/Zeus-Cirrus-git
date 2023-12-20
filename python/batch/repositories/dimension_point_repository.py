
from typing import Any, List, Dict, Tuple

from services.request_service import RequestService
from repositories.base_repository import BaseRepository


class DimensionPointRepository(BaseRepository):

    # private members
    __REST_PATH = "points"
    __BASE_URL = "/riskCirrusObjects/classifications"
    

    def __init__(self, request_service : RequestService) -> None:
        if (request_service is None): raise ValueError(f"request_service cannot be empty")
        
        super().__init__(request_service = request_service, 
                         rest_path = self.__REST_PATH,
                         base_url = self.__BASE_URL)
        

    # public members
    def save_points(self, 
                    named_tree_path_keys : List[str] = []
                   ) -> List[Any]:
        if (named_tree_path_keys is None or len(named_tree_path_keys) == 0): raise ValueError(f"named_tree_path_keys cannot be empty")

        url = f"{self._api_url}/crossProduct"
        query_params = { 
            'start': 0, 
            'limit': 1000 }
        result, _ = self._request_service.post(
            url = url,
            params = query_params,
            payload = { "namedTreePathKeys": named_tree_path_keys },
            headers = self._headers
        )
        if result is not None and (len(result.items) > 0):
            return result.items
        
        return None