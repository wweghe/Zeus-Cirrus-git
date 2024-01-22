from types import SimpleNamespace

from services.request_service import RequestService
from repositories.base_repository import BaseRepository


class DimensionRepository(BaseRepository):

    # private members
    __REST_PATH = "namedTrees"
    __BASE_URL = "/riskCirrusObjects/classifications"


    def __init__(self, request_service : RequestService) -> None:
        if (request_service is None): raise ValueError(f"request_service cannot be empty")
        
        super().__init__(request_service = request_service, 
                         rest_path = self.__REST_PATH,
                         base_url = self.__BASE_URL)
        
    
    # public members
    def get_path_by_path(self,
                         dimension_key : str, 
                         path : str) -> SimpleNamespace:

        if (path is None or len(str(path)) == 0): raise ValueError(f"path cannot be empty")
        if (dimension_key is None or len(str(dimension_key)) == 0): raise ValueError(f"dimension_key cannot be empty")

        url = f"{self._api_url}/{dimension_key}/namedTreePaths"
        query_params = { 
            'start': 0, 
            'limit': 1000 }
        result, _ = self._request_service.get(
            url = url,
            params = query_params
        )
        if result is not None and (len(result.items) > 0):
            return next((item for item in result.items if item.path == path), None)
        
        return None