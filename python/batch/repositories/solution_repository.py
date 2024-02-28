import json
from types import SimpleNamespace
from typing import Any, List

from services.request_service import RequestService
from repositories.base_repository import BaseRepository


class SolutionRepository(BaseRepository):

    # private members
    __REST_PATH = "solutions"
    __BASE_URL = "/riskCirrusBuilder"
    

    def __init__(self, request_service : RequestService) -> None:
        if (request_service is None): raise ValueError(f"request_service cannot be empty")
        
        super().__init__(request_service = request_service,
                         rest_path = self.__REST_PATH, 
                         base_url = self.__BASE_URL)
        

    # public members
    def get_by_short_name(self, short_name : str) -> SimpleNamespace:
        if (short_name is None or len(str(short_name)) == 0): raise ValueError(f"short_name cannot be empty")

        result = self.get_by_filter(filter = f"eq(shortName,'{short_name}')",
                                     start = 0,
                                     limit = 1)
        if (result is not None and len(result) > 0):
            return result[0]
        
        return None