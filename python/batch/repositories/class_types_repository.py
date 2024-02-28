
from repositories.base_repository import BaseRepository
from services.request_service import RequestService


class ClassTypesRepository(BaseRepository):

    # private members
    __REST_PATH = "classTypes"
    __BASE_URL = "/riskCirrusObjects/classifications"
    # protected members

    def __init__(self, request_service : RequestService) -> None:
        if (request_service is None): raise ValueError(f"request_service cannot be empty")
        
        super().__init__(request_service = request_service, 
                         rest_path = self.__REST_PATH,
                         object_type = self.__OBJECT_TYPE,
                         base_url = self.__BASE_URL)