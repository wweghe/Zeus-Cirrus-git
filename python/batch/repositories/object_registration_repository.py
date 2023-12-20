from services.request_service import RequestService
from repositories.base_repository import BaseRepository


class ObjectRegistrationRepository(BaseRepository):

    # private members
    __REST_PATH = "objectRegistrations"
    __BASE_URL = "/riskCirrusObjects"


    def __init__(self, request_service : RequestService) -> None:
        if (request_service is None): raise ValueError(f"request_service cannot be empty")
        
        super().__init__(request_service = request_service, 
                         rest_path = self.__REST_PATH,
                         base_url = self.__BASE_URL)