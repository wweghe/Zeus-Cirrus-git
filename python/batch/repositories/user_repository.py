from types import SimpleNamespace

from services.request_service import RequestService
from repositories.base_repository import BaseRepository


class UserRepository(BaseRepository):

    # private members
    __REST_PATH = "users"
    __BASE_URL = "/identities"
    

    def __init__(self, request_service : RequestService) -> None:
        if (request_service is None): raise ValueError(f"request_service cannot be empty")
        
        super().__init__(request_service = request_service, 
                         rest_path = self.__REST_PATH,
                         base_url = self.__BASE_URL)


    def get_current_user(self) -> SimpleNamespace:

        url = f"{self._api_url}/@currentUser"
        result, _ = self._request_service.get(
            url = url
        )
        return result
    