
from services.request_service import RequestService
from repositories.cirrus_object_repository import CirrusObjectRepository


class CodeLibraryRepository(CirrusObjectRepository):

    # private members
    __REST_PATH = "codeLibraries"
    __OBJECT_TYPE = "CodeLibrary"
    

    def __init__(self, request_service : RequestService) -> None:
        if (request_service is None): raise ValueError(f"request_service cannot be empty")
        
        super().__init__(request_service = request_service, 
                         rest_path = self.__REST_PATH,
                         object_type = self.__OBJECT_TYPE)