
from typing import Any

from repositories.base_repository import BaseRepository
from repositories.cirrus_object_repository import CirrusObjectRepository

from services.request_service import RequestService
from services.object_registration_service import ObjectRegistrationService
from repositories.risk_scenario_set_repository import RiskScenarioSetRepository
from repositories.user_repository import UserRepository
from repositories.generic_repository import GenericRepository


class RepositoryFactory:

    _object_registration_service : ObjectRegistrationService = None
    _scenario_set_repository : RiskScenarioSetRepository = None
    _request_service : RequestService = None
    _user_repository : UserRepository = None


    def __init__(self,
                 request_service : RequestService,
                 object_registration_service : ObjectRegistrationService,
                 scenario_set_repository : RiskScenarioSetRepository,
                 user_repository : UserRepository
                ) -> None:
        if (request_service is None): raise ValueError(f"request_service cannot be empty")
        if (object_registration_service is None): raise ValueError(f"object_registration_service cannot be empty")
        if (scenario_set_repository is None): raise ValueError(f"scenario_set_repository cannot be empty")
        if (user_repository is None): raise ValueError(f"user_repository cannot be empty")

        self._request_service = request_service
        self._object_registration_service = object_registration_service
        self._scenario_set_repository = scenario_set_repository
        self._user_repository = user_repository


    def get_by_rest_path(self, rest_path : str) -> BaseRepository:
        
        object_registration = self._object_registration_service.get_object_registration(
            rest_path,
            throw_error_if_not_found = False)
        # cirrus object repository
        if (object_registration is not None):
            return CirrusObjectRepository(
                request_service = self._request_service, 
                rest_path = rest_path, 
                object_type = object_registration.objectId)

        if (rest_path == self._scenario_set_repository.get_rest_path()):
            return self._scenario_set_repository
        
        if (rest_path == self._user_repository.get_rest_path()):
            return self._user_repository
        
        return GenericRepository(
            request_service = self._request_service,
            rest_path = rest_path
        )