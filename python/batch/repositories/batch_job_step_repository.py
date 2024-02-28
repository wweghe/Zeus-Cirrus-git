from typing import Any, Dict, List, Tuple, Union
from types import SimpleNamespace

from domain.base_config import BaseConfigRunnable
from domain.batch_job_step import *
from domain.launch_arguments import LaunchArguments

from repositories.base_repository import BaseRepository

from services.request_service import RequestService



class BatchJobStepRepository(BaseRepository):

    __REST_PATH = f"batch/jobs/job_id/steps"
    __BASE_URL = "/riskCirrusCore"
    __job_id : str = None


    def __init__(self, 
                 request_service : RequestService,
                 launch_arguments : LaunchArguments
                ) -> None:
        if (request_service is None): raise ValueError(f"request_service cannot be empty")
        if (launch_arguments is None): raise ValueError(f"launch_arguments cannot be empty")

        self.__job_id = launch_arguments.job_id
        self.__REST_PATH = f"batch/jobs/{self.__job_id}/steps"

        super().__init__(request_service = request_service, 
                         rest_path = self.__REST_PATH,
                         base_url = self.__BASE_URL,
                         return_type = BatchJobStep
                        )

    def create(self,
               step : BatchJobStep
              ) -> BatchJobStep:
        if step is None: raise ValueError(f"step cannot be empty")

        result, _ = self._request_service.post(
            url = self._api_url,
            payload = step
        )

        return result


    def update(self,
               step : BatchJobStep
              ) -> BatchJobStep:
        if step is None: raise ValueError(f"step cannot be empty")

        result, _ = self._request_service.put(
            url = f"{self._api_url}/{step.id}",
            payload = step
        )

        return result