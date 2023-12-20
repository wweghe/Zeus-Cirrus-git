from typing import Any, Dict, List, Tuple, Union
from types import SimpleNamespace
import json

import common.constants as constants

from domain.batch_job_step import *

from repositories.base_repository import BaseRepository

from services.request_service import RequestService



class BatchJobRepository(BaseRepository):

    __REST_PATH = f"batch/jobs"
    __BASE_URL = "/riskCirrusCore"
    __HEADERS_DEFAULT = {
            "accept": "application/json",
            "content-type": "application/json-patch"
            }


    def __init__(self, 
                 request_service : RequestService
                ) -> None:
        if (request_service is None): raise ValueError(f"request_service cannot be empty")

        super().__init__(request_service = request_service, 
                         rest_path = self.__REST_PATH,
                         base_url = self.__BASE_URL,
                         return_type = None,
                         return_conversion_func = self._return_conversion
                        )

    def _return_conversion(self, response_text : str) -> BatchJob:
        sn = json.loads(response_text, object_hook = lambda d: SimpleNamespace(**d))
        result : BatchJob = BatchJob(**sn.__dict__)
        steps : List[BatchJobStep] = []
        if hasattr(sn, "steps"):
            for s in sn.steps:
                steps.append(BatchJobStep(**s.__dict__))

            result.steps = steps

        return result


    def update(self, 
               id : str,
               steps : List[BatchJobStep],
               etag: str,
              ) -> Tuple[BatchJob, str]:
        if (id is None or len(str(id)) == 0): raise ValueError(f"id cannot be empty")
        if (steps is None): raise ValueError(f"steps cannot be empty")
        if (etag is None): raise ValueError(f"etag cannot be empty")

        headers = self.__HEADERS_DEFAULT
        headers.update({ "if-match": str(etag) })

        job, resp = self._request_service.patch(
                headers = headers,
                url = f"{self._api_url}/{str(id)}/steps",
                payload = steps,
                return_conversion_func = self._return_conversion_func
            )
        
        return job, str(resp.headers["etag"]) if "etag" in resp.headers else None