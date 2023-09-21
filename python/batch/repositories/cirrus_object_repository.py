from types import SimpleNamespace
from typing import Any, List, Dict, Tuple
import time, copy
import time
from datetime import datetime

from common.errors import *
import common.utils as utils
import common.constants as constants

from domain.cirrus_object import CirrusObject

from repositories.base_repository import BaseRepository

from services.request_service import RequestService


class CirrusObjectRepository(BaseRepository):

    __HEADERS_DEFAULT = \
        {
            "accept": constants.HEADERS_ACCEPT_TYPE_WORKFLOW_JSON,
            "content-type": constants.HEADERS_CONTENT_TYPE_WORKFLOW_JSON
        }
    __BASE_URL : str = "/riskCirrusObjects/objects"


    def __init__(self, 
                 request_service : RequestService, 
                 rest_path : str,
                 object_type : str) -> None:
        if (request_service is None): raise ValueError(f"request_service cannot be empty")
        if (object_type is None): raise ValueError(f"object_type cannot be empty")
        if (rest_path is None): raise ValueError(f"rest_path cannot be empty")
        
        self._object_type = object_type
        super().__init__(request_service, 
                         rest_path, 
                         base_url = self.__BASE_URL, 
                         headers = self.__HEADERS_DEFAULT,
                         return_type = CirrusObject)


    # public members
    def get_object_type(self) -> str: 
        return self._object_type
    

    def delete_by_key(self, 
                      key : str
                     ) -> bool:
        if (key is None or len(str(key)) == 0): raise ValueError(f"key cannot be empty")

        result, resp = self._request_service.post(
            url = f"{self._api_url}/bulkDelete",
            headers = { "content-type": constants.HEADERS_CONTENT_TYPE_JSON },
            payload = {"version":1,"resources":[key]},
            return_type = self._return_type
        )

        return resp.ok
    

    def create(self, 
                cirrus_object : CirrusObject
              ) -> Tuple[CirrusObject, str]:
        if (cirrus_object is None): raise ValueError(f"cirrus_object cannot be empty")

        result, resp = self._request_service.post(
            url = f"{self._api_url}",
            headers = self.__HEADERS_DEFAULT,
            payload = cirrus_object,
            return_type = self._return_type
        )

        if (result is not None):
            return result, str(resp.headers["etag"])
        
        return None, None
    

    def update(self, 
               cirrus_object : CirrusObject, 
               etag: str,
               is_patch : bool = False
              ) -> Tuple[CirrusObject, str]:
        
        if (cirrus_object is None): raise ValueError(f"cirrus_object cannot be empty")
        if (etag is None): raise ValueError(f"etag cannot be empty")

        cirrus_object_with_reason = copy.deepcopy(cirrus_object)
        utils.setattr_if_not_exist(cirrus_object_with_reason, "changeReason", constants.CHANGE_REASON_DEFAULT)
        
        headers = self.__HEADERS_DEFAULT
        headers.update({ "if-match": str(etag) })
        
        if is_patch:
            result, resp = self._request_service.patch(
                url = f"{self._api_url}/{str(cirrus_object.key)}",
                headers = headers,
                payload = cirrus_object_with_reason,
                return_type = self._return_type
            )
        else:
            result, resp = self._request_service.put(
                url = f"{self._api_url}/{str(cirrus_object.key)}",
                headers = headers,
                payload = cirrus_object_with_reason,
                return_type = self._return_type
            )

        if (result is not None):
            return result, str(resp.headers["etag"])
        
        return None, None
    

    def start_workflow(self,
                       cirrus_object_key : str,
                       workflow_definition_id : str,
                       variables : Dict = {}
                      ) -> Tuple[CirrusObject, str]:
        if (cirrus_object_key is None): raise ValueError(f"cirrus_object_key cannot be empty")
        if (workflow_definition_id is None): raise ValueError(f"workflow_definition_id cannot be empty")

        cirrus_object, etag = self.get_by_key(key = cirrus_object_key, fields = ["key"])
        workflow = \
            {
                "definitionId": workflow_definition_id,
                "startWorkflow": "true",
                "variables": variables
            }
        setattr(cirrus_object, "workflow", workflow)

        return self.update(cirrus_object = cirrus_object, 
                           etag = etag,
                           is_patch = True)
    

    def wait_for_workflow(self, 
                          cirrus_object_key : str, 
                          sleep_in_sec : int = 5,
                          timeout_in_sec : int = None,
                          raise_error_on_timeout: bool = True
                         ) -> CirrusObject:
        if (cirrus_object_key is None): raise ValueError(f"cirrus_object_key cannot be empty")
        if (timeout_in_sec is not None and timeout_in_sec < 0): raise ValueError(f"timeout_in_sec cannot be negative")

        start_dttm = datetime.now()
        seconds_passed = 0
        is_timeout = False
        is_complete = False

        while (not is_complete and not is_timeout):
            cirrus_object, etag = self.get_by_key(
                key = cirrus_object_key, 
                fields = ["workflow"])
            if (cirrus_object is None):
                raise CirrusObjectNotFoundError(object_type = self.get_object_type(),
                                                key = cirrus_object_key,
                                                error = "Unable to wait for workflow")
            cirrus_object : CirrusObject = cirrus_object
            if (timeout_in_sec is not None):
                seconds_passed = (datetime.now() - start_dttm).total_seconds()

            if (cirrus_object.has_workflow()):
                is_complete = True
            elif (timeout_in_sec is not None and seconds_passed > timeout_in_sec):
                is_timeout = True
            else:
                time.sleep(sleep_in_sec)
        
        if (raise_error_on_timeout and is_timeout):
            raise WorkflowWaitTimeoutError()
        
        return cirrus_object, etag
    

    def wait_for_workflow_tasks(self, 
                                cirrus_object_key : str, 
                                sleep_in_sec : int = 5,
                                timeout_in_sec : int = None,
                                raise_error_on_timeout: bool = True
                               ) -> Tuple[bool, CirrusObject, str]:
        if (cirrus_object_key is None): raise ValueError(f"cirrus_object_key cannot be empty")
        if (timeout_in_sec is not None and timeout_in_sec < 0): raise ValueError(f"timeout_in_sec cannot be negative")

        start_dttm = datetime.now()
        seconds_passed = 0
        is_timeout = False
        is_complete = False

        while (not is_complete and not is_timeout):
            cirrus_object, etag = self.get_by_key(
                key = cirrus_object_key, 
                fields = ["workflow"])
            
            if (cirrus_object is None):
                raise CirrusObjectNotFoundError(object_type = self.get_object_type(),
                                                key = cirrus_object_key,
                                                error = "Unable to wait for workflow")
            cirrus_object : CirrusObject = cirrus_object
            if (timeout_in_sec is not None):
                seconds_passed = (datetime.now() - start_dttm).total_seconds()

            if (cirrus_object.has_workflow_tasks() or cirrus_object.is_workflow_complete()):
                is_complete = True
            elif (timeout_in_sec is not None and seconds_passed > timeout_in_sec):
                is_timeout = True
            else:
                time.sleep(sleep_in_sec)
        
        if (raise_error_on_timeout and is_timeout):
            raise WorkflowWaitTimeoutError()
        
        return is_complete, cirrus_object, etag
    

    def claim_task(self, 
                   cirrus_object_key : str,
                   task_id: str
                  ) -> None:
        
        if (cirrus_object_key is None): raise ValueError(f"cirrus_object_key cannot be empty")
        if (task_id is None): raise ValueError(f"task_id cannot be empty")

        _, _ = self._request_service.post(
            url = f"{self._api_url}/{str(cirrus_object_key)}/workflow/tasks/{task_id}/claim")
        