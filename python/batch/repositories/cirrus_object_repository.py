from types import SimpleNamespace
from typing import Any, List, Dict, Tuple
import time, copy, json
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
                         return_conversion_func = self._return_conversion
                        )


    def _return_conversion(self, response_text : str) -> SimpleNamespace:
        sn = json.loads(response_text, object_hook = lambda d: SimpleNamespace(**d))
        items : List[CirrusObject] = []
        if hasattr(sn, "items"): # expected to be an array of cirrus objects, so convert them one by one
            for s in sn.items:
                items.append(CirrusObject(**s.__dict__))

            sn.items = items
            
            return sn
        else: # expected to be a CirrusObject
            return CirrusObject(**sn.__dict__)
    

    def get_object_type(self) -> str: 
        return self._object_type
    

    def delete_by_key(self, 
                      key : str
                     ) -> bool:
        if (key is None or len(str(key)) == 0): raise ValueError(f"key cannot be empty")

        result, resp = self._request_service.post(
            url = f"{self._api_url}/bulkDelete",
            headers = { "content-type": constants.HEADERS_CONTENT_TYPE_JSON },
            payload = { "version": 1, "resources": [key] },
            return_type = self._return_type,
            return_conversion_func = self._return_conversion
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
            return_type = self._return_type,
            return_conversion_func = self._return_conversion
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
                return_type = self._return_type,
                return_conversion_func = self._return_conversion
            )
        else:
            result, resp = self._request_service.put(
                url = f"{self._api_url}/{str(cirrus_object.key)}",
                headers = headers,
                payload = cirrus_object_with_reason,
                return_type = self._return_type,
                return_conversion_func = self._return_conversion
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
    

    def claim_task(self, 
                   cirrus_object_key : str,
                   task_id: str
                  ) -> None:
        
        if (cirrus_object_key is None): raise ValueError(f"cirrus_object_key cannot be empty")
        if (task_id is None): raise ValueError(f"task_id cannot be empty")

        _, _ = self._request_service.post(
            url = f"{self._api_url}/{str(cirrus_object_key)}/workflow/tasks/{task_id}/claim")
        