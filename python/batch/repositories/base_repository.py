from types import SimpleNamespace
from typing import Any, List, Dict, Tuple
import time
from datetime import datetime

from common.errors import *
import common.utils as utils
import common.constants as constants

from domain.identifier import Identifier

from services.request_service import RequestService


class BaseRepository:

    _request_service : RequestService = None
    _rest_path : str = None
    _base_url : str = None
    _api_url : str = None
    _headers : Dict[str, str] = None
    _return_type : Any = SimpleNamespace


    def __init__(self, 
                 request_service : RequestService, 
                 rest_path : str,
                 base_url : str,
                 headers: Dict[str, str] = constants.HEADERS_DEFAULT,
                 return_type : Any = SimpleNamespace) -> None:
        if (request_service is None): raise ValueError(f"request_service cannot be empty")
        if (rest_path is None): raise ValueError(f"rest_path cannot be empty")
        if (base_url is None): raise ValueError(f"base_url cannot be empty")
        if (return_type is None): raise ValueError(f"return_type cannot be empty")
        
        self._request_service = request_service
        self._rest_path = rest_path
        self._base_url = base_url
        self._api_url = f"{base_url}/{rest_path}"
        self._headers = headers
        self._return_type = return_type
    

    # public members
    def get_rest_path(self) -> str: 
        return self._rest_path
    
    
    def get_by_key(self, 
                   key : str,
                   fields : List[str] = None
                  ) -> Tuple[SimpleNamespace, str]:
        if (key is None or len(str(key)) == 0): raise ValueError(f"key cannot be empty")

        url = f"{self._api_url}/{key}"
        queyr_params = {}
        if (fields is not None and len(fields) > 0):
            queyr_params.update({"fields": ','.join(fields)})
        result, resp = self._request_service.get(url = url, 
                                                 params = queyr_params,
                                                 headers = self._headers,
                                                 return_type = self._return_type)
        if (result is not None):
            return result, str(resp.headers["etag"]) if "etag" in resp.headers else None
        
        return None, None


    def get_by_identifier(self, id : Identifier, fields : List[str] = None) -> SimpleNamespace:
        return self.get_by_id(id.get_id(), id.get_ssc(), fields)


    def get_by_id(self, 
                  id : str, 
                  ssc : str = constants.SOURCE_SYSTEM_CD_DEFAULT,
                  fields : List[str] = None
                 ) -> SimpleNamespace:
        if (id is None or len(str(id)) == 0): raise ValueError(f"id cannot be empty")
        if (ssc is None or len(str(ssc)) == 0): raise ValueError(f"source system cd (ssc) cannot be empty")

        url = f"{self._api_url}"
        query_params = { 
            'start': 0, 
            'limit': 1,
            'filter': f'and(eq(objectId,"{id}"),eq(sourceSystemCd,"{ssc}"))' }
        if (fields is not None and len(fields) > 0):
            query_params.update({"fields": ','.join(fields)})
        result, _ = self._request_service.get(
            url = url,
            params = query_params,
            headers = self._headers,
            return_type = self._return_type
        )
        if result is not None and (len(result.items) > 0):
            return result.items[0]
        
        return None
    

    def get_by_has_object_link_to(self,
                                  link_type_id : str,
                                  link_type_ssc : str,
                                  object_key : str,
                                  link_side : int = 1,
                                  sort_by : List[str] = None
                                 ) -> List[Any]:
        if (link_type_id is None or len(str(link_type_id)) == 0): raise ValueError(f"link_type_id cannot be empty")
        if (link_type_ssc is None or len(str(link_type_ssc)) == 0): raise ValueError(f"link_type_ssc cannot be empty")
        if (object_key is None or len(str(object_key)) == 0): raise ValueError(f"object_key cannot be empty")
        
        query_params = {
            "filter": f"hasObjectLinkTo('{link_type_ssc}','{link_type_id}','{object_key}',{link_side})"
        }
        if (sort_by is not None):
            query_params.update({ "sortBy": ','.join(sort_by) })

        result, _ = self._request_service.get(
            url = f"{self._api_url}",
            params = query_params,
            headers = self._headers,
            return_type = self._return_type)
        if result is not None and (len(result.items) > 0):
            return result.items
        
        return None
    

    def get_by_filter(self, 
                      filter : str, 
                      start : int = 0,
                      limit : int = 100,
                      fields : List[str] = None,
                      sort_by : List[str] = None
                     ) -> List[Any]:
        # if (filter is None): raise ValueError(f"filter cannot be empty")
        if (start is None or start < 0): raise ValueError(f"start should be possitive integer")
        if (limit is None or limit < 1): raise ValueError(f"limit should be possitive integer bigger than 1")

        url = f"{self._api_url}"
        query_params = { 
            'start': start, 
            'limit': limit }
        if (fields is not None and len(fields) > 0):
            query_params.update({"fields": ','.join(fields)})
        if (filter is not None and len(str(filter)) > 0):
            query_params.update({ "filter": filter })
        if (sort_by is not None):
            query_params.update({ "sortBy": ','.join(sort_by) })

        result, _ = self._request_service.get(
            url = url,
            params = query_params,
            headers = self._headers,
            return_type = self._return_type
        )
        if result is not None and (len(result.items) > 0):
            return result.items
        
        return None
    
        