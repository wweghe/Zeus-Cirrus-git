from types import SimpleNamespace
from typing import Any, List, Dict, Tuple
import time, copy
import time
from datetime import datetime

from common.errors import *
import common.utils as utils
import common.constants as constants

from domain.cirrus_object import CirrusObject

from repositories.cirrus_object_repository import CirrusObjectRepository

from services.request_service import RequestService
from services.batch_job_service import BatchJobService


class CirrusObjectService:

    def __init__(self, 
                 request_service : RequestService,
                 batch_job_service : BatchJobService
                ) -> None:
        if request_service is None: raise ValueError(f"request_service cannot be empty")
        if batch_job_service is None: raise ValueError(f"batch_job_service cannot be empty")

        self._request_service = request_service
        self._batch_job_service = batch_job_service


    _request_service : RequestService = None
    _batch_job_service : BatchJobService = None
    

    def wait_for_workflow(self, 
                          cirrus_object_repository : CirrusObjectRepository,
                          cirrus_object_key : str, 
                          workflow_definition : SimpleNamespace,
                          sleep_in_sec : int = 5,
                          timeout_in_sec : int = None,
                          raise_error_on_timeout: bool = True
                         ) -> CirrusObject:
        if (cirrus_object_repository is None): raise ValueError(f"cirrus_object_repository cannot be empty")
        if (cirrus_object_key is None): raise ValueError(f"cirrus_object_key cannot be empty")
        if (workflow_definition is None): raise ValueError(f"workflow_definition cannot be empty")
        if (timeout_in_sec is not None and timeout_in_sec < 0): raise ValueError(f"timeout_in_sec cannot be negative")

        start_dttm = datetime.now()
        seconds_passed = 0
        is_timeout = False
        is_complete = False

        while (not is_complete and not is_timeout):
            cirrus_object, etag = cirrus_object_repository.get_by_key(
                key = cirrus_object_key, 
                fields = ["workflow"])
            if (cirrus_object is None):
                raise CirrusObjectNotFoundError(object_type = self.get_object_type(),
                                                key = cirrus_object_key,
                                                error = "Unable to wait for workflow")
            cirrus_object : CirrusObject = cirrus_object
            if (timeout_in_sec is not None):
                seconds_passed = (datetime.now() - start_dttm).total_seconds()

            if (cirrus_object.has_workflow(workflow_definition_id = workflow_definition.id)):
                is_complete = True
            elif (timeout_in_sec is not None and seconds_passed > timeout_in_sec):
                is_timeout = True
            else:
                time.sleep(sleep_in_sec)
        
        if (raise_error_on_timeout and is_timeout):
            raise WorkflowWaitTimeoutError()
        
        return cirrus_object, etag
    

    def wait_for_workflow_tasks(self,
                                cirrus_object_repository : CirrusObjectRepository, 
                                cirrus_object_key : str, 
                                workflow_definition : SimpleNamespace,
                                sleep_in_sec : int = 5,
                                timeout_in_sec : int = None,
                                raise_error_on_timeout: bool = True,
                                batch_step_id : str = None
                               ) -> Tuple[bool, CirrusObject, str]:
        if (cirrus_object_repository is None): raise ValueError(f"cirrus_object_repository cannot be empty")
        if (cirrus_object_key is None): raise ValueError(f"cirrus_object_key cannot be empty")
        if (workflow_definition is None): raise ValueError(f"workflow_definition cannot be empty")
        if (timeout_in_sec is not None and timeout_in_sec < 0): raise ValueError(f"timeout_in_sec cannot be negative")

        start_dttm = datetime.now()
        seconds_passed = 0
        is_timeout = False
        is_complete = False

        while (not is_complete and not is_timeout):
            cirrus_object, etag = cirrus_object_repository.get_by_key(
                key = cirrus_object_key, 
                fields = ["workflow"])
            
            if (cirrus_object is None):
                raise CirrusObjectNotFoundError(object_type = self.get_object_type(),
                                                key = cirrus_object_key,
                                                error = "Unable to wait for workflow")
            cirrus_object : CirrusObject = cirrus_object

            self._batch_job_service.is_cancelation_requested(batch_step_id)
            
            if (timeout_in_sec is not None):
                seconds_passed = (datetime.now() - start_dttm).total_seconds()

            if (cirrus_object.has_workflow_tasks(workflow_definition_id = workflow_definition.id) \
                    or cirrus_object.is_workflow_complete(workflow_definition_id = workflow_definition.id)):
                is_complete = True
            elif (timeout_in_sec is not None and seconds_passed > timeout_in_sec):
                is_timeout = True
            else:
                time.sleep(sleep_in_sec)
        
        if (raise_error_on_timeout and is_timeout):
            raise WorkflowWaitTimeoutError()
        
        return is_complete, cirrus_object, etag
    
