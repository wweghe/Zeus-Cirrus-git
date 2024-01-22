
from typing import Any
from datetime import datetime
import time

from domain.base_config import BaseConfigRunnable
from domain.batch_run_action_enum import BatchRunActionEnum

from services.batch_job_service import BatchJobService


class RunnableService:

    _SLEEP_IN_SEC : int = 3


    def __init__(self,
                 batch_job_service : BatchJobService
                ) -> None:
        if batch_job_service is None: raise ValueError(f"batch_job_service cannot be empty")

        self._batch_job_service = batch_job_service


    def execute_action(self, config : BaseConfigRunnable, *args, **kwargs) -> Any:
        
        if self.is_sleep(config):
            return self.run_sleep(config)
        elif self.is_skip(config):
            return None
        else:
            self.raise_error_action_not_supported(config)


    def is_skip(self, config : BaseConfigRunnable) -> bool:
        return config.get_action() == BatchRunActionEnum.SKIP
    

    def is_sleep(self, config : BaseConfigRunnable) -> bool:
        return config.get_action() == BatchRunActionEnum.SLEEP
    

    def raise_error_action_not_supported(self, config : BaseConfigRunnable) -> None:
        raise NotImplementedError(f"{config.get_rest_path()} action '{config.get_action()}' is not supported.")
    

    def run_sleep(self, config : BaseConfigRunnable) -> None:
        
        batch_step_id = self._batch_job_service.get_step_id_by_config(config = config)
        self._batch_job_service.update_step(config)

        while True:
            time.sleep(self._SLEEP_IN_SEC)
            self._batch_job_service.is_cancelation_requested(batch_step_id)