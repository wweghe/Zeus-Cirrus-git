from typing import Union
import time

from domain.cycle_config import CycleConfig
from domain.analysis_run_config import AnalysisRunConfig
from domain.batch_run_action_enum import BatchRunActionEnum
from domain.batch_job_step import BatchJobStepStateEnum


class BatchRunResult:

    # private members
    def __init__(self,
                 is_success : bool,
                 is_canceled : bool,
                 is_skip : bool, 
                 is_timeout : bool,
                 elapsed_time : float,
                 config : Union[CycleConfig, AnalysisRunConfig],
                 error : Exception = None
                ) -> None:
        
        self.is_success = is_success
        self.is_skip = is_skip
        self.is_timeout = is_timeout
        self.is_canceled = is_canceled
        self.error = error
        self.error_message = str(error) if error is not None else None
        self.elapsed_time = elapsed_time
        self.elapsed_time_str = time.strftime('%H:%M:%S', time.gmtime(elapsed_time))
        if (config is not None):
            self.rest_path = config.get_rest_path()
            self.object_type = config.get_object_type()
            self.object_id = config.objectId
            self.source_system_cd = config.sourceSystemCd
            self.action = config.get_action()

        self.status = BatchJobStepStateEnum.FAILED
        if is_success:
            self.status = BatchJobStepStateEnum.COMPLETED
        if is_timeout:
            self.status = BatchJobStepStateEnum.TIMEDOUT
        if is_canceled:
            self.status = BatchJobStepStateEnum.CANCELED
        if is_skip:
            self.status = BatchJobStepStateEnum.SKIPPED


    rest_path : str
    object_type : str
    object_id : str
    source_system_cd : str
    action : BatchRunActionEnum
    error : str
    error_message : str