from typing import Union, Any, Callable
import time

from common.errors import *
from common.logger import BatchLogger

from domain.cycle_config import CycleConfig
from domain.analysis_run_config import AnalysisRunConfig
from domain.batch_run_result import BatchRunResult
from domain.launch_arguments import LaunchArguments
from domain.batch_run_action_enum import BatchRunActionEnum

from services.cycle_service import CycleService
from services.analysis_run_service import AnalysisRunService
from services.batch_run_progress_service import BatchRunProgressService
from services.batch_job_service import BatchJobService
from services.runnable_service_factory import RunnableServiceFactory


class BatchRunWorker:
    
    def __init__(self, 
                 cycle_service : CycleService,
                 analysis_run_service : AnalysisRunService,
                 progress_service : BatchRunProgressService,
                 batch_job_service : BatchJobService,
                 runnable_service_factory : RunnableServiceFactory
                ) -> None:
        if (cycle_service is None): raise ValueError(f"cycle_service cannot be empty")
        if (analysis_run_service is None): raise ValueError(f"analysis_run_service cannot be empty")
        if (progress_service is None): raise ValueError(f"progress_service cannot be empty")
        if (batch_job_service is None): raise ValueError(f"batch_job_service cannot be empty")
        if (runnable_service_factory is None): raise ValueError(f"runnable_service_factory cannot be empty")

        self._cycle_service = cycle_service
        self._analysis_run_service = analysis_run_service
        self._progress_service = progress_service
        self._batch_job_service = batch_job_service
        self._runnable_service_factory = runnable_service_factory

    
    def __call__(self, 
                 config : Union[CycleConfig, AnalysisRunConfig], 
                 init_logger : Callable[[], BatchLogger] = None, 
                 reinit_request : Callable = None,
                 logger : BatchLogger = None
                ) -> Any:
        logger  = init_logger() if init_logger is not None else logger
        if reinit_request is not None: reinit_request()

        self._progress_service.progress(config = config)
        start_time = time.time()
        elapsed_time = 0
        if logger is not None:
            logger.info(f"{'Executing:':21s} {config.get_action().value} for {config.get_object_type()}:{config.objectId}:{config.sourceSystemCd}")
        result = self._run_config(config)
        elapsed_time = time.time() - start_time
        if logger is not None:
            logger.info(f"{time.strftime('Completed (%H:%M:%S):', time.gmtime(elapsed_time)):21s} {config.get_action().value} for {config.get_object_type()}:{config.objectId}:{config.sourceSystemCd}")
        self._progress_service.progress(result = result, config = config)
        
        return result
    

    def _run_config(self, 
                     config : Union[CycleConfig, AnalysisRunConfig]
                    ) -> BatchRunResult:
        
        is_skip = False
        start_time = time.time()

        try:
            self._batch_job_service.is_job_cancelation_requested(
                raise_error_if_true = True)

            service = self._runnable_service_factory.get(config)
            is_skip = service.is_skip(config)
            if not is_skip:
                service.execute_action(config)
            
            elapsed_time = time.time() - start_time

        except Exception as e:
            elapsed_time = time.time() - start_time
            is_job_canceled = isinstance(e, BatchJobCancelationError)
            is_step_canceled = isinstance(e, BatchJobStepCancelationError)
            is_timeout = isinstance(e, WorkflowWaitTimeoutError) or isinstance(e, ScriptExecutionTimeoutError)
            result = BatchRunResult(
                is_success = False,
                is_canceled = is_job_canceled or is_step_canceled,
                is_skip = False,
                is_timeout = is_timeout,
                elapsed_time = elapsed_time,
                config = config,
                error = e)
            self._batch_job_service.complete_step(result)
            
            return result
        
        result = BatchRunResult(
            is_success = True,
            is_canceled = False,
            is_skip = is_skip,
            is_timeout = False,
            elapsed_time = elapsed_time,
            config = config)
        self._batch_job_service.complete_step(result)
        
        return result