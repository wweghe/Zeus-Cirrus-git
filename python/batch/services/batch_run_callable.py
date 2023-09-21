from typing import Union, Any
import time

from domain.cycle_config import CycleConfig
from domain.analysis_run_config import AnalysisRunConfig
from domain.batch_run_result import BatchRunResult
from domain.launch_arguments import LaunchArguments
from domain.batch_run_action_enum import BatchRunActionEnum

from services.cycle_service import CycleService
from services.analysis_run_service import AnalysisRunService
from services.batch_run_progress_service import BatchRunProgressService


class BatchRunCallable:
    
    def __init__(self, 
                 launch_args : LaunchArguments,
                 cycle_service : CycleService,
                 analysis_run_service : AnalysisRunService,
                 progress_service : BatchRunProgressService
                ) -> None:
        if (cycle_service is None): raise ValueError(f"cycle_service cannot be empty")
        if (analysis_run_service is None): raise ValueError(f"analysis_run_service cannot be empty")
        if (launch_args is None): raise ValueError(f"launch_args cannot be empty")
        if (progress_service is None): raise ValueError(f"progress_service cannot be empty")
        
        self._cycle_service = cycle_service
        self._analysis_run_service = analysis_run_service
        self._progress_service = progress_service

    
    def __call__(self, config : Union[CycleConfig, AnalysisRunConfig]) -> Any:

        self._progress_service.progress(config = config)
        result = self._run_config(config)
        self._progress_service.progress(result = result, config = config)
        
        return result
    

    def _run_config(self, 
                     config : Union[CycleConfig, AnalysisRunConfig]
                    ) -> BatchRunResult:
        
        is_skip = False
        start_time = time.time()

        try:
            if (type(config) == CycleConfig):
                
                action = config.get_action()

                if (action == BatchRunActionEnum.DELETE):
                    self._cycle_service.delete(cycle_config = config)
                elif (action == BatchRunActionEnum.CREATE):
                    self._cycle_service.create(cycle_config = config)
                elif (action == BatchRunActionEnum.UPDATE):
                    self._cycle_service.update(cycle_config = config)
                elif (action == BatchRunActionEnum.RUN):
                    self._cycle_service.run(cycle_config = config)
                elif (action is None or action == BatchRunActionEnum.SKIP):
                    is_skip = True
                else:
                    raise NotImplementedError(f"{config.get_rest_path()} action '{action}' is not supported.")
            
            elif (type(config) == AnalysisRunConfig):

                action = config.get_action()
                
                if (action == BatchRunActionEnum.DELETE):
                    self._analysis_run_service.delete(analysis_run_config = config)
                elif (action == BatchRunActionEnum.CREATE):
                    self._analysis_run_service.create(analysis_run_config = config)
                elif (action == BatchRunActionEnum.UPDATE):
                    self._analysis_run_service.update(analysis_run_config = config)
                elif (action == BatchRunActionEnum.RUN):
                    self._analysis_run_service.run(analysis_run_config = config)
                elif (action is None or action == BatchRunActionEnum.SKIP):
                    is_skip = True
                else:
                    raise NotImplementedError(f"{config.get_rest_path()} action '{action}' is not supported.")

            else: 
                raise NotImplementedError(f"Configuration of type '{type(config)}' is not supported.")
            
            elapsed_time = time.time() - start_time

        except Exception as e:
            elapsed_time = time.time() - start_time
            return BatchRunResult(
                is_success = False,
                is_skip = False,
                elapsed_time = elapsed_time,
                config = config,
                error = e)
        
        return BatchRunResult(
            is_success = True,
            is_skip = is_skip,
            elapsed_time = elapsed_time,
            config = config)