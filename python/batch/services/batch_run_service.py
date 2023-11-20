
import multiprocessing as mp
from multiprocessing import Pool
from typing import Any, Union, List
import time

import common.constants as constants
from common.logger import BatchLogger

from domain.batch_config import BatchConfig
from domain.batch_run_result import BatchRunResult
from domain.launch_arguments import LaunchArguments

from services.analysis_run_service import AnalysisRunService
from services.cycle_service import CycleService
from services.script_execution_service import ScriptExecutionService
from services.batch_run_report_service import BatchRunReportService
from services.batch_run_progress_service import BatchRunProgressService
from services.batch_run_callable import BatchRunCallable
from services.batch_job_service import BatchJobService


class BatchRunService:

    _analysis_run_service : AnalysisRunService = None
    _script_execution_service : ScriptExecutionService = None
    _cycle_service : CycleService = None
    _batch_run_report_service : BatchRunReportService = None
    _progress_service : BatchRunProgressService = None
    _launch_args : LaunchArguments = None
    _batch_logger : BatchLogger = None


    def __init__(self, 
                cycle_service : CycleService,
                analysis_run_service : AnalysisRunService,
                script_execution_service : ScriptExecutionService,
                batch_run_report_service : BatchRunReportService,
                progress_service : BatchRunProgressService,
                launch_args : LaunchArguments,
                batch_run_callable : BatchRunCallable,
                batch_logger : BatchLogger,
                batch_job_service : BatchJobService
            ) -> None:

        if (cycle_service is None): raise ValueError(f"cycle_service cannot be empty")
        if (analysis_run_service is None): raise ValueError(f"analysis_run_service cannot be empty")
        if (script_execution_service is None): raise ValueError(f"script_execution_service cannot be empty")
        if (batch_run_report_service is None): raise ValueError(f"batch_run_report_service cannot be empty")
        if (progress_service is None): raise ValueError(f"progress_service cannot be empty")
        if (launch_args is None): raise ValueError(f"launch_args cannot be empty")
        if (batch_run_callable is None): raise ValueError(f"batch_run_callable cannot be empty")

        self._cycle_service = cycle_service
        self._analysis_run_service = analysis_run_service
        self._script_execution_service = script_execution_service
        self._batch_run_report_service = batch_run_report_service
        self._progress_service = progress_service
        self._launch_args = launch_args
        self._batch_run_callable = batch_run_callable
        self._batch_logger = batch_logger
        self._batch_job_service = batch_job_service


    # public members
    def run(self, batch_config: BatchConfig) -> List[BatchRunResult]:

        if (batch_config is None): raise ValueError(f"batch_config cannot be empty")

        cycle_configs = \
            [config for config in batch_config.cycle_configs if not config.get_is_parallel()] \
            if batch_config.cycle_configs is not None else []
        cycle_configs_async = \
            [config for config in batch_config.cycle_configs if config.get_is_parallel()] \
            if batch_config.cycle_configs is not None else []
        analysis_run_configs_async = \
            [config for config in batch_config.analysis_run_configs if config.get_is_parallel()] \
            if batch_config.analysis_run_configs is not None else []
        analysis_run_configs = \
            [config for config in batch_config.analysis_run_configs if not config.get_is_parallel()] \
            if batch_config.analysis_run_configs is not None else []
        
        configs_async = cycle_configs_async + analysis_run_configs_async
        configs = cycle_configs + analysis_run_configs

        result : List[BatchRunResult] = []
        total = len(configs) + len(configs_async)

        self._progress_service.start(total = total)

        self._batch_job_service.create_steps(configs = configs + configs_async)

        if (len(configs) > 0):
            for config in configs:
                result.append(self._batch_run_callable(config))

        if (len(configs_async) > 0):
            # ctx = mp.get_context('forkserver')
            # with ctx.Pool(processes = self._launch_args.max_parallel_processes) as pool:
            with Pool(processes = self._launch_args.max_parallel_processes) as pool:
                res_async = pool.map(self._batch_run_callable, configs_async)
                pool.close()
                pool.join()
                result += res_async
    
        log_report_file_path = None
        if self._launch_args.log_report:
            log_report_file_path = self._batch_run_report_service.create_log_report(
                batch_config = batch_config, 
                batch_run_results = result, 
                report_dir_path = self._launch_args.log_report_dir_path, 
                report_file_name = self._launch_args.log_report_file_name)
        
        log_file_path = self._batch_logger.get_log_file_path() if self._batch_logger else None
        self._progress_service.stop(
            log_report_file_path = log_report_file_path,
            log_file_path = log_file_path)

        return result
        
