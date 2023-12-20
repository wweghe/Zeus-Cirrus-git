from typing import Dict, List, Tuple, Union
import os

from common.errors import *

from domain.launch_arguments import LaunchArguments
from domain.batch_run_action_enum import BatchRunActionEnum
from domain.batch_job_step import *
from domain.base_config import BaseConfigRunnable
from domain.state import SharedStateProxy
from domain.state import SharedStateKeyEnum
from domain.batch_run_result import BatchRunResult

from repositories.batch_job_repository import BatchJobRepository
from repositories.batch_job_step_repository import BatchJobStepRepository


class BatchJobService:

    def __init__(self,
                 launch_args : LaunchArguments,
                 batch_job_repository : BatchJobRepository,
                 batch_job_step_repository : BatchJobStepRepository,
                 shared_state :  SharedStateProxy,
                ) -> None:
        if launch_args is None: raise ValueError(f"launch_args cannot be empty")
        if batch_job_repository is None: raise ValueError(f"batch_job_repository cannot be empty")
        if batch_job_step_repository is None: raise ValueError(f"batch_job_step_repository cannot be empty")
        if shared_state is None: raise ValueError(f"shared_state cannot be empty")

        self._launch_args = launch_args
        self._batch_job_repository = batch_job_repository
        self._state = shared_state


    _state : SharedStateProxy = None
    _launch_args : LaunchArguments = None
    _batch_job_steps : Dict[str, BatchJobStep] = {}
    _batch_job_repository : BatchJobRepository = None
    
    
    def __save_batch_job_to_state(self, job : BatchJob) -> None:

        self._state.lock()

        try:
            self._state.update(SharedStateKeyEnum.BATCH_JOB, job)
        finally:
            self._state.unlock()

    
    def __get_batch_job_from_state(self) -> BatchJob:

        job : BatchJob = None
        self._state.lock()

        try:
            job = self._state.get(SharedStateKeyEnum.BATCH_JOB)
        finally:
            self._state.unlock()

        return job


    def __get_batch_job_step_by_id(self, id : str) -> BatchJobStep:
        
        job : BatchJob = None

        self._state.lock()

        try:
            job = self._state.get(SharedStateKeyEnum.BATCH_JOB)
        finally:
            self._state.unlock()
        
        if job is not None and job.steps is not None:
            return next((s for s in job.steps if s.id == id), None)

        return None
    

    def __get_batch_job_step_by_key(self, key : str) -> BatchJobStep:
        
        job : BatchJob = None
        self._state.lock()

        try:
            job = self._state.get(SharedStateKeyEnum.BATCH_JOB)
        finally:
            self._state.unlock()
        
        if job is not None and job.steps is not None:
            return next((s for s in job.steps if s.get_key() == key), None)

        return None
    

    def __batch_run_result_to_step(self, batch_run_result : BatchRunResult) -> BatchJobStep:
        
        step = BatchJobStep()
        
        step.jobId = self._launch_args.job_id
        step.objectId = batch_run_result.object_id
        step.sourceSystemCd = batch_run_result.source_system_cd
        step.action = batch_run_result.action
        step.restPath = batch_run_result.rest_path

        return step


    def __get_step_key_from_batch_run_result(self, batch_run_result : BatchRunResult) -> str:
        
        return self.__batch_run_result_to_step(batch_run_result).get_key()
    

    def __config_to_step(self, config : BaseConfigRunnable) -> BatchJobStep:
        
        step = BatchJobStep()
        
        step.jobId = self._launch_args.job_id
        step.restPath = config.get_rest_path()
        step.objectId = config.objectId
        step.sourceSystemCd = config.sourceSystemCd
        step.action = config.get_action()

        return step

    def __get_step_key_from_config(self, config : BaseConfigRunnable) -> str:
        
        return self.__config_to_step(config).get_key()
    

    def __create_step_from_config(self, 
                                  config : BaseConfigRunnable,
                                  state : BatchJobStepStateEnum,
                                  error : str = None,
                                  pid : int = os.getpid()
                                 ) -> BatchJobStep:
        
        step = self.__config_to_step(config)
        step.state = state
        step.error = error
        step.pid = pid
        step.jobId = self._launch_args.job_id

        return step
    

    def __create_steps_from_configs(self, configs : List[BaseConfigRunnable]) -> List[BatchJobStep]:

        steps : List[BatchJobStep] = []
        for config in configs:
            step = self.__create_step_from_config(
                config,
                state = BatchJobStepStateEnum.QUEUED
            )
            steps.append(step)
        
        return steps


    def get_step_id_by_config(self, config : BaseConfigRunnable) -> str:
        if config is None: raise ValueError(f"config cannot be empty")

        if self._launch_args.job_id is None:
            return None

        step_key = self.__get_step_key_from_config(config)
        step = self.__get_batch_job_step_by_key(step_key)

        return step.id if step is not None else None


    def create_steps(self, configs : List[BaseConfigRunnable]) -> None:
        
        if configs is None: raise ValueError(f"configs cannot be empty")
        if self._launch_args.job_id is None:
            return None
        if len(configs) == 0: return []

        job, etag = self._batch_job_repository.get_by_key(
            key = self._launch_args.job_id, 
            fields = ["key"])
        
        if job is None:
            raise ValueError(f"Batch job with id '{self._launch_args.job_id}' does not exist.")
        
        steps = self.__create_steps_from_configs(configs)
        job, _ = self._batch_job_repository.update(
            id = self._launch_args.job_id,
            steps = steps,
            etag = etag
        )
        self.__save_batch_job_to_state(job)
    

    def update_step(self, 
                    config : BaseConfigRunnable,
                    state : BatchJobStepStateEnum = BatchJobStepStateEnum.RUNNING,
                    error : str = None,
                    pid : int = os.getpid()
                   ) -> None:
        if self._launch_args.job_id is None:
            return None
        
        job : BatchJob = None
        step : BatchJobStep = None

        job, etag = self._batch_job_repository.get_by_key(
            key = self._launch_args.job_id, 
            fields = ["key"])
        
        if job is None:
            raise ValueError(f"Batch job with id '{self._launch_args.job_id}' does not exist.")

        if job.steps is not None:
            step_key = self.__get_step_key_from_config(config)
            step = next((s for s in job.steps if s.get_key() == step_key), None)
        
        if step is None:
            raise ValueError(f"Batch job step with key '{step_key}' does not exist.")
        
        step.state = state
        step.error = error
        step.pid = pid

        job, _ = self._batch_job_repository.update(
            id = job.id,
            steps = [step],
            etag = etag
            )
        self.__save_batch_job_to_state(job)
    

    def complete_step(self, 
                      batch_run_result : BatchRunResult
                     ) -> None:
        if batch_run_result is None: raise ValueError(f"batch_run_result cannot be empty")

        if self._launch_args.job_id is None:
            return None
        
        # job : BatchJob = self.__get_batch_job_from_state() # job should exist by now
        job : BatchJob = None
        step : BatchJobStep = None

        job, etag = self._batch_job_repository.get_by_key(
            key = self._launch_args.job_id, 
            fields = ["key"])
        
        if job is None:
            raise ValueError(f"Batch job with id '{self._launch_args.job_id}' does not exist.")
        
        if job.steps is not None:
            step_key = self.__get_step_key_from_batch_run_result(batch_run_result)
            step = next((s for s in job.steps if s.get_key() == step_key), None)

        if step is None:
            raise ValueError(f"Batch job step with key '{step_key}' does not exist.")
        else:
            # backend does not return pid on purpose
            if getattr(step, "pid", 0) == 0:
                step.pid = os.getpid()
        
        step.state = BatchJobStepStateEnum.FAILED

        if batch_run_result.is_success:
            step.state = BatchJobStepStateEnum.COMPLETED

        if batch_run_result.is_timeout:
            step.state = BatchJobStepStateEnum.TIMEDOUT

        if batch_run_result.is_canceled:
            step.state = BatchJobStepStateEnum.CANCELED
        
        if batch_run_result.error_message is not None:
            step.error = batch_run_result.error_message

        if batch_run_result.is_skip:
            step.state = BatchJobStepStateEnum.SKIPPED
        
        job, _ = self._batch_job_repository.update(
            id = job.id,
            steps = [step],
            etag = etag
            )
        self.__save_batch_job_to_state(job)
    
    
    def is_cancelation_requested(self, 
                                 step_id : str, 
                                 raise_error_if_true : bool = True,
                                ) -> bool:

        if step_id is None: return False, None
        if self._launch_args.job_id is None:
            return False, None

        is_job_cancel = self.is_job_cancelation_requested(raise_error_if_true = raise_error_if_true)
        is_job_step_cancel = self.is_step_cancelation_requested(step_id, raise_error_if_true = raise_error_if_true)
        
        return is_job_cancel or is_job_step_cancel


    def is_job_cancelation_requested(self, 
                                     raise_error_if_true : bool = True
                                    ) -> bool:
        
        if self._launch_args.job_id is None:
            return False

        job, _ = self._batch_job_repository.get_by_key(
            key = self._launch_args.job_id)
        
        self.__save_batch_job_to_state(job)

        if job is not None \
            and str(job.state).lower() == BatchJobStepStateEnum.CANCELING:

            if raise_error_if_true:
                raise BatchJobCancelationError()
            
            return True

        return False


    def is_step_cancelation_requested(self, id : str, raise_error_if_true : bool = True) -> bool:
        
        if id is None: return False
        if self._launch_args.job_id is None:
            return False
        
        step = self.__get_batch_job_step_by_id(id)
        
        if step is not None \
            and str(step.state).lower() == BatchJobStepStateEnum.CANCELING:
            
            if raise_error_if_true:
                raise BatchJobStepCancelationError()
            
            return True

        return False
    
