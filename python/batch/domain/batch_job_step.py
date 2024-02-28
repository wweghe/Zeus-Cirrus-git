from typing import List, Any, Dict
from types import SimpleNamespace
from enum import Enum
import hashlib

from domain.batch_run_action_enum import BatchRunActionEnum


class BatchJobStateEnum(str, Enum):
    QUEUED = 'queued'
    RUNNING = 'running'
    COMPLETED = 'completed'
    FAILED = 'failed'
    CANCELING = 'canceling'
    CANCELED = 'canceled'
    TIMEDOUT = 'timedOut'


class BatchJobStepStateEnum(str, Enum):
    QUEUED = 'queued'
    RUNNING = 'running'
    COMPLETED = 'completed'
    SKIPPED = 'skipped'
    FAILED = 'failed'
    CANCELING = 'canceling'
    CANCELED = 'canceled'
    TIMEDOUT = 'timedOut'


class BatchJobStep(SimpleNamespace):

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)


    id : str = None
    jobId : str = None
    restPath : str = None
    objectId : str = None
    sourceSystemCd : str = None
    action : BatchRunActionEnum
    state : BatchJobStepStateEnum = BatchJobStepStateEnum.QUEUED
    pid : int
    error : str = None


    def get_key(self) -> str:
        return hashlib.sha256(f"{self.jobId}:{self.restPath}:{self.objectId}:{self.sourceSystemCd}:{self.action.value.upper() if isinstance(self.action, BatchRunActionEnum) else str(self.action)}".encode('utf-8')).hexdigest()
    

class BatchJob(SimpleNamespace):

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)

    id : str = None
    name : str = None
    state : BatchJobStateEnum = BatchJobStateEnum.QUEUED
    solution : str = None
    stepsCount : int
    steps : List[BatchJobStep] = []
