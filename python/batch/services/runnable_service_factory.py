
from domain.base_config import BaseConfigRunnable
from domain.cycle_config import CycleConfig
from domain.analysis_run_config import AnalysisRunConfig

from services.analysis_run_service import AnalysisRunService
from services.cycle_service import CycleService
from services.runnable_service import RunnableService


class RunnableServiceFactory:

    def __init__(self,
                 analysis_run_service : AnalysisRunService,
                 cycle_service : CycleService
                ) -> None:
        
        if analysis_run_service is None: raise ValueError(f"analysis_run_service cannot be empty")
        if cycle_service is None: raise ValueError(f"cycle_service cannot be empty")

        self._analysis_run_service = analysis_run_service
        self._cycle_service = cycle_service


    def get(self, config : BaseConfigRunnable) -> RunnableService:

        if isinstance(config, AnalysisRunConfig):
            return self._analysis_run_service
        if isinstance(config, CycleConfig):
            return self._cycle_service
        
        raise NotImplementedError(f"Configuration of type '{type(config)}' is not supported.")