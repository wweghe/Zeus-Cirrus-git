from types import SimpleNamespace

import common.constants as constants
from services.request_service import RequestService
from repositories.cirrus_object_repository import CirrusObjectRepository


class AnalysisRunRepository(CirrusObjectRepository):

    # private members
    __REST_PATH = "analysisRuns"
    __OBJECT_TYPE = constants.OBJECT_TYPE_ANALYSIS_RUN


    def __init__(self, request_service : RequestService) -> None:
        if (request_service is None): raise ValueError(f"request_service cannot be empty")
        
        super().__init__(request_service = request_service, 
                         rest_path = self.__REST_PATH,
                         object_type = self.__OBJECT_TYPE
                        )
    
    # public members
    def get_by_cycle_task_name_last(self,
                                    cycle : SimpleNamespace,
                                    task_name : str
                                   ) -> SimpleNamespace:
        if (cycle is None): raise ValueError(f"cycle cannot be empty")
        if (task_name is None or len(str(task_name)) == 0): raise ValueError(f"task_name cannot be empty")
        
        analysis_runs = self.get_by_has_object_link_to(
            link_type_id = constants.LINK_TYPE_ANALYSIS_RUN_CYCLE["id"],
            link_type_ssc = constants.LINK_TYPE_ANALYSIS_RUN_CYCLE["ssc"],
            object_key = cycle.key,
            link_side = 1,
            sort_by = ["creationTimeStamp:descending"])
        if (analysis_runs is not None):
            return next((ar for ar in analysis_runs \
                        if hasattr(ar.customFields, "userTaskName") \
                            and ar.customFields.userTaskName == task_name), None)

        return None