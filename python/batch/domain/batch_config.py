from typing import List, Dict, Union, Set

from domain.cycle_config import CycleConfig
from domain.analysis_run_config import AnalysisRunConfig
from domain.general_config import GeneralConfig
from domain.script_parameter_config import ScriptParameterConfig
from domain.workflow_config import WorkflowConfig


class BatchConfig:

    # private members
    def __init__(self, 
                 file_path : str,
                 general_config : GeneralConfig, 
                 cycle_configs : List[CycleConfig], 
                 cycle_script_parameter_configs : Dict[str, Dict[str, List[ScriptParameterConfig]]],
                 cycle_workflow_configs : Dict[str, Dict[str, List[WorkflowConfig]]],
                 analysis_run_configs : List[AnalysisRunConfig],
                 analysis_run_script_parameter_configs : Dict[str, List[ScriptParameterConfig]],
                ) -> None:

        if (file_path is None or len(str(file_path)) == 0):
            raise ValueError(f"file_path cannot be empty")
        if (general_config is None):
            raise ValueError(f"general_config cannot be empty")
        # if (cycle_configs is None):
        #     raise ValueError(f"cycle_config cannot be empty")
        # if (cycle_script_parameter_configs is None):
        #     raise ValueError(f"cycle_script_parameter_configs cannot be empty")
        # if (cycle_workflow_configs is None):
        #     raise ValueError(f"cycle_workflow_configs cannot be empty")
        # if (analysis_run_configs is None):
        #     raise ValueError(f"analysis_run_config cannot be empty")
        # if (analysis_run_script_parameter_configs is None):
        #     raise ValueError(f"analysis_run_script_parameter_configs cannot be empty")
        
        self.file_path = file_path
        self.general_config = general_config

        self.cycle_configs = cycle_configs or {}
        self.cycle_script_parameter_configs = cycle_script_parameter_configs or {}
        self.cycle_workflow_configs = cycle_workflow_configs or {}

        self.analysis_run_configs = analysis_run_configs or {}
        self.analysis_run_script_parameter_configs = analysis_run_script_parameter_configs or {}
                    

    # public members
    def get_cycle_script_parameters_by_key_task_name(self, 
                                                     config_key : str,
                                                     task_name : str,
                                                     parameter_set : str
                                                    ) -> Union[List[ScriptParameterConfig], None]:
        if (config_key in self.cycle_script_parameter_configs):
            task_name_param_set_key = f"{task_name}:{parameter_set or ''}"
            if (task_name_param_set_key in self.cycle_script_parameter_configs[config_key]):
                return self.cycle_script_parameter_configs[config_key][task_name_param_set_key]
        return None
    

    def get_uncompleted_cycle_workflow_by_task_name(self, 
                                                    config_key : str, 
                                                    task_name : str
                                                   ) -> WorkflowConfig:
        if (config_key in self.cycle_workflow_configs):
            configs = self.cycle_workflow_configs[config_key][task_name]
            for config in configs:
                if not config.is_processed():
                    return config
        return None
    

    def get_uncompleted_cycle_workflows_task_names(self, 
                                                   config_key : str
                                                  ) -> Set[str]:
        if (config_key in self.cycle_workflow_configs):
            result : Set[str] = set()
            configs = self.cycle_workflow_configs[config_key]
            for task_name in configs.keys():
                if len([config for config in configs[task_name] \
                        if not config.is_processed()]) > 0:
                    result.add(task_name)
            
            return result
        
        return None
    

    def get_analysis_run_script_parameters_by_key(self, config_key : str) -> Union[List[ScriptParameterConfig], None]:
        if (config_key in self.analysis_run_script_parameter_configs):
            return self.analysis_run_script_parameter_configs[config_key]
        return None
    
        