from typing import Union, Tuple
import os, time
from multiprocessing import Lock

from domain.batch_run_result import BatchRunResult
from domain.state import ProgressStateProxy
from domain.cycle_config import CycleConfig
from domain.analysis_run_config import AnalysisRunConfig
from domain.launch_arguments import LaunchArguments


class BatchRunProgressService:

    # private members
    # ref: https://tldp.org/HOWTO/Bash-Prompt-HOWTO/x361.html
    __CONSOLE_CURSOR_UP : str = "\033[1A"  # Move the cursor up 1 line
    __LINE_LENGTH : int = 100
    __print_enabled : bool = True


    def __init__(self,
                 state : ProgressStateProxy,
                 launch_args : LaunchArguments
                ) -> None:
        if state is None: raise ValueError(f"state cannot be empty")
        if (launch_args is None): raise ValueError(f"launch_args cannot be empty")

        self._state = state
        self.__print_enabled = not (launch_args.log_console or launch_args.hide_progress)
    

    def __get_header_message(self):
        msg = f"Running SAS Risk Cirrus Core batch utility...\n" + \
            f"-" * self.__LINE_LENGTH + \
            f"\n{'pid':10s}{'object type':15s}{'object id':25s}{'action':15s}{'status':15s}{'elapsed time':15s}\n" + \
            f"-" * self.__LINE_LENGTH
        return msg


    def __print_header(self):
        print(f"-" * self.__LINE_LENGTH, flush = True)
        print(f"{'pid':10s}{'object type':15s}{'object id':25s}{'action':15s}{'status':15s}{'elapsed time':15s}", flush = True)
        print(f"-" * self.__LINE_LENGTH, flush = True)

    
    def __print_footer(self, 
                       total_elapsed_time : float,
                       log_report_file_path : str = None,
                       log_file_path : str = None):
        print(f"-" * self.__LINE_LENGTH, flush = True)
        print(f"Batch run has completed: {time.strftime('%H:%M:%S', time.gmtime(total_elapsed_time))}", flush = True)
        if log_report_file_path is not None:
            print(f"Log report file is available for review at:\n\t{log_report_file_path}", flush = True)
        if log_file_path is not None:
            print(f"Log file is available for review at:\n\t{log_file_path}", flush = True)
        

    def __get_progress_message(self, 
                               config : Union[CycleConfig, AnalysisRunConfig],
                              ) -> str:
        if (config is None): return ""
        status = 'in progress'
        action = config.get_action()
        message = f"{str(os.getpid()):10s}{config.get_object_type():15s}{str(config.objectId) + ':' + str(config.sourceSystemCd):25s}{action + ' ' if action is not None else '':15s}{status}"
        filled_length = self.__LINE_LENGTH - len(message)
        message += " " * (filled_length if filled_length < self.__LINE_LENGTH else 1)

        return message


    def __get_result_message(self, 
                             result : BatchRunResult
                             ) -> int:
        if (result is None): return 0
        status = 'completed' if result.is_success else 'failed'
        if (result.is_skip): status = 'skipped'

        message = f"{str(os.getpid()):10s}{result.object_type:15s}{str(result.object_id) + ':' + str(result.source_system_cd):25s}{result.action + ' ' if result.action is not None else '':15s}{status:15s}{result.elapsed_time_str}"
        filled_length = self.__LINE_LENGTH - len(message)
        message += " " * (filled_length if filled_length < self.__LINE_LENGTH else 1)

        return message


    def __print_result(self, 
                     result : BatchRunResult
                    ) -> int:
        if (result is None): return 0
        status = 'completed' if result.is_success else 'failed'
        if (result.is_skip): status = 'skipped'

        message = f"{str(os.getpid()):10s}{result.object_type:15s}{str(result.object_id) + ':' + str(result.source_system_cd):25s}{result.action + ' ' if result.action is not None else '':15s}{status:15s}{result.elapsed_time_str}"
        error_message = ""
        filled_length = self.__LINE_LENGTH - len(message)
        message += " " * (filled_length if filled_length < self.__LINE_LENGTH else 1)
        # if (not result.is_success and result.error_message is not None):
        #     for line in f"ERROR: {result.error_message}".splitlines():
        #         filled_length = self.__LINE_LENGTH - len(line)
        #         line += " " * (filled_length if filled_length < self.__LINE_LENGTH else 1)
        #         error_message += f"\n{line}"
        #     message += error_message

        print(message, flush = True)

        return message.count('\n')
    

    def __print_progress_bar(self,
                             step : int, 
                             total : int, 
                             prefix : str = 'Progress', 
                             suffix : str = 'Complete', 
                             decimals : int = 0, 
                             length : int = 50, 
                             fill : str = '█',
                             print_end : str = "\r"
                            ) -> None:
        """
        Call in a loop to create terminal progress bar
        @params:
            step        - Required  : current iteration (Int)
            total       - Required  : total iterations (Int)
            prefix      - Optional  : prefix string (Str)
            suffix      - Optional  : suffix string (Str)
            decimals    - Optional  : positive number of decimals in percent complete (Int)
            length      - Optional  : character length of bar (Int)
            fill        - Optional  : bar fill character (Str)
            print_end   - Optional  : end character (e.g. "\r", "\r\n") (Str)
        """
        percent = ("{0:." + str(decimals) + "f}").format(100 * (step / float(total)))
        filled_length = int(length * step // total)
        bar = fill * filled_length + '-' * (length - filled_length)
        print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = print_end, flush = True)
        # print New Line on Complete
        if step == total: 
            print(" " * length * 2, flush = True)


    def start(self, total : int, step : int = 0) -> None:
        self._state.lock()
        
        try:
            self._state.start(total, step)
        finally:
            self._state.unlock()
        
        if self.__print_enabled:
            msg = self.__get_header_message()
            self._state.update_in_progress(
                    key = "start",
                    msg = msg)
    

    def stop(self, log_report_file_path : str = None, log_file_path : str = None):
        self._state.lock()

        try:
            if self.__print_enabled:
                self.__print_footer(
                    total_elapsed_time = self._state.get_total_elapsed_time(),
                    log_report_file_path = log_report_file_path,
                    log_file_path = log_file_path)
        finally:
            self._state.unlock()


    def progress(self, 
                 config : Union[CycleConfig, AnalysisRunConfig], 
                 result : BatchRunResult = None
                ):
        self._state.lock()

        try:
            os.system('cls' if os.name == 'nt' else 'clear')

            # progress_lines = self._state.count_lines_for_all_in_progess() + 1
            progress_key = f"{os.getpid()}{config.get_key()}"
            if not self._state.is_in_progress(progress_key):
                msg = self.__get_progress_message(config)
                self._state.update_in_progress(
                    key = progress_key,
                    msg = msg)

            if (result is not None):
                # if self.__print_enabled:
                #     print(self.__CONSOLE_CURSOR_UP * progress_lines, flush = True)
                self._state.increment_step()
                # self._state.remove_in_progress(progress_key)
                msg = self.__get_result_message(result)
                self._state.update_in_progress(
                    key = progress_key,
                    msg = msg)
                self._state.add_elapsed_time(result.elapsed_time)
                # if self.__print_enabled:
                #     self.__print_result(result)
            
            if self.__print_enabled:
                # if (result is None):
                #     print(self.__CONSOLE_CURSOR_UP * progress_lines, flush = True)
                if self._state.count_in_progress() > 0:
                    print(self._state.get_all_in_progress(), flush = True)
            
                self.__print_progress_bar(
                    step = self._state.get_step(), 
                    total = self._state.get_total())
        finally:
            self._state.unlock()
