import logging
from typing import Any, Callable, List, Optional
import multiprocessing as mp
from multiprocessing.context import DefaultContext

import common.constants as constants
from common.logger import BatchLogger, NoSysCallsLogFilter

from domain.launch_arguments import LaunchArguments
from domain.state import BatchLogQueueProxy


class BatchLogWorker:

    def __init__(self,
                 launch_args : LaunchArguments, 
                 log_queue : BatchLogQueueProxy,
                ) -> None:
        if launch_args is None: raise ValueError(f"launch_args cannot be empty")
        if log_queue is None: raise ValueError(f"log_queue cannot be empty")

        self._log_queue = log_queue
        self._launch_args = launch_args


    def __call__(self, 
                 init_message : str = None,
                 filters : List[logging.Filter] = []
                ) -> Any:
        # unfortunatelly python loggers are not fully pickled, 
        # thus we contruct it here for simplicity :(
        logger = BatchLogger(
            log_level = self._launch_args.log_level,
            log_format = self._launch_args.log_format, 
            log_to_file = self._launch_args.log_file,
            log_dir_path = self._launch_args.log_dir_path,
            log_file_name = self._launch_args.log_file_name,
            log_to_console = self._launch_args.log_console,
            filters = filters
        )

        if init_message is not None:
            logger.info(init_message)

        while True:
            # consume a log message, block until one arrives
            log_record = self._log_queue.get()
            # check for shutdown
            if log_record is None:
                break
            # log the message
            logger.handle(log_record)


class BatchLogService:

    def __init__(self,
                 log_worker : BatchLogWorker,
                 logger : BatchLogger,
                 log_queue : BatchLogQueueProxy,
                ) -> None:
        if log_worker is None: raise ValueError(f"log_worker cannot be empty")
        if logger is None: raise ValueError(f"logger cannot be empty")
        if log_queue is None: raise ValueError(f"log_queue cannot be empty")
        
        self._log_worker = log_worker
        self._log_process : mp.Process = None
        self._log_queue : mp.Queue = None
        self._logger = logger
        self._log_queue = log_queue


    def start(self, 
              ctx : DefaultContext, 
              filters = [NoSysCallsLogFilter()],
              start_message : str = constants.APP_FULL_NAME
             ) -> None:
        if ctx is None: raise ValueError(f"ctx cannot be empty")

        self._log_process = ctx.Process(
            target = self._log_worker, 
            args = (start_message, filters))
        self._log_process.start()
    

    def stop(self, timeout_to_force : int = 5) -> None:
        if self._log_process is None or not self._log_process.is_alive():
            return
        
        if self._log_queue is not None:
            if self._logger is not None:
                self._logger.info(f"Gracefully stopping logging process within {timeout_to_force} sec...")
            self._log_queue.put(None)
            self._log_queue.close()
            self._log_process.join(timeout = timeout_to_force)
        
        if self._log_process.exitcode is None:
            self._log_process.terminate()
            self._log_process.join()
        
        self._log_queue = None
        self._log_process = None