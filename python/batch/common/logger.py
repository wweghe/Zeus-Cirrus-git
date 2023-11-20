import inspect, os, logging, time
from datetime import datetime
from typing import Any, Callable, List, Optional

import common.utils as utils
import common.constants as constants
from common.auth import AuthTokenRefeshDecorator

from services.request_service import RequestService


class BatchLogger:

    __LOGGER_NAME : str = "BatchLogger"
    __log_file_path : Optional[str] = None 

    # private members
    def __init__(self, 
                 log_level : str = "INFO",
                 log_format : str = constants.LOG_FORMAT,
                 log_to_file : bool = True,
                 log_dir_path : Optional[str] = None,
                 log_file_name : Optional[str] = None,
                 log_file_mode : str = 'w',
                 log_to_console : bool = False,
                 filters : List[logging.Filter] = []):
        
        is_null_handler = (not log_to_file and not log_to_console)
        if (not is_null_handler and log_format is None or len(str(log_format)) == 0):
            raise ValueError(f"format cannot be empty")
        
        log_level = 'NOTSET' if log_level is None else log_level 
        
        formatter = logging.Formatter(fmt = log_format)
        self._logger = logging.getLogger(self.__LOGGER_NAME)
        self._logger.setLevel(log_level)
        
        if log_to_file:
            log_dir_path = utils.get_dir_path(log_dir_path)

            if log_file_name is None or len(str(log_file_name)) == 0:
                log_file_name = f"batch_run_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
            self.__log_file_path = f"{log_dir_path}/{log_file_name}"
            file_handler = logging.FileHandler(
                filename = self.__log_file_path, 
                mode = log_file_mode,
                encoding = 'utf-8')
            file_handler.setLevel(log_level)
            file_handler.setFormatter(formatter)
            self.__add_filters(handler = file_handler, filters = filters)
            self._logger.addHandler(file_handler)
        
        if log_to_console:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.__add_filters(handler = console_handler, filters = filters)
            self._logger.addHandler(console_handler)

        if is_null_handler:
            self._logger.addHandler(logging.NullHandler())
        
        self.__silence_root_handlers()


    def __silence_root_handlers(self):
        if (self._logger.root.handlers is not None and len(self._logger.root.handlers) > 0):
            for handler in self._logger.root.handlers:
                self._logger.root.removeHandler(handler)
        
        self._logger.root.addHandler(logging.NullHandler())

    
    def __add_filters(self, 
                      handler : logging.Handler, 
                      filters : List[logging.Filter]):
        if (handler is None or filters is None or len(filters) == 0): return
        
        for filter in filters:
            handler.addFilter(filter)
    
    # def __getattr__(self, func):
    #     def method(*args, **kwargs):
    #         return getattr(self._logger, func)(*args, **kwargs)
    #     return method

    def get_log_file_path(self):
        return self.__log_file_path


    # multiprocessing limitation: loggers are not serializable (not pickled) between processes, 
    # for this reason wrapper is created
    def info(self, *args: Any, **kwargs: Any):
        self._logger.info(*args, **kwargs)

    def warning(self, *args: Any, **kwargs: Any):
        self._logger.warning(*args, **kwargs)
    
    def exception(self, *args: Any, **kwargs: Any):
        self._logger.exception(*args, **kwargs)
    
    def error(self, *args: Any, **kwargs: Any):
        self._logger.error(*args, **kwargs)
    
    def debug(self, *args: Any, **kwargs: Any):
        return self._logger.debug(*args, **kwargs)
    

class LogDecorator:
    def __init__(self, 
                 func, 
                 log_func : Callable[[str], None], 
                 logger: BatchLogger) -> None:
        self._func = func
        self._logger = logger
        self._log_func = log_func or self.__printnull
        self._log_err_func = self._logger.exception \
            if (self._logger is not None) else self.__printnull
        # try:
        #     functools.update_wrapper(self, func)
        # except:
        #     pass
    
    def __printnull(self, s: str) -> None: pass

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        is_error = False
        func_result = None
        start_time = time.time()
        elapsed_time = 0

        try:
            log_params = repr(kwargs) if repr(args) == '()' else repr(args)
            self._log_func(f"{'Executing:':21s} {self._func.__qualname__} with parameters: {log_params}")

            try:
                func_result = self._func(*args, **kwargs)
                return func_result
            except Exception as e:
                elapsed_time = time.time() - start_time
                self._log_err_func(f"{'Failed:':21s} {self._func.__qualname__}")
                is_error = True
                raise e
        finally:
            elapsed_time = time.time() - start_time
            if not is_error:
                self._log_func(f"{time.strftime('Completed (%H:%M:%S):', time.gmtime(elapsed_time)):21s} "
                         f"{self._func.__qualname__} with result: {repr(func_result)}")


class trace(object):

    __DEBUG_SERVICES = ["RequestService", "BaseRepository", "BatchConfig", "IdentifierService"]


    def __init__(self, logger: BatchLogger):
        
        self._logger = logger
        self.__log_methods = \
            {
                'info': self.printnull if logger is None else logger.info,
                'debug': self.printnull if logger is None else logger.debug,
                'error': self.printnull if logger is None else logger.error,
                'warning': self.printnull if logger is None else logger.warning,
                'exception': self.printnull if logger is None else logger.exception
            }
            
            
    def printnull(self, s: str) -> None: pass

    def __call__(self, cls):

        if self._logger is None: return cls
        
        members = inspect.getmembers(
            cls, 
            predicate = lambda f: ((inspect.isfunction(f) \
                                   or inspect.ismethod(f)) \
                                   and not f.__name__ in ['__init__'])
            )
        for name, m in members:
            if (str(m.__name__).startswith("__")): 
                continue # skipping private methods/functions
            if str(m.__name__).startswith("_") or any(str(m.__qualname__).startswith(ds) for ds in self.__DEBUG_SERVICES):
                level = "debug"
            else:
                level = "info"
            
            decorator = LogDecorator(
                func = m, 
                log_func = self.__log_methods[level], 
                logger = self._logger)
            
            if isinstance(cls, RequestService) \
                and not str(m.__name__).startswith("_") \
                and not str(m.__name__) in ["refresh_token"]:

                rs : RequestService = cls
                decorator = AuthTokenRefeshDecorator(
                    func = decorator,
                    token_refresh_func = rs.refresh_token
                    )

            setattr(cls, name, decorator)

        return cls
    
    
def log_decorator(func, 
                  log_func : Callable[[str],str], 
                  logger: BatchLogger):
    def wrapped(*args, **kwargs):
        is_error = False
        log_err_func = logger.exception if (logger is not None) else print
        func_result = None
        start_time = time.time()
        elapsed_time = 0

        try:
            log_params = repr(kwargs) if repr(args) == '()' else repr(args)
            log_func(f"PID: {os.getpid()} {'Executing:':21s} {func.__qualname__} with parameters: {log_params}")

            try:
                func_result = func(*args, **kwargs)
                return func_result
            except Exception as e:
                elapsed_time = time.time() - start_time
                log_err_func(f"{'Failed:':21s} {func.__qualname__}")
                is_error = True
                raise e
        finally:
            elapsed_time = time.time() - start_time
            if not is_error:
                log_func(f"{time.strftime('Completed (%H:%M:%S):', time.gmtime(elapsed_time)):21s} "
                         f"{func.__qualname__} with result: {repr(func_result)}")
                 
    return wrapped


class NoSysCallsLogFilter(logging.Filter):

    __SYS_CALLS_LIST = ["BaseProxy.", "Client", "BatchRunProgressService.", "BatchLogger."]

    def filter(self, record):
        
        if (record.levelname not in ["ERROR", "CRITICAL"]):
            msg = record.getMessage()
            is_progress_msg = any(msg.find(s) >= 0 for s in self.__SYS_CALLS_LIST)
            return not is_progress_msg
        return True