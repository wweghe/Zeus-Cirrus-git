from typing import Any, Callable
import multiprocessing as mp
from multiprocessing.context import DefaultContext
from multiprocessing.synchronize import Event

from common.logger import BatchLogger

from domain.access_session import AccessSession
from domain.state import SharedStateProxy, SharedStateKeyEnum
from domain.launch_arguments import LaunchArguments

from services.request_service import RequestService


class AccessMonitorWorker:

    def __init__(self,
                 state : SharedStateProxy, 
                 request_service : RequestService
                ) -> None:
        if state is None: raise ValueError(f"state cannot be empty")
        if request_service is None: raise ValueError(f"request_service cannot be empty")

        self._state = state
        self._request_service = request_service


    __REFRESH_TOKEN_SKEW_MIN_SEC = 30


    def __get_access_session(self, state : SharedStateProxy) -> AccessSession:
        
        state.lock()

        try:
            return state.get(SharedStateKeyEnum.ACCESS_SESSION, None)
        finally:
            state.unlock()


    def __update_access_session(self, state : SharedStateProxy, access_session : AccessSession) -> None:
        
        state.lock()

        try:
            state.update(SharedStateKeyEnum.ACCESS_SESSION, access_session)
        finally:
            state.unlock()


    def __call__(self, 
                 init_logger : Callable[[], BatchLogger], 
                 reinit_request : Callable, 
                 shutdown_event : Event
                ) -> Any:
        
        logger : BatchLogger = init_logger() if init_logger is not None else None
        if reinit_request is not None: reinit_request()
        error : Exception = None

        try:
            access_session = self.__get_access_session(self._state)
            if access_session is None:
                access_session = self._request_service.get_new_access_session()
            
            while not shutdown_event.is_set():
                if access_session.is_access_expiring(self.__REFRESH_TOKEN_SKEW_MIN_SEC):
                    access_session = self._request_service.get_new_access_session()
                    self.__update_access_session(self._state, access_session)
                
                wait_sec = access_session.get_token_expiration_from_now() - self.__REFRESH_TOKEN_SKEW_MIN_SEC
                if logger is not None:
                    logger.debug(f"Waiting to refresh access in {wait_sec} sec.")
                shutdown_event.wait(wait_sec)
        except Exception as e:
            error = e

        if error is not None and logger is not None:
            logger.error(f"Ending access monitoring due to error: {error}")
        elif logger is not None:
            logger.info(f"Ending access monitoring due to shutdown.")


class AccessMonitoringService:

    def __init__(self, 
                 access_monitor : AccessMonitorWorker,
                 request_service : RequestService,
                 logger : BatchLogger
                ) -> None:
        if access_monitor is None: raise ValueError(f"access_monitor cannot be empty")
        if request_service is None: raise ValueError(f"request_service cannot be empty")
        if logger is None: raise ValueError(f"logger cannot be empty")

        self.access_monitor = access_monitor
        self._request_service = request_service
        self._logger = logger

        self._monitor_process : mp.Process
        self._shutdown_event : Event

    
    def start(self, ctx : DefaultContext) -> None:

        self._shutdown_event = ctx.Event()
        self._monitor_process = ctx.Process(
            target = self.access_monitor, 
            args = (self._logger.init_queue, self._request_service.reinit, self._shutdown_event))
        self._logger.info(f"Starting access monitoring process.")
        self._monitor_process.start()
    

    def stop(self, timeout_to_force : int = 5) -> None:
        if self._monitor_process is None or not self._monitor_process.is_alive():
            return
        if self._shutdown_event is not None and not self._shutdown_event.is_set():
            self._logger.info(f"Gracefully stopping access monitoring process within {timeout_to_force} sec...")
            self._shutdown_event.set()
            self._monitor_process.join(timeout = timeout_to_force)
        
        if self._monitor_process.exitcode is None:
            self._logger.info(f"Terminating access monitoring process.")
            self._monitor_process.terminate()
            self._monitor_process.join()
        else:
            self._logger.info(f"Access monitoring process stopped.")