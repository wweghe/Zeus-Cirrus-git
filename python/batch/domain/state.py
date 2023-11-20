from typing import Dict, Any
import types
from enum import Enum
from multiprocessing.managers import BaseProxy, SyncManager, DictProxy, NamespaceProxy

from domain.batch_config import BatchConfig


class ConfigState:

    def __init__(self, lock) -> None:
        if (lock is None): raise ValueError(f"lock cannot be empty")
        self._lock = lock
        self._config : BatchConfig = None

    def get(self) -> BatchConfig:
        return self._config
    
    def update(self, batch_config : BatchConfig) -> None:
        self._config = batch_config
    
    def lock(self):
        self._lock.acquire()

    def unlock(self):
        self._lock.release()


class SharedStateKeyEnum(str, Enum):
    OBJECT_REGISTRATIONS : str = "object_registrations"
    OBJECT_REST_PATHS : str = "object_registration_rest_paths"
    SOLUTION_DETAILS : str = "solution_details"
    CURRENT_USER : str = "current_user"
    BATCH_JOB : str = "batch_job"


class SharedState:

    def __init__(self, lock) -> None:
        if (lock is None): raise ValueError(f"lock cannot be empty")
        self._lock = lock
        self._state : Dict[str, Any] = {}

    def get(self, key : str, default : Any = None) -> Any:
        return self._state.get(key, default)
    
    def update(self, key : str, value : Any) -> None:
        self._state.update({ key: value })
    
    def lock(self):
        self._lock.acquire()

    def unlock(self):
        self._lock.release()


class ProgressState:

    def __init__(self, 
                 lock):
        if (lock is None): raise ValueError(f"lock cannot be empty")
        self._step = 0
        self._total = 0
        self._lock = lock
        self._total_elapsed_time : float = 0
        self._in_progress_state : Dict[str, str] = {}

    def start(self, total : int, step : int = 0):
        self._step = step
        self._total = total
        self._total_elapsed_time = 0


    def increment_step(self) -> int: self._step += 1; return self._step


    def add_elapsed_time(self, elapsed_time : float) -> float: 
        self._total_elapsed_time += elapsed_time;
        return self._total_elapsed_time


    def get_step(self) -> int: return self._step


    def get_total(self) -> int: return self._total


    def get_total_elapsed_time(self) -> float: return self._total_elapsed_time
    

    def is_in_progress(self, key : str) -> bool:
        return key in self._in_progress_state


    def update_in_progress(self, key : str, msg : str) -> None:
        self._in_progress_state.update({key: msg})

    
    def remove_in_progress(self, key : str) -> None:
        del self._in_progress_state[key]

    
    def get_all_in_progress(self) -> str:
        if (len(self._in_progress_state) == 0):
            return ""
        return "\n".join(self._in_progress_state.values())
    

    def count_lines_for_all_in_progess(self) -> int:
        #return self.get_all_in_progress().count("\n") + 1
        return len(self._in_progress_state)

    
    def count_in_progress(self): return len(self._in_progress_state)

    def lock(self):
        self._lock.acquire()

    def unlock(self):
        self._lock.release()


class ProxyBase(NamespaceProxy):
    # _exposed_ = ('__getattribute__', '__setattr__', '__delattr__')
    _isauto = False
     # proxy object does not contain __bases__ attribute, 
     # however, container will be looking for it, 
     # so to avoid the error, we explicitly set base class to empty tuple
    __bases__ = ()
    

    def __getattr__(self, name):
        result = super().__getattr__(name)
        if isinstance(result, types.MethodType):
            def wrapper(*args, **kwargs):
                return self._callmethod(name, args, kwargs)
            return wrapper
        return result

    # def __enter__(self):
    #     return self._callmethod('__enter__')
    
    # def __exit__(self, exc_type, exc_val, exc_tb):
    #     return self._callmethod('__exit__', (exc_type, exc_val, exc_tb))

class ConfigStateProxy(ProxyBase):
    _exposed_ = tuple(dir(ConfigState))


class SharedStateProxy(ProxyBase):
    _exposed_ = tuple(dir(SharedState))


class ProgressStateProxy(ProxyBase): 
    _exposed_ = tuple(dir(ProgressState))


class StateManager(SyncManager):
    pass