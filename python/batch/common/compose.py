
import importlib, inspect, sys
from pathlib import Path
from typing import List
from multiprocessing.context import DefaultContext

from common.logger import *
import common.utils as utils
from common.container import Container

from domain.state import *
from domain.launch_arguments import LaunchArguments

from services.request_service import RequestService
from services.batch_run_service import BatchRunService
from services.solution_service import SolutionService


def register_classes(container : Container, 
                     dir_name : str = ".",
                     exclude_modules : List[str] = [],
                     exclude_classes : List[str] = []
                    ) -> Container:
    if (container is None): raise ValueError(f"container cannot be empty")
    if (dir_name is None or len(str(dir_name)) == 0): raise ValueError(f"dir_name cannot be empty")

    main_module_path = str(sys.modules["__main__"].__file__)
    dir_base, _ = os.path.split(os.path.abspath(main_module_path))
    files = list(Path(f"{dir_base}/{dir_name}").rglob("*.[pP][yY]"))
    
    for file in files:
        module_name = f"{file.parent.name}.{file.stem}"
        if (module_name in exclude_modules): break
        module = importlib.import_module(module_name)
        for name, cls in inspect.getmembers(module, inspect.isclass):
            if (cls.__module__ == module_name and not name in exclude_classes):
                container.register(cls)
                
    return container


def compose(launch_args : LaunchArguments, ctx : DefaultContext) -> Container:

    # Shared State accross processes
    StateManager.register('ConfigState', ConfigState, ConfigStateProxy)
    StateManager.register('ProgressState', ProgressState, ProgressStateProxy)
    StateManager.register('SharedState', SharedState, SharedStateProxy)
    StateManager.register('BatchLogQueue', BatchLogQueue, BatchLogQueueProxy)

    state_manager = StateManager(ctx = ctx)
    state_manager.start()
    config_state = state_manager.ConfigState(lock = state_manager.Lock())
    shared_state = state_manager.SharedState(lock = state_manager.Lock())
    progress_state = state_manager.ProgressState(lock = state_manager.Lock())
    log_queue = state_manager.BatchLogQueue(queue = state_manager.Queue())
    logger = BatchLogger(
        log_queue = log_queue,
        log_level = launch_args.log_level,
        log_format = launch_args.log_format, 
        log_to_file = launch_args.log_file,
        log_dir_path = launch_args.log_dir_path,
        log_file_name = launch_args.log_file_name,
        log_to_console = launch_args.log_console,
        filters = [NoSysCallsLogFilter()])
    
    container = Container(logger = logger)

    container.register(dependency_type = ConfigStateProxy,
                       implementation = config_state,
                       is_singleton = True)
    container.register(dependency_type = ProgressStateProxy,
                       implementation = progress_state,
                       is_singleton = True)
    container.register(dependency_type = SharedStateProxy,
                       implementation = shared_state,
                       is_singleton = True)
    container.register(dependency_type = BatchLogQueueProxy,
                       implementation = log_queue,
                       is_singleton = True)
    
    # launch arguments
    container.register(dependency_type = LaunchArguments,
                       implementation = launch_args,
                       is_singleton = True)


    # Repositories
    register_classes(container, dir_name = "repositories")

    # Services
    # get the certificate verification flag
    cert_pem_file, cert_verify = utils.get_certificate_verification_flag()
    is_http_logging_enabled = launch_args.log_http_requests and str(launch_args.log_level).upper() == 'DEBUG'
    request_service = RequestService(
        url = launch_args.host
        , shared_state = shared_state
        , auth_url = launch_args.auth_url
        , oidc_url = launch_args.oidc_url
        , user = launch_args.user
        , password = launch_args.password
        , token = launch_args.auth_token
        , client_auth = launch_args.client_auth
        , cert_verify = cert_verify
        , cert_file_path = cert_pem_file
        , tenant = launch_args.tenant
        , disable_insecure_warning = launch_args.disable_https_insecure_warning
        , logger_debug_func = logger.debug_http if is_http_logging_enabled else None
    )
    container.register(dependency_type = RequestService, 
                       implementation = request_service,
                       is_singleton = True)
    container.register(SolutionService, is_singleton = True)
    container.register(BatchRunService, is_singleton = True)
    register_classes(container, dir_name = "services")

    # container.register(Container, container, is_singleton = True)

    return container