import os, argparse
from typing import Tuple
import multiprocessing as mp

from common.compose import *
import common.constants as constants
from common.logger import BatchLogger

from domain.launch_arguments import LaunchArguments

from services.batch_config_service import BatchConfigService
from services.batch_run_service import BatchRunService
from services.access_monitoring_service import AccessMonitoringService
from services.batch_log_service import BatchLogService


def usage() -> LaunchArguments:
    parser = argparse.ArgumentParser(
        prog = constants.APP_FULL_NAME,
        description = "Runs risk cirrus core cycles and/or analysis runs in a batch.")
    parser.add_argument(    
        "--file", "-f", 
        help = "Configuration file path (Excel spreadsheet)", 
        required = True)
    parser.add_argument(
        "--host", "--url", "-s",
        help = "SAS Viya host url for calling application endpoints, e.g. http(s)://test.sas.com:80. " \
            "If host is not specified, it is assumed that the application is running inside a SAS Viya cluster " \
                "and will derive host name based on the cluster environment variables.",
        required = False)
    parser.add_argument(
        "--auth-url", 
        help = "Third party authentication url (absolute url) for getting authentication token, e.g. http(s)://hostname.example.com/oauth2/token. ",
        required = False)
    parser.add_argument(
        "--oidc-url",
        help = "Third party open id configuration url (absolute url) for getting open id configuration information, e.g. http(s)://hostname.example.com/oauth2/.well-known/openid-configuration. ",
        required = False)
    parser.add_argument(
        "--user", "-u",
        help = "SAS Viya user name. Default value: environment variable SAS_USER_NAME.", 
        required = False,
        default = os.environ.get("SAS_USER_NAME"))
    parser.add_argument(
        "--password", "-p",
        help = "SAS Viya user password. " \
            "Default value: environment variable SAS_USER_PASSWORD.", 
        required = False,
        default = os.environ.get("SAS_USER_PASSWORD"))
    parser.add_argument(
        "--client-auth", "-ca",
        help = "Authentication client id and secret in the format of <client_id>:<client_secret>. " \
            "Default value: environment variable SAS_CLIENT_AUTH or sas.cli:", 
        required = False, 
        default = os.environ.get("SAS_CLIENT_AUTH") or f"{constants.CLIENT_ID_DEFAULT}:")
    parser.add_argument(
        "--auth-token", "-at",
        help = "Authentication token. " \
            "Default value: environment variable SAS_AUTH_TOKEN.", 
        required = False,
        default = os.environ.get("SAS_AUTH_TOKEN"))
    parser.add_argument(
        "--hide-progress", "--no-progress", 
        help = "Do not show progress in the console. If log-console option is enabled progress output will be turned off. " \
            "Default value: False.", 
        action = "store_true")
    parser.add_argument(
        "--log-console", 
        help = "Enable logging to console. If True, progress output will be turned off. " \
            "Default value: False.", 
        default = False,
        action = "store_true")
    parser.add_argument(
        "--log-level", 
        help = "Logging detailisation level. " \
            "Default value: INFO", 
        choices = ["DEBUG", "INFO", "WARNING", "ERROR"],
        default = "INFO")
    parser.add_argument(
        "--log-http-requests", 
        help = "Enable detailed logging of all http requests. " \
            "Default value: False.", 
        default = False,
        action = "store_true")
    parser.add_argument(
        "--log-format", 
        help = f"Log format record. " \
            f"Default value: {constants.LOG_FORMAT.replace('%', '%%')}", 
        default = constants.LOG_FORMAT)
    parser.add_argument(
        "--log-file", 
        help = "Enable logging to file. " \
            "Default value: True.", 
        default = True,
        action = "store_true")
    parser.add_argument(
        "--log-dir-path", 
        help = "Directory where log files will be stored. " \
            "Default value: os temporary directory")
    parser.add_argument(
        "--log-file-name", 
        help = "Log file name. If log-file is enabled and file name is not specified the " \
            "following name will be generated: batch_run_<timestamp>.log.")
    parser.add_argument(
        "--log-report", 
        help = "Enables creation log report file (Excel spreadsheet) by the end of the batch. " \
            "Default value: True.", 
        default = True,
        action = "store_true")
    parser.add_argument(
        "--log-report-dir-path", 
        help = "Directory where to store log report files. " \
            "Default value: os temporary directory.")
    parser.add_argument(
        "--log-report-file-name", 
        help = "Log report file name (Excel spreadsheet). " \
            "If log-report is enabled and file name is not specified report file name will be based on batch configuration file name plus timestamp.")
    parser.add_argument(
        "--tenant", 
        help = "The tenant value. " \
            "Default value: default", 
        required = False, 
        default = "default")
    parser.add_argument(
        "--disable-https-insecure-warning",
        help = "Disables log warning messages for requests made without certificate verification. " \
            "Default value: False",
        action = "store_true")
    parser.add_argument(
        "--max-parallel-processes", "-mpp",
        help = "Number of parallel processes. " \
            "Default value: number of os logical processors",
        default = os.cpu_count() or 1)
    parser.add_argument(
        "--solution", "-sln",
        help = "Risk Cirrus Solution ID (e.g. ECL, ST) to run against. " \
            "Default value: CORE",
        required = True, 
        default = "CORE")
    parser.add_argument(
        "--job-id",
        help = argparse.SUPPRESS,
        default = None)
    parser.add_argument(
        "--version", 
        help = "Display version of the Risk Cirrus Core Batch Utility",
        action = "version",
        version = constants.APP_FULL_NAME)
    
    args, _ = parser.parse_known_args(namespace = LaunchArguments())
    return args


def main():
    launch_args = usage()
    
    ctx = mp.get_context("spawn")
    di_container = compose(launch_args, ctx)
    
    # order matters: 
    # turn on logging -> monitor access -> parse config -> run -> end access monitoring -> end logging
    log_service : BatchLogService = di_container.resolve(BatchLogService)
    log_service.start(ctx)

    access_monitoring_service : AccessMonitoringService = di_container.resolve(AccessMonitoringService)
    access_monitoring_service.start(ctx)

    batch_config_service : BatchConfigService = di_container.resolve(BatchConfigService)
    batch_config = batch_config_service.parse(launch_args.file)
    batch_config_service.put_config_to_state(batch_config)
    
    batch_run_service : BatchRunService = di_container.resolve(BatchRunService)
    batch_run_service.run(ctx, batch_config)

    access_monitoring_service.stop()
    log_service.stop()


if __name__=="__main__":
    main()