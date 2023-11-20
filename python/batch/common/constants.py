
from datetime import datetime, date

"""This module defines project-level constants."""
# TODO : maybe move this to general config
URI_GET_TOKEN = "/SASLogon/oauth/token"

SOURCE_SYSTEM_CD_DEFAULT = "RCC"

OBJECT_TYPE_CYCLE = "Cycle"
OBJECT_TYPE_ANALYSIS_RUN = "AnalysisRun"

REST_PATH_CYCLE = "cycles"
REST_PATH_ANALYSIS_RUN = "analysisRuns"

SHEET_NAME_GENERAL = "general"
SHEET_NAME_CYCLES = "cycles"
SHEET_NAME_CYCLE_SCRIPT_PARAMETERS = "cycle_script_parameters"
SHEET_NAME_CYCLE_WORKFLOW = "cycle_workflow"
SHEET_NAME_ANALYSIS_RUNS = "analysis_runs"
SHEET_NAME_ANALYSIS_RUN_SCRIPT_PARAMETERS = "analysis_run_script_parameters"

SHEET_NAME_OBJECT_TYPE_RUNNABLE_MAP = {
    SHEET_NAME_CYCLES                               : OBJECT_TYPE_CYCLE,
    SHEET_NAME_ANALYSIS_RUNS                        : OBJECT_TYPE_ANALYSIS_RUN
}

SHEET_NAME_REST_PATH_MAP = {
    SHEET_NAME_CYCLES                               : REST_PATH_CYCLE,
    SHEET_NAME_CYCLE_SCRIPT_PARAMETERS              : REST_PATH_CYCLE,
    SHEET_NAME_CYCLE_WORKFLOW                       : REST_PATH_CYCLE,

    SHEET_NAME_ANALYSIS_RUNS                        : REST_PATH_ANALYSIS_RUN,
    SHEET_NAME_ANALYSIS_RUN_SCRIPT_PARAMETERS       : REST_PATH_ANALYSIS_RUN
}

SHEET_COLUMN_COMMENT = "#"
SHEET_COLUMN_CLASSIFICATION = "classification"

CHANGE_REASON_DEFAULT = "No change reason is required."

LINK_TYPE_ANALYSIS_RUN_CODE_LIBRARY = { "id": "analysisRun_codeLibrary", "ssc": "RCC" }
LINK_TYPE_ANALYSIS_RUN_CODE_LIBRARY_DEPENDENTS = { "id": "analysisRun_codeLibrary_dependents", "ssc": "RCC" }
LINK_TYPE_ANALYSIS_RUN_SCRIPT = { "id": "analysisRun_script", "ssc": "RCC" }
LINK_TYPE_ANALYSIS_RUN_CONFIGURATION_SET = { "id": "analysisRun_configurationSet", "ssc": "RCC" }
LINK_TYPE_ANALYSIS_RUN_JOB_OWNER = { "id": "analysisRun_jobOwner", "ssc": "RCC" }
LINK_TYPE_ANALYSIS_RUN_CYCLE = { "id": "analysisRun_cycle", "ssc": "RCC" }

LINK_TYPE_CYCLE_CODE_LIBRARY = { "id": "cycle_codeLibrary", "ssc": "RCC" }
LINK_TYPE_CYCLE_CONFIGURATION_SET = { "id": "cycle_configurationSet", "ssc": "RCC" }
LINK_TYPE_CYCLE_CONFIGURATION_SET = { "id": "cycle_configurationSet", "ssc": SOURCE_SYSTEM_CD_DEFAULT }
LINK_TYPE_CYCLE_CODE_LIBRARY = { "id": "cycle_codeLibrary", "ssc": SOURCE_SYSTEM_CD_DEFAULT }
LINK_TYPE_CYCLE_CODE_LIBRARY_DEPENDENTS = { "id": "cycle_codeLibrary_dependents", "ssc": SOURCE_SYSTEM_CD_DEFAULT }

LINK_TYPE_WORKFLOW_TEMPLATE_CYCLE = { "id": "wfTemplate_cycle", "ssc": SOURCE_SYSTEM_CD_DEFAULT }
LINK_TYPE_WORKFLOW_TEMPLATE_SCRIPT = {"id" : "wfTemplate_script", "ssc": SOURCE_SYSTEM_CD_DEFAULT}

LINK_TYPE_CODE_LIBRARY_DEPENDS_ON = { "id": "codeLibrary_dependsOn_codeLibrary", "ssc": "RCC" }

CIRRUS_OBJECT_ROOT_PROPERTIES = {
                                    "key": "string", 
                                    "objectId": "string", 
                                    "sourceSystemCd" : "string", 
                                    "name": "string", 
                                    "description": "string", 
                                    "createdBy": "string", 
                                    "creationTimeStamp": "timestamp",
                                    "modifiedBy": "string", 
                                    "modifiedTimeStamp": "timestamp",
                                    "changeReason": "string", 
                                    "createdInTag": "string"
                                }

CIRRUS_FIELD_TYPE_MAP = {
    "string": str,
    "jsonObject": object,
    "jsonArray": list,
    "html": str,
    "boolean": bool,
    "date": date,
    "timestamp": datetime,
    "double": float,
    "optionCd": str
}

HEADERS_CONTENT_TYPE_JSON = "application/json"
HEADERS_ACCEPT_TYPE_JSON_TEXT =  "application/json, text/plain, */*"
HEADERS_DEFAULT = { "content-type": HEADERS_CONTENT_TYPE_JSON, "accept": HEADERS_ACCEPT_TYPE_JSON_TEXT }
HEADERS_CONTENT_TYPE_WORKFLOW_JSON = "application/vnd.sas.business.objects.object.workflow.instance+json"
HEADERS_ACCEPT_TYPE_WORKFLOW_JSON = "application/vnd.sas.business.objects.object.workflow.instance+json, application/json, text/plain, */*"

LOG_FORMAT = "%(asctime)-25s - %(levelname)-6s - %(process)d - %(message)s"
FETCH_OBJECTS_LIMIT_COUNT = 1000

CLIENT_ID_RISK_CIRRUS_OBJECTS = "sas.riskCirrusObjects"
CLIENT_ID_RISK_CIRRUS_CORE = "sas.riskCirrusCore"
CLIENT_ID_DEFAULT = "sas.cli"