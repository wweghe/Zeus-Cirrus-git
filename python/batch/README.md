# risk-cirrus-core-batch

Risk Cirrus Core Batch Utility is a command line application that provides deletion, creation,
update and execution of Analysis Runs and/or Cycles in batch mode.

- **Install dependencies :**

  - Requires Python 3.11.2+
  - Please, see requirements.txt for package dependencies.

- **Code structure :**

  - console.py : Command line application.
      <pre>
      usage: Risk Cirrus Core Batch Utility [-h] --file FILE [--host HOST] [--auth-host AUTH_HOST] [--user USER] [--password PASSWORD]
      [--client-details CLIENT_DETAILS] [--auth-token AUTH_TOKEN] [--consul-token CONSUL_TOKEN]
      [--hide-progress] [--log-console] [--log-level {DEBUG,INFO,WARNING,ERROR}]
      [--log-format LOG_FORMAT] [--log-file] [--log-dir-path LOG_DIR_PATH]
      [--log-file-name LOG_FILE_NAME] [--log-report] [--log-report-dir-path LOG_REPORT_DIR_PATH]
      [--log-report-file-name LOG_REPORT_FILE_NAME] [--tenant TENANT]
      [--disable-https-insecure-warning] [--max-parallel-processes MAX_PARALLEL_PROCESSES]
    
      Options:
        -h, --help            show this help message and exit

        --file FILE, -f FILE    Configuration file path (Excel spreadsheet)

        --host HOST, --url HOST, -s HOST
                                SAS Viya host url for calling application endpoints, e.g. http(s)://test.sas.com:80. If host is not
                                specified, it is assumed that the application is running inside a SAS Viya cluster and will derive host name based on the cluster environment variables.

        --auth-host AUTH_HOST, --auth-url AUTH_HOST, -ah AUTH_HOST
                                Third party authentication host url for getting authentication token, e.g. http(s)://test.sas.com:80. If authentication url is not provided, application will derive the url based on host option.

        --user USER, -u USER  SAS Viya user name. Default value: environment variable SAS_USER_NAME.

        --password PASSWORD, -p PASSWORD
                                SAS Viya user password. 
                                Default value: environment variable SAS_USER_PASSWORD.

        --client-details CLIENT_DETAILS, -cd CLIENT_DETAILS
                                Authentication client id and secret in the format of <client_id>:<client_secret>. 
                                Default value: environment variable SAS_CLIENT_DETAILS or sas.cli:
                                
        --auth-token AUTH_TOKEN, -at AUTH_TOKEN
                                Authentication token. 
                                Default value: environment variable SAS_AUTH_TOKEN.

        --hide-progress, --no-progress
                                Do not show progress in the console. If log-console option is enabled progress output will be turned off.
                                Default value: False.

        --log-console           Enable logging to console. If True, progress output will be turned off. 
                                Default value: False.

        --log-level {DEBUG,INFO,WARNING,ERROR}
                                Logging detailisation level. Default value: INFO

        --log-format LOG_FORMAT
                                Log format record. 
                                Default value: %(asctime)-25s - %(levelname)-6s - %(process)d - %(message)s

        --log-file              Enable logging to file. 
                                Default value: True.

        --log-dir-path LOG_DIR_PATH
                                Directory where log files will be stored. 
                                Default value: os temporary directory

        --log-file-name LOG_FILE_NAME
                                Log file name. If log-file is enabled and file name is not specified the following name will be generated: batch_run_<timestamp>.log. 
                                Default value: None.

        --log-report            Enables creation log report file (Excel spreadsheet) by the end of the batch. 
                                Default value: True.

        --log-report-dir-path LOG_REPORT_DIR_PATH
                                Directory where to store log report files. 
                                Default value: os temporary directory.

        --log-report-file-name LOG_REPORT_FILE_NAME
                                Log report file name (Excel spreadsheet). If log-report is enabled and file name is not specified report file name will be based on batch configuration file name plus timestamp. 

        --tenant TENANT         The tenant value. 
                                Default value: default

        --disable-https-insecure-warning
                                Disables log warning messages for requests made without certificate verification. 
                                Default value: False

        --max-parallel-processes MAX_PARALLEL_PROCESSES, -mpp MAX_PARALLEL_PROCESSES
                                Number of parallel processes. 
                                Default value: number of os logical processors

        --solution SOLUTION, -sln SOLUTION
                                Risk Cirrus Solution ID (e.g. ECL, ST) to run against. Default value: CORE
      </pre>
  ## Codebase
  - console.py : console application
  - requirements.txt : python package dependecies
  - common : cross-cutting functions, helpers and utilities
    - compose : application composition root
    - constants : the title speaks for itself :)
    - container : poor-man's dependency injection container
    - errors : exception classes
    - logger : logging decorator, logging filters
    - utils : utility functions
  - domain : data structures and state management
    - analysis_run_config : analysis run specific batch run configuration
    - base_config : abstract classes for different types of configurations: runnable (e.g. cycle /
      analysis run), parameters (e.g. script parameters, cycle workflow)
    - batch_config : holds all batch run configuration
    - batch_run_action_enum : available batch actions for cycle / analysis run
    - cirrus_object : representation of the cirrus object, holds some helper functions to work with
      fields / attributes
    - cycle_config : cycle specific batch run configuration
    - diagram_node_status_enum : statuses for workflow diagram
    - general_config : cross cutting configuration for batch_run (e.g. solution name, wait time...)
    - launch_arguments : command line options / arguments
    - script_parameter_config : configuration for script execution parameters for both cycle and
      analysis run
    - state : implements state structures shared accross processes
    - workflow_config : cycle workflow specific batch run configuration
  - repositories : data access layer to cirrus objects or any other viya objects
    - base_repository : abstract class for all repositories
    - cirrus_object_repository : abstract class for all cirrus object repositories
    - generic_repository : generic purpose repository to access viya resource, used for parameter
      query expressions
  - services : business logic layer
    - analysis_run_service : batch run logic for analysis runs
    - batch_config_service : parses and caches configuration file (xlsx) to BatchConfig instance
    - batch_run_callable : helper class for multiprocessing / parallel execution
    - batch_run_progress_service: reports batch run progress to console (stdout)
    - batch_run_report_service : produces xlsx report based on the batch run results
    - batch_run_service : main entry point for executing batch run
    - cycle_service : batch run logic for cycles
    - identifier_service : various operations with id ssc object keys
    - link_instance_service : various operations with linked objects
    - object_registration_service : provides and caches cirrus object registration information
    - repository_factory : factory for repositories, used by script parameter expressions
    - request_service : helper class for all http(s) requests (wrapper over requests package)
    - script_execution_service : script execution and wait logic
    - solution_service : risk cirrus solution configuration access and state logic
    - workflow_diagram_service : workflow diagram update logic
  - sample : sample batch run configuration files (examples)
    - core.xlsx : batch run configuration sample file to end-to-end CORE cycle
    - ecl.xlsx : batch run configuration sample file to end-to-end ECL cycle
  - tests : unit tests

## Configuration file structure

| Sheet name                     | Description                                                |
| :----------------------------- | :--------------------------------------------------------- |
| general                        | Holds cross cutting properties for the batch run.          |
| analysis_runs                  | Holds analysis run records to delete, create, update, run. |
| analysis_run_script_parameters | Contains script parameters for analsys runs                |
| cycles                         | Holds cycle  records to delete, create, update, run.       |
| cycle_script_parameters        | Contains script parameters for cycles.                     |
| cycle_workflow                 | Contains cycle workflows to run.                           |

### General sheet 
| Column    | Description             |
| :-------- | :---------------------- |
| parameter | Name of the paramater.  |
| value     | Value of the parameter. |

#### General parameters
| Parameter             | Description                                                                                    |
| :-------------------- | :--------------------------------------------------------------------------------------------- |
| workflow_wait_sleep   | Waiting time in seconds before checking the status of the workflow step for each cycle record. |
| workflow_wait_timeout | Time limit in seconds to wait for the workflow step to finish for each cycle record.           |
| script_wait_sleep     | Waiting time in seconds before checking the status of each executing script.                   |
| script_wait_timeout   | Time limit in seconds to wait for each script to finish.                                       |

### analysis_runs sheet / cycles sheet 
| Column               | Description                                                                                                                                                                                                                                                                           |
| :------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| #                    | Comment the record. If record is marked with '#' sign, it will be will be ignored.                                                                                                                                                                                                    |
| objectId             | ID of the object. Mandatory. 'objectId:sourceSystemCd' key is used to identify the record within the solution. 'objectId:sourceSystemCd:_action' key is used to identify the record within the batch configuration.                                                                   |
| sourceSystemCd       | Source system code of the object. Mandatory.                                                                                                                                                                                                                                          |
| _action              | Action to perform on the object: DELETE, CREATE, UPDATE, RUN.                                                                                                                                                                                                                         |
|                      | DELETE - silently deletes the record. If record does not exist, it will not throw an error.                                                                                                                                                                                           |
|                      | CREATE - creates a new record. If record already exists, it will record an error.                                                                                                                                                                                                     |
|                      | UPDATE - updates existing record. If record does not exist, it will record an error.                                                                                                                                                                                                  |
|                      | RUN - Creates the records if does not exist or updates existing record and executes the script. If script execution fails, batch will throw an error.                                                                                                                                 |
| _is_parallel         | TRUE / FALSE flag that indicates to perform _action in parallel mode or sequentially. Records that are NOT marked as _is_parallel run first. Default value is FALSE.                                                                                                                  |
| classification       | JSON array that holds dimensions to be assigned to the object with the following structure: [{"sourceSystemCd": "RCC", "namedTreeId": "entity_id", "path": "Organization.SASBank.SASBank_1"}]                                                                                         |
| <i>field name</i>    | Object field  / custom field that needs to be updated. Column name should contain the Name of the field and needs to be spelled exactly as it was registered within the solution. If the field name for the object does not exist, this column will be ignored.                       |
| <i>link instance</i> | Object link instance that needs to be created. Column name should be in the format of <link type id>:<link type source system code (ssc)> or <link type id> (if link type ssc is omitted, default 'RCC' value will be used). If link type does not exist, the column will be ignored. |

### analysis_run_script_parameters sheet 
| Column                  | Description                                                                                                                                                                                                                |
| :---------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| #                       | Comment the record. If record is marked with '#' sign, it will be will be ignored.                                                                                                                                         |
| objectId                | ID of the cirrus object. Mandatory. 'objectId:sourceSystemCd' key is used to identify the record within the solution. 'objectId:sourceSystemCd:_action' key is used to identify the record within the batch configuration. |
|                         | For script parameters sheet _action is considered as RUN.                                                                                                                                                                  |
| sourceSystemCd          | Source system code of the object. Mandatory.                                                                                                                                                                               |
| parameter_name          | Name of the individual script parameter. Name should be the same as in the ui of script parameters object builder.                                                                                                         |
| parameter_value         | Any value that the script parameter ui component expects to load / display parameter correctly in the solution.                                                                                                            |
|                         | For example, if the paramater related to a component that expects json or json array structure, this column should provide the value in the same format / structure.                                                       |
| parameter_expression    | JSON structure that allows to construct the parameter_value during batch runtime. If parameter_value is not empty, this column will be ignored.                                                                            |
|                         | * Please, refer to the parameter_expression section for structure description.                                                                                                                                             |
| parent_parameter        | Name of the parent parameter that is related to EmbeddedBuilderWithObjectSelector component (EBPWithObjectSelector / EBPwOS). EBPwOS component allows to load child or nested script parameters.                           |
| parent_field_parameters | Name of the field, where child / nested parameters are stored within a CirrusObject represented by the parent EBPwOS paremeter. Default value is "parameters".                                                             |

### cycle_script_parameters sheet 
| Column                  | Description                                                                                                                                                                                                                |
| :---------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| #                       | Comment the record. If record is marked with '#' sign, it will be will be ignored.                                                                                                                                         |
| objectId                | ID of the cirrus object. Mandatory. 'objectId:sourceSystemCd' key is used to identify the record within the solution. 'objectId:sourceSystemCd:_action' key is used to identify the record within the batch configuration. |
|                         | For script parameters sheet _action is considered as RUN.                                                                                                                                                                  |
| sourceSystemCd          | Source system code of the object. Mandatory.                                                                                                                                                                               |
| task_name               | Name of the workflow task that executes the script                                                                                                                                                                         |
| parameter_set           | Name of the parameter set that allows to use same parameter names / values for different workflow step iterations.                                                                                                         |
| parameter_name          | Name of the individual script parameter. Name should be the same as in the ui of script parameters object builder.                                                                                                         |
| parameter_value         | Any value that the script parameter ui component expects to load / display parameter correctly in the solution.                                                                                                            |
|                         | For example, if the paramater related to a component that expects json or json array structure, this column should provide the value in the same format / structure.                                                       |
| parameter_expression    | JSON structure that allows to construct the parameter_value during batch runtime. If parameter_value is not empty, this column will be ignored.                                                                            |
|                         | Please, refer to the 'Parameter expression structure' section for the expression syntax description.                                                                                                                       |
| parent_parameter        | Name of the parent parameter that is only related to EmbeddedBuilderWithObjectSelector component (EBPWithObjectSelector / EBPwOS) parameter. EBPwOS component allows to load child or nested script parameters.            |
| parent_field_parameters | Name of the field, where child / nested parameters are stored within a CirrusObject represented by the parent EBPwOS paremeter. Default value is "parameters".                                                             |

### cycle_workflow sheet 
| Column                | Description                                                                                                                                                                                                                |
| :-------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| #                     | Comment the record. If record is marked with '#' sign, it will be will be ignored.                                                                                                                                         |
| objectId              | ID of the cirrus object. Mandatory. 'objectId:sourceSystemCd' key is used to identify the record within the solution. 'objectId:sourceSystemCd:_action' key is used to identify the record within the batch configuration. |
|                       | For script parameters sheet _action is considered as RUN.                                                                                                                                                                  |
| sourceSystemCd        | Source system code of the object. Mandatory.                                                                                                                                                                               |
| task_name             | Name of the workflow task. The name should be the same as it is defined in the workflow definition.                                                                                                                        |
| transition_name       | Name of the workflow task transition. The name should be the same as it is defined in the workflow definition.                                                                                                             |
| error_transition_name | Fallback transition name of the workflow task, in case if the first transition fails (throws an error). The name should be the same as it is defined in the workflow definition.                                           |
| parameter_set         | Name of the parameter set that allows to use same parameter names / values for different workflow step iterations.                                                                                                         |
| iteration             | Workflow step iteration that allows to execute the same workflow routes.                                                                                                                                                   |

### Parameter expression structure
Parameter expression allows to resolve parameter values during batch runtime.
It should hold the following structure in the following JSON format:<br>
Simple example:
```
{
	"user":{
		"query": {
			"restPath": "users",
			"key": "@currentUser"
		},
		"result": "CASUSER({user.id})"
	}
}
```
Equivalent:
```
{
	"user":{
		"query": {
			"restPath": "/identities/users",
			"key": "@currentUser"
		},
		"result": "CASUSER({user.id})"
	}
}

```
More complex example:
```
{
	"cycle_query":{
		"query": {
			"restPath": "cycles",
			"start": 0,
			"limit": 1,
			"filter": "and(eq(sourceSystemCd,'ECL'),eq(objectId,'aaa'))"
		},
		"result": {
			"analysisData_query": {
				"query":{
					"restPath": "analysisData",
					"sortBy": "creationTimeStamp:descending",
					"start": 0,
					"limit": 1,
					"filter": "and(eq(sourceSystemCd,'ECL'),eq(dataCategoryCd, 'PORTFOLIO'),hasObjectLinkTo('RCC','analysisData_cycle','{cycle_query[0].key}',1))"
				},
				"result": "{{ \"type\": \"cirrusObjectArray\", \"restPath\": \"analysisData\", \"value\": [{{ \"objectId\": \"{analysisData_query[0].objectId}\", \"sourceSystemCd\": \"{analysisData_query[0].sourceSystemCd}\" }}] }}"
			}
		}
	}
}
```
| Element / path                               | Description                                                                                                                                                         |
| :------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| cycle_query                                  | Represents query variable name that can be referenced to form output                                                                                                |
| analysisData_query                           |                                                                                                                                                                     |
| cycle_query.query                            | The body of the query that should take place, holds query parameters.                                                                                               |
| cycle_query.query.restPath                   | Holds rest path that should be queried. For example, "cycles" is a shorthand to the '/riskCirrusObjects/objects/cycles' endpoint.                                   |
| cycle_query.query.start                      | Starting element for the query.                                                                                                                                     |
| cycle_query.query.limit                      | Limit number for the elements for the query.                                                                                                                        |
| cycle_query.query.filter                     | Filter expression for the query.                                                                                                                                    |
| cycle_query.result.analysisData_query.filter | Can contain expression like '{cycle_query[0].key}' to get value(s) from query results, for example, take 'key' attribute value from 'cycle_query' array first item. |
|                                              | Upper level query variables are accessable to the nested queries.                                                                                                   |
| cycle_query.result                           | The body of result that should be formed. The result can holds nested query or the final output.                                                                    |
| cycle_query.result.analysisData_query.result | In case of JSON in the final output, double quotes (") should be wraped as escape character (\").                                                                   |