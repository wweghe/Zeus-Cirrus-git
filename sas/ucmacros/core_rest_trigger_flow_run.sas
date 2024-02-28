/*
 Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor core_rest_trigger_flow_run

   \brief   Run a specific flow definition in SAS Process Orchestration

   \param [in] host (optional) Host url, including the protocol
   \param [in] server Name of the Web Application Server that provides the REST service (Default: riskCirrusObjects)
   \param [in] solution Solution identifier (Source system code) for Cirrus Core content packages (Default: currently blank)
   \param [in] port (optional) Server port
   \param [in] logonHost (Optional) Host/IP of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app host/ip is the same as the host/ip in the url parameter
   \param [in] logonPort (Optional) Port of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app port is the same as the port in the url parameter
   \param [in] username (optional) Username credentials
   \param [in] password (optional) Password credentials: it can be plain text or SAS-Encoded (it will be masked during execution).
   \param [in] authMethod: Authentication method (accepted values: BEARER). (Default: BEARER).
   \param [in] client_id The client id registered with the Viya authentication server. If blank, the internal SAS client id is used (only if GRANT_TYPE = password).
   \param [in] client_secret The secret associated with the client id.
   \param [in] analysisRunKey (Optional) The key of the analysis run this macro is being called in. If none is provided, no links will be created. (Default: &__CORE_AR_KEY__.).
   \param [in] analysisRunFolderPath (Optional) The PVC directory of the analysis run this macro is being called in. (Default: &__CORE_AR_DIR__.).
   \param [in] flow_id Id of the flow definition that is executed with this REST request. If blank, the flow_name is used.
   \param [in] flow_name Name of the flow definition that is executed with this REST request. Used only when the flow_id is missing.
   \param [in] flow_namespace (optional) Namespace of the flow definition that is executed with this REST request. Used only when the flow_id is missing and flow_name has been provided.
   \param [in] flow_run_id (optional) Id of the flow run that is created with this REST request. If no Id is specified, it will be randomly generated by the macro.
   \param [in] flow_run_name (optional) Name of the flow run that is created with this REST request. If no name is specified, it will be automatically generated by SAS Process Orchestration.
   \param [in] ds_in_flow_parameters (optional) Table containing the flow run parameters where each row rapresents a key-value pair. Required columns: name, value. Optional column: description.
   \param [in] wait_flg Flag (Y/N). If Y the macro will wait for the flow run completion (Default: Y)
   \param [in] pollInterval Number of seconds to wait in between consecutive flow run status requests to the server (Default: 1)
   \param [in] maxWait Maximum amount of seconds the macro will wait for the execution to complete before giving up (Default: 3600 -> 1 hour)
   \param [in] timeoutSeverity Controls the severity of the log message used to notify the user that the maximum waiting time has been reached before the execution has been completed. Valid values: NOTE, WARNING, ERROR. (Default: ERROR)
   \param [in] debug True/False. If True, debugging informations are printed to the log (Default: false)
   \param [in] logOptions Logging options (i.e. mprint mlogic symbolgen ...)
   \param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y)
   \param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y)
   \param [out] outFlowRunId Name of the output macro variable containing the id of the flow run that has been created (Default: runId)
   \param [out] outFlowRunStatus Name of the output macro variable containing the flow run execution status (Default: runStatus)
   \param [out] outVarToken Name of the output macro variable which will contain the access token (Default: accessToken)
   \param [out] outSuccess Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess)
   \param [out] outResponseStatus Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus)

   \details
   This macro sends a POST request to <b><i>\<host\>:\<port\>/processOrchestration/flows/&flow_id./runs</i></b> to trigger a flow run. \n
   If the <i>wait_flg</i> keyword parameter is Y, this macro also sends intermittent GET requests to <b><i>\<host\>:\<port\>/processOrchestration/flowRuns/&flow_run_id.</i></b> until the execution is complete. \n
   See \link core_rest_request.sas \endlink for details about how to send GET requests and parse the response.


   <b>Example:</b>

   1) Set up the environment (set SASAUTOS and required LUA libraries).  Assumes the spre folder is under /riskcirruscore/core/code_libraries/release-core-{cadence-version}
   \code
      %let cadence_version=2023.10;
      %let core_root_path=/riskcirruscore/core/code_libraries/release-core-&cadence_version.;
      option insert = (
         SASAUTOS = (
            "&core_root_path./spre/sas/ucmacros"
            )
         );
      filename LUAPATH ("&core_root_path./spre/lua");
   \endcode

   2) Execute a given flow definition and waits up to 1h for the run to complete.
   \code
      %let accessToken=;
      %core_rest_trigger_flow_run(flow_id = 1a5f8b78-f3c3-4a04-833b-3240def82058
                                 , ds_in_flow_parameters = work.flow_parameters
                                 , wait_flg = Y
                                 , pollInterval = 1
                                 , maxWait = 3600
                                 , timeoutSeverity = ERROR
                                 , outVarToken =accessToken
                                 , outSuccess = httpSuccess
                                 , outResponseStatus = responseStatus
                                 );
      %put &=accessToken;
      %put &=httpSuccess;
      %put &=responseStatus;
   \endcode


   The structure of the <b><i>DS_IN_FLOW_PARAMETERS</i></b> table is as follows:

   | name                  | value                    | description                                                            |
   |-----------------------|--------------------------|------------------------------------------------------------------------|
   | CAS_DataSource        | cas-shared-default       | Default CAS Data Source                                                |
   | CAS_Library           | Public                   | Default CAS Library                                                    |
   | LOG_LEVEL             | 1                        | Logging Level: 0-4 (0: ERROR, 1: WARNING, 2: INFO, 3: DEBUG, 4: TRACE) |


   \ingroup coreRestUtils

   \author  SAS Institute Inc.
   \date    2023
*/
%macro core_rest_trigger_flow_run(host =
                                 , server = processOrchestration
                                 , solution =
                                 , port =
                                 , logonHost =
                                 , logonPort =
                                 , username =
                                 , password =
                                 , authMethod = bearer
                                 , client_id =
                                 , client_secret =
                                 , analysisRunKey =
                                 , analysisRunFolderPath =
                                 , flow_id =
                                 , flow_name =
                                 , flow_namespace =
                                 , flow_run_id =
                                 , flow_run_name =
                                 , ds_in_flow_parameters =
                                 , wait_flg = Y
                                 , pollInterval = 1
                                 , maxWait = 3600
                                 , timeoutSeverity = ERROR
                                 , outFlowRunId = runId
                                 , outFlowRunStatus = runStatus
                                 , outVarToken =accessToken
                                 , outSuccess = httpSuccess
                                 , outResponseStatus = responseStatus
                                 , debug = false
                                 , logOptions =
                                 , restartLUA = Y
                                 , clearCache = Y
                                 )  / minoperator;

   %local
      oldLogOptions
      requestUrl
      tmplib
      tmp_flow_def
      fdef_filter
      _err_msg_1_
      _err_msg_2_
      flow_def_count
      fbody
      blank_key_count
      user_flow_parameters_flg
      tmp_system_flow_parameters
      keep_option
      fresp
      libref
      root
      resp_message
      flow_run_id_created
      keyPrefixAnalysisRun
      keyPrefixFlowRun
      tmp_link_instance
      tmp_flow_run_state
      execution_status
      current_dttm
      start_dttm
      rc
      tmp_flow_run
      tmp_task_details
      partitioned_task_id_list
      task_id_list
      TotLogRequests
      i
      k
      flowTaskId
   ;

   /* Set the required log options */
   %if(%length(&logOptions.)) %then
      options &logOptions.;
   ;

   /* Get the current value of mlogic and symbolgen options */
   %let oldLogOptions = %sysfunc(getoption(mlogic)) %sysfunc(getoption(symbolgen));

   /* Make sure output variable outRunId is set. */
   %if %sysevalf(%superq(outFlowRunId) =, boolean) %then
      %let outFlowRunId = runId;

   /* Declare output variable outFlowRunId as global if it does not exist */
   %if(not %symexist(&outFlowRunId.)) %then
      %global &outFlowRunId.;

   /* Make sure output variable outFlowRunStatus is set. */
   %if %sysevalf(%superq(outFlowRunStatus) =, boolean) %then
      %let outFlowRunStatus = runStatus;

   /* Declare output variable outFlowRunStatus as global if it does not exist */
   %if(not %symexist(&outFlowRunStatus.)) %then
      %global &outFlowRunStatus.;

   /* Initialize output variable */
   %let &outFlowRunStatus. = not started;
   
   /* Validate input parameter solution */
   %if (%sysevalf(%superq(analysisRunKey) ne, boolean) and %sysevalf(%superq(solution) eq, boolean)) %then %do;
      %put ERROR: solution is required.;
      %abort;
   %end;
   
   /* Set the base request URL */
   %core_set_base_url(host=&host, server=&server., port=&port.);
   %let requestUrl = &baseUrl./&server./flows;
   
   %let tmplib = work;

   /* Validate input parameter flow_id */
   %if(%sysevalf(%superq(flow_id) ne, boolean)) %then %do;
   
      %let tmp_flow_def = &tmplib.._tmp_flow_def_;
      
      /* Send the REST request to check if provided flow definition exists */
      %core_rest_get_flow_definitions(host = &host.
                                    , server = processOrchestration
                                    , solution = &solution.
                                    , port = &port.
                                    , logonHost = &logonHost.
                                    , logonPort = &logonPort.
                                    , username = &username.
                                    , password = &password.
                                    , authMethod = bearer
                                    , client_id = &client_id.
                                    , client_secret = &client_secret.
                                    , flow_id = &flow_id.
                                    , outds = &tmp_flow_def.
                                    , debug = &debug.
                                    , logOptions = &oldLogOptions.
                                    , restartLUA = &restartLUA.
                                    , clearCache = &clearCache.
                                    );
                                    
      %if (not %rsk_dsexist(&tmp_flow_def.)) %then %do;
         %put ERROR: Could not find any flow definition with id "&flow_id.".;
         %abort;
      %end;
      %else %if (%rsk_attrn(&tmp_flow_def., nlobs) eq 0) %then %do;
         %put ERROR: Could not find any flow definition with id "&flow_id.".;
         %abort;
      %end;
      
      /* Get the flow name */
      data _null_;
         set &tmp_flow_def.;
         call symputx("flow_name", name, "L");
      run;
      
      %if("%upcase(&debug.)" ne "TRUE") %then %do;
         /* Delete temporary table if it exists */
         %if (%rsk_dsexist(&tmp_flow_def.)) %then %do;
            proc sql;
               drop table &tmp_flow_def.;
            quit;
         %end;
      %end;
      
   %end; /* %if(%sysevalf(%superq(flow_id) ne, boolean)) %then %do; */
   %else %do; /* the flow_id was not provided */
   
      /* Validate input parameter flow_name */
      %if(%sysevalf(%superq(flow_name) ne, boolean)) %then %do;
      
         %let fdef_filter = eq(name,"&flow_name.");
         %let _err_msg_1_ = ERROR: Could not find any flow definition with name "&flow_name.".;
         %let _err_msg_2_ = ERROR: There is more than one Flow Definition matching the same name "&flow_name.".;
      
         %if(%sysevalf(%superq(flow_namespace) ne, boolean)) %then %do;
            %let fdef_filter = and(eq(name,"&flow_name."),eq(namespace,"&flow_namespace."));
            %let _err_msg_1_ = ERROR: Could not find any flow definition with name "&flow_name." and namespace "&flow_namespace.".;
            %let _err_msg_2_ = ERROR: There is more than one Flow Definition matching the same name "&flow_name." and namespace "&flow_namespace.".;
         %end;
         
         %let tmp_flow_def = &tmplib.._tmp_flow_def_;
         
         /* Send the REST request to check if we can get the flow id from the flow name */
         %core_rest_get_flow_definitions(host = &host.
                                       , server = processOrchestration
                                       , solution = &solution.
                                       , port = &port.
                                       , logonHost = &logonHost.
                                       , logonPort = &logonPort.
                                       , username = &username.
                                       , password = &password.
                                       , authMethod = bearer
                                       , client_id = &client_id.
                                       , client_secret = &client_secret.
                                       , filter = &fdef_filter.
                                       , outds = &tmp_flow_def.
                                       , debug = &debug.
                                       , logOptions = &oldLogOptions.
                                       , restartLUA = &restartLUA.
                                       , clearCache = &clearCache.
                                       );
                                       
         %if (not %rsk_dsexist(&tmp_flow_def.)) %then %do;
            %put &_err_msg_1_.;
            %abort;
         %end;
         %else %if (%rsk_attrn(&tmp_flow_def., nlobs) eq 0) %then %do;
            %put &_err_msg_1_.;
            %abort;
         %end;
         
         /* Get the flow id */
         data _null_;
            set &tmp_flow_def. end=eof;
            call symputx("flow_id", id, "L");
            if eof then call symputx("flow_def_count", _N_, "L");
         run;
         
         %if (&flow_def_count. gt 1) %then %do;
            %put &_err_msg_2_.;
            %abort;
         %end;
         
         %if("%upcase(&debug.)" ne "TRUE") %then %do;
            /* Delete temporary table if it exists */
            %if (%rsk_dsexist(&tmp_flow_def.)) %then %do;
               proc sql;
                  drop table &tmp_flow_def.;
               quit;
            %end;
         %end;
         
      %end; /* %if(%sysevalf(%superq(flow_name) ne, boolean)) %then %do; */
      %else %do; /* both flow_id and flow_name were not provided */
         %put ERROR: Either the flow_id or flow_name parameter must be provided. You must specify the flow definition to execute.;
         %abort;
      %end;
      
   %end; /* end flow_id and flow_name validation */
   
   /* Set the request URL */
   %let requestUrl = &requestUrl./&flow_id./runs;
   
   /* Initialize user parameters flag */
   %let user_flow_parameters_flg = N;
   
   /* Validate input dataset ds_in_flow_parameters */
   %if(%sysevalf(%superq(ds_in_flow_parameters) ne, boolean)) %then %do;
      %if (%rsk_dsexist(&ds_in_flow_parameters.)) %then %do;
         
         /*************************************************/
         /* name and value columns are required           */
         /*************************************************/
         %if not %rsk_varexist(&ds_in_flow_parameters., name) %then %do;
            %put ERROR: Required variable "name" is missing in data set "&ds_in_flow_parameters.".;
            %abort;
         %end;
      
         %if not %rsk_varexist(&ds_in_flow_parameters., value) %then %do;
            %put ERROR: Required variable "value" is missing in data set "&ds_in_flow_parameters.".;
            %abort;
         %end;
         
         %let keep_option = %str(keep=name value);
         
         /* the description of the parameter is optional */
         %if %rsk_varexist(&ds_in_flow_parameters., description) %then %do;
            %let keep_option = %str(keep=name value description);
         %end;
         
         /*************************************************/
         /* the key name cannot be blank                  */
         /*************************************************/
         %let blank_key_count = 0;
         
         proc sql noprint;
            select count(*) into :blank_key_count
            from &ds_in_flow_parameters.
            where name is null
            ;
         quit;
         %let blank_key_count = &blank_key_count.;
         
         %if (&blank_key_count. ge 1) %then %do;
            %put ERROR: At least one key name is blank. Please verify the parameters data set "&ds_in_flow_parameters.".;
            %abort;
         %end;
         
         /* The flow run parameters table was provided */
         %let user_flow_parameters_flg = Y;
      %end;
      %else %do;
         %put ERROR: Could not find any parameters dataset with name "&ds_in_flow_parameters.".;
         %abort;
      %end;
   %end;
   
   /* Inject system variable(s) into flow run parameters table */
   %if(%sysevalf(%superq(analysisRunKey) ne, boolean) or %sysevalf(%superq(analysisRunFolderPath) ne, boolean)) %then %do;
      
      %let tmp_system_flow_parameters = &tmplib.._tmp_system_flow_parameters_;
      
      data &tmp_system_flow_parameters.;
         length
            name $256.
            value $4096.
            description $512.
         ;
         %if(%sysevalf(%superq(analysisRunKey) ne, boolean)) %then %do;
            name = "__CORE_AR_KEY__";
            value = "&analysisRunKey.";
            description = "AnalysisRun Key";
            output;
         %end;
         %if(%sysevalf(%superq(analysisRunFolderPath) ne, boolean)) %then %do;
            name = "__CORE_AR_DIR__";
            value = "&analysisRunFolderPath.";
            description = "AnalysisRun PVC directory";
            output;
         %end;
      run;
      
      %if("&user_flow_parameters_flg." = "Y") %then %do;
         /* Append data */
         %rsk_append(base = &ds_in_flow_parameters.
            , data = &tmp_system_flow_parameters.
            , length_selection = longest);
      %end;
      %else %do;
         %let ds_in_flow_parameters = &tmp_system_flow_parameters;
      %end;
      
   %end;
   
   /* Create a unique (enough) flow run id, if not provided */
   %if(%sysevalf(%superq(flow_run_id) =, boolean)) %then
      %let flow_run_id = %sysfunc(uuidgen());
   
   /* Get a Unique fileref and assign a temp file to it */
   %let fbody = %rsk_get_unique_ref(prefix = body, engine = temp);
   
   /* Build the request body to trigger the flow run */
   proc json out=&fbody. pretty;
      
      write open object;
      
         /* Add flow run name */
         %if(%sysevalf(%superq(flow_run_name) ne, boolean)) %then %do;
            write values "name" "%superq(flow_run_name)";
         %end;
      
         /* Add flow run id */
         %if(%sysevalf(%superq(flow_run_id) ne, boolean)) %then %do;
            write values "id" "&flow_run_id.";
         %end;
         
         /* Add parameters */
         %if(%sysevalf(%superq(ds_in_flow_parameters) ne, boolean)) %then %do;
            write values "parameters";
               write open array;
                  export &ds_in_flow_parameters.(&keep_option.) / nosastags;
               write close;
         %end;
      
      write close;
      
   run;
   
   %if("%upcase(&debug.)" = "TRUE") %then %do;
      /* Print the body of the POST request */
      %rsk_print_file(file = %sysfunc(pathname(&fbody.))
                     , title = Body of the POST request sent to the Server:
                     , logSeverity = WARNING
                     );
   %end;
   
   /* Get a Unique fileref and assign a temp file to it */
   %let fresp = %rsk_get_unique_ref(prefix = resp, engine = temp);
   
   /* Temporary disable mlogic and symbolgen options to avoid printing of userid/pwd to the log */
   option nomlogic nosymbolgen;
   /* Send the REST request to trigger the flow run */
   %core_rest_request(url = &requestUrl.
                     , method = POST
                     , logonHost = &logonHost.
                     , logonPort = &logonPort.
                     , username = &username.
                     , password = &password.
                     , authMethod = &authMethod.
                     , headerIn = Accept:application/json
                     , body = &fbody.
                     , contentType = application/json
                     , parser =
                     , outds =
                     , fout = &fresp.
                     , printResponse = N
                     , outVarToken = &outVarToken.
                     , outSuccess = &outSuccess.
                     , outResponseStatus = &outResponseStatus.
                     , debug = &debug.
                     , logOptions = &oldLogOptions.
                     , restartLUA = &restartLUA.
                     , clearCache = &clearCache.
                     );

   /* Assign libref to parse the JSON response */
   %let libref = %rsk_get_unique_ref(type = lib, engine = JSON, args = fileref = &fresp.);

   %let root = &libref..root;
   
   /* Exit in case of errors */
   %if(not &&&outSuccess..) %then %do;
      %put ERROR: The request to execute the flow definition "&flow_id." was not successful.;
      %if(%upcase(&debug.) eq TRUE) %then %do;
         data _null_;
            set &root.(keep=message);
            call symputx("resp_message", message, "L");
         run;
         %put ERROR: %superq(resp_message);
      %end;
      %abort;
   %end;
   
   /* Check the response */
   data _null_;
      set &root.;
      call symputx("flow_run_id_created", id, "L");
      call symputx("execution_status", state, "L");
   run;
      
   /* Assign the output variable */
   %let &outFlowRunId. = &flow_run_id_created.;
   
   /* Update the output variable */
   %let &outFlowRunStatus. = &execution_status.;
      
   /* This should never happen, but just in case.. */
   %if "&flow_run_id." ne "&flow_run_id_created." %then %do;
      %put ERROR: Something went wrong when trying to execute the flow definition "&flow_id." using the flow run id "&flow_run_id.".;
      %abort;
   %end;
   
   /* Clear out the temporary files */
   filename &fbody. clear;
   filename &fresp. clear;
   libname &libref. clear;
   
   /* Create link from analysis run to flow run */
   %if %sysevalf(%superq(analysisRunKey)^=,boolean) %then %do;
   
      /* Get the keys required to build a link */
      %let keyPrefixAnalysisRun = %substr(&analysisRunKey., 1, 7);
      %let keyPrefixFlowRun = %substr(&flow_run_id., 1, 7);
      
      %let tmp_link_instance = &tmplib.._tmp_link_instance_;
      
      /* Send request to create a link */
      %let &outSuccess. = 0;
      %core_rest_create_link_inst(host = &host.
                                 , port = &port.
                                 , logonHost = &logonHost.
                                 , logonPort = &logonPort.
                                 , username = &username.
                                 , password = &password.
                                 , authMethod = &authMethod.
                                 , client_id = &client_id.
                                 , client_secret = &client_secret.
                                 , link_instance_id = analysisRun_processOrchestration_&keyPrefixAnalysisRun._&keyPrefixFlowRun.
                                 , linkSourceSystemCd = &solution.
                                 , link_type = analysisRun_processOrchestration
                                 , solution = &solution.
                                 , business_object1 = &analysisRunKey.
                                 , business_object2 = &flow_run_id.
                                 , collectionObjectKey = &analysisRunKey.
                                 , collectionName = analysisRuns
                                 , outds = &tmp_link_instance.
                                 , outVarToken = &outVarToken.
                                 , outSuccess = &outSuccess.
                                 , outResponseStatus = &outResponseStatus.
                                 , debug = &debug.
                                 , logOptions = &oldLogOptions.
                                 , restartLUA = &restartLUA.
                                 , clearCache = &clearCache.
                                 );

      /* Exit in case of errors */
      %if(not &&&outSuccess..) %then %do;
         %put ERROR: Unable to create link between analysis run: &analysisRunKey. to flow run: &flow_run_id.;
         %abort;
      %end;
      
      %if("%upcase(&debug.)" ne "TRUE") %then %do;
         /* Delete temporary table if it exists */
         %if (%rsk_dsexist(&tmp_link_instance.)) %then %do;
            proc sql;
               drop table &tmp_link_instance.;
            quit;
         %end;
      %end;
      
   %end;
   
   /* Wait for the flow run to finish */
   %let wait_flg = %upcase(&wait_flg.);
   %if(%sysevalf(%superq(wait_flg) = Y, boolean)) %then %do;
      /* Check the status of the flow run */
      %let start_dttm = %sysfunc(datetime());
      
      %do %while ("&execution_status." in ("pending" "queued" "running"));
      
         %let tmp_flow_run_state = &tmplib.._tmp_flow_run_state_;
         
         /* Send the REST request to get the flow run's status*/
         %let &outSuccess. = 0;
         %core_rest_get_flow_runs(host = &host.
                                 , server = processOrchestration
                                 , solution = &solution.
                                 , port = &port.
                                 , logonHost = &logonHost.
                                 , logonPort = &logonPort.
                                 , username = &username.
                                 , password = &password.
                                 , authMethod = bearer
                                 , client_id = &client_id.
                                 , client_secret = &client_secret.
                                 , flow_run_id = &flow_run_id.
                                 , outds = &tmp_flow_run_state.
                                 , outVarToken = &outVarToken.
                                 , outSuccess = &outSuccess.
                                 , outResponseStatus = &outResponseStatus.
                                 , debug = &debug.
                                 , logOptions = &oldLogOptions.
                                 , restartLUA = &restartLUA.
                                 , clearCache = &clearCache.
                                 );
                                 
         /* Exit in case of errors */
         %if(not &&&outSuccess..) %then %do;
            %put ERROR: There was an error checking the status of the flow run with id "&flow_run_id.".;
            %abort;
         %end;
         %else %if (not %rsk_dsexist(&tmp_flow_run_state.)) %then %do;
            %put ERROR: There was an error checking the status of the flow run with id "&flow_run_id.".;
            %abort;
         %end;
         %else %if (%rsk_attrn(&tmp_flow_run_state., nlobs) eq 0) %then %do;
            %put ERROR: There was an error checking the status of the flow run with id "&flow_run_id.".;
            %abort;
         %end;
         
         data _null_;
            set &tmp_flow_run_state.;
            call symputx("execution_status", state, "L");
         run;
         
         /* Update the output variable */
         %let &outFlowRunStatus. = &execution_status.;
         
         %let current_dttm = %sysfunc(datetime());
         %if("&execution_status." in ("pending" "queued" "running")) %then %do;
            %if (%sysevalf(&current_dttm. - &start_dttm. > &maxWait.)) %then %do;
               %put ------------------------------------------------------------------------------------;
               %put %upcase(&timeoutSeverity.): Maximum waiting time has expired before the flow completion. Exiting macro..;
               %put ------------------------------------------------------------------------------------;
               %let execution_status = timeout reached;
               /* Update the output variable */
               %let &outFlowRunStatus. = &execution_status.;
            %end;
            %else %do;
               %put --------------------------------------------------------;
               %put Flow is still &execution_status., waiting &pollInterval. seconds...;
               %put --------------------------------------------------------;
               %let rc = %sysfunc(sleep(&pollInterval., 1));
            %end;
         %end;
         
         %if("%upcase(&debug.)" ne "TRUE") %then %do;
            /* Delete temporary table if it exists */
            %if (%rsk_dsexist(&tmp_flow_run_state.)) %then %do;
               proc sql;
                  drop table &tmp_flow_run_state.;
               quit;
            %end;
         %end;
      
      %end; /* %do %while ("&execution_status." in ("pending" "queued" "running")); */
      
      %if "&execution_status." ne "success" %then %do;
      
         %let tmp_flow_run = &tmplib.._tmp_flow_run_;
         %let tmp_task_details = &tmplib.._tmp_task_details_;
         
         /* Send the REST request to get task execution details */
         %core_rest_get_flow_runs(host = &host.
                                 , server = processOrchestration
                                 , solution = &solution.
                                 , port = &port.
                                 , logonHost = &logonHost.
                                 , logonPort = &logonPort.
                                 , username = &username.
                                 , password = &password.
                                 , authMethod = bearer
                                 , client_id = &client_id.
                                 , client_secret = &client_secret.
                                 , flow_run_id = &flow_run_id.
                                 , outds = &tmp_flow_run.
                                 , outds_details = &tmp_task_details.
                                 , debug = &debug.
                                 , logOptions = &oldLogOptions.
                                 , restartLUA = &restartLUA.
                                 , clearCache = &clearCache.
                                 );
                                 
         /* Get failed task(s) */
         %if(%rsk_varexist(&tmp_task_details., cardinality) and %rsk_varexist(&tmp_task_details., states1)) %then %do;
            proc sql noprint;
               select task_id into :partitioned_task_id_list separated by " "
               from &tmp_task_details.
               where states1="failed" and cardinality is not null
               ;
            quit;
            
            proc sql noprint;
               select task_id into :task_id_list separated by " "
               from &tmp_task_details.
               where states1="failed" and cardinality is null
               ;
            quit;
         %end;
         %else %if(%rsk_varexist(&tmp_task_details., states1)) %then %do;
            proc sql noprint;
               select task_id into :task_id_list separated by " "
               from &tmp_task_details.
               where states1="failed"
               ;
            quit;
         %end;
         
         %if("%upcase(&debug.)" ne "TRUE") %then %do;
            /* Delete temporary table if it exists */
            %if (%rsk_dsexist(&tmp_flow_run.)) %then %do;
               proc sql;
                  drop table &tmp_flow_run.;
               quit;
            %end;
            
            /* Delete temporary table if it exists */
            %if (%rsk_dsexist(&tmp_task_details.)) %then %do;
               proc sql;
                  drop table &tmp_task_details.;
               quit;
            %end;
         %end;
         
         %if %sysevalf(%superq(partitioned_task_id_list) ne, boolean) %then %do;
            %let TotLogRequests = %sysfunc(countw(&partitioned_task_id_list, %str( )));
            %do i = 1 %to &TotLogRequests.;
               %let flowTaskId = %scan(&partitioned_task_id_list., &i., %str( ));
               
               /* Send the REST request to get the task log */
               %core_rest_get_flow_run_task_log(host = &host.
                                              , server = processOrchestration
                                              , solution = &solution.
                                              , port = &port.
                                              , logonHost = &logonHost.
                                              , logonPort = &logonPort.
                                              , username = &username.
                                              , password = &password.
                                              , authMethod = bearer
                                              , client_id = &client_id.
                                              , client_secret = &client_secret.
                                              , flow_run_id = &flow_run_id.
                                              , flow_task_id = &flowTaskId.
                                              , flow_task_rank = 1
                                              , printLog = Y
                                              , outVarToken = accessToken
                                              , outSuccess = httpSuccess
                                              , outResponseStatus = responseStatus
                                              , debug = &debug.
                                              , logOptions = &oldLogOptions.
                                              , restartLUA = &restartLUA.
                                              , clearCache = &clearCache.
                                              );
            %end;
         %end;
         
         %if %sysevalf(%superq(task_id_list) ne, boolean) %then %do;
            %let TotLogRequests = %sysfunc(countw(&task_id_list, %str( )));
            %do k = 1 %to &TotLogRequests.;
               %let flowTaskId = %scan(&task_id_list., &k., %str( ));
               
               /* Send the REST request to get the task log */
               %core_rest_get_flow_run_task_log(host = &host.
                                              , server = processOrchestration
                                              , solution = &solution.
                                              , port = &port.
                                              , logonHost = &logonHost.
                                              , logonPort = &logonPort.
                                              , username = &username.
                                              , password = &password.
                                              , authMethod = bearer
                                              , client_id = &client_id.
                                              , client_secret = &client_secret.
                                              , flow_run_id = &flow_run_id.
                                              , flow_task_id = &flowTaskId.
                                              , printLog = Y
                                              , outVarToken = accessToken
                                              , outSuccess = httpSuccess
                                              , outResponseStatus = responseStatus
                                              , debug = &debug.
                                              , logOptions = &oldLogOptions.
                                              , restartLUA = &restartLUA.
                                              , clearCache = &clearCache.
                                              );
            %end;
         %end;
         
         %put ERROR: The flow run with id "&flow_run_id." completed with the following status: "&execution_status.".;
         %abort;
      %end;
      
   %end; /* %if(%sysevalf(%superq(wait_flg) = Y, boolean)) %then %do; */
   
%mend;