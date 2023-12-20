/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor core_rest_get_flow_run_task_log

   \brief   Retrieve the log of a flow task executed in SAS Process Orchestration

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
   \param [in] flow_run_id Id of the parent flow run that executed the task specified in the 'flow_task_id' parameter.
   \param [in] flow_task_id Id of the task for which the log will be retrieved with this REST request.
   \param [in] flow_task_rank (optional) When used with partitioned tasks, it allows you to specify which partition's log to retrieve with this REST request.
   \param [in] fout_log (optional) Fileref for the task log. A temporary fileref is created if missing.
   \param [in] printLog Flag (Y/N). Controls whether the task log is printed to the log (Default: N).
   \param [in] debug True/False. If True, debugging informations are printed to the log (Default: false)
   \param [in] logOptions Logging options (i.e. mprint mlogic symbolgen ...)
   \param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y)
   \param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y)
   \param [out] outVarToken Name of the output macro variable which will contain the access token (Default: accessToken)
   \param [out] outSuccess Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess)
   \param [out] outResponseStatus Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus)

   \details
   This macro sends a GET request to <b><i>\<host\>:\<port\>/processOrchestration/flowRuns/&flow_run_id./tasks/&flow_task_id./logs</i></b> to get the log of the task. \n
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

   2) Get the log of a given task.
   \code
      %let accessToken=;
      %core_rest_get_flow_run_task_log(flow_run_id = c177c29f-b76e-40f2-ab52-887725dcdae6
                                     , flow_task_id = 817de341-c9c6-426d-b981-80f143a91116
                                     , printLog = Y
                                     , outVarToken = accessToken
                                     , outSuccess = httpSuccess
                                     , outResponseStatus = responseStatus
                                     );
      %put &=accessToken;
      %put &=httpSuccess;
      %put &=responseStatus;
   \endcode


   \ingroup coreRestUtils

   \author  SAS Institute Inc.
   \date    2023
*/
%macro core_rest_get_flow_run_task_log(host =
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
                                     , flow_run_id =
                                     , flow_task_id =
                                     , flow_task_rank =
                                     , fout_log =
                                     , printLog = N
                                     , outVarToken = accessToken
                                     , outSuccess = httpSuccess
                                     , outResponseStatus = responseStatus
                                     , debug = false
                                     , logOptions =
                                     , restartLUA = Y
                                     , clearCache = Y
                                     );

   %local
      oldLogOptions
      auto_flog_flg
      requestUrl
      libref
      root
      resp_message
      log_title
   ;

   /* Set the required log options */
   %if(%length(&logOptions.)) %then
      options &logOptions.;
   ;

   /* Get the current value of mlogic and symbolgen options */
   %let oldLogOptions = %sysfunc(getoption(mlogic)) %sysfunc(getoption(symbolgen));

   /* Make sure the printLog parameter is set */
   %if %sysevalf(%superq(printLog) eq, boolean) %then %do;
      %let printLog = N;
   %end;
   %else
      %let printLog = %upcase(&printLog.);

   /* Validate flow_run_id parameter */
   %if (%sysevalf(%superq(flow_run_id) eq, boolean)) %then %do;
      %put ERROR: The flow_run_id parameter is required.;
      %abort;
   %end;
   
   /* Validate flow_task_id parameter */
   %if (%sysevalf(%superq(flow_task_id) eq, boolean)) %then %do;
      %put ERROR: The flow_task_id parameter is required.;
      %abort;
   %end;
   
   %let auto_flog_flg = N;
   /* Validate fout_log parameter */
   %if %sysevalf(%superq(fout_log) eq, boolean) %then %do;
      /* Get a Unique fileref and assign a temp file to it */
      %let fout_log = %rsk_get_unique_ref(prefix = autof, engine = temp);
      %let auto_flog_flg = Y;
   %end;
   %else %do;
      /* Check if the fileref is valid */
      %if(%sysfunc(fileref(&fout_log.)) gt 0) %then %do;
         %put ERROR: The provided value for the fout_log parameter is not valid. You must specify a valid fileref.;
         %abort;
      %end;
   %end;
   
   /* Set the base request URL */
   %core_set_base_url(host=&host, server=&server., port=&port.);
   %let requestUrl = &baseUrl./&server./flowRuns/&flow_run_id./tasks/&flow_task_id./logs;
   
   %if (%sysevalf(%superq(flow_task_rank) ne, boolean)) %then %do;
      %let requestUrl = %superq(requestUrl)%str(?)rank=&flow_task_rank.;
   %end;
   
   /* Temporary disable mlogic and symbolgen options to avoid printing of userid/pwd to the log */
   option nomlogic nosymbolgen;
   /* Send the REST request */
   %core_rest_request(url = &requestUrl.
                     , method = GET
                     , logonHost = &logonHost.
                     , logonPort = &logonPort.
                     , username = &username.
                     , password = &password.
                     , authMethod = &authMethod.
                     , parser =
                     , outds =
                     , fout = &fout_log.
                     , printResponse = N
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
      %put ERROR: The request to retrieve the log for task "&flow_task_id." within flow run "&flow_run_id." was not successful.;
      %if(%upcase(&debug.) eq TRUE) %then %do;
         /* Assign libref to parse the JSON response */
         %let libref = %rsk_get_unique_ref(type = lib, engine = JSON, args = fileref = &fout_log.);
         %let root = &libref..root;
         data _null_;
            set &root.(keep=message);
            call symputx("resp_message", message, "L");
         run;
         %put ERROR: %superq(resp_message);
         libname &libref. clear;
      %end;
      %abort;
   %end;
   
   %if("&printLog." = "Y") %then %do;
   
      %if (%sysevalf(%superq(flow_task_rank) eq, boolean)) %then %do;
         %let log_title = Execution log for task id="&flow_task_id.":;
      %end;
      %else %do;
         %let log_title = Execution log for task id="&flow_task_id." and rank="&flow_task_rank.":;
      %end;
      
      /* Print the task log */
      %rsk_print_file(file = %sysfunc(pathname(&fout_log.))
                     , title = &log_title.
                     , logSeverity = WARNING
                     );
   %end;
   
   /* Clear out the temporary files */
   %if("&auto_flog_flg." = "Y") %then %do;
      filename &fout_log. clear;
   %end;
   
%mend;