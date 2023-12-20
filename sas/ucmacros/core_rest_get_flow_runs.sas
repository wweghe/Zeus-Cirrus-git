/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor core_rest_get_flow_runs

   \brief   Retrieve the flow run(s) executed in SAS Process Orchestration

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
   \param [in] flow_run_id Id of the flow run that is fetched with this REST request. If no Id is specified, the records are fetched using filter parameters.
   \param [in] filter Filters to apply on the GET request when no value for flow_run_id is specified. (e.g. eq(createdBy,'sasadm'))
   \param [in] start Specify the starting point of the records to get. Start indicate the starting index of the subset. Start SHOULD be a zero-based index. The default start SHOULD be 0. Applicable only when a filter is used.
   \param [in] limit Limit controls the maximum number of items to get from the start position (Default = 1000). Applicable only when a filter is used.
   \param [in] debug True/False. If True, debugging informations are printed to the log (Default: false)
   \param [in] logOptions Logging options (i.e. mprint mlogic symbolgen ...)
   \param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y)
   \param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y)
   \param [out] outds Name of the output table that contains the flow run(s) (Default: flow_runs)
   \param [out] outds_details (optional) Name of the output table that contains the task details of the flow run(s)
   \param [out] outVarToken Name of the output macro variable which will contain the access token (Default: accessToken)
   \param [out] outSuccess Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess)
   \param [out] outResponseStatus Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus)

   \details
   This macro sends a GET request to <b><i>\<host\>:\<port\>/processOrchestration/flowRuns</i></b> and collects the results in the output table. \n
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

   2) Send a Http GET request and parse the JSON response into the output table WORK.flow_runs
   \code
      %let accessToken=;
      %core_rest_get_flow_runs(outds = flow_runs
                             , outVarToken =accessToken
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
%macro core_rest_get_flow_runs(host =
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
                              , filter =
                              , start =
                              , limit = 1000
                              , outds = flow_runs
                              , outds_details =
                              , outVarToken =accessToken
                              , outSuccess = httpSuccess
                              , outResponseStatus = responseStatus
                              , debug = false
                              , logOptions =
                              , restartLUA = Y
                              , clearCache = Y
                              );

   %local
      oldLogOptions
      requestUrl
      fref
      libref
      root
      items
      tasks
      tasks_source
      tasks_states
      flow_id
      flow_run_id_list
      TotRequests
      i
      flowRunId
      resp_message
   ;

   /* Set the required log options */
   %if(%length(&logOptions.)) %then
      options &logOptions.;
   ;

   /* Get the current value of mlogic and symbolgen options */
   %let oldLogOptions = %sysfunc(getoption(mlogic)) %sysfunc(getoption(symbolgen));

   /* Set the base request URL */
   %core_set_base_url(host=&host, server=&server., port=&port.);
   %let requestUrl = &baseUrl./&server./flowRuns;

   %if(%sysevalf(%superq(flow_run_id) ne, boolean)) %then %do;
      %let requestUrl = &requestUrl./&flow_run_id.;
   %end;
   %else %do;
   
      /************************************************/
      /* Add filter on the requestUrl*/
      /************************************************/
      %if(%sysevalf(%superq(filter) ne, boolean)) %then
         %let requestUrl = %superq(requestUrl)%str(?)filter=&filter.;

      /************************************************/
      /* Add Start and Limit options on the requestUrl*/
      /************************************************/
      %if(%sysevalf(%superq(start) ne, boolean)) %then %do;
         %if(%index(%superq(requestUrl),?) = 0) %then
            %let requestUrl = %superq(requestUrl)%str(?)start=&start.;
         %else
            %let requestUrl = %superq(requestUrl)%str(&)start=&start.;
      %end;

      %if(%sysevalf(%superq(limit) ne, boolean)) %then %do;
         %if(%index(%superq(requestUrl),?) = 0) %then
            %let requestUrl = %superq(requestUrl)%str(?)limit=&limit.;
         %else
            %let requestUrl = %superq(requestUrl)%str(&)limit=&limit.;
      %end;

   %end;

   /* Delete output table if it exists */
   %if(%sysevalf(%superq(outds) ne, boolean)) %then %do;
      %if (%rsk_dsexist(&outds.)) %then %do;
         proc sql;
            drop table &outds.;
         quit;
      %end;
   %end;
   
   /* Delete output table if it exists */
   %if(%sysevalf(%superq(outds_details) ne, boolean)) %then %do;
      %if (%rsk_dsexist(&outds.)) %then %do;
         proc sql;
            drop table &outds.;
         quit;
      %end;
   %end;
   
   /* Get a Unique fileref and assign a temp file to it */
   %let fref = %rsk_get_unique_ref(prefix = resp, engine = temp);
   
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
                     , fout = &fref.
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
   %let libref = %rsk_get_unique_ref(type = lib, engine = JSON, args = fileref = &fref.);

   %let root = &libref..root;
   %let items = &libref..items;
   %let tasks = &libref..tasks;
   %let tasks_source = &libref..tasks_source;
   %let tasks_states = &libref..tasks_states;
   
   /* Exit in case of errors */
   %if(not &&&outSuccess..) %then %do;
      %put ERROR: The request to get the flow run(s) was not successful.;
      %if(%upcase(&debug.) eq TRUE) %then %do;
         data _null_;
            set &root.(keep=message);
            call symputx("resp_message", message, "L");
         run;
         %put ERROR: %superq(resp_message);
      %end;
      %abort;
   %end;
   
   %if (%rsk_dsexist(&items.)) %then %do; /* logic when using a filter */
   
      %if(%sysevalf(%superq(outds_details) ne, boolean)) %then %do; /* if requested: go deeper and retrieve detailed information for each flow run */
      
         proc sql noprint;
            select id into :flow_run_id_list separated by " "
            from &items.
            ;
         quit;
         
         %if(%sysevalf(%superq(flow_run_id_list) ne, boolean)) %then %do;
            %let TotRequests = %sysfunc(countw(&flow_run_id_list, %str( )));
            %do i = 1 %to &TotRequests.;
               %let flowRunId = %scan(&flow_run_id_list., &i., %str( ));
               
               /* Delete temporary table if it exists */
               %if (%rsk_dsexist(__tmp_flowRuns_&i.__)) %then %do;
                  proc sql;
                     drop table __tmp_flowRuns_&i.__;
                  quit;
               %end;
               
               /* Delete temporary table if it exists */
               %if (%rsk_dsexist(__tmp_taskDetails_&i.__)) %then %do;
                  proc sql;
                     drop table __tmp_taskDetails_&i.__;
                  quit;
               %end;
               
               %core_rest_get_flow_runs(host = &host.
                                      , server = &server.
                                      , solution = &solution.
                                      , port = &port.
                                      , logonHost = &logonHost.
                                      , logonPort = &logonPort.
                                      , username = &username.
                                      , password = &password.
                                      , authMethod = &authMethod.
                                      , client_id = &client_id.
                                      , client_secret = &client_secret.
                                      , flow_run_id = &flowRunId.
                                      , outds = __tmp_flowRuns_&i.__
                                      , outds_details = __tmp_taskDetails_&i.__
                                      , debug = &debug.
                                      , logOptions = &oldLogOptions.
                                      , restartLUA = &restartLUA.
                                      , clearCache = &clearCache.
                                      );

               %if (%rsk_dsexist(__tmp_flowRuns_&i.__)) %then %do;
                  /* Append data */
                  %rsk_append(base = &outds.
                           , data = __tmp_flowRuns_&i.__
                           , length_selection = longest);
               %end;

               %if (%rsk_dsexist(__tmp_taskDetails_&i.__)) %then %do;
                  /* Append data */
                  %rsk_append(base = &outds_details.
                           , data = __tmp_taskDetails_&i.__
                           , length_selection = longest);
               %end;
               
               /* Remove temporary data artefacts from the WORK */
               proc datasets library = work
                             memtype = (data)
                             nolist nowarn;
                  delete __tmp_flowRuns_&i.__
                         __tmp_taskDetails_&i.__
                         ;
               quit;
               
            %end; /* %do i = 1 %to &TotRequests.; */
         %end; /* %if(%sysevalf(%superq(flow_run_id_list) ne, boolean)) %then %do; */
      %end; /* %if(%sysevalf(%superq(outds_details) ne, boolean)) %then %do; */
      %else %do;
         data &outds.;
            set &items.(drop=ordinal_root ordinal_items);
         run;
      %end;
      
   %end;
   %else %if (%rsk_dsexist(&tasks.)) %then %do; /* logic when using flow_run_id */
   
      data &outds.;
         set &root.(drop=ordinal_root);
         call symputx("flow_id", flowId, "L");
      run;
      
      %if(%sysevalf(%superq(outds_details) ne, boolean)) %then %do;
         proc sql;
            create table &outds_details.(drop=ordinal_root ordinal_tasks ordinal_source ordinal_states) as
            select  "&flow_id." as flow_id length=36
                  , "&flow_run_id." as flow_run_id length=36
                  , tasks.*
                  , tasks_source.*
                  , tasks_states.*
            from &tasks.(rename=(id=task_id)) as tasks
               inner join &tasks_source.(rename=(name=tasks_source_name)) as tasks_source
                  on tasks.ordinal_tasks = tasks_source.ordinal_tasks
               inner join &tasks_states. as tasks_states
                  on tasks.ordinal_tasks = tasks_states.ordinal_tasks
            ;
         quit;
      %end;
      
   %end;
   
   filename &fref. clear;
   libname &libref. clear;

%mend;