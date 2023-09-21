/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor core_rest_get_re_pipeline

   \brief   Return the risk pipeline(s) from Risk Engine.

   \param [in] host Viya host url, including the protocol
   \param [in] server Name of the Web Application Server that provides the REST service (Default: riskPipeline)
   \param [in] port (optional) Server port
   \param [in] logonHost (Optional) Host/IP of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app host/ip is the same as the host/ip in the url parameter
   \param [in] logonPort (Optional) Port of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app port is the same as the port in the url parameter
   \param [in] username (optional) Username credentials
   \param [in] password (optional) Password credentials: it can be plain text or SAS-Encoded (it will be masked during execution).
   \param [in] authMethod: Authentication method (accepted values: BEARER). (Default: BEARER).
   \param [in] client_id The client id registered with the Viya authentication server. If blank, the internal SAS client id is used (only if GRANT_TYPE = password).
   \param [in] client_secret The secret associated with the client id.
   \param [in] rePipelineKey Key or list of keys (space-separated) of the Risk Engine pipeline(s) to retrieve.
   \param [in] reProjectKey Key of the Risk Engine project. Applicable only when filter is used.
   \param [in] filter Filters to apply on the GET request when no value for rePipelineKey is specified.
   \param [in] start Specify the starting point of the records to get. Start indicate the starting index of the subset. Start SHOULD be a zero-based index. The default start SHOULD be 0. Applicable only when filter is used.
   \param [in] limit Limit controls the maximum number of items to get from the start position (Default = 1000). Applicable only when filter is used.
   \param [in] outds Name of the output table that contains the Risk Pipeline(s) summary (Default: risk_pipeline_summary).
   \param [in] outds_execution_results Name of the output table that contains the Risk Pipeline(s) execution results (Default: risk_pipeline_results).
   \param [in] debug True/False. If True, debugging informations are printed to the log (Default: false)
   \param [in] logOptions Logging options (i.e. mprint mlogic symbolgen ...)
   \param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y)
   \param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y)
   \param [out] outVarToken Name of the output macro variable which will contain the Service Ticket (Default: accessToken)
   \param [out] outSuccess Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess)
   \param [out] outResponseStatus Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus)

   \details
   This macro sends a GET request to <b><i><host>/riskPipeline/riskPipelines/\<pipeline id\></i></b> and collects the results in the output table. \n
   See \link core_rest_request.sas \endlink for details about how to send GET requests and parse the response.

   <b>Example:</b>

   1) Set up the environment (set SASAUTOS and required LUA libraries)
   \code
      %let cadence_version=2023.03;
      %let core_root_path=/riskcirruscore/core/code_libraries/release-core-&cadence_version.;
      option insert = (
         SASAUTOS = (
            "&core_root_path./spre/sas/ucmacros"
            )
         );
      filename LUAPATH ("&core_root_path./spre/lua");
   \endcode

   2) Send a Http GET request and parse the JSON response into the output table work.risk_pipeline_summary
   \code
     %let accessToken =;
     %core_rest_get_re_pipeline(rePipelineKey = 92054448-2c6f-4233-8fe9-c8c498c5661b
                                , outds = risk_pipeline_summary
                                , outVarToken = accessToken
                                , outSuccess = httpSuccess
                                , outResponseStatus = responseStatus
                                , debug = false
                                );
     %put &=accessToken;
     %put &=httpSuccess;
     %put &=responseStatus;
   \endcode

   \ingroup rgfRestUtils

   \author  SAS Institute Inc.
   \date    2023
*/
%macro core_rest_get_re_pipeline(host =
                               , port =
                               , server = riskPipeline
                               , logonHost =
                               , logonPort =
                               , username =
                               , password =
                               , authMethod = bearer
                               , client_id =
                               , client_secret =
                               , rePipelineKey =
                               , reProjectKey =
                               , filter =
                               , start =
                               , limit = 1000
                               , outds = work.risk_pipeline_summary
                               , outds_execution_results = work.risk_pipeline_results
                               , outVarToken = accessToken
                               , outSuccess = httpSuccess
                               , outResponseStatus = responseStatus
                               , debug = false
                               , logOptions =
                               , restartLUA = Y
                               , clearCache = Y
                               );

   %local
      configuration
      execution
      fref
      i
      items
      libref
      oldLogOptions
      project
      requestUrl
      resp_message
      results_links
      riskPipelineKey
      riskPipelinesUrl
      root
      TotRequests
   ;

   /* Set the required log options */
   %if(%length(&logOptions.)) %then
      options &logOptions.;
   ;

   /* Get the current value of mlogic and symbolgen options */
   %let oldLogOptions = %sysfunc(getoption(mlogic)) %sysfunc(getoption(symbolgen));
   
   /* Validate input parameters */
   %if(%sysevalf(%superq(rePipelineKey) eq, boolean) and %sysevalf(%superq(filter) eq, boolean)) %then %do;
      %put ERROR: Both rePipelineKey and filter parameters are empty. Please provide a value for at least one of the parameters.;
      %return;
   %end;
   
   %if(%sysevalf(%superq(rePipelineKey) ne, boolean) and %sysevalf(%superq(filter) ne, boolean)) %then %do;
      %put ERROR: Both rePipelineKey and filter parameters have been provided, but only one is allowed. Please remove one of the parameters.;
      %return;
   %end;
   
   %if(%sysevalf(%superq(filter) ne, boolean) and %sysevalf(%superq(reProjectKey) eq, boolean)) %then %do;
      %put ERROR: The reProjectKey parameter is missing, but is required when using the filter parameter. Please provide a value for reProjectKey.;
      %return;
   %end;
   
   /* Determine the base url */
   %core_set_base_url(host=&host, server=&server., port=&port.);
   %let riskPipelinesUrl = &baseUrl./&server./riskPipelines;
   %let requestUrl = &riskPipelinesUrl.;
   
   /* Delete output table if it exists */
   %if %sysevalf(%superq(outds)^=,boolean) %then %do;
      %if (%rsk_dsexist(&outds.)) %then %do;
         proc sql;
            drop table &outds.;
         quit;
      %end;
   %end;
   
   /* Delete output table if it exists */
   %if %sysevalf(%superq(outds_execution_results)^=,boolean) %then %do;
      %if (%rsk_dsexist(&outds_execution_results.)) %then %do;
         proc sql;
            drop table &outds_execution_results.;
         quit;
      %end;
   %end;
   
   /* Create empty output table */
   data &outds.;
      length
         modifiedTimeStamp             $24.
         createdBy                     $32.
         modifiedBy                    $32.
         description                   $256.
         name                          $32.
         id                            $36.
         version                       8.
         server                        $32.
         type                          $32.
         developer                     $32.
         execution_state               $32.
         execution_modifiedTimeStamp   $32.
         execution_startTimeStamp      $24.
         execution_warningCount        8.
         execution_createdBy           $32.
         execution_canceled            8.
         project_name                  $32.
         project_id                    $36.
         project_version               8.
         project_type                  $32.
         configuration_runAsOfDate     $10.
         configuration_outputCaslib    $256.
         ;
      stop;
   run;
   
   /* Create empty output table */
   data &outds_execution_results.;
      length
         pipeline_id             $36.
         results_links_method    $256.
         results_links_rel       $256.
         results_links_href      $1000.
         results_links_uri       $1000.
         results_links_type      $256.
         ;
      stop;
   run;
   
   %macro __selectVar__(DS, VAR, NM, TYPE);
      %if %rsk_dsexist(&DS.) %then %do;
         %if %rsk_varexist(&DS., &VAR.) %then %do;
            %scan(&DS., 2, %str(.)).&VAR. as &NM. %str( )
            %return;
         %end;
      %end;
      %if "&TYPE." = "NUM" %then %str(.); %else %str(''); %str( as &NM. )
   %mend __selectVar__;
   
   %macro __createSummaryTable__(root=, execution=, project=, configuration=, key=, ds_out=);
      proc sql;
         create table &ds_out. as
         select %__selectVar__(&root.           , modifiedTimeStamp  , modifiedTimeStamp           , CHAR )
               , %__selectVar__(&root.          , createdBy          , createdBy                   , CHAR )
               , %__selectVar__(&root.          , modifiedBy         , modifiedBy                  , CHAR )
               , %__selectVar__(&root.          , description        , description                 , CHAR )
               , %__selectVar__(&root.          , name               , name                        , CHAR )
               , %__selectVar__(&root.          , id                 , id                          , CHAR )
               , %__selectVar__(&root.          , version            , version                     , NUM  )
               , %__selectVar__(&root.          , server             , server                      , CHAR )
               , %__selectVar__(&root.          , type               , type                        , CHAR )
               , %__selectVar__(&root.          , developer          , developer                   , CHAR )
               , %__selectVar__(&execution.     , state              , execution_state             , CHAR )
               , %__selectVar__(&execution.     , modifiedTimeStamp  , execution_modifiedTimeStamp , CHAR )
               , %__selectVar__(&execution.     , startTimeStamp     , execution_startTimeStamp    , CHAR )
               , %__selectVar__(&execution.     , warningCount       , execution_warningCount      , NUM  )
               , %__selectVar__(&execution.     , createdBy          , execution_createdBy         , CHAR )
               , %__selectVar__(&execution.     , canceled           , execution_canceled          , NUM  )
               , %__selectVar__(&project.       , name               , project_name                , CHAR )
               , %__selectVar__(&project.       , id                 , project_id                  , CHAR )
               , %__selectVar__(&project.       , version            , project_version             , NUM  )
               , %__selectVar__(&project.       , type               , project_type                , CHAR )
               , %__selectVar__(&configuration. , runAsOfDate        , configuration_runAsOfDate   , CHAR )
               , %__selectVar__(&configuration. , outputCaslib       , configuration_outputCaslib  , CHAR )
         from &root. as root
         %if (%rsk_dsexist(&execution.)) %then %do;
            inner join &execution. as execution
            on root.&key. = execution.&key.
         %end;
         %if (%rsk_dsexist(&project.)) %then %do;
            inner join &project. as project
            on root.&key. = project.&key.
         %end;
         %if (%rsk_dsexist(&configuration.)) %then %do;
            inner join &configuration. as configuration
            on root.&key. = configuration.&key.
         %end;
         ;
      quit;
   %mend __createSummaryTable__;
   
   %macro __createExecutionResultsTable__(pipeline_id=, results_links=, ds_out=);
      data __resultsLinks__;
         length method rel type $256. href uri $1000.;
         set &results_links.:;
      run;
      
      proc sql;
         create table &ds_out. as
         select "&pipeline_id." as pipeline_id
                  , links.method as results_links_method
                  , links.rel as results_links_rel
                  , links.href as results_links_href
                  , links.uri as results_links_uri
                  , links.type as results_links_type
         from __resultsLinks__ as links
         ;
      quit;
   %mend __createExecutionResultsTable__;
   
   /* Get a key list for returned pipeline(s) when filter is used */
   %if(%sysevalf(%superq(reProjectKey) ne, boolean) or 
      (%sysevalf(%superq(reProjectKey) eq, boolean) and %sysevalf(%superq(rePipelineKey) eq, boolean))) %then %do;
      
      /* Add project id to the request URL */
      %if(%sysevalf(%superq(reProjectKey) ne, boolean)) %then
         %let requestUrl = &requestUrl.?project.id=%superq(reProjectKey);
      
      %if(%sysevalf(%superq(filter) ne, boolean)) %then %do;
         /* Add filters to the request URL */
         %core_set_rest_filter(filter=%superq(filter), start=&start., limit=&limit.);
      %end;
      %else %do;
         %if(%sysevalf(%superq(reProjectKey) ne, boolean)) %then %do;
            /* Set Start and Limit options */
            %if(%sysevalf(%superq(start) ne, boolean)) %then
               %let requestUrl = &requestUrl.%str(&)start=&start.;
            %if(%sysevalf(%superq(limit) ne, boolean)) %then
               %let requestUrl = &requestUrl.%str(&)limit=&limit.;
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
                        , authMethod = bearer
                        , client_id = &client_id.
                        , client_secret = &client_secret.
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
      
      /* Exit in case of errors */
      %if(not &&&outSuccess..) %then %do;
         %put ERROR: The request to get the risk pipeline(s) was not successful.;
         %if(%upcase(&debug.) eq TRUE) %then %do;
            data _null_;
               set &root.(keep=message);
               call symputx("resp_message",message);
            run;
            %put ERROR: &resp_message.;
         %end; /* (%upcase(&debug.) eq TRUE) */
         %return;
      %end;
      
      %if(%rsk_varexist(&items., id)) %then %do;
         proc sql noprint;
            select id into :rePipelineKey separated by " "
            from &items.
            ;
         quit;
      %end;
      
      filename &fref. clear;
      libname &libref. clear;
      
   %end; /* (%sysevalf(%superq(reProjectKey) ne, boolean) or ..*/
   
   %if(%sysevalf(%superq(rePipelineKey) ne, boolean)) %then %do;
      %let TotRequests = %sysfunc(countw(&rePipelineKey, %str( )));
      %do i = 1 %to &TotRequests.;
         %let riskPipelineKey = %scan(&rePipelineKey., &i., %str( ));
         
         /*********************************/
         /* Get the Risk Engines pipeline */
         /*********************************/
         
         /* Get a Unique fileref and assign a temp file to it */
         %let fref = %rsk_get_unique_ref(prefix = resp, engine = temp);

         /* Temporary disable mlogic and symbolgen options to avoid printing of userid/pwd to the log */
         option nomlogic nosymbolgen;
         /* Send the REST request */
         %core_rest_request(url = &riskPipelinesUrl./&riskPipelineKey.
                           , method = GET
                           , logonHost = &logonHost.
                           , logonPort = &logonPort.
                           , username = &username.
                           , password = &password.
                           , authMethod = bearer
                           , client_id = &client_id.
                           , client_secret = &client_secret.
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
         %let execution = &libref..execution;
         %let project = &libref..project;
         %let configuration = &libref..configuration;
         %let results_links = &libref..results_links;
         
         /* Exit in case of errors */
         %if(not &&&outSuccess..) %then %do;
            %put ERROR: The request to get the risk pipeline was not successful.;
            %if(%upcase(&debug.) eq TRUE) %then %do;
               data _null_;
                  set &root.(keep=message);
                  call symputx("resp_message",message);
               run;
               %put ERROR: &resp_message.;
            %end; /* (%upcase(&debug.) eq TRUE) */
            %return;
         %end;
         
         %__createSummaryTable__(root=&root., execution=&execution., project=&project., configuration=&configuration., key=ordinal_root, ds_out=__tmp&i.__);
         
         /* Append data */
         %rsk_append(base = &outds.
                     , data = __tmp&i.__
                     , length_selection = longest);

         %if (%rsk_dsexist(&results_links.)) %then %do;
            %__createExecutionResultsTable__(pipeline_id=&riskPipelineKey., results_links=&results_links., ds_out=__tmpRes&i.__);
            
            /* Append data */
            %rsk_append(base = &outds_execution_results.
                        , data = __tmpRes&i.__
                        , length_selection = longest);
         %end;
         
         filename &fref. clear;
         libname &libref. clear;
         
         /* Remove temporary data artefacts from the WORK */
         proc datasets library = work
                       memtype = (data)
                       nolist nowarn;
            delete __tmp&i.__
                   __tmpRes&i.__
                   __resultsLinks__
                   ;
         quit;
         
      %end; /* %do i = 1 %to &TotRequests. */
   %end; /* %if(%sysevalf(%superq(rePipelineKey) ne, boolean)) */
   
%mend;