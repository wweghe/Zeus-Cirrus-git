/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor core_rest_wait_re_pipeline

   \brief   Checks the execution status and waits for the completion of a given SAS Risk Engine pipeline.

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
   \param [in] wait_flg Flag (Y/N). If Y the macro will wait for the pipeline execution completion (Default: Y)
   \param [in] pollInterval Number of seconds to wait in between consecutive pipeline execution status requests to the server (Default: 5)
   \param [in] maxWait Maximum amount of seconds the macro will wait for the execution to complete before giving up (Default: 3600 -> 1 hour)
   \param [in] timeoutSeverity Controls the severity of the log message used to notify the user that the maximum waiting time has been reached before the execution has been completed. Valid values: NOTE, WARNING, ERROR. (Default: ERROR)
   \param [in] casSessionName The name of a CAS session to use for local CAS actions.  If one doesn't exist, a new session with this name will be created.
   \param [in] debug True/False. If True, debugging informations are printed to the log (Default: false)
   \param [in] logOptions Logging options (i.e. mprint mlogic symbolgen ...)
   \param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y)
   \param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y)
   \param [out] outQvals Name of the global-scope output query values CAS table created from the pipeline's QVALS table. (Default: pipeline_qvals_table)
   \param [out] outCasLib Name of CAS lib for the output outQvals table. (Default: Public)
   \param [out] outEnvTableInfo Name of the output table containing the pipeline's environment information (Default: pipeline_env_table)
   \param [out] outVarToken Name of the output macro variable which will contain the Service Ticket (Default: accessToken)
   \param [out] outSuccess Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess)
   \param [out] outResponseStatus Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus)

   \details
   This macro sends a GET request to <b><i><host>/riskPipeline/riskPipelines/\<pipeline id\></i></b> and checks the status of the pipeline:
   - If the pipeline status is "running" then it will wait the specified number of seconds (<i>pollInterval</i>) before checking again.
   - Irrespectively of the pipeline status, the macro will stop checking after the specified number of seconds (<i>maxWait</i>) \n
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

   2) Check the pipeline status every 5 seconds until the pipeline has completed. Stop waiting after 1 hour (3600 secs).
   \code
     %let accessToken =;
     %core_rest_wait_re_pipeline(rePipelineKey = 70611b1d-52c4-4fc5-89a6-3dfadde54e84
                               , outQvals = pipeline_qvals_table
                               , outEnvTableInfo = pipeline_env_table
                               , outCasLib = Public
                               , casSessionName = casauto
                               , wait_flg = Y
                               , pollInterval = 1
                               , maxWait = 3600
                               , timeoutSeverity = ERROR
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
%macro core_rest_wait_re_pipeline(host =
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
                                 , outQvals = pipeline_qvals_table
                                 , outEnvTableInfo = pipeline_env_table
                                 , outCasLib = Public
                                 , casSessionName =
                                 , wait_flg = Y
                                 , pollInterval = 5
                                 , maxWait = 3600
                                 , timeoutSeverity = ERROR
                                 , outVarToken = accessToken
                                 , outSuccess = httpSuccess
                                 , outResponseStatus = responseStatus
                                 , debug = false
                                 , logOptions =
                                 , restartLUA = Y
                                 , clearCache = Y
                                 );

   %local
      cas_table_exists
      current_dttm
      env_table_uri
      execution_state
      pipeline_name
      fref
      libref
      oldLogOptions
      pipelineName
      projectName
      qvalsCasLib
      qvalsCasTable
      rc
      rc_load
      requestUrl
      start_dttm
      table_uri
   ;

   /* Set the required log options */
   %if(%length(&logOptions.)) %then
      options &logOptions.;
   ;

   /* Get the current value of mlogic and symbolgen options */
   %let oldLogOptions = %sysfunc(getoption(mlogic)) %sysfunc(getoption(symbolgen));

   /* Validate input parameters */
   %if(%sysevalf(%superq(rePipelineKey) eq, boolean)) %then %do;
      %put ERROR: Risk Engine pipeline key was not provided.;
      %abort;
   %end;

   /* Set a default outQvals value */
   %if(%sysevalf(%superq(outQvals) =, boolean)) %then
      %let outQvals = pipeline_qvals_table;

   /* Drop the outQvals table if it already exists */
   %core_cas_drop_table(cas_session_name = &casSessionName.
                        , cas_libref = &outCasLib.
                        , cas_table = &outQvals.);

   %if(%sysevalf(%superq(outEnvTableInfo) ne, boolean)) %then %do;
      /* Delete output table if it exists */
      %if %sysevalf(%superq(outEnvTableInfo)^=,boolean) %then %do;
         %if (%rsk_dsexist(&outEnvTableInfo.)) %then %do;
            proc sql;
               drop table &outEnvTableInfo.;
            quit;
         %end;
      %end;
   %end;

   /* Set the request URL for the riskPipeline requests */
   %core_set_base_url(host=&host, server=&server., port=&port.);
   %let requestUrl = &baseUrl./&server./riskPipelines;

   %let wait_flg = %upcase(&wait_flg.);
   %if(%sysevalf(%superq(wait_flg) = Y, boolean)) %then %do;

      %let execution_state = running;
      %let start_dttm = %sysfunc(datetime());

      %do %while (&execution_state. = running);

         %if (%rsk_dsexist(work._tmp_risk_pipeline_summary_)) %then %do;
            proc sql;
               drop table work._tmp_risk_pipeline_summary_;
            quit;
         %end;

         %if (%rsk_dsexist(work._tmp_risk_pipeline_results_)) %then %do;
            proc sql;
               drop table work._tmp_risk_pipeline_results_;
            quit;
         %end;

         /****************************************/
         /* Get the Risk Engines pipeline status */
         /****************************************/
         %core_rest_get_re_pipeline(host = &host.
                                  , port = &port.
                                  , server = &server.
                                  , logonHost = &logonHost.
                                  , logonPort = &logonPort.
                                  , username = &username.
                                  , password = &password.
                                  , authMethod = &authMethod.
                                  , client_id = &client_id.
                                  , client_secret = &client_secret.
                                  , rePipelineKey = &rePipelineKey.
                                  , outds = work._tmp_risk_pipeline_summary_
                                  , outds_execution_results = work._tmp_risk_pipeline_results_
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
            %put ERROR: Failed to get the risk pipeline status.;
            %abort;
         %end;

         data _null_;
            set work._tmp_risk_pipeline_summary_;
            call symputx('execution_state', execution_state, 'L');
            call symputx('pipeline_name', name, 'L');
         run;

         %let current_dttm = %sysfunc(datetime());
         %if(&execution_state. = running) %then %do;
            %if (%sysevalf(&current_dttm. - &start_dttm. > &maxWait.)) %then %do;
               %put ------------------------------------------------------------------------------------;
               %put %upcase(&timeoutSeverity.): Maximum waiting time has expired before the job completion. Exiting macro..;
               %put ------------------------------------------------------------------------------------;
               %let execution_state = timeout reached;
            %end;
            %else %do;
               %put -------------------------------------------------------;
               %put Job is still running, waiting &pollInterval. seconds...;
               %put -------------------------------------------------------;
               %let rc = %sysfunc(sleep(&pollInterval., 1));
            %end;
         %end;

      %end; /* %do %while (&execution_state. = running) */

      %if "&execution_state." = "completed" %then %do;

         %if (%rsk_dsexist(work._tmp_risk_pipeline_results_)) %then %do;
            data _null_;
               set work._tmp_risk_pipeline_results_;
               where results_links_type = 'application/vnd.sas.risk.common.cas.table' and
                     results_links_uri like '%results/VALUES';
               call symputx('table_uri', results_links_uri, 'L');
            run;
         %end;

         /* If the pipeline has a query node that produced query results, use that QVALS table
         Otherwise, the post-execution must produce the QVALS table: &outCasLib..&RE_PIPELINE_NAME._&RE_PROJECT_NAME._QValues.
         Note: Generally the QVALS table will have this name if produced by a query node */
         %if %sysevalf(%superq(table_uri) ne, boolean) %then %do;
            %put Note: Pipeline has query node results. Retrieving;

            /* Get VALUES table name and caslib */
            /* Temporary disable mlogic and symbolgen options to avoid printing of userid/pwd to the log */
            option nomlogic nosymbolgen;
            /* Send the REST request */
            %core_rest_request(url = &baseUrl.&table_uri.
                              , method = GET
                              , logonHost = &logonHost.
                              , logonPort = &logonPort.
                              , username = &username.
                              , password = &password.
                              , authMethod = &authMethod.
                              , client_id = &client_id.
                              , client_secret = &client_secret.
                              , printResponse = N
                              , parser = coreRestPlain
                              , outds = work._tmp_value_ds_details
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
               %put ERROR: Failed to retrieve the SAS Risk Engine pipeline query results info from &table_uri. (Pipeline Name= &pipeline_name.);
               %abort;
            %end;

            /* This table will have:
               tableName:        &pipelineName._&projectName._QValues         - present if output tables saved or promoted
               sourceTableName:  &pipelineName._&projectName._QValues.sashdat - present only if output tables saved
            */
            data _null_;
               set work._tmp_value_ds_details;
               call symputx('qvalsCasLib', caslibName, 'L');
               call symputx('qvalsCasTable', tableName, 'L');
            run;

         %end; /* End if a query node has VALUES table */
         %else %do;

            %put Note: Pipeline does not have query node results. Assuming a custom code node produced query results.;

            data _null_;
               set work._tmp_risk_pipeline_summary_;
               call symputx('projectName', project_name, 'L');
               call symputx('pipelineName', name, 'L');
            run;

            %let qvalsCasLib = &outCasLib.;
            %let qvalsCasTable = &pipelineName._&projectName._QValues;

         %end;

         /* See if the query node or post-execution code promoted the outQvals table into our output CAS lib */
         %rsk_dsexist_cas(cas_lib=%superq(qvalsCasLib),cas_table=%superq(qvalsCasTable), cas_session_name=&casSessionName.);

         /* It did not, so the query node, or post-execution code must have saved the QVALS table to disk.  Load it. */
         %if not &cas_table_exists. %then %do;

            %put Note: No promoted query results were found for the pipeline.  Attempting to load query results from disk. (Pipeline Name=&pipeline_name.);

            /* The pipeline's QVALS table is saved to disk.  Load it from disk to outQvals and promote */
            proc cas;
               session &casSessionName.;
               table.loadTable status=rc /
                  caslib="&qvalsCasLib."
                  path="&qvalsCasTable..sashdat"
                  casOut={caslib="&outCasLib." name="&outQvals." promote=TRUE}
               ;
               symputx("rc_load", rc.severity, "L"); /* rc=0 if successful load */
               run;
            quit;

            /* Verify that the QVALS table now exists. If it still doesn't, error out. */
            %rsk_dsexist_cas(cas_lib=%superq(outCasLib),cas_table=%superq(outQvals), cas_session_name=&casSessionName.);
            %if &rc_load. or not &cas_table_exists. %then %do;
               %put ERROR: Failed to load the pipeline query values table: &qvalsCasLib..&qvalsCasTable. (Pipeline Name= &pipeline_name.);
               %abort;
            %end;
            %else %do;
               %put Note: Successfully loaded the pipeline query node results to: &outCasLib..&outQvals. (Pipeline Name= &pipeline_name.);
            %end;

         %end; /* %if not &cas_table_exists. */
         %else %do;

            %put Note: Pipeline custom code produced and promoted query node results.;
            %put Note: Creating a global view to the query results: &outCasLib..&outQvals (Pipeline Name= &pipeline_name.);

            /* The pipeline's QVALS table is global scope.  Create a global view to it. */
            proc cas;
               session &casSessionName.;
               table.view /
                  caslib="&outCasLib." name="&outQvals." promote=TRUE
                  tables = { { caslib="&qvalsCasLib." name="&qvalsCasTable." } }
               ;
            run;

         %end;


         /* If requested, save off the pipeline's risk environment. */
         %if(%sysevalf(%superq(outEnvTableInfo) ne, boolean)) %then %do;

            %if (%rsk_dsexist(work._tmp_risk_pipeline_results_)) %then %do;
               data _null_;
                  set work._tmp_risk_pipeline_results_;
                  where results_links_type = 'application/vnd.sas.risk.common.cas.table' and
                        results_links_uri like '%results/environment';
                  call symputx('env_table_uri', results_links_uri, 'L');
               run;
            %end;

            %if %sysevalf(%superq(env_table_uri) ne, boolean) %then %do;
               %put Note: Pipeline has SAS Risk Engine pipeline environment table. Retrieving;

               /* Get the environment table details */
               /* Temporary disable mlogic and symbolgen options to avoid printing of userid/pwd to the log */
               option nomlogic nosymbolgen;
               /* Send the REST request */
               %core_rest_request(url = &baseUrl.&env_table_uri.
                                 , method = GET
                                 , logonHost = &logonHost.
                                 , logonPort = &logonPort.
                                 , username = &username.
                                 , password = &password.
                                 , authMethod = &authMethod.
                                 , client_id = &client_id.
                                 , client_secret = &client_secret.
                                 , printResponse = N
                                 , parser = coreRestPlain
                                 , outds = &outEnvTableInfo.
                                 , outVarToken = &outVarToken.
                                 , outSuccess = &outSuccess.
                                 , outResponseStatus = &outResponseStatus.
                                 , debug = &debug.
                                 , logOptions = &oldLogOptions.
                                 , restartLUA = &restartLUA.
                                 , clearCache = &clearCache.
                                 );

               /* Throw an error if the table request fails */
               %if(not &&&outSuccess..) %then %do;
                  %put ERROR: Failed to retrieve the SAS Risk Engine pipeline environment table info from &env_table_uri. (Pipeline Name= &pipeline_name.);
                  %abort;
               %end;

               /* Throw an error if the environment info table was not produced or has no information about the environment */
               %if (not %rsk_dsexist(&outEnvTableInfo.) or %rsk_attrn(&outEnvTableInfo., nlobs) eq 0) %then %do;
                  %put ERROR: No information was found for the SAS Risk Engine pipeline environment table from &env_table_uri. (Pipeline Name= &pipeline_name.);
                  %abort;
               %end;
            %end; /* %if %sysevalf(%superq(env_table_uri) ne, boolean) */
            %else %do;
               /* Throw an error if the environment info table was not produced by a Manage Environment node */
               %put ERROR: No information was found for the SAS Risk Engine pipeline environment table. (Pipeline Name= &pipeline_name.);
               %abort;
            %end;
         %end; /* %if(%sysevalf(%superq(outEnvTableInfo) ne, boolean)) */

      %end; /* %if "&execution_state." = "completed" */
      %else %do;
         %put ERROR: The SAS Risk Engine pipeline with name &pipeline_name. completed with the following status: &execution_state.;
         %abort;
      %end;

      /* Remove temporary data artefacts from the WORK */
      proc datasets library = work
                    memtype = (data)
                    nolist nowarn;
         delete _tmp_risk_pipeline_summary_
                _tmp_risk_pipeline_results_
                _tmp_value_ds_details
                ;
      quit;
   %end; /* %if(%sysevalf(%superq(wait_flg) = Y, boolean)) */

%mend;