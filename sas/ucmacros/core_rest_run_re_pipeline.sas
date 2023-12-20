/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor core_rest_run_re_pipeline

   \brief   Run a specific risk pipeline in Risk Engine.

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
   \param [in] rePipelineKey Instance key of the Risk Engine pipeline that is executed with this REST request.
   \param [in] wait_flg Flag (Y/N). If Y the macro will wait for the pipeline execution completion (Default: N)
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
   \param [out] outEnvTableInfo Name of the output table containing the pipeline's environment information
   \param [out] outVarToken Name of the output macro variable which will contain the Service Ticket (Default: access_token)
   \param [out] outSuccess Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess)
   \param [out] outResponseStatus Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus)

   \details
   This macro sends a PUT request to <b><i><host>/riskPipeline/riskPipelines/\<pipeline id\>/execution/state?value=running</i></b> \n
   If the <i>wait_flg</i> keyword parameter is Y, this macro also sends intermittent GET requests to <b><i>\<host\>/riskPipeline/riskPipelines/\<pipeline id\></i></b> until the execution is complete. \n
   See \link core_rest_request.sas \endlink for details about how to send GET requests and parse the response.

   <b>Example:</b>

   1) Set up the environment (set SASAUTOS and required LUA libraries)
   \code
      %let source_path = <Path to the root folder of the Federated Content Area (root folder, excluding the Federated Content folder)>;
      %let fa_id = <Name of the Federated Area Content folder>;
      %include "&source_path./&fa_id./source/sas/ucmacros/irm_setup.sas";
      %irm_setup(source_path = &source_path.
                , fa_id = &fa_id.
                );
   \endcode

   2) Send a Http GET request and parse the JSON response into the output table WORK.vre_run_result
   \code
      %let access_token =;
      %core_rest_run_re_pipeline(host = <Viya host>
                               , username = <user id>
                               , password = <pwd>
                               , rePipelineKey = 3feb9566-b3f5-48a0-90f7-ca266b4f3b5a
                               , ds_out = vre_run_result
                               , wait_flg = Y
                               , pollInterval = 5
                               , maxWait = 3600
                               , outVarToken = access_token
                               , outSuccess = httpSuccess
                               , outResponseStatus = responseStatus
                               , debug = false
                               );
      %put &=access_token;
      %put &=httpSuccess;
      %put &=responseStatus;
   \endcode

   \ingroup rgfRestUtils

   \author  SAS Institute Inc.
   \date    2021
*/
%macro core_rest_run_re_pipeline(host =
                               , server = riskPipeline
                               , port =
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
      current_dttm
      execution_status
      log_file_fref
      log_file_uri
      table_uri
      oldLogOptions
      rc
      start_dttm
      vre_fout
      wait_flg
      cas_table_exists
      qvalsCasLib
      qvalsCasTable
      requestUrl
      env_table_uri
      rc_load
      projectName
      pipelineName
   ;

   /* Set the required log options */
   %if(%length(&logOptions.)) %then
      options &logOptions.;
   ;

   /* Get the current value of mlogic and symbolgen options */
   %let oldLogOptions = %sysfunc(getoption(mlogic)) %sysfunc(getoption(symbolgen));

   /* Set a default outQvals value */
   %if(%sysevalf(%superq(outQvals) =, boolean)) %then
      %let outQvals = pipeline_qvals_table;

   /* Drop the outQvals table if it already exists */
   %core_cas_drop_table(cas_session_name = &casSessionName.
                        , cas_libref = &outCasLib.
                        , cas_table = &outQvals.);

   /* Set the request URL for the riskPipeline requests */
   %core_set_base_url(host=&host, server=&server., port=&port.);
   %let requestUrl = &baseUrl./&server./riskPipelines;

   /* Temporary disable mlogic and symbolgen options to avoid printing of userid/pwd to the log */
   option nomlogic nosymbolgen;
   /* Send the REST request to run the pipeline*/
   %core_rest_request(url = &requestUrl./&rePipelineKey./execution/state?value=running
                   , method = PUT
                   , logonHost = &logonHost.
                   , logonPort = &logonPort.
                   , username = &username.
                   , password = &password.
                   , authMethod = &authMethod.
                   , client_id = &client_id.
                   , client_secret = &client_secret.
                   , headerIn = Accept: application/json;charset=utf-8
                   , body = {"executionLevel": "execute"}
                   , contentType = application/json;charset=utf-8
                   , parser =
                   , outds =
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
      %put ERROR: the request to execute Risk Pipeline with key &rePipelineKey. failed.;
      %abort;
   %end;

   /* Wait for the pipeline to finish running */
   %let wait_flg = %upcase(&wait_flg.);
   %if(%sysevalf(%superq(wait_flg) = Y, boolean)) %then %do;
      /* Check the status of the pipeline */
      %let execution_status = running;
      %let start_dttm = %sysfunc(datetime());

      %do %while (&execution_status. = running);
         /* Get a Unique fileref and assign a temp file to it */
         %let vre_fout = %rsk_get_unique_ref(prefix = vre, engine = temp);
         /* Temporary disable mlogic and symbolgen options to avoid printing of userid/pwd to the log */
         option nomlogic nosymbolgen;
         /* Send the REST request to get the pipeline's status*/
         %core_rest_request(url = &requestUrl./&rePipelineKey.
                           , method = GET
                           , logonHost = &logonHost.
                           , logonPort = &logonPort.
                           , username = &username.
                           , password = &password.
                           , authMethod = &authMethod.
                           , client_id = &client_id.
                           , client_secret = &client_secret.
                           , parser =
                           , outds =
                           , fout = &vre_fout.
                           , printResponse = N
                           , outVarToken = &outVarToken.
                           , outSuccess = &outSuccess.
                           , outResponseStatus = &outResponseStatus.
                           , debug = &debug.
                           , logOptions = &oldLogOptions.
                           , restartLUA = &restartLUA.
                           , clearCache = &clearCache.
                           );

         libname vre_mdl json fileref=&vre_fout.;
         filename &vre_fout. clear;

         /* Exit in case of errors */
         %if(not &&&outSuccess.. or not %rsk_dsexist(vre_mdl.execution)) %then %do;
            %put ERROR: There was an error checking the status of the Risk Engine pipeline with id &rePipelineKey.;
            %abort;
         %end;

         data _null_;
            set vre_mdl.execution;
            call symputx('execution_status', state, 'L');
         run;

         %let log_file_uri = &requestUrl./&rePipelineKey./nodes/@flow/results/execution.log/content;

         %let current_dttm = %sysfunc(datetime());
         %if(&execution_status. = running) %then %do;
            %if (%sysevalf(&current_dttm. - &start_dttm. > &maxWait.)) %then %do;
               %put ------------------------------------------------------------------------------------;
               %put %upcase(&timeoutSeverity.): Maximum waiting time has expired before the job completion. Exiting macro..;
               %put ------------------------------------------------------------------------------------;
               %let execution_status = timeout reached;
            %end;
            %else %do;
               %put -------------------------------------------------------;
               %put Job is still running, waiting &pollInterval. seconds...;
               %put -------------------------------------------------------;
               %let rc = %sysfunc(sleep(&pollInterval., 1));
            %end;
         %end;

      %end; /* %do %while (&execution_status. = running) */

      %if "&execution_status." ^= "timeout reached" %then %do; /* Any execution_status except "timeout reached"*/

         %if %rsk_varexist(vre_mdl.results_links, type) and %rsk_varexist(vre_mdl.results_links, label) %then %do; /*If the execution NOT timed out but was able to generated result log location*/

	    /* Retrieve the link to the execution log from the returned JSON */
	    data results;
	       length name type label $200.;
	       set vre_mdl.execution_results:;
	       where upcase(label) = 'EXECUTION LOG';
	    run;

	    data links;
	       length method rel type $200. uri href $1000.;
	       set vre_mdl.results_links:;
	       where type = 'application/vnd.sas.risk.pipeline.result.log' and uri like "%@flow%";
	    run;

	    proc sort data=results;
	       by ordinal_results;
	    run;

	    proc sort data=links;
	       by ordinal_results;
	    run;

	    data _null_;
	       merge results(in=a) links(in=b);
	       by ordinal_results;
	       if a and b then do;
	          call symputx('log_file_uri', uri, 'L');
	       end;
	    run;

	    %if %sysevalf(%superq(log_file_uri) ne, boolean) %then %do;
	       /* Retrieve the pipeline log */
	       %let log_file_fref = vre_log;
	       filename &log_file_fref temp;
	       data _null_;
	          file &log_file_fref.;
	          put;
	       run;

	       /* Temporary disable mlogic and symbolgen options to avoid printing of userid/pwd to the log */
	       option nomlogic nosymbolgen;
	       /* Send the REST request */
	       %core_rest_request(url = &baseUrl.&log_file_uri./content
	   		         , method = GET
			         , logonHost = &logonHost.
			         , logonPort = &logonPort.
			         , username = &username.
			         , password = &password.
			         , authMethod = &authMethod.
			         , client_id = &client_id.
			         , client_secret = &client_secret.
			         , printResponse = N
			         , parser =
			         , outds =
			         , fout = &log_file_fref.
			         , outVarToken = &outVarToken.
			         , outSuccess = &outSuccess.
			         , outResponseStatus = &outResponseStatus.
			         , debug = &debug.
			         , logOptions = &oldLogOptions.
			         , restartLUA = &restartLUA.
			         , clearCache = &clearCache.
			         );

	       /* Throw a warning if the log file request fails */
	       %if(not &&&outSuccess..) %then %do;
	          %put WARNING: Failed to retrieve the SAS Risk Engine pipeline log from &log_file_uri./content;
	       %end;
	       %else %do;

	          /* Print pipeline log */
	          %put NOTE: - ---------------------------------------------------- -;
	          %put NOTE: SAS Risk Engine pipeline execution log (Pipeline ID= &rePipelineKey.):;
	          %put NOTE: - ---------------------------------------------------- -;
	          data _null_;
		     infile &log_file_fref. lrecl=32000;
		     input;
		     put _infile_;
	          run;
	          %put NOTE: - ---------------------------------------------------- -;
	          %put;
	          %put;

	       %end;

	       /* Cleanup */
	       filename &log_file_fref. clear;
	    %end;
	    %else %do;
	       %put WARNING: SAS Risk Engine pipeline execution log (Pipeline ID= &rePipelineKey.) could not be retrieved from the following link: &host.&log_file_uri./content.;
	    %end;

         %end; /*if-then*/
         %else %do; /*result_links does not have type variable*/
            %put WARNING: SAS Risk Engine pipeline execution log (Pipeline ID= &rePipelineKey.) could not be retrieved.;
         %end;
      %end; /* Any execution_status except "timeout reached" */

      /* Retrieve the output dataset from the pipeline */
      %if "&execution_status." = "completed" %then %do;

         /* Determine link to VALUES table details, if there is one */
         data results;
            length name type label $200.;
            set vre_mdl.execution_results:;
            where upcase(name) = 'VALUES';
         run;

         data links;
            length method rel type $200. uri href $1000.;
            set vre_mdl.results_links:;
            where type = 'application/vnd.sas.risk.common.cas.table';
         run;

         proc sort data=results;
            by ordinal_results;
         run;

         proc sort data=links;
            by ordinal_results;
         run;

         data _null_;
            merge results(in=a) links(in=b);
            by ordinal_results;
            if a and b then do;
               call symputx('table_uri', uri, 'L');
            end;
         run;

         /* If the pipeline has a query node that produced query results, use that QVALS table
         Otherwise, the post-execution must produce the QVALS table: &outCasLib..&RE_PIPELINE_NAME._&RE_PROJECT_NAME._QValues.
         Note: Generally the QVALS table will have this name if produced by a query node */
         %if %sysevalf(%superq(table_uri) ne, boolean) %then %do;

            %put Note: Pipeline has query node results.  Retrieving;

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
                              , outds = _tmp_value_ds_details
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
               %put ERROR: Failed to retrieve the SAS Risk Engine pipeline query results info from &table_uri./content (Pipeline ID= &rePipelineKey.);
               %abort;
            %end;

            /* This table will have:
               tableName:        &pipelineName._&projectName._QValues         - present if output tables saved or promoted
               sourceTableName:  &pipelineName._&projectName._QValues.sashdat - present only if output tables saved
            */
            data _null_;
               set _tmp_value_ds_details;
               call symputx('qvalsCasLib', caslibName, 'L');
               call symputx('qvalsCasTable', tableName, 'L');
            run;

         %end; /* End if a query node has VALUES table */
         %else %do;

            %put Note: Pipeline does not have query node results.  Assuming a custom code node produced query results.;

            data _null_;
               set vre_mdl.project;
               call symputx("projectName", name, "L");
            run;

            data _null_;
               set vre_mdl.root;
               call symputx("pipelineName", name, "L");
            run;

            %let qvalsCasLib = &outCasLib.;
            %let qvalsCasTable = &pipelineName._&projectName._QValues;

         %end;

         /* See if the query node or post-execution code promoted the outQvals table into our output CAS lib */
         %rsk_dsexist_cas(cas_lib=%superq(qvalsCasLib),cas_table=%superq(qvalsCasTable), cas_session_name=&casSessionName.);

         /* It did not, so the query node, or post-execution code must have saved the QVALS table to disk.  Load it. */
         %if not &cas_table_exists. %then %do;

            %put Note: No promoted query results were found for the pipeline.  Attempting to load query results from disk. (Pipeline ID=&rePipelineKey.);

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

            /* Verify that the QVALS table now exists.  If it still doesn't, error out. */
            %rsk_dsexist_cas(cas_lib=%superq(outCasLib),cas_table=%superq(outQvals), cas_session_name=&casSessionName.);
            %if &rc_load. or not &cas_table_exists. %then %do;
               %put ERROR: Failed to load the pipeline query values table: &qvalsCasLib..&qvalsCasTable. (Pipeline ID=&rePipelineKey.);
               %abort;
            %end;
            %else %do;
               %put Note: Successfully loaded the pipeline query node results to: &outCasLib..&outQvals. (Pipeline ID=&rePipelineKey.);
            %end;

         %end;
         %else %do;

            %put Note: Pipeline custom code produced and promoted query node results.;
            %put Note: Creating a global view to the query results: &outCasLib..&outQvals (Pipeline ID=&rePipelineKey.);

            /* The pipeline's QVALS table is global scope.  Create a global view to it. */
            proc cas;
               session &casSessionName.;
               table.view /
                  caslib="&outCasLib." name="&outQvals." promote=TRUE
                  tables = { { caslib="&qvalsCasLib." name="&qvalsCasTable." } }
               ;
            run;

         %end;


         /* If requested, save off the pipeline's risk environment information */
         %if(%sysevalf(%superq(outEnvTableInfo) ne, boolean)) %then %do;

            /* Get the pipeline's environment table uri */
            data results;
               length name type label $200.;
               set vre_mdl.execution_results:;
               where upcase(label) = 'RISK ENVIRONMENT';
            run;

            data links;
               length method rel type $200. uri href $1000.;
               set vre_mdl.results_links:;
               where type = 'application/vnd.sas.risk.common.cas.table';
            run;

            proc sort data=results;by ordinal_results; run;
            proc sort data=links; by ordinal_results; run;

            data _null_;
               merge results(in=a) links(in=b);
               by ordinal_results;
               if a and b then do;
                  call symputx('env_table_uri', uri, 'L');
               end;
            run;

            /* Get the environment table details */
            %if %sysevalf(%superq(env_table_uri) ne, boolean) %then %do;

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

               /* Throw an error if the environment table request fails */
               %if(not &&&outSuccess..) %then %do;
                  %put ERROR:Failed to retrieve the SAS Risk Engine pipeline environment table info from &env_table_uri. (Pipeline ID= &rePipelineKey.);
                  %abort;
               %end;

            %end;

            /* Throw an error if the environment info table was not produced or has no information about the environment */
            %if (not %rsk_dsexist(&outEnvTableInfo.) or %rsk_attrn(&outEnvTableInfo., nobs) eq 0) %then %do;
               %put ERROR: No information was found for the SAS Risk Engine pipeline environment table from &env_table_uri. (Pipeline ID= &rePipelineKey.);
               %abort;
            %end;

         %end;

      %end; /* End execution_status = completed */
      %else %do;
         %put ERROR: The SAS Risk Engine pipeline with id &rePipelineKey. completed with the following status: &execution_status.;
         %abort;
      %end;

      libname vre_mdl clear;

   %end; /* %if(%sysevalf(%superq(wait_flg) = Y, boolean)) */

%mend;