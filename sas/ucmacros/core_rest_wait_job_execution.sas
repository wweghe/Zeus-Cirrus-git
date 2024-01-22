/*
 Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor core_rest_wait_job_execution

   \brief   Checks the execution status and waits for the completion of a given SAS Job Execution.

   \param [in] host Viya host url, including the protocol
   \param [in] server Name of the Web Application Server that provides the REST service (Default: jobExecution)
   \param [in] port (optional) Server port
   \param [in] logonHost (Optional) Host/IP of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app host/ip is the same as the host/ip in the url parameter
   \param [in] logonPort (Optional) Port of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app port is the same as the port in the url parameter
   \param [in] username (optional) Username credentials
   \param [in] password (optional) Password credentials: it can be plain text or SAS-Encoded (it will be masked during execution).
   \param [in] authMethod: Authentication method (accepted values: BEARER). (Default: BEARER).
   \param [in] client_id The client id registered with the Viya authentication server. If blank, the internal SAS client id is used (only if GRANT_TYPE = password).
   \param [in] client_secret The secret associated with the client id.
   \param [in] jobID ID of the job execution service process.
   \param [in] wait_flg Flag (Y/N). If Y the macro will wait for the job execution execution completion (Default: Y)
   \param [in] pollInterval Number of seconds to wait in between consecutive job execution status requests to the server (Default: 1)
   \param [in] maxWait Maximum amount of seconds the macro will wait for the execution to complete before giving up (Default: 3600 -> 1 hour)
   \param [in] timeoutSeverity Controls the severity of the log message used to notify the user that the maximum waiting time has been reached before the execution has been completed. Valid values: NOTE, WARNING, ERROR. (Default: ERROR)
   \param [in] debug True/False. If True, debugging informations are printed to the log (Default: false)
   \param [in] logOptions Logging options (i.e. mprint mlogic symbolgen ...)
   \param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y)
   \param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y)
   \param [out] outJobStatus Name of the output macro variable which will contain the job status (Default: jobStatus) 
   \param [out] outVarToken Name of the output macro variable which will contain the Service Ticket (Default: accessToken)
   \param [out] outSuccess Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess)
   \param [out] outResponseStatus Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus)

   \details
   This macro sends a GET request to <b><i><host>/jobExecution\></i></b> and checks the status of the job execution:
   - If the job execution status is "RUNNING" then it will wait the specified number of seconds (<i>pollInterval</i>) before checking again.
   - Irrespectively of the job status, the macro will stop checking after the specified number of seconds (<i>maxWait</i>) \n
   - If the job is ends with a status not COMPLETED it will display the logs \n
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

   2) Check the job status every 1 seconds until the job has completed. Stop waiting after 1 hour (3600 secs).
   \code
     %let accessToken =;
     %core_rest_wait_job_execution(jobID = 70611b1d-52c4-4fc5-89a6-3dfadde54e84
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
%macro core_rest_wait_job_execution(host =
                                 , port =
                                 , server = jobExecution
                                 , logonHost =
                                 , logonPort =
                                 , username =
                                 , password =
                                 , authMethod = bearer
                                 , client_id =
                                 , client_secret =
                                 , jobID =
                                 , wait_flg = Y
                                 , pollInterval = 1
                                 , maxWait = 3600
                                 , timeoutSeverity = ERROR
                                 , outVarToken = accessToken
                                 , outSuccess = httpSuccess
                                 , outResponseStatus = responseStatus
                                 , outJobStatus = jobStatus
                                 , debug = false
                                 , logOptions =
                                 , restartLUA = Y
                                 , clearCache = Y
                                 );
    %local current_dttm
            execution_state
            rc
            log_file_fref
            errorCode
            errorMessage
            ;

    /* Set the required log options */
    %if(%length(&logOptions.)) %then
        options &logOptions.;
    ;

    /* Get the current value of mlogic and symbolgen options */
    %let oldLogOptions = %sysfunc(getoption(mlogic)) %sysfunc(getoption(symbolgen));

    /* Validate input parameters */
    %if(%sysevalf(%superq(jobID) eq, boolean)) %then %do;
        %put ERROR: Job execution ID was not provided.;
        %abort;
    %end;

    /* Set a default outQvals value */
    %if(%sysevalf(%superq(outJobStatus) =, boolean)) %then
    %let outJobStatus = jobStatus;

    %let wait_flg = %upcase(&wait_flg.);

    %let execution_state=;

    %if(%sysevalf(%superq(wait_flg) = Y, boolean)) %then %do;
        %let execution_state = RUNNING;
        %let start_dttm = %sysfunc(datetime());
    

        %do %while (&execution_state. = RUNNING);

            /****************************************/
            /* Get the Job Execution status */
            /****************************************/
            %core_rest_get_job_excecution(host = &host.
                                            , port = &port.
                                            , server = &server.
                                            , logonHost = &logonHost.
                                            , logonPort = &logonPort.
                                            , username = &username.
                                            , password = &password.
                                            , authMethod = &authMethod.
                                            , client_id = &client_id.
                                            , client_secret = &client_secret.
                                            , jobId = &jobId.
                                            , filter =
                                            , start = 0
                                            , limit = 100
                                            , logSeverity = WARNING
                                            , outds_jobExecutionInfo = work.job_execution_info
                                            , outds_jobExecutionResults = work.job_execution_results
                                            , outVarToken = &outVarToken.
                                            , outSuccess = &outSuccess.
                                            , outResponseStatus = &outResponseStatus.
                                            , debug = &debug.
                                            , logOptions = &logOptions.
                                            , restartLUA = &restartLUA.
                                            , clearCache = &clearCache.
                                            );
            %if (not &&&outSuccess..) %then %do;
                %put ERROR: The request to get the status of the execution of the Job &jobID. failed.;
                %return;
            %end;

            data _null_;
                set WORK.JOB_EXECUTION_INFO(keep=state);
                call symputx("execution_state",upcase(state),'L');
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

        %if "&execution_state." ne "COMPLETED" %then %do;

            /****************************************/
            /* Get the Job Execution log */
            /****************************************/
            data _null_;
                set WORK.JOB_EXECUTION_RESULTS;

                if index(dataParamKey, ".log.txt")>1 then
                    call symputx("logLocation",dataParamValues,'L');
                if upcase(dataParamKey) = "ERRORCODE" then
                    call symputx("errorCode",dataParamValues,'L');
                if upcase(dataParamKey) = "MESSAGE" then
                    call symputx("errorMessage",dataParamValues,'L');
            run;

            %if %sysevalf(%superq(logLocation) ne, boolean) %then %do;
                %let log_file_fref = job_log;
                filename &log_file_fref temp;

                %core_set_base_url(host=&host, server=files, port=&port.);
                /* Temporary disable mlogic and symbolgen options to avoid printing of userid/pwd to the log */
                option nomlogic nosymbolgen;
                /* Send the REST request */
                /* macro var 'logLocation' already retrieves the /files/files/ - just need to pass the baseUrl with service */
                %core_rest_request(url = &baseUrl.&logLocation./content
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
                                , fout = &log_file_fref.
                                , outVarToken = &outVarToken.
                                , outSuccess = &outSuccess.
                                , outResponseStatus = &outResponseStatus.
                                , debug = &debug.
                                , logOptions = &oldLogOptions.
                                , restartLUA = &restartLUA.
                                , clearCache = &clearCache.
                                );

                %put ERROR: Job (&jobID.) not completed with the following status: &execution_state.;
                /* Throw a warning if the log file request fails */
                %if(not &&&outSuccess..) %then %do;
                    %put ERROR: Failed to retrieve the SAS Job Execution log from &logLocation.;
                %end;
                %else %do;
                    /* Print job log */
                    %put ERROR: - ---------------------------------------------------- -;
                    %put ERROR: SAS Job execution log: ;
                    %put ERROR: - ---------------------------------------------------- -;
                    %put ERROR: The job terminated with errorCode: &errorCode. and message: &errorMessage.;
                    data _null_;
                        infile &log_file_fref. lrecl=32000;
                        input;
                        put _infile_;
                    run;
                    %put ERROR: - ---------------------------------------------------- -;
                    %put;
                    %put;
                %end;
                /* Cleanup */
                filename &log_file_fref. clear;
            %end;
        %end;
    %end;

    %let  &outJobStatus. = &execution_state.;

%mend;