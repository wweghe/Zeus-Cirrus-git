/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor core_rest_run_batch_job

   \brief   Run a specific batch job

   \param [in] host (optional) Host url, including the protocol
   \param [in] server Name of the Web Application Server that provides the REST service (Default: riskCirrusObjects)
   \param [in] solution Solution identifier (Source system code) for Cirrus Core content packages (Default: currently blank)
   \param [in] port (optional) Server port
   \param [in] logonHost (Optional) Host/IP of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app host/ip is the same as the host/ip in the url parameter
   \param [in] logonPort (Optional) Port of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app port is the same as the port in the url parameter
   \param [in] username Username credentials
   \param [in] password Password credentials: it can be plain text or SAS-Encoded (it will be masked during execution).
   \param [in] authMethod: Authentication method (accepted values: BEARER). (Default: BEARER).
   \param [in] client_id The client id registered with the Viya authentication server. If blank, the internal SAS client id is used (only if GRANT_TYPE = password).
   \param [in] client_secret The secret associated with the client id.
  
   \param [in] ds_in_flow_parameters (optional) Table containing the flow run parameters where each row rapresents a key-value pair. Required columns: name, value. Optional column: description.
 
   \param [in] wait_flg Flag (Y/N). If Y the macro will wait for the flow run completion (Default: Y)
   \param [in] pollInterval Number of seconds to wait in between consecutive flow run status requests to the server (Default: 1)
   \param [in] maxWait Maximum amount of seconds the macro will wait for the execution to complete before giving up (Default: 3600 -> 1 hour)
   \param [in] timeoutSeverity Controls the severity of the log message used to notify the user that the maximum waiting time has been reached before the execution has been completed. Valid values: NOTE, WARNING, ERROR. (Default: ERROR)
   \param [in] debug True/False. If True, debugging informations are printed to the log (Default: false)
   \param [in] logOptions Logging options (i.e. mprint mlogic symbolgen ...)
   \param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y)
   \param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y)

   \param [out] outbatchJobId Name of the output macro variable containing the id of the batch job that has been created (Default: jobId)
   \param [out] outBatchJobStatus Name of the output macro variable containing the batch job execution status (Default: jobStatus)
   
   \param [out] outVarToken Name of the output macro variable which will contain the access token (Default: accessToken)
   \param [out] outVarRefreshToken Name of the output macro variable which will contain the refresh token (Default: refToken)
   \param [out] outSuccess Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess)
   \param [out] outResponseStatus Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus)

   \details
   This macro sends a POST request to <b><i>\<host\>:\<port\>/riskCirrusCore/batch/job</i></b> to trigger a batch job. \n
   If the <i>wait_flg</i> keyword parameter is Y, this macro also sends intermittent GET requests to <b><i>\<host\>:\<port\>/riskCirrusCore/batch/jobs/&job_id.</i></b> until the execution is complete. \n
   See \link core_rest_request.sas \endlink for details about how to send GET requests and parse the response.


   <b>Example:</b>

   1) Set up the environment (set SASAUTOS and required LUA libraries).  Assumes the spre folder is under /riskcirruscore/core/code_libraries/release-core-{cadence-version}
   \code
      %let cadence_version=2023.10;
      %let core_root_path=/riskcirruscore/core/code_libraries/release-core-&cadence_version.;
      option insert = (
         SASAUTOS = (
            "&core_root_path.sas/ucmacros"
            )
         );
      filename LUAPATH ("&core_root_path./lua");
   \endcode

   2) Execute a given flow definition and waits up to 1h for the run to complete.
   \code
      %let accessToken=;
      %core_rest_run_batch_job(
                                   ds_in_batch_parameters = work.batch_parameters
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


   The structure of the <b><i>DS_IN_BATCH_PARAMETERS</i></b> table is as follows:

   | name                  | value                                          |
   |-----------------------|------------------------------------------------|
   | solution              | ECL                                            |
   | name                  | TEST_BATCH_v1                                  |
   | file                  | /riskcirruscore/porjma/batch_ecl.xlsx          |
   | filename              | batch_ecl.xlsx                                 |


   \ingroup coreRestUtils

   \author  SAS Institute Inc.
   \date    2023
*/
%macro core_rest_run_batch_job(    host =
                                 , server = riskCirrusCore
                                 , solution =
                                 , port =
                                 , logonHost =
                                 , logonPort =
                                 , username =
                                 , password =
                                 , authMethod = bearer
                                 , client_id =
                                 , client_secret =
                                 , ds_in_batch_parameters =
                                 , wait_flg = Y
                                 , pollInterval = 1
                                 , maxWait = 3600
                                 , timeoutSeverity = ERROR
                                 , outbatchJobId = jobId
                                 , outbatchJobStatus = jobStatus
                                 , outVarToken =accessToken
                                 , outVarRefreshToken =refToken
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
      respfile
      body
      resp
      libref
      root
      tmplib
      resp_message
      batch_job_id_created
      tmp_batch_job_state
      rc
      execution_status
      current_dttm
      start_dttm
   ;

   %let log_level=4;
   %rsk_set_logging_options (outDebugVar = trace);

   /* Set the required log options */
   %if(%length(&logOptions.)) %then
      options &logOptions.;
   ;

   /* Get the current value of mlogic and symbolgen options */
   %let oldLogOptions = %sysfunc(getoption(mlogic)) %sysfunc(getoption(symbolgen));

   /* Make sure output variable outbatchJobId is set. */
   %if %sysevalf(%superq(outbatchJobId) =, boolean) %then
      %let outbatchJobId = jobId;

   /* Declare output variable outBatchJobId as global if it does not exist */
   %if(not %symexist(&outbatchJobId.)) %then
      %global &outbatchJobId.;

   /* Make sure output variable outbatchJobStatus is set. */
   %if %sysevalf(%superq(outbatchJobStatus) =, boolean) %then
      %let outbatchJobStatus = jobStatus;

   /* Declare output variable outbatchJobStatus as global if it does not exist */
   %if(not %symexist(&outbatchJobStatus.)) %then
      %global &outbatchJobStatus.;

   /* Initialize output variable */
   %let &outbatchJobStatus. = not started;


   /* Set the base request URL */
   %core_set_base_url(host=&host, server=&server., port=&port.);
   %let requestUrl = &baseUrl./&server./batch/job;


   %if %sysevalf(%superq(ds_in_batch_parameters) ne, boolean) %then %do;
      /* Make sure the batch parameters table exists */
      %if(%rsk_dsexist(&ds_in_batch_parameters.)) %then %do;
          data _null_;
             set &ds_in_batch_parameters.;
             call symput (name, compress(value));
          run;
      %end;
   %end;   
   
   %if (%symexist(solution)) %then %do;
	   %if(%sysevalf(%superq(solution) ne, boolean)) %then %do;
	       %if(%index(%superq(requestUrl),?) = 0) %then
	           %let requestUrl = %superq(requestUrl)%str(?)solution=&solution.;
	       %else
	           %let requestUrl = %superq(requestUrl)%str(&)solution=&solution.;
	   %end;
   %end;

   %if (%symexist(codeLibraryKey)) %then %do;
	   %if(%sysevalf(%superq(codeLibraryKey) ne, boolean)) %then %do;
	       %if(%index(%superq(requestUrl),?) = 0) %then
	           %let requestUrl = %superq(requestUrl)%str(?)codeLibraryKey=&codeLibraryKey.;
	       %else
	           %let requestUrl = %superq(requestUrl)%str(&)codeLibraryKey=&codeLibraryKey.;
	   %end;
   %end;
   
   %if (%symexist(name)) %then %do;
	   %if(%sysevalf(%superq(name) ne, boolean)) %then %do;
	       %if(%index(%superq(requestUrl),?) = 0) %then
	           %let requestUrl = %superq(requestUrl)%str(?)name=&name.;
	       %else
	           %let requestUrl = %superq(requestUrl)%str(&)name=&name.;
	   %end;
   %end;

   %if (%symexist(jobWaitSleepSec)) %then %do;
	   %if(%sysevalf(%superq(jobWaitSleepSec) ne, boolean)) %then %do;
	       %if(%index(%superq(requestUrl),?) = 0) %then
	           %let requestUrl= %superq(requestUrl)%str(?)jobWaitSleepSec=&jobWaitSleepSec.;
	       %else
	           %let requestUrl= %superq(requestUrl)%str(&)jobWaitSleepSec=&jobWaitSleepSec.;
	   %end;
   %end;

   %if (%symexist(maxParallelProcesses)) %then %do;
	   %if(%sysevalf(%superq(maxParallelProcesses) ne, boolean)) %then %do;
	       %if(%index(%superq(requestUrl),?) = 0) %then
	           %let requestUrl= %superq(requestUrl)%str(?)maxParallelProcesses=&maxParallelProcesses.;
	       %else
	           %let requestUrl= %superq(requestUrl)%str(&)maxParallelProcesses=&maxParallelProcesses.;
	   %end;
   %end;

   %if (%symexist(codeLibraryID)) %then %do;
	   %if(%sysevalf(%superq(codeLibraryID) ne, boolean)) %then %do;
	       %if(%index(%superq(requestUrl),?) = 0) %then
	           %let requestUrl= %superq(requestUrl)%str(?)codeLibraryID=&codeLibraryID.;
	       %else
	           %let requestUrl= %superq(requestUrl)%str(&)codeLibraryID=&codeLibraryID.;
	   %end;
   %end;

   %if (%symexist(codeLibrarySSC)) %then %do;
	   %if(%sysevalf(%superq(codeLibrarySSC) ne, boolean)) %then %do;
	       %if(%index(%superq(requestUrl),?) = 0) %then
	           %let requestUrl= %superq(requestUrl)%str(?)codeLibrarySSC=&codeLibrarySSC.;
	       %else
	           %let requestUrl= %superq(requestUrl)%str(&)codeLibrarySSC=&codeLibrarySSC.;
	   %end;
   %end;

   %if (%symexist(logLevel)) %then %do;
	   %if(%sysevalf(%superq(logLevel) ne, boolean)) %then %do;
	       %if(%index(%superq(requestUrl),?) = 0) %then
	           %let requestUrl = %superq(requestUrl)%str(?)logLevel=&logLevel.;
	       %else
	           %let requestUrl = %superq(requestUrl)%str(&)logLevel=&logLevel.;
	   %end;
   %end;

   %if (%symexist(logLevel)) %then %do;
	   %if(%sysevalf(%superq(jobTimeoutMin) ne, boolean)) %then %do;
	       %if(%index(%superq(requestUrl),?) = 0) %then
	           %let requestUrl = %superq(requestUrl)%str(?)jobTimeoutMin=&jobTimeoutMin.;
	       %else
	           %let requestUrl = %superq(requestUrl)%str(&)jobTimeoutMin=&jobTimeoutMin.;
	   %end;
   %end;

   %if (%symexist(cancelTimeoutSec)) %then %do;
	   %if(%sysevalf(%superq(cancelTimeoutSec) ne, boolean)) %then %do;
	       %if(%index(%superq(requestUrl),?) = 0) %then
	           %let requestUrl = %superq(requestUrl)%str(?)cancelTimeoutSec=&cancelTimeoutSec.;
	       %else
	           %let requestUrl = %superq(requestUrl)%str(&)cancelTimeoutSec=&cancelTimeoutSec.;
	   %end;
   %end;

   filename xlsin "&file.";

   %let respFile = %rsk_get_unique_ref(prefix=_, type=FILE, engine=TEMP);

   %let body = MULTI FORM ('file'= xlsin header='Content-Type: application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');

   %let resp = WORK.__resp;

      %core_rest_request(
        url = &requestUrl.
        , logonHost = &logonHost.
        , logonPort = &logonPort.
        , username = &username.
        , password = &password.
        , authMethod = &authMethod.
        , client_id = &client_id.
        , client_secret = &client_secret.
        , method = POST
        , debug = true
        , body = &body.
        , outds = &resp.
        , outVarToken = accessToken
        , outVarRefreshToken =refToken
        , outSuccess = httpSuccess
        , outResponseStatus = responseStatus
        , fout = &respFile
        );
  
    
   /* Assign libref to parse the JSON response */
   %let libref = %rsk_get_unique_ref(type = lib, engine = JSON, args = fileref = &respFile.);

   %let root = &libref..root;
   
   /* Exit in case of errors */
   %if(not &&&outSuccess..) %then %do;
      %put ERROR: The request to execute the batch job "&name." was not successful.;
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
      call symputx("batch_job_id_created", id, "L");
      call symputx("execution_status", state, "L");
   run;
      
   /* Assign the output variable */
   %let &outBatchJobId. = &batch_job_id_created.;
   
   /* Update the output variable */
   %let &outBatchJobStatus. = &execution_status.;
   
   /* Clear out the temporary files */
   filename &respFile. clear;
   libname &libref. clear;
   
      
   /* Wait for the flow run to finish */
   %let wait_flg = %upcase(&wait_flg.);
   %if(%sysevalf(%superq(wait_flg) = Y, boolean)) %then %do;
      /* Check the status of the flow run */
      %let start_dttm = %sysfunc(datetime());
      
      %do %while ("&execution_status." in ("pending" "queued" "running"));

         %let tmplib = work;
      
         %let tmp_batch_job_state = &tmplib.._tmp_batch_job_state_;

        
         /* Send the REST request to get the batch job status*/
         %let &outSuccess. = 0;
         %core_rest_get_batch_job(host = &host.
                                 , server = riskCirrusCore
                                 , solution = &solution.
                                 , port = &port.
                                 , logonHost = &logonHost.
                                 , logonPort = &logonPort.
                                 , username = &username.
                                 , password = &password.
                                 , authMethod = bearer
                                 , client_id = &client_id.
                                 , client_secret = &client_secret.
                                 , jobId = &batch_job_id_created.
                                 , outds_batch_job_info = &tmp_batch_job_state.
                                 , outVarToken = &outVarToken.
                                 , outVarRefreshToken =&outVarRefreshToken.
                                 , outSuccess = &outSuccess.
                                 , outResponseStatus = &outResponseStatus.
                                 , debug = &debug.
                                 , logOptions = &oldLogOptions.
                                 , restartLUA = &restartLUA.
                                 , clearCache = &clearCache.
                                 );
                                 
         /* Exit in case of errors */
         %if(not &&&outSuccess..) %then %do;
            %put ERROR: There was an error checking the status of the flow run with id "&batch_job_id_created.".;
            %abort;
         %end;
         %else %if (not %rsk_dsexist(&tmp_batch_job_state.)) %then %do;
            %put ERROR: There was an error checking the status of the flow run with id "&batch_job_id_created.".;
            %abort;
         %end;
         %else %if (%rsk_attrn(&tmp_batch_job_state., nlobs) eq 0) %then %do;
            %put ERROR: There was an error checking the status of the flow run with id "&batch_job_id_created.".;
            %abort;
         %end;
         
         data _null_;
            set &tmp_batch_job_state.;
            call symputx("execution_status", state, "L");
         run;
         
         /* Update the output variable */
         %let &outBatchJobStatus. = &execution_status.;
         
         %let current_dttm = %sysfunc(datetime());
         %if("&execution_status." in ("pending" "queued" "running")) %then %do;
            %if (%sysevalf(&current_dttm. - &start_dttm. > &maxWait.)) %then %do;
               %put ------------------------------------------------------------------------------------;
               %put %upcase(&timeoutSeverity.): Maximum waiting time has expired before the flow completion. Exiting macro..;
               %put ------------------------------------------------------------------------------------;
               %let execution_status = timeout reached;
               /* Update the output variable */
               %let &outBatchJobStatus. = &execution_status.;
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
            %if (%rsk_dsexist(&tmp_batch_job_state.)) %then %do;
               proc sql;
                  drop table &tmp_batch_job_state.;
               quit;
            %end;
         %end;
      
      %end; /* %do %while ("&execution_status." in ("pending" "queued" "running")); */
      
      %if "&execution_status." ne "success" %then %do;
                          
               /* Send the REST request to get the task log */
               %core_rest_get_batch_job_log(    host = &host.
                                              , solution = &solution.
                                              , port = &port.
                                              , logonHost = &logonHost.
                                              , logonPort = &logonPort.
                                              , username = &username.
                                              , password = &password.
                                              , authMethod = bearer
                                              , client_id = &client_id.
                                              , client_secret = &client_secret.
                                              , job_id = &batch_job_id_created.
                                              , printLog = Y
                                              , outVarToken = accessToken
                                              , outVarRefreshToken =refToken
                                              , outSuccess = httpSuccess
                                              , outResponseStatus = responseStatus
                                              , debug = &debug.
                                              , logOptions = &oldLogOptions.
                                              , restartLUA = &restartLUA.
                                              , clearCache = &clearCache.
                                              );
            
         
                 
         %put ERROR: The batch job with id "&batch_job_id_created." completed with the following status: "&execution_status.".;
         %abort;
      %end;
      
   %end; /* %if(%sysevalf(%superq(wait_flg) = Y, boolean)) %then %do; */
   
%mend;
