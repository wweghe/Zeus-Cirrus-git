/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor core_rest_get_batch_job_log

   \brief   Retrieve the log of a batch job executed in SAS Risk Cirrus Core

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
   \param [in] job_id Id of the executed batch job
   \param [in] logpath File path for the task log. If not provided a temp filename is used
   \param [in] logname File name for the task log. If not provided a temp filename is used
   \param [in] logtype can be either log (txt) or report (xls)
   \param [in] printLog Flag (Y/N). Controls whether the task log is printed to the log (Default: N).
   \param [in] debug True/False. If True, debugging informations are printed to the log (Default: false)
   \param [in] logOptions Logging options (i.e. mprint mlogic symbolgen ...)
   \param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y)
   \param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y)
   \param [out] outVarToken Name of the output macro variable which will contain the access token (Default: accessToken)
   \param [out] outVarRefreshToken Name of the output macro variable which will contain the refresh token (Default: refToken) 
   \param [out] outSuccess Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess)
   \param [out] outResponseStatus Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus)

   \details
   This macro sends a GET request to <b><i>\<host\>:\<port\>/riskCirrusCore/batch/jobs/&job_id./log</i></b> to get the log of the task (log_type=log) or \n
   This macro sends a GET request to <b><i>\<host\>:\<port\>/riskCirrusCore/batch/jobs/&job_id./logReport</i></b> to get the log of the task (log_type=report) \n
   See \link core_rest_request.sas \endlink for details about how to send GET requests and parse the response.


   <b>Example:</b>

   1) Set up the environment (set SASAUTOS and required LUA libraries).  Assumes the spre folder is under /riskcirruscore/core/code_libraries/release-core-{cadence-version}
   \code
      %let cadence_version=2023.12;
      %let core_root_path=/riskcirruscore/core/code_libraries/release-core-&cadence_version.;
      option insert = (
         SASAUTOS = (
            "&core_root_path./sas/ucmacros"
            )
         );
      filename LUAPATH ("&core_root_path./lua");
   \endcode

   2) Get the log of a given task.
   \code

      %let accessToken=;
      %core_rest_get_batch_job_log(job_id = 736aa609-8e6c-4bb8-a209-c6bc985ffabe
                                     , printLog = Y
                                     , logpath = /riskcirruscore
                                     , logname = test
                                     , logtype = log
                                     , outVarToken = accessToken
                                     , outVarRefreshToken = refToken
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
%macro core_rest_get_batch_job_log(host =
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
                                     , job_id =
                                     , logtype = log
                                     , logname =
                                     , logpath = 
                                     , printLog = N
                                     , outVarToken = accessToken
                                     , outVarRefreshToken = refToken
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

   /* Validate job_id parameter */
   %if (%sysevalf(%superq(job_id) eq, boolean)) %then %do;
      %put ERROR: The batch job id parameter is required.;
      %abort;
   %end;

   /* Make sure the logtype is set */
   %if %sysevalf(%superq(logtype) eq, boolean) %then %do;
      %let logtype = log;
      %PUT NOTE: Missing Log Type. Default type log will be used.;
   %end;

   %if %sysevalf(%superq(logtype) ne log, boolean) and %sysevalf(%superq(logtype) ne report, boolean) %then %do;
      %let logtype = log;
      %PUT NOTE: Invalid Log Type. Default type log will be used.;
   %end;


   %let auto_flog_flg = N;
   /* Validate fout_log parameter */
   %if %sysevalf(%superq(logname) ne, boolean) and %sysevalf(%superq(logpath) ne, boolean) %then %do;

      %if(%rsk_dir_exists(DIR=&logpath.) ne 1) %then %do;
         %put NOTE: The provided file location is not valid. A TEMP location will be used;
         %let fout_log = %rsk_get_unique_ref(prefix = autol, engine = temp);
         %let auto_flog_flg = Y;
         %PUT NOTE: Invalid Log path. TEMP location will be used.;
      %end;
      %else %do;
          %if %sysevalf(%superq(logtype) eq log, boolean) %then %do;
             filename logout "&logpath./&logname..txt";
             %PUT NOTE: Log file -> &logpath./&logname..txt.;
          %end;
          %else %if %sysevalf(%superq(logtype) eq report, boolean) %then %do;
             filename logout "&logpath./&logname..xlsx";
             %PUT NOTE: Log file -> &logpath./&logname..xlsx.;
          %end;
          %let fout_log = logout;
      %end;
   %end;
   %else %do; 
      /* Get a Unique fileref and assign a temp file to it */
      %let fout_log = %rsk_get_unique_ref(prefix = autol, engine = temp);
      %let auto_flog_flg = Y;
      %PUT NOTE: Invalid Log file info. TEMP location will be used.;
   %end;

   %if(%sysfunc(fileref(&fout_log.)) gt 0) %then %do;
         %put ERROR: The provided file info is not valid. You must specify a valid file name or path.;
         %abort;
   %end;

   %let accessToken =;
   %core_rest_get_batch_job(
                                    server = riskCirrusCore
                                  , authMethod = bearer
                                  , jobId = &job_id.
                                  , start = 0
                                  , limit = 100
                                  , logSeverity = WARNING
                                  , outds_batch_job_info = work.batch_jobid
                                  , outVarToken = accessToken
                                  , outVarRefreshToken = refToken
                                  , outSuccess = httpSuccess
                                  , outResponseStatus = responseStatus
                                  , debug = false
                                  , logOptions =
                                  , restartLUA = Y
                                  , clearCache = Y
                                  );
      %put &=accessToken;
      %put &=httpSuccess;
      %put &=responseStatus;

   /* Exit in case of errors */
   %if(&&&outSuccess..) %then %do;
      data _null_;
         set work.batch_jobid;
         call symputx('logURI',logUri);
         call symputx('logReportURI',logReportUri);
      run;

   %end;
   %else %do;
      %put ERROR: The request to retrieve the job "&job_id." was not successful. .;
      %abort;
   %end;

   /* Set the base request URL */
   %core_set_base_url(host=&host, server=files, port=&port.);

  %if %sysevalf(%superq(logtype) eq log, boolean) %then %do;
     %if %sysevalf(%superq(logURI) ne ., boolean) %then %do;
        %let requestUrl = &baseUrl.&logURI.;
     %end;
     %else %do;
        %PUT WARNING: The batch job did not return a valid Log.;
        %return;
     %end;
  %end;
  %else %if %sysevalf(%superq(logtype) eq report, boolean) %then %do;
     %if %sysevalf(%superq(logReportURI) ne ., boolean) %then %do;
        %let requestUrl = &baseUrl.&logReportURI.;
     %end;
     %else %do;
        %PUT WARNING: The batch job did not return a valid Report Log.;
        %return;
     %end;
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
                     , fout = &fout_log.
                     , parser=
                     , outds=
                     , outVarToken = &outVarToken.
                     , outVarRefreshToken = &outVarRefreshToken.
                     , outSuccess = &outSuccess.
                     , outResponseStatus = &outResponseStatus.
                     , debug = &debug.
                     , logOptions = &oldLogOptions.
                     , restartLUA = &restartLUA.
                     , clearCache = &clearCache.
                     );


   /* Exit in case of errors */
   %if(not &&&outSuccess..) %then %do;
      %put ERROR: The request to retrieve the log for job "&job_id." was not successful.;
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

      %let log_title = Execution log for job: "&job_id.";

      /* Print the task log */
      %rsk_print_file(file = %sysfunc(pathname( &fout_log.))
                     , title = &log_title.
                     , logSeverity = WARNING
                     );
   %end;

   /* Clear out the temporary files */
   %if("&auto_flog_flg." = "Y") %then %do;
      filename &fout_log. clear;
   %end;


%mend;

