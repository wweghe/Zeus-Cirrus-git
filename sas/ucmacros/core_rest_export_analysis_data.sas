/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor core_rest_export_analysis_data

   \brief   Retrieve Analysis Data instance(s) registered in SAS Risk Cirrus Objects

   \param [in] host (optional) Host url, including the protocol
   \param [in] server Name of the Web Application Server that provides the REST service (Default: riskData)
   \param [in] port (optional) Server port
   \param [in] logonHost (Optional) Host/IP of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app host/ip is the same as the host/ip in the url parameter
   \param [in] logonPort (Optional) Port of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app port is the same as the port in the url parameter
   \param [in] username (optional) Username credentials
   \param [in] password (optional) Password credentials: it can be plain text or SAS-Encoded (it will be masked during execution).
   \param [in] authMethod: Authentication method (accepted values: BEARER). (Default: BEARER).
   \param [in] client_id The client id registered with the Viya authentication server. If blank, the internal SAS client id is used (only if GRANT_TYPE = password).
   \param [in] client_secret The secret associated with the client id.
   \param [in] key Instance key of the analysis data object to export
   \param [in] locationType (DIRECTORY, LIBNAME, or FOLDER)The type of server location to which analysis data will be exported.
   \param [in] location The server location to which data will be imported.  If locationType=LIBNAME, this is a library name.  If locationType=DIRECTORY,
      this is a filesystem path on the server.  If locationType=FOLDER, this is the display path or ID of a SAS folder.
   \param [in] fileName The name of the file or table to which data will be exported.  (Only use the extension when locationType=DIRECTORY or FOLDER)
   \param [in] replace (true/false) If true and if we are exporting to CAS, the CAS table (fileName) will first be dropped.
   \param [in] casScope (GLOBAL or SESSION).  If exporting to a CAS table, the CAS table will be created with this scope.
   \param [in] casSessionId The ID of an existing CAS session to connect to.  This allows the export service to access session based CAS libraries.
   \param [in] casSessionName The name of a CAS session to use for local CAS actions.  If one doesn't exist, a new session with this name will be created.
   \param [in] debug True/False. If True, debugging informations are printed to the log (Default: false)
   \param [in] logOptions Logging options (i.e. mprint mlogic symbolgen ...)
   \param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y)
   \param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y)
   \param [out] outExportResponseDs The output SAS dataset containing the export response information.
   \param [out] outVarToken Name of the output macro variable which will contain the access token (Default: accessToken)
   \param [out] outSuccess Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess)
   \param [out] outResponseStatus Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus)

   \details
   This macro sends a GET request to <b><i>\<host\>:\<port\>/riskCirrusObjects/objects/analysisData</i></b> and collects the results in the output table. \n
   See \link core_rest_request.sas \endlink for details about how to send GET requests and parse the response.


   <b>Example:</b>

   1) Set up the environment (set SASAUTOS and required LUA libraries).  Assumes the spre folder is under /riskcirruscore/core/code_libraries/release-core-2022.1.4
   \code
      %let core_root_path=/riskcirruscore/core/code_libraries/release-core-2022.1.4;
      option insert = (
         SASAUTOS = (
            "&core_root_path./spre/sas/ucmacros"
            )
         );
      filename LUAPATH ("&core_root_path./spre/lua");
   \endcode

   2) Send a Http GET request and parse the JSON response into the output table WORK.analysis_data
   \code
      %let accessToken=;
      %core_rest_export_analysis_data(outds = analysis_data
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
   \date    2018
*/
%macro core_rest_export_analysis_data(host =
                                      , server = riskData
                                      , port =
                                      , logonHost =
                                      , logonPort =
                                      , username =
                                      , password =
                                      , authMethod = bearer
                                      , client_id =
                                      , client_secret =
                                      , key =
                                      , locationType = LIBNAME
                                      , location = Public
                                      , fileName = analysis_data
                                      , replace = true
                                      , casScope =
                                      , casSessionId =
                                      , casSessionName =
                                      , outExportResponseDs = analysisdata_export_response
                                      , outVarToken = accessToken
                                      , outSuccess = httpSuccess
                                      , outResponseStatus = responseStatus
                                      , debug = false
                                      , logOptions =
                                      , restartLUA = Y
                                      , clearCache = Y
                                      );

   %local oldLogOptions drop_cas_table requestUrl engine;

   /* Set the required log options */
   %if(%length(&logOptions.)) %then
      options &logOptions.;
   ;

   /* Get the current value of mlogic and symbolgen options */
   %let oldLogOptions = %sysfunc(getoption(mlogic)) %sysfunc(getoption(symbolgen));

   %if(%sysevalf(%superq(key) eq, boolean)) %then %do;
      %put ERROR: key is required (the key of the analysis data object);
      %abort;
   %end;

   %if(%sysevalf(%superq(locationType) eq, boolean)) %then %do;
      %put ERROR: locationType is required (options are: DIRECTORY, LIBNAME, FOLDER);
      %abort;
   %end;

   %if(%sysevalf(%superq(location) eq, boolean)) %then %do;
      %put ERROR: location is required;
      %abort;
   %end;

   %if(%sysevalf(%superq(fileName) eq, boolean)) %then %do;
      %put ERROR: fileName is required;
      %abort;
   %end;

   %if "&locationType."="LIBNAME" %then
      %let engine = %rsk_get_lib_engine(&location.);

   %if %upcase("&casScope.") eq "SESSION" %then %do;

      /* for SESSION scope, we must have the current CAS session ID.  We can either get this directly (casSessionId) or from casSessionName.
      If not provided, riskData will export the table to its local cas session which will not be visible to this one or any others */
      %if "&casSessionName." eq "" and "&casSessionId." eq "" %then %do;
         %put ERROR: casSessionName or casSessionId is required when casScope is SESSION.;
         %abort;
      %end;

      /* If the casSesssionId was not provided, get it from the casSessionName */
      %if "&casSessionId." eq "" %then %do;
         proc cas;
            session &casSessionName.;
            session.sessionId result=res;
            symputx("casSessionId", res[1], "L");
            run;
         quit;
      %end;

      /* if the casSessionId is still missing, there is no point in proceeding since we won't be able to see the exported CAS table */
      %if "&casSessionId." eq "" %then %do;
         %put ERROR: casScope is SESSION but casSessionId could not be determined from casSessionName &casSessionName..;
         %abort;
      %end;

   %end;

   /* if we're replacing the CAS table and it's a global table, we need to delete it ourselves first (risk-data will not if it's global scope) */
   %let drop_cas_table = N;
   %if "&replace." = "true" and "&engine." ne "V9" %then %do;
      %if   (%upcase("&casScope.") = "GLOBAL")
         or (%upcase("&locationType") = "LIBNAME" and "&casSessionId" = "")
         or (%upcase("&locationType") = "DIRECTORY" or %upcase("&locationType") = "FOLDER") %then
         %let drop_cas_table=Y;
   %end;

   %if &drop_cas_table. = Y %then %do;

      %core_cas_drop_table(cas_session_name = &casSessionName.
                        , cas_session_id = &casSessionId.
                        , cas_libref = &location.
                        , cas_table = &fileName.);

      /* even if the table doesn't exist, if we send the request to risk-data with replace=true and scope=global, it will fail */
      /* so set replace=false here since we've already dropped the table */
      %let replace=false;

   %end;

   /* URL encoded to REST request */
   %let location=%sysfunc(urlencode(%bquote(&location.)));
   %let fileName=%sysfunc(urlencode(%bquote(&fileName.)));
   /* Set the base Request URL */
   %core_set_base_url(host=&host, server=&server., port=&port.);
   %let requestUrl = &baseUrl./&server./objects/&key./export?locationType=%upcase(&locationType)%str(&)location=&location.%str(&)fileName=&fileName.;

   %if(%sysevalf(%superq(replace) ne, boolean)) %then
      %let requestUrl = &requestUrl.%str(&)replace=&replace.;

   %if(%sysevalf(%superq(casScope) ne, boolean)) %then
      %let requestUrl = &requestUrl.%str(&)scope=%lowcase(&casScope.);

   %if(%sysevalf(%superq(casSessionId) ne, boolean)) %then
      %let requestUrl = &requestUrl.%str(&)sessionId=&casSessionId.;

   filename _resp temp;

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
                     , parser = sas.risk.cirrus.core_rest_parser.coreRestPlain
                     , outds = &outExportResponseDs.
                     , outVarToken = &outVarToken.
                     , outSuccess = &outSuccess.
                     , outResponseStatus = &outResponseStatus.
                     , debug = &debug.
                     , logOptions = &oldLogOptions.
                     , restartLUA = &restartLUA.
                     , clearCache = &clearCache.
                     , contentType = application/vnd.sas.job.execution.job+json
                     , fout = _resp
                     );
   libname _resp json fileref=_resp noalldata;

   /* Exit in case of errors */
   %if( (not &&&outSuccess..) or not(%rsk_dsexist(&outExportResponseDs)) or %rsk_attrn(&outExportResponseDs, nobs) = 0 ) %then %do;
      %put ERROR: Unable to export analysis data with key: &key.;
      data _null_;
         set _resp.root(keep=message);
         call symputx("resp_message",message);
      run;
      %put ERROR: &resp_message.;

      filename _resp clear;
      libname _resp clear;
      %abort;

   %end;

   /* Get Job Id for monitoring the execution status */
   data _null_;
         set _resp.root(keep=id);
         call symputx("jobId",id);
   run;

   %let jobState=;
   %core_rest_wait_job_execution(jobID = &jobId.
                                 , wait_flg = Y
                                 , pollInterval = 1
                                 , maxWait = 3600
                                 , timeoutSeverity = ERROR
                                 , outJobStatus = jobState
                                 , outVarToken = accessToken
                                 , outSuccess = httpSuccess
                                 , outResponseStatus = responseStatus
                                 , debug = false
                                 );

   /* Exit in case of errors */
   %if "&jobState." ne "COMPLETED" or (not &&&outSuccess..) %then %do;
      %put ERROR: Unable to export analysis data with key: &key.;
      %if(%sysevalf(%superq(outExportResponseDs) ne, boolean)) %then %do;
         %if(%rsk_dsexist(&outExportResponseDs.)) %then %do;
            data _null_;
               set &outExportResponseDs.;
               put response;
            run;
         %end;
      %end;
      %abort;
   %end;

%mend;
