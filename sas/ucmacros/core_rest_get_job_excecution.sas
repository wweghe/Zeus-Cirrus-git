/*
 Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor core_rest_get_job_excecution

   \brief   Retrieve the Job Id and the endpoint execution log registered in SAS Job Execution Service

   \param [in] host (Optional) Host url, including the protocol.
   \param [in] port (Optional) Server port.
   \param [in] server Name that provides the REST service (Default: jobExecution)
   \param [in] logonHost (Optional) Host/IP of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app host/ip is the same as the host/ip in the url parameter.
   \param [in] logonPort (Optional) Port of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app port is the same as the port in the url parameter.
   \param [in] username Username credentials.
   \param [in] password Password credentials: it can be plain text or SAS-Encoded (it will be masked during execution).
   \param [in] authMethod: Authentication method (accepted values: BEARER). (Default: BEARER).
   \param [in] client_id The client id registered with the Viya authentication server. If blank, the internal SAS client id is used (only if GRANT_TYPE = password).
   \param [in] client_secret The secret associated with the client id.
   \param [in] solution The solution short name from which this request is being made. This will get stored in the createdInTag and sharedWithTags attributes on the object (Default: 'blank').
   \param [in] jobId The jobId key for script execution in Job Execution from which this request is being made.
   \param [in] filter Filters to apply on the GET request when no value for key is specified (e.g. eq(createdBy,'sasadm') | and(eq(name,'datastore_config'),eq(createdBy,'sasadm')) ).
   \param [in] debug True/False. If True, debugging informations are printed to the log (Default: false).
   \param [in] start Specify the starting point of the records to get. Start indicate the starting index of the subset. Start SHOULD be a zero-based index. The default start SHOULD be 0. Applicable only when a filter is used.
   \param [in] limit Limit controls the maximum number of items to get from the start position (Default = 1000). Applicable only when a filter is used.
   \param [in] logOptions Logging options (i.e. mprint mlogic symbolgen ...)
   \param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y).
   \param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y).
   \param [out] outds_jobExecutionInfo Name of the table
   \param [out] outds_jobExecutionResults
   \param [out] outVarToken Name of the output macro variable which will contain the access token (Default: accessToken).
   \param [out] outSuccess Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess).
   \param [out] outResponseStatus Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus).

   \details
   This macro sends a GET request to <b><i>\<host\>:\<port\>/jobExecution/jobs/</i></b> and collects the results in the output table. \n
   See \link core_rest_request.sas \endlink for details about how to send GET requests and parse the response.

   <b>Example:</b>

   1) Set up the environment (set SASAUTOS and required LUA libraries).  Assumes the spre folder is under /riskcirruscore/core/code_libraries/release-core-{cadence-version}
   \code
      %let cadence_version=2022.11;
      %let core_root_path=/riskcirruscore/core/code_libraries/release-core-&cadence_version.;
      option insert = (
         SASAUTOS = (
            "&core_root_path./spre/sas/ucmacros"
            )
         );
      filename LUAPATH ("&core_root_path./spre/lua");
   \endcode

   2) Send a Http GET request and parse the JSON response into the output table work.configuration_tables
   \code
      %let accessToken =;
      %core_rest_get_job_excecution(host = <host>
                                  , port = <port>
                                  , server = jobExecution
                                  , logonHost =
                                  , logonPort =
                                  , username =
                                  , password =
                                  , authMethod = bearer
                                  , client_id =
                                  , client_secret =
                                  , solution =
                                  , jobId =
                                  , filter =
                                  , start = 0
                                  , limit = 100
                                  , logSeverity = WARNING
                                  , outds_jobExecutionInfo = work.job_execution_info
                                  , outds_jobExecutionResults = work.job_execution_results
                                  , outVarToken = accessToken
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
   \endcode

   <b>Sample output OUTDS_JOB_EXECUTION_INFO: </b>

   |      if                              |    state   |  submittedByApplication  | logLocation                                         |
   |--------------------------------------|------------|------------------------- |-----------------------------------------------------|
   | e9f984c0-02bb-474a-b1da-ad7c0d36f0a8 | completed  |       riskCirrusCor      | /files/files/ed9cee12-8d34-4f10-ae0f-7673083612d0   |

   <b>Sample output OUTDS_JOB_EXECUTION_RESULTS: </b>

   |dataParamKey           | dataParamValues          |
   |----------------------|---------------------------|
   |/files/files/464d67b6-60ee-4c03-91f0-ee2caf2badd8 |

   \ingroup rgfRestUtils

   \author  SAS Institute Inc.
   \date    2022
*/

%macro core_rest_get_job_excecution(host =
                                  , port =
                                  , server = jobExecution
                                  , logonHost =
                                  , logonPort =
                                  , username =
                                  , password =
                                  , authMethod = bearer
                                  , client_id =
                                  , client_secret =
                                  , solution =
                                  , jobId =
                                  , filter =                                     /* Any other global filters to json attributes */
                                  , start =
                                  , limit = 1000
                                  , logSeverity = WARNING
                                  , outds_jobExecutionInfo = work.job_execution_info         /* job execution table info */
                                  , outds_jobExecutionResults = work.job_execution_results   /* job execution log */
                                  , outVarToken = accessToken
                                  , outSuccess = httpSuccess
                                  , outResponseStatus = responseStatus
                                  , debug = false
                                  , logOptions =
                                  , restartLUA = Y
                                  , clearCache = Y
                                  );


   %local
   requestUrl
   ;

   /* Set the required log options */
   %if(%length(&logOptions.)) %then
   options &logOptions.;
   ;

   /* Get the current value of mlogic and symbolgen options */
   %local oldLogOptions;
   %let oldLogOptions = %sysfunc(getoption(mlogic)) %sysfunc(getoption(symbolgen));

   %if %sysevalf(%superq(jobId) eq, boolean) %then %do;
      %put ERROR: Parameter job Id is required;
      %abort;
   %end;

   /* Determine the base url */
   %core_set_base_url(host=&host, server=&server., port=&port.);
   %let requestUrl = &baseUrl./&server./jobs;

   %if(%sysevalf(%superq(jobId) ne, boolean)) %then
      /* Request the specified resource by the id */
      %let requestUrl = &requestUrl./&jobId.;
   %else %do;
      /* Add filters to the request URL */
      %core_set_rest_filter(filter=%superq(filter), start=&start., limit=&limit.);
   %end;

   filename respJobs temp;

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
                    , client_id = &client_id.
                    , client_secret = &client_secret.
                    , fout = respJobs
                    , parser = sas.risk.cirrus.core_rest_parser.coreRestJobId
                    , outds = &outds_jobExecutionInfo.
                    , arg1 = &outds_jobExecutionResults.
                    , outVarToken = &outVarToken.
                    , outSuccess = &outSuccess.
                    , outResponseStatus = &outResponseStatus.
                    , debug = &debug.
                    , logOptions = &oldLogOptions.
                    , restartLUA = &restartLUA.
                    , clearCache = &clearCache.
                    );

   /* Clear references if we're not debugging */
   %if %upcase(&debug) ne TRUE %then %do;
      filename respJobs clear;
   %end;

%mend core_rest_get_job_excecution;