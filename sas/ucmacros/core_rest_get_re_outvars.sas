/*
 Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor core_rest_get_re_outvars

   \brief   Return the output variables from Risk Engine (project|pipeline).

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
   \param [in] reProjectKey Key of the Risk Engine project. Applicable only when filter is used.
   \param [in] reOutVarLevel Name that refers the re object to retrieve the output variables. Used when reProjectKey is provided and working now only for type 'project' (Default: project)
   \param [in] filter Filters to apply on the GET '/outputVariables' request when no value for reProjectKey is specified.
   \param [in] start Specify the starting point of the records to get. Start indicate the starting index of the subset. Start SHOULD be a zero-based index. The default start SHOULD be 0. Applicable only when filter is used.
   \param [in] limit Limit controls the maximum number of items to get from the start position (Default = 1000). Applicable only when filter is used.
   \param [in] outds Name of the output table that contains the Risk reOutVarLevel summary (Default: risk_reOutVarLevel_summary).
   \param [in] debug True/False. If True, debugging informations are printed to the log (Default: false)
   \param [in] logOptions Logging options (i.e. mprint mlogic symbolgen ...)
   \param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y)
   \param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y)
   \param [out] reLevelOutputVariables Name of the ouput macro variable that contains the RE Project list of output variables.
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
     %core_rest_get_re_outvars(reProjectKey = 92054448-2c6f-4233-8fe9-c8c498c5661b
                                , reOutVarLevel = project
                                , outVarToken = accessToken
                                , outSuccess = httpSuccess
                                , outResponseStatus = responseStatus
                                , debug = false
                                );
     %put &=accessToken;
     %put &=httpSuccess;
     %put &=responseStatus;
   \endcode

   \ingroup coreRestUtils

   \author  SAS Institute Inc.
   \date    2023
*/
%macro core_rest_get_re_outvars(host =
                               , port =
                               , server = riskPipeline
                               , logonHost =
                               , logonPort =
                               , username =
                               , password =
                               , authMethod = bearer
                               , client_id =
                               , client_secret =
                               , reProjectKey =
                               , reOutVarLevel = project
                               , reLevelOutputVariables = re_project_output_variables
                               , filter =
                               , start =
                               , limit = 1000
                               , outds = risk_&reOutVarLevel._summary
                               , outVarToken = accessToken
                               , outSuccess = httpSuccess
                               , outResponseStatus = responseStatus
                               , debug = false
                               , logOptions =
                               , restartLUA = Y
                               , clearCache = Y
                               );

   %local
      fref
      items
      libref
      oldLogOptions
      requestUrl
      resp_message
   ;

   /* Set the required log options */
   %if(%length(&logOptions.)) %then
      options &logOptions.;
   ;

   /* Get the current value of mlogic and symbolgen options */
   %let oldLogOptions = %sysfunc(getoption(mlogic)) %sysfunc(getoption(symbolgen));

   /* Validate input parameters */
   %if(%sysevalf(%superq(reProjectKey) eq, boolean) and %sysevalf(%superq(filter) eq, boolean)) %then %do;
      %put ERROR: Both reProjectKey and filter parameters are empty. Please provide a value for at least one of the parameters.;
      %return;
   %end;

   %if(%sysevalf(%superq(reProjectKey) ne, boolean) and %sysevalf(%superq(filter) ne, boolean)) %then %do;
      %put ERROR: Both reProjectKey and filter parameters have been provided, but only one is allowed. Please remove one of the parameters.;
      %return;
   %end;

   /* Determine the base url */
   %core_set_base_url(host=&host, server=&server., port=&port.);

   %let requestUrl = &baseUrl./&server./outputVariables;

   /* Get a key list for returned pipeline(s) when filter is used */
   %if( %sysevalf(%superq(reProjectKey) ne, boolean) ) %then %do;
      /* Add project id to the request URL */
      %let requestUrl = &requestUrl.?&reOutVarLevel..id=%superq(reProjectKey);
      /* Set Start and Limit options */
      %if(%sysevalf(%superq(start) ne, boolean)) %then
         %let requestUrl = &requestUrl.%str(&)start=&start.;
      %if(%sysevalf(%superq(limit) ne, boolean)) %then
         %let requestUrl = &requestUrl.%str(&)limit=&limit.; 
   %end;
   %else %do;
            %if(%sysevalf(%superq(filter) ne, boolean)) %then %do;
               /* Add filters to the request URL */
               %core_set_rest_filter(filter=%superq(filter), start=&start., limit=&limit.);
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
                        , outds = &outds.
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

      %if(%rsk_varexist(&items., name)) %then %do;
         /* Declare the output variable as global if it does not exist */
         %if(not %symexist(&reLevelOutputVariables.)) %then
         %global &reLevelOutputVariables.;

         proc sql noprint;
            select name into :&reLevelOutputVariables. separated by " "
            from &items.
            ;
         quit;
      %end;

      filename &fref. clear;
      libname &libref. clear;

%mend core_rest_get_re_outvars;