/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor core_rest_import_scenarios_sets

   \brief   Import Scenarios Sets into SAS Risk Scenario Manager

   \param [in] host Host url, including the protocol
   \param [in] server Name of the Web Application Server that provides the REST service (Default: riskCirrusObjects)
   \param [in] solution Solution identifier (Source system code) for Cirrus Core content packages (Default: currently blank)
   \param [in] port (Optional) Server port.
   \param [in] logonHost (Optional) Host/IP of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app host/ip is the same as the host/ip in the url parameter.
   \param [in] logonPort (Optional) Port of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app port is the same as the port in the url parameter.
   \param [in] username Username credentials.
   \param [in] password Password credentials: it can be plain text or SAS-Encoded (it will be masked during execution).
   \param [in] authMethod: Authentication method (accepted values: BEARER). (Default: BEARER).
   \param [in] client_id The client id registered with the Viya authentication server. If blank, the internal SAS client id is used (only if GRANT_TYPE = password).
   \param [in] client_secret The secret associated with the client id.
   \param [in] asOfDate Provides the start date from which interval based horizon dates will be computed. Date format: 2022-12-31.
   \param [in] scenSetName Name to assign to the Set scenario in RSM.
   \param [in] listScenariosId List of scenarios Ids keys. (list with comma separated)
   \param [in] listScenariosWeight List of the weights per scenarioId in 'listScenariosId'. Numeric value and/or admits value null when no weight specified. (i.e. '0.4,null,0.6' | '0.4,,0.6')
   \param [in] forecastTimeFlag (Y/N) value stored as a custom attribute in the scenario set.  Needed for ST divergent scenarios only.
   \param [in] debug True/False. If True, debugging informations are printed to the log (Default: false)
   \param [in] logOptions Logging options (i.e. mprint mlogic symbolgen ...)
   \param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y)
   \param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y)
   \param [out] outds Name of the output table that contains the link_instance information (Default: link_instance)
   \param [out] outVarToken Name of the output macro variable which will contain the access token (Default: accessToken)
   \param [out] outSuccess Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess)
   \param [out] outResponseStatus Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus)

   \details
   This macro sends a POST request to <b><i>\<host\>:\<port\>//riskScenarios/scenarioSets</i></b> and creates a scenario set in RSM  \n
   See \link core_rest_request.sas \endlink for details about how to send POST requests and parse the response.
   \n
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

   2) Send a Http GET request and parse the JSON response into the output table WORK.link_instance
   \code
      %let accessToken =;
      %core_rest_import_scenarios_sets(host = <host>
                                     , port = <port>
                                     , asOfDate =
                                     , listScenariosId = 77b02c82-aed5-4bdc-8373-38f3c00af3c4,6223b630-c030-4347-a2c4-a2821f15ee9b
                                     , listScenariosWeight = 1,3,6 | null,null,null
                                     , forecastTimeFlag =
                                     , scenSetName =
                                     , outds = work.scenarios_parsed
                                     , outVarToken = accessToken
                                     , outSuccess = httpSuccess
                                     , outResponseStatus = responseStatus
                                     );
      %put &=accessToken;
      %put &=httpSuccess;
      %put &=responseStatus;
   \endcode

   \ingroup coreRestUtils

   \author  SAS Institute Inc.
   \date    2022
*/
%macro core_rest_import_scenarios_sets(host =
                                       , port =
                                       , server = riskScenarios
                                       , logonHost =
                                       , logonPort =
                                       , username =
                                       , password =
                                       , authMethod = bearer
                                       , client_id =
                                       , client_secret =
                                       , asOfDate =                                 /* must be lower or equal to the first scenario horizon */
                                       , scenSetName =
                                       , listScenariosId =                          /* comma separated */
                                       , listScenariosWeight =                      /* comma separated */
                                       , forecastTimeFlag =                         /* Y/N value */
                                       , shockSet = false
                                       , outds = work.scenarios
                                       , outVarToken = accessToken
                                       , outSuccess = httpSuccess
                                       , outResponseStatus = responseStatus
                                       , debug = false
                                       , logOptions =
                                       , restartLUA = Y
                                       , clearCache = Y
                                       );

   /* Set the required log options */
   %if(%length(&logOptions.)) %then
      options &logOptions.;
   ;

   /* Initialize outputs */
   %let &outSuccess. = 0;
   %let &outResponseStatus. =;

   /* Get the current value of mlogic and symbolgen options */
   %local oldLogOptions;
   %let oldLogOptions = %sysfunc(getoption(mlogic)) %sysfunc(getoption(symbolgen));

   /* Construct the parameters of endpoint URL (if applicable) and add them to the URL */
   %core_set_base_url(host=&host, server=&server., port=&port.);
   %let requestUrl =  &baseUrl./&server./scenariosSets;

   %if (%sysevalf(%superq(asOfDate) ne, boolean)) %then %do;
      %let requestUrl = &requestUrl.%str(&)asOfDate=&asOfDate.;
   %end;
   %else %do;
      %put ERROR: AsOfDate date is required.;
      %abort;
   %end;

   %if (%sysevalf(%superq(scenSetName) eq, boolean)) %then %do;
      %put ERROR: The name of the Scenario Set is required.;
      %abort;
   %end;

   %if(%sysevalf(%superq(listScenariosId) eq, boolean)) %then %do;
      %put ERROR: Scenario(s) Key(s) required.;
      %abort;
   %end;

   /* First, verify that the scenario set with this name and as-of-date does not exist.  This is necessary
   to avoid 409 errors in the subsequent POST request to create the scenario set */
   %core_rest_get_rsm_scenario_set(host = &host.
                                 , port = &port.
                                 , logonHost = &logonHost.
                                 , logonPort = &logonPort.
                                 , username = &username.
                                 , password = &password.
                                 , authMethod = &authMethod.
                                 , client_id = &client_id.
                                 , client_secret = &client_secret.
                                 , filter = %bquote(filter=and(eq(name,"&scenSetName."), eq(asOfDate,"&asOfDate.")))
                                 , details_flg = N
                                 , outds = scenario_sets
                                 , outSuccess = &outSuccess.
                                 , outVarToken = &outVarToken.
                                 , debug = &debug.
                                 );

   /* Exit in case of errors */
   %if(not &&&outSuccess..) %then %do;
      %put ERROR: Unable to determine if the scenario set with name &scenSetName. and as-of date &asOfDate. already exists;
      %abort;
   %end;

   /* Gracefully exit if the scenario set already exists */
   %if %rsk_getattr(scenario_sets, NOBS) ne 0 %then %do;
      %put NOTE: Scenario set with name &scenSetName. and as-of date &asOfDate. already exists - skipping import.;
      %return;
   %end;

   /* Set the request body */
   filename bodySet temp;

   data _null_;
      length str $200;
      file bodySet;
      put '{';
      str =  '"name": "'||strip("&scenSetName.") ||'"'; put str;
      str =  ',"description": "'||strip("&scenSetName.") ||'"'; put str;
      str =  ',"asOfDate": "'||strip("&asOfDate.") ||'"'; put str;
      str =  ',"shockSet": "'||strip("&shockSet.") ||'"'; put str;
      put ',"customAttributes": {';
         if "&forecastTimeFlag." ne '' then do;
           str = '"forecastTimeFlag":' || quote("&forecastTimeFlag.") || '}'; put str;
         end;
         else do;
           str = '}'; put str;
         end;
      put ',"scenarios": [';
      i = 1;
         do while (strip(scan(strip("&listScenariosId."),i,',')) ne '');
            put '{';
            str = '"scenarioId": "'||strip(scan(strip("&listScenariosId."),i,','))||'"'; put str;
            if strip(scan(strip("&listScenariosWeight."),i,',','m')) eq "" then do;
             str =  ',"weight": null'; put str;
            end;
            else do;
                  str =  ',"weight": '||strip(scan(strip("&listScenariosWeight."),i,',','m'))||''; put str;
            end;
            i=i+1;
            if strip(scan(strip("&listScenariosId."),i,',')) ne "" then do;
               str = '},'; put str;
            end;
            else do;
               str = '}'; put str;
            end;
         end;
      put ']';
      put '}';
   run;

   %let requestUrl = &baseUrl./&server./scenarioSets;

   filename _scenset temp;

   %let contentType = application/json;
   %let headerIn = Accept: application/vnd.sas.risk.scenario.set+json;

   /* Temporary disable mlogic and symbolgen options to avoid printing of userid/pwd to the log */
   option nomlogic nosymbolgen;
   %core_rest_request(url = &requestUrl.
                     , method = POST
                     , logonHost = &logonHost.
                     , logonPort = &logonPort.
                     , username = &username.
                     , password = &password.
                     , authMethod = &authMethod.
                     , headerIn = &headerIn
                     , contentType = &contentType.
                     , body = bodySet
                     , fout = _scenset
                     , outds = &outds.
                     , debug = &debug.
                     , logOptions = &logOptions.
                     , outVarToken = &outVarToken.
                     , outSuccess = &outSuccess.
                     , outResponseStatus = &outResponseStatus.
                     , restartLUA = &restartLUA.
                     , clearCache = &clearCache.
                     );

   /* Exit in case of errors */
   %if(not &&&outSuccess..) %then %do;
      %put ERROR: Unable to import Scenario Set.;
      %if(%rsk_dsexist(&outds.)) %then %do;
         data _null_;
            set &outds.;
            put response;
         run;
      %end;
      %abort;
   %end;
%mend core_rest_import_scenarios_sets;