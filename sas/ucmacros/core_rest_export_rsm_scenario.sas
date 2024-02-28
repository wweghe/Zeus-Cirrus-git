/*
 Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor core_rest_export_rsm_scenario

   \brief   Export Scenarios registered in SAS Risk Scenario Manager to a CAS table or a CSV file in the files service

   \param [in] host (optional) Host url, including the protocol
   \param [in] server Name of the Web Application Server that provides the REST service (Default: riskCirrusObjects)
   \param [in] port (optional) Server port (Default: 443)
   \param [in] logonHost (Optional) Host/IP of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app host/ip is the same as the host/ip in the url parameter
   \param [in] logonPort (Optional) Port of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app port is the same as the port in the url parameter
   \param [in] username (optional) Username credentials
   \param [in] password (optional) Password credentials: it can be plain text or SAS-Encoded (it will be masked during execution).
   \param [in] authMethod: Authentication method (accepted values: BEARER). (Default: BEARER).
   \param [in] client_id The client id registered with the Viya authentication server. If blank, the internal SAS client id is used (only if GRANT_TYPE = password).
   \param [in] client_secret The secret associated with the client id.
   \param [in] scenarioIdsArray A string array of the Scenario IDs to be exported with this REST request.  If not provided, scenarioCollection must be provided
   \param [in] scenarioCollection A JSON string or fileref pointing to a JSON scenarios collection body.  The scenarios in this body will be retrieved with this REST request.  If not provided, scenarioIdsArray must be provided
   \param [in] baselineId (optional) The ID of a value-based scenario that is optionally used when exporting shocks. The indicated scenario defines a baseline that can be used to compute the value of shocks which are expressed as absolute or relative changes.
   \param [in] includeScenarioHistory (optional) (true/false) When true, include any historical data associated with the scenarios. Historical data is any value in a value-based scenario that occurs before the scenario's asOfDate. When false, only values associated with dates after the scenario's asOfDate will be included (Default: true)
   \param [in] dateBasedFormat (true/false) When true, the scenarios will be written in Date Based format; when false or omitted, the scenarios will be written in Expanded format (Default: true)
   \param [in] locationType (LIBNAME or FOLDER).
      -If LIBNAME, the scenarios are exported to the CAS libref/table specified in the &outScenariosCasLib./&outScenariosDs. parameters
      -If FOLDER, the scenarios are exported to the files service folder/file specified in the &outScenariosFilePath./&outScenariosFile. parameters
   \param [in] casSessionName CAS session name that gets started before the export request is performed.  (Default: casauto)
   \param [in] replaceOutScenarios (Y\N).  If Y, the &outScenariosCasLib./&outScenariosDs CAS table or &outScenariosFilePath./&outScenariosFile. file is first removed
      (from memory and from disk) before the export request is made.  Note that if N, the request will fail (with 400 error code) if the table or file already exists.  (Default: Y)
   \param [in] debug True/False. If True, debugging informations are printed to the log (Default: false)
   \param [in] logOptions Logging options (i.e. mprint mlogic symbolgen ...)
   \param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y)
   \param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y)
   \param [out] outScenariosCasLib Name of the output CAS lib the scenarios are exported to (required if locationType=LIBNAME) (Ex: Public)
   \param [out] outScenariosDs Name of the CAS output table the scenarios are exported to (required if locationType=LIBNAME) (Ex: scenarios)
   \param [out] outScenariosFilePath Display name of the folder the scenarios are exported to (required if locationType=FOLDER) (Ex: /Public)
   \param [out] outScenariosFile Name of the file scenarios are exported to in the files service (required if locationType=FOLDER) (Ex: scenarios.csv)
   \param [out] outExportResponseDs Name of the output SAS table holding the REST results of the export request.
   \param [out] outVarToken Name of the output macro variable which will contain the access token (Default: accessToken)
   \param [out] outSuccess Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess)
   \param [out] outResponseStatus Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus)

   \details
   This macro sends a POST request to <b><i>\<host\>:\<port\>/riskScenarios/scenarios/export</i></b> to export scenarios to a CAS table. \n
   See \link core_rest_request.sas \endlink for details about how to send GET requests and parse the response.


   <b>Example:</b>

   1) Set up the environment (set SASAUTOS and required LUA libraries).  Assumes the spre folder is under /riskcirruscore/core/code_libraries/release-core-{cadence-version}
   \code
      %let core_root_path=/riskcirruscore/core/code_libraries/release-core-%sysget(SAS_RISK_CIRRUS_CADENCE);
      option insert = (
         SASAUTOS = (
            "&core_root_path./spre/sas/ucmacros"
            )
         );
      filename LUAPATH ("&core_root_path./spre/lua");
   \endcode

   2) Send a Http POST request to export the scenarios in scenarioIdsArray into the Public.scenarios CAS table
   \code
  %core_rest_export_rsm_scenario(scenarioIdsArray = %bquote(["b0b563d3-f45c-4e9a-a44b-694aa36b38a0","f32f37b5-7b1f-4306-aff9-0eded8b95086","30c3b3d0-8f27-4b62-9ce2-f13bd6e2f451"])
                              , locationType = LIBNAME
                              , outScenariosCasLib = Public
                              , outScenariosDs = scenarios
                              , debug = true
                              , outSuccess = httpSuccess
                              , outResponseStatus = responseStatus
                              );
      %put &=httpSuccess;
      %put &=responseStatus;
   \endcode

   <b>Sample output:</b>

   \ingroup coreRestUtils

   \author  SAS Institute Inc.
   \date    2022
*/

%macro core_rest_export_rsm_scenario(host =
                                    , server = riskScenarios
                                    , port =
                                    , logonHost =
                                    , logonPort =
                                    , username =
                                    , password =
                                    , authMethod = bearer
                                    , client_id =
                                    , client_secret =
                                    , scenarioIdsArray =
                                    , scenarioCollection =
                                    , baselineId =
                                    , includeScenarioHistory = true
                                    , dateBasedFormat = false
                                    , locationType = LIBNAME
                                    , outScenariosCasLib = casuser
                                    , outScenariosDs = scenarios
                                    , outScenariosFilePath = /Public
                                    , outScenariosFile = scenarios.csv
                                    , outExportResponseDs = scenarios_export_response
                                    , casSessionName =
                                    , replaceOutScenarios = Y
                                    , outVarToken = accessToken
                                    , outSuccess = httpSuccess
                                    , outResponseStatus = responseStatus
                                    , debug = false
                                    , logOptions =
                                    , restartLUA = Y
                                    , clearCache = Y
                                    );

   %local oldLogOptions requestUrl reqBody contentType headerIn location fileName scenFileRef;

   /* Set the required log options */
   %if(%length(&logOptions.)) %then
      options &logOptions.;
   ;

   /* Get the current value of mlogic and symbolgen options */
   %let oldLogOptions = %sysfunc(getoption(mlogic)) %sysfunc(getoption(symbolgen));

   %let locationType=%upcase(&locationType.);
   %if "&locationType" = "LIBNAME" %then %do;

      %if (%sysevalf(%superq(outScenariosCasLib) eq, boolean)) or (%sysevalf(%superq(outScenariosDs) eq, boolean)) %then %do;
         %put ERROR: Parameters 'outScenariosCasLib' and 'outScenariosDs' are required if locationType=LIBNAME;
         %abort;
      %end;

      %if "&replaceOutScenarios"= "Y" %then %do;

         /* Delete the CAS table and source file if they exist */
         %core_cas_drop_table(cas_session_name = &casSessionName.
                              , cas_libref = &outScenariosCasLib.
                              , cas_table = &outScenariosDs.);

      %end;

      %let location=&outScenariosCasLib.;
      %let fileName=&outScenariosDs.;

   %end;
   %else %if "&locationType" = "FOLDER" %then %do;

      %if (%sysevalf(%superq(outScenariosFilePath) eq, boolean)) or (%sysevalf(%superq(outScenariosFile) eq, boolean)) %then %do;
         %put ERROR: Parameters 'outScenariosFilePath' and 'outScenariosFile' are required if locationType=FOLDER;
         %abort;
      %end;

      %if "&replaceOutScenarios."= "Y" %then %do;

         /* Delete the scenario file from the files service if it exists */
         %let scenFileRef = %rsk_get_unique_ref(prefix=scen, type=Fileref, engine=filesrvc, args=folderPath="&outScenariosFilePath." filename="&outScenariosFile.");
         %if %sysfunc(fexist(&scenFileRef.)) %then %do;
            %if %sysfunc(fdelete(&scenFileRef.)) ne 0 %then
               %put ERROR: Failed to delete scenario file from the file service: &outScenariosFilePath./&outScenariosFile.;
         %end;

      %end;

      %let location=&outScenariosFilePath.;
      %let fileName=&outScenariosFile.;

   %end;
   %else %do;
      %put ERROR: locationType must be either LIBNAME or FOLDER.  Provided value is: &locationType.;
      %abort;
   %end;

   /* Determine the base url */
   %core_set_base_url(host=&host, server=&server., port=&port.);
   %let requestUrl = &baseUrl./&server./scenarios/export;

   /* URL encoded to REST request */
   %let location=%sysfunc(urlencode(%bquote(&location.)));
   %let fileName=%sysfunc(urlencode(%bquote(&fileName.)));

   /* Set the query parameters */
   %let requestUrl = &requestUrl.?locationType=&locationType.%str(&)location=&location.%str(&)fileName=&fileName.;

   %if(%sysevalf(%superq(baselineId) ne, boolean)) %then
      %let requestUrl = &requestUrl.%str(&)baselineId=&baselineId.;

   %if(%sysevalf(%superq(includeScenarioHistory) ne, boolean)) %then
      %let requestUrl = &requestUrl.%str(&)includeScenarioHistory=%lowcase(&includeScenarioHistory.);

   %if(%sysevalf(%superq(dateBasedFormat) ne, boolean)) %then
      %let requestUrl = &requestUrl.%str(&)dateBasedFormat=%lowcase(&dateBasedFormat.);

   /* Set the request body */
   %if(%sysevalf(%superq(scenarioIdsArray) ne, boolean)) %then %do;
      %let reqBody = &scenarioIdsArray.;
      %let contentType = application/json;
      %let headerIn = Accept: application/json;
   %end;
   %else %if(%sysevalf(%superq(scenarioCollection) ne, boolean)) %then %do;
      %let reqBody = &scenarioCollection.;
      %let contentType = application/vnd.sas.collection+json;
      %let headerIn = %str(Accept: application/json;Content-Item: application/vnd.sas.risk.scenario+json);
   %end;
   %else %do;
      %put ERROR: scenarioIdsArray or scenarioCollection must be provided;
      %abort;
   %end;

   /* Temporary disable mlogic and symbolgen options to avoid printing of userid/pwd to the log */
   option nomlogic nosymbolgen;
   /* Send the REST request to export the scenarios to CAS*/
   %core_rest_request(url = &requestUrl.
                     , method = POST
                     , logonHost = &logonHost.
                     , logonPort = &logonPort.
                     , username = &username.
                     , password = &password.
                     , authMethod = &authMethod.
                     , headerIn = &headerIn.
                     , body = &reqBody.
                     , contentType = &contentType.
                     , outds = &outExportResponseDs.
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
      %put ERROR: Unable to export scenarios with ids: &scenarioIdsArray.;
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
