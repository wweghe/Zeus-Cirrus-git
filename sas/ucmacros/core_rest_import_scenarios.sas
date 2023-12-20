/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor core_rest_import_scenarios

   \brief   Import Scenarios into SAS Risk Scenario Manager

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
   \param [in] tableFormat (BANKING, DATEBASED, EXPANDED) The scenarios will be written in the format specified; (Default: DATEBASED).
   \param [in] locationType The type of server location from which scenarios will be read (eg: 'LIBNAME' 'FOLDER' 'DIRECTORY')
   \param [in] location The server location to|from which scenarios will be read. (eg: When FOLDER, the display path of a SAS Drive folder containing the document to be read).
   \param [in] filename name of the excel file from which Scenarios and Scenarios Sets will be read. Only Excel type format is acceptable. (e.g. Scenarios - 202112.xlsx)
   \param [in] scenSetName when the parameter is filled, it refers to the spreadsheet with the Set scenario. Scenario Set receives the same spreadsheet name in RSM. Only Excel type format is acceptable and valid when load scenarios.
   \param [in] debug True/False. If True, debugging informations are printed to the log (Default: false)
   \param [in] logOptions Logging options (i.e. mprint mlogic symbolgen ...)
   \param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y)
   \param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y)
   \param [out] outds Name of the output table that contains the link_instance information (Default: link_instance)
   \param [out] outVarToken Name of the output macro variable which will contain the access token (Default: accessToken)
   \param [out] outSuccess Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess)
   \param [out] outResponseStatus Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus)

   \details
   This macro sends a POST request to <b><i>\<host\>:\<port\>/riskScenarios/scenarios?locationType=&location=&fileName=</i></b> and imports dateBased scenarios into Risk Scenario Manager.  \n
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

   <b>Sample input excel spreadsheet with scenario data. </b>

   |    forecastTime    |    date    |  AsOfDate  |  shock_factor  | <risk factor 1> | <risk factor 2> | <risk factor ...> | <risk factor n> |
   |--------------------|------------|------------|----------------|-----------------|-----------------|-------------------|-----------------|
   |          0         |2021-12-31  | 2021-12-31 |       1        | <numeric value> | <numeric value> |  <numeric value>  | <numeric value> |

   <b>Sample input excel spreadsheet for scenario __history__. </b>

   |    date    |  AsOfDate  |  shock_factor  | <risk factor 1> | <risk factor 2> | <risk factor ...> | <risk factor n> |
   |------------|------------|----------------|-----------------|-----------------|-------------------|-----------------|
   | 1983-09-30 | 2021-12-31 |        1       | <numeric value> | <numeric value> |  <numeric value>  | <numeric value> |

   <b>Sample input excel spreadsheet for scenarioSet. </b>

   |    scenario_name   |  weight  |  forecastTimeFlag  |
   |--------------------|----------|--------------------|
   | Basecase - 2021.12 | 3        |          Y         |

   2) Send a Http GET request and parse the JSON response into the output table work.scenarios_parsed
   \code
      %let accessToken =;
      %core_rest_import_scenarios(host = <host>
                                     , port = <port>
                                     , asOfDate =
                                     , locationType =
                                     , location =
                                     , filename =
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
%macro core_rest_import_scenarios(host =
                                 , port =
                                 , server = riskScenarios
                                 , logonHost =
                                 , logonPort =
                                 , username =
                                 , password =
                                 , authMethod = bearer
                                 , client_id =
                                 , client_secret =
                                 , asOfDate =  /* must be lower or equal to the first scenario horizon */
                                 , tableFormat = DATEBASED
                                 , locationType =
                                 , location =
                                 , filename =
                                 , scenSetName =
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

   %local
      listScenariosId
      listScenariosName
      listScenariosWeight
      listScenariosId
      listScenariosName
      listScenariosWeight
   ;

   /* Initialize outputs */
   %let &outVarToken. =;
   %let &outSuccess. = 0;
   %let &outResponseStatus. =;

   /* Set the required log options */
   %if(%length(&logOptions.)) %then
      options &logOptions.;
   ;

   /* Get the current value of mlogic and symbolgen options */
   %local oldLogOptions;
   %let oldLogOptions = %sysfunc(getoption(mlogic)) %sysfunc(getoption(symbolgen));

   /* Construct the parameters of endpoint URL (if applicable) and add them to the URL */
   %core_set_base_url(host=&host, server=&server., port=&port.);
   %let requestUrl =  &baseUrl./&server./scenarios;

   %if (%sysevalf(%superq(locationType) eq, boolean)) %then %do;
      %put ERROR: The 'locationType' parameter is required. Available values : DIRECTORY, LIBNAME, FOLDER;
      %abort;
   %end;

   %if (%sysevalf(%superq(location) eq, boolean)) %then %do;
      %put ERROR: The 'location' parameter is required.;
      %abort;
   %end;

   %if (%sysevalf(%superq(fileName) eq, boolean)) %then %do;
      %put ERROR: The 'filename' parameter is required.;
      %abort;
   %end;

   /* URL encoded to REST request */
   %let __location__=%sysfunc(urlencode(%bquote(&location.)));
   %let __fileName__=%sysfunc(urlencode(%bquote(&fileName.)));
   %let requestUrl = &requestUrl.?locationType=&locationType.%str(&)location=&__location__.%str(&)fileName=&__fileName__.;

   %if (%sysevalf(%superq(asOfDate) ne, boolean)) %then
      %let requestUrl = &requestUrl.%str(&)asOfDate=&asOfDate.;

   %if (%sysevalf(%superq(tableFormat) ne, boolean)) %then
      %let requestUrl = &requestUrl.%str(&)tableFormat=&tableFormat.;

   /* first Import the excel Set sheet from file Service. */
   FILENAME scenSet
         filesrvc
         folderpath="&location."
         filename="&filename."
         TERMSTR=LF;

   %if &SYSFILRC. = 1 %then %do;
      %put ERROR: The 'filename' does not exist.;
      %abort;
   %end;

   filename rsm_hin  temp;
   data _null_;
      file rsm_hin;
      put 'Accept:*/*';
   run;

   filename _scen temp;

   /* Temporary disable mlogic and symbolgen options to avoid printing of userid/pwd to the log */
   option nomlogic nosymbolgen;
   %core_rest_request(url = &requestUrl.
                     , method = POST
                     , logonHost = &logonHost.
                     , logonPort = &logonPort.
                     , username = &username.
                     , password = &password.
                     , authMethod = &authMethod.
                     , headerIn = rsm_hin
                     , contentType =
                     , fout = _scen
                     , parser =
                     , body =
                     , debug = &debug.
                     , logOptions = &logOptions.
                     , outds = &outds.
                     , outVarToken = &outVarToken.
                     , outSuccess = &outSuccess.
                     , outResponseStatus = &outResponseStatus.
                     , restartLUA = &restartLUA.
                     , clearCache = &clearCache.
                     );

   /* Exit in case of errors */
   %if(not &&&outSuccess..) %then %do;
      %put ERROR: Unable to import scenarios ;
      %if(%sysevalf(%superq(outds) ne, boolean)) %then %do;
         %if(%rsk_dsexist(&outds.)) %then %do;
            data _null_;
               set &outds.;
               put response;
            run;
         %end;
      %end;
      %abort;
   %end;
   %else %do;
            /* Load Scenarios Set if parameter 'scenSetName' is specified. */
            %if (%sysevalf(%superq(scenSetName) ne, boolean)) %then %do;
               /* Load scenarios Set with scenarios ID */

               /* Merge _scenset.scenarios with scenariosSets excel input to get weights */

               libname _scen json fileref=_scen noalldata;

               /**  Import the excel to a temporary table. **/
               PROC IMPORT DATAFILE=scenSet
                           DBMS=XLSX
                           OUT=WORK.import_scenariosSets
                           replace ;
                           GETNAMES=yes;
                           SHEET="&scenSetName.";
               run;

               %if ( %rsk_dsexist(work.import_scenariosSets) and %rsk_attrn(work.import_scenariosSets, nobs) ne 0) %then %do;

                  proc sql;
                  create table work.scenarios_sets as
                  select t1.id, t1.name
                        %if %rsk_varexist(work.import_scenariosSets, weight) > 0 %then %do;
                           ,case when missing(t2.weight) then ""
                             else cats(t2.weight)
                           end as weight length = 4
                        %end;
                        %else %do;
                           ,"" as weight length = 4
                        %end;

                        %if %rsk_varexist(work.import_scenariosSets, forecastTimeFlag) > 0 %then %do;
                           ,case when missing(t2.forecastTimeFlag) then ""
                             else cats(t2.forecastTimeFlag)
                           end as forecastTimeFlag length = 25
                        %end;
                        %else %do;
                           ,"" as forecastTimeFlag length = 25
                        %end;
                  from _scen.scenarios as t1, work.import_scenariosSets as t2
                     where t1.name = t2.scenario_name
                  ;quit;

                  proc sql noprint;
                     select id, name, weight, forecastTimeFlag
                           into
                              :listScenariosId separated by ',',
                              :listScenariosName separated by ',',
                              :listScenariosWeight separated by ',',
                              :forecastTimeFlag trimmed
                     from work.scenarios_sets
                     where id is not missing
                  ;
                  quit;

                  %let &outSuccess. = 0;
                  %core_rest_import_scenarios_sets(host =
                                                , port =
                                                , server = riskScenarios
                                                , logonHost = &logonHost.
                                                , logonPort = &logonPort.
                                                , username = &username.
                                                , password = &password.
                                                , authMethod = bearer
                                                , client_id = &client_id.
                                                , client_secret = &client_secret.
                                                , asOfDate = &asOfDate.
                                                , scenSetName = &scenSetName.
                                                , listScenariosId = %bquote(&listScenariosId.)
                                                , listScenariosWeight = %bquote(&listScenariosWeight.)
                                                , forecastTimeFlag = %bquote(&forecastTimeFlag.)
                                                , shockSet = false
                                                , outds = work.scenarios_set
                                                , outVarToken = &outVarToken.
                                                , outSuccess = &outSuccess.
                                                , outResponseStatus = &outResponseStatus.
                                                , debug = false
                                                , logOptions =
                                                , restartLUA = Y
                                                , clearCache = Y
                                                );

               %end; /*  %if ( %rsk_dsexist(work.scenariosSets) and %rsk_attrn(work.scenariosSets, nobs) ne 0) */
               %else %do;
                  %put WARNING: No scenario Sets available in filename.;
                  %return;
               %end;
            %end;
      %end; /*%if ( (&&&outSuccess..) or (%rsk_dsexist(_scen.scenarios)) or %rsk_attrn(_scen.scenarios, nobs) <> 0 ) */

%mend core_rest_import_scenarios;