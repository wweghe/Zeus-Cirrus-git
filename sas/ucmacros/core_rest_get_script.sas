/*
Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/**
\file
\anchor core_rest_get_script

\brief   Retrieve the script registered in SAS Risk Cirrus Objects

\param [in] host (optional) Host url, including the protocol
\param [in] port (optional) Server port (Default: 443)
\param [in] server Name that provides the REST service (Default: riskCirrusObjects)
\param [in] logonHost (Optional) Host/IP of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app host/ip is the same as the host/ip in the url parameter
\param [in] logonPort (Optional) Port of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app port is the same as the port in the url parameter
\param [in] username (optional) Username credentials
\param [in] password (optional) Password credentials: it can be plain text or SAS-Encoded (it will be masked during execution).
\param [in] authMethod: Authentication method (accepted values: BEARER). (Default: BEARER).
\param [in] client_id The client id registered with the Viya authentication server. If blank, the internal SAS client id is used (only if GRANT_TYPE = password).
\param [in] client_secret The secret associated with the client id.
\param [in] key Instance key of the Cirrus object that is fetched with this REST request. If no Key is specified, the records are fetched using filter parameters.
\param [in] sourceSystemCd The source system code to assign to the object when registering it in Cirrus Objects (Default: 'blank').
\param [in] solution The solution short name from which this request is being made. This will get stored in the createdInTag and sharedWithTags attributes on the object (Default: 'blank').
\param [in] filter Filters to apply on the GET request when no value for key is specified. (e.g. eq(objectId,'<script objectId>' | and(eq(name,'script name'),eq(modifiedBy,'sasadm')) )
\param [in] start Specify the starting point of the records to get. Start indicate the starting index of the subset. Start SHOULD be a zero-based index. The default start SHOULD be 0. Applicable only when a filter is used.
\param [in] limit Limit controls the maximum number of items to get from the start position (Default = 1000). Applicable only when a filter is used.
\param [in] debug True/False. If True, debugging informations are printed to the log (Default: false)
\param [in] logOptions Logging options (i.e. mprint mlogic symbolgen ...)
\param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y)
\param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y)
\param [out] outds_scriptInfo Name of the output table that contains script main info (Default: _tmp_script_info_)
\param [out] outds_scriptCustomFields Name of the output table that contains script custom fields info (Default: _tmp_script_customfields_)
\param [out] outVarToken Name of the output macro variable which will contain the access token (Default: accessToken)
\param [out] outSuccess Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess)
\param [out] outResponseStatus Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus)

\details
This macro sends a GET request to <b><i>\<host\>:\<port\>/riskCirrusObjects/objects/&collectionName./</i></b> and collects the results in the output table. \n
See \link core_rest_request.sas \endlink for details about how to send GET requests and parse the response.

<b>Example:</b>

1) Set up the environment (set SASAUTOS and required LUA libraries).  Assumes the spre folder is under /riskcirruscore/core/code_libraries/release-core-{cadence-version}
\code
   %let cadence_version=2023.07;
   %let core_root_path=/riskcirruscore/core/code_libraries/release-core-&cadence_version.;
   option insert = (
      SASAUTOS = (
         "&core_root_path./spre/sas/ucmacros"
         )
      );
   filename LUAPATH ("&core_root_path./spre/lua");
\endcode

2) Send a Http GET request and parse the JSON response into the output table work.configuration_sets
\code
   %let accessToken =;
   %core_rest_get_script(solution = ECL
                        , sourceSystemCd =
                        , filter =
                        , start = 0
                        , limit = 100
                        , logSeverity = WARNING
                        , outds_scriptInfo = work._tmp_script_info_
                        , outds_scriptCustomFields = work._tmp_script_customfields_
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

<b>Sample outds_scriptInfo:</b>

|                  key                  |           objectId         | sourceSystemCd |            name            |           description                                       |     creationTimeStamp    |      modifiedTimeStamp   | createdBy | engineCd | typeCd     | statusCd  |modifiedBy | createdInTag | mediaTypeVersion |
|---------------------------------------|----------------------------|----------------|----------------------------|-------------------------------------------------------------|--------------------------|--------------------------|-----------|----------|------------|-----------|-----------|--------------|------------------|
| f9b3ecef-3dab-4700-a3d9-9b41abc81336  | ECL-002-DataPrep-2023.07   | ECL            | ECL-002-DataPrep-2023.07   | Creates an Analysis Run to perform the following steps: ... | 2022-07-02T15:37:48.044Z | 2022-07-02T15:37:48.926Z | sasadm    | SAS      | PROD       |sasadm     |           | CORE         | 1                |


<b>Sample outds_scriptCustomFields:</b>

|                  scriptKey            |           code                   | language   |
|---------------------------------------|----------------------------------|------------|
| f9b3ecef-3dab-4700-a3d9-9b41abc81336  | <sintaxe code for the engineCd>  | SAS        |

\ingroup coreRestUtils

\author  SAS Institute Inc.
\date    2023
*/


%macro core_rest_get_script(host =
                           , port =
                           , server = riskCirrusObjects
                           , logonHost =
                           , logonPort =
                           , username =
                           , password =
                           , authMethod = bearer
                           , client_id =
                           , client_secret =
                           , key =
                           , sourceSystemCd =
                           , solution =
                           , filter =
                           , start =
                           , limit = 1000
                           , logSeverity = WARNING
                           , outds_scriptInfo = work.configuration_sets
                           , outds_scriptCustomFields = work._tmp_script_customfields_
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
   customFilter
   object_key
   ;

   /* Set the required log options */
   %if(%length(&logOptions.)) %then
   options &logOptions.;
   ;

   /* Get the current value of mlogic and symbolgen options */
   %local oldLogOptions;
   %let oldLogOptions = %sysfunc(getoption(mlogic)) %sysfunc(getoption(symbolgen));

   /* Set the base request URL */
   %core_set_base_url(host=&host, server=&server., port=&port.);
   %let requestUrl = &baseUrl./&server./objects/scripts;


   /* Add filters to the request URL */
   %if (%sysevalf(%superq(sourceSystemCd) ne, boolean)) %then %do;
      %let customFilter = eq(sourceSystemCd,%27&sourceSystemCd.%27);
   %end;
   %if (%sysevalf(%superq(filter) ne, boolean) ) %then %do;
      %if (%sysevalf(%superq(sourceSystemCd) ne, boolean)) %then %do;
         %let customFilter = and(&filter.,eq(sourceSystemCd,%27&sourceSystemCd.%27));
      %end;
      %else %do;
               %let customFilter = &filter.;
         %end;
   %end;
   %core_set_rest_filter(key=&key., solution=&solution., customFilter=&customFilter., start=&start., limit=&limit.);

   filename rScript temp;

   /* Temporary disable mlogic and symbolgen options to avoid printing of userid/pwd to the log */
   option nomlogic nosymbolgen;
   /* Send the REST request */
   %core_rest_request(url = &requestUrl.
                  , method = GET
                  , logonHost = &logonHost.
                  , logonPort = &logonPort.
                  , username = &username.
                  , password = &password.
                  , authMethod = &authMethod
                  , client_id = &client_id.
                  , client_secret = &client_secret.
                  , fout = rScript
                  , parser = sas.risk.cirrus.core_rest_parser.coreRestScripts
                  , outds = &outds_scriptInfo.
                  , arg1 = &outds_scriptCustomFields.
                  , outVarToken = &outVarToken.
                  , outSuccess = &outSuccess.
                  , outResponseStatus = &outResponseStatus.
                  , debug = &debug.
                  , logOptions = &oldLogOptions.
                  , restartLUA = &restartLUA.
                  , clearCache = &clearCache.
                  );

   /* Throw an error and exit if the configuration table request fails */
   %if ((not &&&outSuccess..) or not(%rsk_dsexist(&outds_scriptInfo.))) %then %do;
      %put ERROR: The request to get the configurationTable failed.;
      %abort;
   %end;

%mend core_rest_get_script;
