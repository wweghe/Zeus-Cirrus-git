/*
Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/**
\file
\anchor core_rest_get_config_set

\brief   Retrieve the configuration sets registered in SAS Risk Cirrus Objects

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
\param [in] configSetId Object Id filter to apply on the GET request when a value for key is specified.
\param [in] filter Filters to apply on the GET request when no value for key is specified. (e.g. eq(objectId,'ConfigSet-2022.1.4' | and(eq(name,'Configuration Set for Core'),eq(modifiedBy,'sasadm')) )
\param [in] start Specify the starting point of the records to get. Start indicate the starting index of the subset. Start SHOULD be a zero-based index. The default start SHOULD be 0. Applicable only when a filter is used.
\param [in] limit Limit controls the maximum number of items to get from the start position (Default = 1000). Applicable only when a filter is used.
\param [in] debug True/False. If True, debugging informations are printed to the log (Default: false)
\param [in] logOptions Logging options (i.e. mprint mlogic symbolgen ...)
\param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y)
\param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y)
\param [out] outds Name of the output table that contains the allocation schemes (Default: link_types)
\param [out] outVarToken Name of the output macro variable which will contain the access token (Default: accessToken)
\param [out] outSuccess Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess)
\param [out] outResponseStatus Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus)

\details
This macro sends a GET request to <b><i>\<host\>:\<port\>/riskCirrusObjects/objects/&collectionName./</i></b> and collects the results in the output table. \n
See \link core_rest_request.sas \endlink for details about how to send GET requests and parse the response.

<b>Example:</b>

1) Set up the environment (set SASAUTOS and required LUA libraries).  Assumes the spre folder is under /riskcirruscore/core/code_libraries/release-core-{cadence-version}
\code
   %let cadence_version=2022.10;
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
   %core_rest_get_config_set(SourceSystemCd =
                                 , configSetId = ConfigSet-2022.1.4
                                 , filter =
                                 , start = 0
                                 , limit = 100
                                 , logSeverity = WARNING
                                 , outds = work.configuration_sets
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

<b>Sample output:</b>

|                  key                  |           objectId         | sourceSystemCd |            name            |           description        |     creationTimeStamp    |      modifiedTimeStamp   | createdBy | modifiedBy | createdInTag | mediaTypeVersion |
|---------------------------------------|----------------------------|----------------|----------------------------|------------------------------|--------------------------|--------------------------|-----------|------------|--------------|------------------|
| b904693c-235e-4f05-a9a1-f2aa4cdbdc2d  | ConfigSet-2022.1.4         | RCC            | Configuration Set for Core | Config set for Core solution | 2022-07-02T15:37:48.044Z | 2022-07-02T15:37:48.926Z | sasadm    | sasadm     | CORE         | 1                |

\ingroup coreRestUtils

\author  SAS Institute Inc.
\date    2022
*/


%macro core_rest_get_config_set(host =
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
                                 , sourceSystemCd = RCC
                                 , solution =
                                 , configSetId =                       /* Config Set Id to filter i.e ConfigSet-2022.1.4 */
                                 , filter =                            /* add any other filters */
                                 , start =
                                 , limit = 1000
                                 , logSeverity = WARNING
                                 , outds = work.configuration_sets
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

   %if(%sysevalf(%superq(configSetId) ne, boolean)) %then %do;
      %if (%sysevalf(%superq(sourceSystemCd) eq, boolean)) %then %do;
         %put ERROR: Parameter 'sourceSystemCd' is required when parameter configSetId is provided;
         %abort;
      %end;
   %end;

   /* Set the base request URL */
   %core_set_base_url(host=&host, server=&server., port=&port.);
   %let requestUrl = &baseUrl./&server./objects/configurationSets;

   /* Add filters to the request URL */
   %if(%sysevalf(%superq(configSetId) ne, boolean)) %then
      %let customFilter = and(eq(objectId,%27&configSetId.%27),eq(sourceSystemCd,%27&sourceSystemCd.%27));
   %core_set_rest_filter(key=&key., solution=&solution., filter=%superq(filter), customFilter=&customFilter., start=&start., limit=&limit.);

   filename respSets temp;

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
                  , fout = respSets
                  , parser = sas.risk.cirrus.core_rest_parser.coreRestConfigSet
                  , outds = &outds.
                  , outVarToken = &outVarToken.
                  , outSuccess = &outSuccess.
                  , outResponseStatus = &outResponseStatus.
                  , debug = &debug.
                  , logOptions = &oldLogOptions.
                  , restartLUA = &restartLUA.
                  , clearCache = &clearCache.
                  );

   %if ((not &&&outSuccess..) or not(%rsk_dsexist(&outds.))) %then %do;
      %put ERROR: the request to get the configurationSet failed.;
      %abort;
   %end;

%mend core_rest_get_config_set;
