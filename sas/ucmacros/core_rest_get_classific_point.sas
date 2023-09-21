   /*
   Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
   */

/**
\file
   \anchor core_rest_get_classific_point
   \brief   Retrieve the classification point(s) from SAS Risk Cirrus Objects

   \param [in] host (Optional) Host url, including the protocol
   \param [in] port (Optional) Server port.
   \param [in] LogonPort (Optional) port where the /SASLogon web application is listening. If blank, it is assumed that the /SASLogon is on the same port as /SASIRMServer (Default: blank)
   \param [in] server Name of the Web Application Server that provides the REST service (Default: SASRiskGovernanceFramework)
   \param [in] sourceSystemCd The source system code to assign to the object when registering it in Cirrus Objects (Default: 'blank').
   \param [in] solution The solution short name from which this request is being made. This will get stored in the createdInTag and sharedWithTags attributes on the object (Default: 'blank').
   \param [in] username Username credentials
   \param [in] password Password credentials: it can be plain text or SAS-Encoded (it will be masked during execution).
   \param [in] authMethod: Authentication method (accepted values: BEARER). (Default: BEARER)
   \param [in] client_id The client id registered with the Viya authentication server. If blank, the internal SAS client id is used (only if GRANT_TYPE = password).
   \param [in] client_secret The secret associated with the client id.
   \param [in] key Instance key of the Cirrus object that is fetched with this REST request. If no Key is specified, the records are fetched using filter parameters\param [in] restPath Path object to search for the classification key (i.e. models, analysisData, cycles)
   \param [in] filter Filters to apply on the GET request when no value for key is specified. Example: request GET /cycles?name=Cycle1|cycle2&entityId=Bank1
   \param [in] start Specify the starting point of the records to get. Start indicate the starting index of the subset. Start SHOULD be a zero-based index. The default start SHOULD be 0. Applicable only when filter is used.
   \param [in] limit Limit controls the maximum number of items to get from the start position (Default = 1000). Applicable only when filter is used.
   \param [in] debug True/False. If True, debugging informations are printed to the log (Default: false)
   \param [in] logOptions Logging options (i.e. mprint mlogic symbolgen ...)
   \param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y)
   \param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y)
   \param [out] outds Name of the output table that contains the location information (Default: dimensional_points)
   \param [out] outVarToken Name of the output macro variable which will contain the access token (Default: accessToken).
   \param [out] outSuccess Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess)
   \param [out] outResponseStatus Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus)

   \details
   This macro sends a GET request to <b><i>\<host\>:\<port\>/riskCirrusObjects/objects/<restPath></i></b> and collects the results in the output table. \n
   See \link core_rest_request.sas \endlink for details about how to send GET requests and parse the response.

   <b>Example:</b>

   1) Set up the environment (set SASAUTOS and required LUA libraries).  Assumes the spre folder is under /riskcirruscore/core/code_libraries/release-core-{cadence-version}
   \code
      %let cadence_version=2022.12;
      %let core_root_path=/riskcirruscore/core/code_libraries/release-core-&cadence_version.;
      option insert = (
         SASAUTOS = (
            "&core_root_path./spre/sas/ucmacros"
            )
         );
      filename LUAPATH ("&core_root_path./spre/lua");
   \endcode

   2) Send a Http GET request and parse the JSON response into the output table work.dimensional_points
   \code
      %let accessToken =;
      %core_rest_get_classific_point( host = <host>
                              , port =  <port>
                              , server = riskCirrusObjects
                              , authMethod = bearer
                              , key = <key>
                              , restPath = models
                              , filter =
                              , start =
                              , limit = 1000
                              , outds = work.classification_points
                              , outVarToken = accessToken
                              , outSuccess = httpSuccess
                              , outResponseStatus = responseStatus
                              , debug = true
                              , restartLUA = Y
                              , clearCache = Y
                        );

      %put &=httpSuccess;
      %put &=responseStatus;
   \endcode

   <b>Sample output: outds</b>

   | objectKey                            | objectDescription | objectId          | sourceSystemCd  | createdInTag | classificationSeq | classificationKey                    |
   |--------------------------------------|-------------------|-------------------|-----------------|--------------|-------------------|--------------------------------------|
   | 5904af27-0d75-4f6a-b7d9-4e1502704b8f | AR_porbsr_test_05 | AR_porbsr_test_05 | RCC             | ECL          | 1                 | 855794fd-d049-4f8c-ad68-486ee69c91d7 |

   \ingroup coreRestUtils

   \author  SAS Institute Inc.
   \date    2022
*/

%macro core_rest_get_classific_point( host =
                                          , port =
                                          , LogonHost =
                                          , LogonPort =
                                          , server = riskCirrusObjects
                                          , sourceSystemCd =
                                          , solution =
                                          , username =
                                          , password =
                                          , authMethod = bearer
                                          , client_id =
                                          , client_secret =
                                          , key =
                                          , restPath =
                                          , filter =
                                          , start =
                                          , limit = 1000
                                          , outds = work.classification_points
                                          , outVarToken = accessToken
                                          , outSuccess = httpSuccess
                                          , outResponseStatus = responseStatus
                                          , debug = false
                                          , logOptions =
                                          , restartLUA = Y
                                          , clearCache = Y
                                          );
   %local requestUrl;

   /* Set the required log options */
   %if(%length(&logOptions.)) %then
      options &logOptions.;
   ;

   /* Get the current value of mlogic and symbolgen options */
   %local oldLogOptions;
   %let oldLogOptions = %sysfunc(getoption(mlogic)) %sysfunc(getoption(symbolgen));

   %core_set_base_url(host=&host, server=&server., port=&port.);
   %let requestUrl = &baseUrl./&server./objects/&restPath.;

   %if(%sysevalf(%superq(key) ne, boolean)) %then %do;
   /* Request the specified resource by the key */
      %let requestUrl = &requestUrl./&key.;
   %end;
   %else %do;
   	/* Add filters to the request URL */
   	%core_set_rest_filter(solution=&solution., filter=%superq(filter), start=&start., limit=&limit.);
   %end;

   filename rClassPt temp;

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
                        , fout = rClassPt
                        , parser = sas.risk.cirrus.core_rest_parser.coreRestClassifications
                        , outds = &outds.
                        , outVarToken = &outVarToken.
                        , outSuccess = &outSuccess.
                        , outResponseStatus = &outResponseStatus.
                        , debug = &debug.
                        , logOptions = &oldLogOptions.
                        , restartLUA = &restartLUA.
                        , clearCache = &clearCache.
                        );

   %if %upcase(&debug) ne TRUE %then %do;
   filename rClassPt CLEAR;
   %end;

   /* Exit in case of errors */
   %if(not &&&outSuccess..) %then %do;
      %put WARNING: Unable to get classification point.;
      %if(%rsk_dsexist(&outds.)) %then %do;
         data _null_;
            set &outds.;
            put response;
         run;
      %end;
   %end;

%mend core_rest_get_classific_point;