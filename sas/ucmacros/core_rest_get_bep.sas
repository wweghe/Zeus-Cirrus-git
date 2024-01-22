/*
 Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor core_rest_get_bep

   \brief   Retrieve Business Evolution Plan(BEP) instance(s) registered in SAS Risk Cirrus Objects

   \param [in] host (optional) Host url, including the protocol
   \param [in] port (Optional) Server port.
   \param [in] server Name of the Web Application Server that provides the REST service (Default: riskCirrusObjects)
   \param [in] solution Solution identifier (Source system code) for Cirrus Core content packages (Default: currently blank)
   \param [in] logonHost (Optional) Host/IP of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app host/ip is the same as the host/ip in the url parameter
   \param [in] logonPort (Optional) Port of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app port is the same as the port in the url parameter
   \param [in] username (optional) Username credentials
   \param [in] password (optional) Password credentials: it can be plain text or SAS-Encoded (it will be masked during execution).
   \param [in] authMethod: Authentication method (accepted values: BEARER). (Default: BEARER).
   \param [in] client_id The client id registered with the Viya authentication server. If blank, the internal SAS client id is used (only if GRANT_TYPE = password).
   \param [in] client_secret The secret associated with the client id.
   \param [in] key Instance key of the Cirrus object that is fetched with this REST request. If no Key is specified, the records are fetched using filter parameters.
   \param [in] filter Filters to apply on the GET request when no value for key is specified. Example: request GET /businessEvolutionPlans?name=Data1|Data2&statusCd=Draft
   \param [in] start Specify the starting point of the records to get. Start indicate the starting index of the subset. Start SHOULD be a zero-based index. The default start SHOULD be 0. Applicable only when a filter is used.
   \param [in] limit Limit controls the maximum number of items to get from the start position (Default = 1000). Applicable only when a filter is used.
   \param [in] inDsLinkTypes (optional) a SAS dataset containing link type information from GET /linkTypes.  If not provided, the macro will get the link type information.
   \param [in] debug True/False. If True, debugging informations are printed to the log. (Default: false)
   \param [in] logOptions Logging options. (i.e. mprint mlogic symbolgen ...)
   \param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y. (Default: Y)
   \param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y)
   \param [out] outds Name of the output table that contains the BEP instances. (Default: bep_summary)
   \param [out] outds_details Name of the output table that contains the BEP spreadsheet data. (Default: bep_details)
   \param [out] outds_link_instances Name of the output table that contains the BEP link instances. (Default: bep_link_instances)
   \param [out] outds_target_vars Name of the output table that contains the BEP target portfolio data.   \param [out] outVarToken Name of the output macro variable which will contain the access token. (Default: accessToken)
   \param [out] outSuccess Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess)
   \param [out] outResponseStatus Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus)

   \details
   This macro sends a GET request to <b><i>\<host\>:\<port\>/riskCirrusObjects/objects/businessEvolutionPlans</i></b> and collects the results in the output tables. \n
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

   2) Send a Http GET request and parse the JSON response into the output table work.bep_summary and work.bep_details
   \code
      %let accessToken =;
      %core_rest_get_bep(key = f5cbf66a-1b06-4df7-914e-f108ae9c80a8
                           , outds = work.bep_summary
                           , outds_details = bep_details
                           , outds_link_instances = bep_link_instances
                           , outds_target_vars = bep_target_vars
                           , debug = true
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
   \date    2023
*/
%macro core_rest_get_bep(host =
                        , port =
                        , server = riskCirrusObjects
                        , solution =
                        , logonHost =
                        , logonPort =
                        , username =
                        , password =
                        , authMethod = bearer
                        , client_id =
                        , client_secret =
                        , key =
                        , filter =
                        , start =
                        , limit = 1000
                        , inDsLinkTypes =
                        , outds = bep_summary
                        , outds_details = bep_details
                        , outds_link_instances = bep_link_instances
                        , outds_target_vars =
                        , outVarToken = accessToken
                        , outSuccess = httpSuccess
                        , outResponseStatus = responseStatus
                        , debug = false
                        , logOptions =
                        , restartLUA = Y
                        , clearCache = Y
                        );

   %local requestUrl link_types_ds;

   /* Set the required log options */
   %if(%length(&logOptions.)) %then
      options &logOptions.;
   ;

   /* Get the current value of mlogic and symbolgen options */
   %local oldLogOptions;
   %let oldLogOptions = %sysfunc(getoption(mlogic)) %sysfunc(getoption(symbolgen));

   /* Set the base request URL */
   /* Note: objectLinks is not returned by default for objects in a collection-level request.  to add that field to the
   default fields that come, add ?includeFields=objectLinks to the URL */
   %core_set_base_url(host=&host, server=&server., port=&port.);
   %let requestUrl = &baseUrl./&server./objects/businessEvolutionPlans?includeFields=objectLinks;

   /* Add filters to the request URL */
   %core_set_rest_filter(key=&key., solution=&solution., filter=%superq(filter), start=&start., limit=&limit.);

   /* Get the BEP link types, if needed */
   %if(%sysevalf(%superq(inDsLinkTypes) eq, boolean)) %then %do;

      %let link_types_ds=work.bep_link_types;

      %core_rest_get_link_types(host = &host.
                                 , port = &port.
                                 , solution = &solution.
                                 , logonHost = &logonHost.
                                 , logonPort = &logonPort.
                                 , username = &username.
                                 , password = &password.
                                 , authMethod = &authMethod.
                                 , client_id = &client_id.
                                 , client_secret = &client_secret.
                                 , filter = in(objectId,%27businessEvolutionPlan_planningData%27,%27businessEvolutionPlan_hierarchyData%27,%27businessEvolutionPlan_auxiliaryData%27)
                                 , outds = &link_types_ds.
                                 , outVarToken = &outVarToken.
                                 , outSuccess = &outSuccess.
                                 , outResponseStatus = &outResponseStatus.
                                 , debug = &debug.
                                 , logOptions = &LogOptions.
                                 , restartLUA = &restartLUA.
                                 , clearCache = &clearCache.
                                 );

      /* Exit in case of errors */
      %if(not &&&outSuccess.. or not %rsk_dsexist(bep_link_types)) %then %do;
         %put ERROR: Unable to get the BEP link types;
         %abort;
      %end;

   %end;
   %else
      %let link_types_ds=&inDsLinkTypes.;

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
                     , parser = sas.risk.cirrus.core_rest_parser.coreRestBusinessEvolution
                     , outds = &outds.
                     , arg1 = &link_types_ds.
                     , arg2 = &outds_details.
                     , arg3 = &outds_link_instances.
                     , arg4 = &outds_target_vars.
                     , outVarToken = &outVarToken.
                     , outSuccess = &outSuccess.
                     , outResponseStatus = &outResponseStatus.
                     , debug = &debug.
                     , logOptions = &oldLogOptions.
                     , restartLUA = &restartLUA.
                     , clearCache = &clearCache.
                     );

   /* Exit in case of errors */
   %if(not &&&outSuccess.. or not %rsk_dsexist(&outds.)) %then %do;
      %put ERROR: Unable to get the business evolution plan;
      %abort;
   %end;

%mend core_rest_get_bep;