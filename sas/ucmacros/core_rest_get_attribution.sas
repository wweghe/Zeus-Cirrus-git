/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor core_rest_get_attribution

   \brief   Retrieve Attribution Template instance(s) registered in SAS Risk Cirrus Objects

   \param [in] host (optional) Host url, including the protocol
   \param [in] server Name of the Web Application Server that provides the REST service (Default: riskCirrusObjects)
   \param [in] solution Solution identifier (Source system code) for Cirrus Core content packages (Default: currently blank)
   \param [in] port (optional) Server port
   \param [in] logonHost (Optional) Host/IP of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app host/ip is the same as the host/ip in the url parameter
   \param [in] logonPort (Optional) Port of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app port is the same as the port in the url parameter
   \param [in] username (optional) Username credentials
   \param [in] password (optional) Password credentials: it can be plain text or SAS-Encoded (it will be masked during execution).
   \param [in] authMethod: Authentication method (accepted values: BEARER). (Default: BEARER).
   \param [in] client_id The client id registered with the Viya authentication server. If blank, the internal SAS client id is used (only if GRANT_TYPE = password).
   \param [in] client_secret The secret associated with the client id.
   \param [in] key Instance key of the Cirrus object that is fetched with this REST request. If no Key is specified, the records are fetched using filter parameters.
   \param [in] filter Filters to apply on the GET request when no value for key is specified. Example: request GET /attributionTemplates?name=attributionTemplate1|attributionTemplate2&statusCd=Draft
   \param [in] start Specify the starting point of the records to get. Start indicate the starting index of the subset. Start SHOULD be a zero-based index. The default start SHOULD be 0. Applicable only when a filter is used.
   \param [in] limit Limit controls the maximum number of items to get from the start position (Default = 1000). Applicable only when a filter is used.
   \param [in] debug True/False. If True, debugging informations are printed to the log (Default: false)
   \param [in] logOptions Logging options (i.e. mprint mlogic symbolgen ...)
   \param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y)
   \param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y)
   \param [out] outds Name of the output table that contains the attribution template instances (Default: analysis_data)
   \param [out] outVarToken Name of the output macro variable which will contain the access token (Default: accessToken)
   \param [out] outSuccess Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess)
   \param [out] outResponseStatus Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus)

   \details
   This macro sends a GET request to <b><i>\<host\>:\<port\>/riskCirrusObjects/objects/attributionTemplates</i></b> and collects the results in the output table. \n
   See \link core_rest_request.sas \endlink for details about how to send GET requests and parse the response.


   <b>Example:</b>

   1) Set up the environment (set SASAUTOS and required LUA libraries).  Assumes the spre folder is under /riskcirruscore/core/code_libraries/release-core-{cadence-version}
   \code
      %let cadence_version=2023.05;
      %let core_root_path=/riskcirruscore/core/code_libraries/release-core-&cadence_version.;
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
      %core_rest_get_attribution(key = d0b17325-d99e-4635-8904-668f51328ff0
                                 , outds = attribution_analysis
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
   \date    2023
*/
%macro core_rest_get_attribution(host =
                              , server = riskCirrusObjects
                              , solution =
                              , port =
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
                              , outds = attribution_analysis
                              , outVarToken =accessToken
                              , outSuccess = httpSuccess
                              , outResponseStatus = responseStatus
                              , debug = false
                              , logOptions =
                              , restartLUA = Y
                              , clearCache = Y
                              );

   %local requestUrl ds_tmp table_nm;

   /* Set the required log options */
   %if(%length(&logOptions.)) %then
      options &logOptions.;
   ;

   /* Get the current value of mlogic and symbolgen options */
   %local oldLogOptions;
   %let oldLogOptions = %sysfunc(getoption(mlogic)) %sysfunc(getoption(symbolgen));

   /* Set the base request URL */
   %core_set_base_url(host=&host, server=&server., port=&port.);
   %let requestUrl = &baseUrl./&server./objects/attributionTemplates;

   /* Add filters to the request URL */
   %core_set_rest_filter(key=&key., solution=&solution., filter=%superq(filter), start=&start., limit=&limit.);

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
                     , parser = sas.risk.cirrus.core_rest_parser.coreRestAttributionAnalysis
                     , outds = &outds.
                     , arg1 = attribution_summary
                     , outVarToken = &outVarToken.
                     , outSuccess = &outSuccess.
                     , outResponseStatus = &outResponseStatus.
                     , debug = &debug.
                     , logOptions = &oldLogOptions.
                     , restartLUA = &restartLUA.
                     , clearCache = &clearCache.
                     );


   /* Exit in case of errors */
   %if(not &&&outSuccess.. or not %rsk_dsexist(&outds.) or %rsk_attrn(&outds., nlobs) eq 0)%then %do;
      %put ERROR: Failed to get the attribution template object info.;
      %abort;
   %end;

   %let ds_tmp = %rsk_get_unique_dsname(work);
   proc sql noprint undo_policy=none;
      create table &ds_tmp. as
      select a.*, 
            coalesce(b.attributionGroupName, a.attributionGroupName) as groupName  
      from &outds. as a LEFT JOIN
         (select distinct attributionKey, attributeName as attributionGroupName
         from &outds.) as b
      on a.attributionGroupKey = b.attributionKey
      where not missing(a.attributionGroupKey) 
      order by attributionGroupNo;
   quit;

   %if(%rsk_dsexist(&ds_tmp.) and %rsk_attrn(&ds_tmp., nlobs) eq %rsk_attrn(&outds., nlobs))%then %do;
      data &outds.(drop=groupName);
         set &ds_tmp.(where=(upcase(attributionType) ne "GROUP" and upcase(attributionType) ne "OTHER"));
         attributeName = attributionKey;
         attributionGroupName = groupName;
      run;
   %end;

   %if %rsk_dsexist(&ds_tmp.) %then %do;
      %let table_nm = %scan(&ds_tmp.,-1,%str(.));
      proc datasets library = work nolist nodetails;
         delete &table_nm.;
      quit;	
   %end;
    
%mend;
