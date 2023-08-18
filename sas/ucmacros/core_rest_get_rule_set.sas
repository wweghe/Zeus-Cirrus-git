/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file 
\anchor core_rest_get_rule_set

   \brief   Retrieve the Rule set(s) registered in SAS Risk Cirrus Objects

   \param [in] host (Optional) Host url, including the protocol.
   \param [in] port (Optional) Server port.
   \param [in] server Name that provides the REST service (Default: riskCirrusObjects)
   \param [in] logonHost (Optional) Host/IP of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app host/ip is the same as the host/ip in the url parameter.
   \param [in] logonPort (Optional) Port of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app port is the same as the port in the url parameter.
   \param [in] username Username credentials.
   \param [in] password Password credentials: it can be plain text or SAS-Encoded (it will be masked during execution).
   \param [in] authMethod: Authentication method (accepted values: BEARER). (Default: BEARER).
   \param [in] client_id The client id registered with the Viya authentication server. If blank, the internal SAS client id is used (only if GRANT_TYPE = password).
   \param [in] client_secret The secret associated with the client id.
   \param [in] key Instance key of the Cirrus object that is fetched with this REST request. If no Key is specified, the records are fetched using filter parameters.
   \param [in] solution The solution short name from which this request is being made. This will get stored in the createdInTag and sharedWithTags attributes on the object (Default: 'blank').
   \param [in] filter Filters to apply on the GET request when no value for key is specified (e.g. eq(createdBy,'sasadm') | and(eq(name,'datastore_config'),eq(createdBy,'sasadm')) ).
   \param [in] start Specify the starting point of the records to get. Start indicate the starting index of the subset. Start SHOULD be a zero-based index. The default start SHOULD be 0. Applicable only when a filter is used.
   \param [in] limit Limit controls the maximum number of items to get from the start position (Default = 1000). Applicable only when a filter is used.
   \param [in] ruleSetId Object Id filter to apply on the GET request.
   \param [in] debug True/False. If True, debugging informations are printed to the log (Default: false).
   \param [in] logOptions Logging options (i.e. mprint mlogic symbolgen ...)
   \param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y).
   \param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y).
   \param [out] outds_ruleSetInfo Name of the output table that contains attributes of requested ruleset(s).
   \param [out] outds_ruleSetData Name of the output table that contains rows of rules of requested ruleset(s).
   \param [out] outVarToken Name of the output macro variable which will contain the access token (Default: accessToken).
   \param [out] outSuccess Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess).
   \param [out] outResponseStatus Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus).

   \details
   This macro sends a GET request to <b><i>\<host\>:\<port\>/riskCirrusObjects/objects/&collectionName.</i></b> and collects the results in the output table. \n
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

   2) Send a Http GET request and parse the JSON response into the output table WORK.rule_set
   \code
      %let accessToken =;

      %core_rest_get_rule_set(
                                ruleSetId = <rule_set_id>
                                , outds_ruleSetInfo = work.ruleset_info        
                                , outds_ruleSetData = work.ruleset_data        
                                , outVarToken = accessToken
                                , outSuccess = httpSuccess
                                , outResponseStatus = responseStatus
                                );

      %put &=accessToken;
      %put &=httpSuccess;
      %put &=responseStatus;
   \endcode

   <b>Sample output outds_ruleSetData :</b>

   | lookup_key | key     | operator | aggr_group_by_vars | rule_desc              | primary_key               | lookup_data | rule_details | parenthesis | rule_reporting_lev1  | rule_id     | lookup_table | rule_component | rule_name              | rule_type        | rule_reporting_lev2 | aggr_var_nm | rule_weight | ruleSetKey | ruleSetType    | column_nm              | aggr_expression_txt | rule_reporting_lev3 | aggregated_rule_flg | message_txt                                              | description | name | objectId | target_table | type |
   |------------|---------|----------|--------------------|------------------------|---------------------------|-------------|--------------|-------------|----------------------|-------------|--------------|----------------|------------------------|------------------|---------------------|-------------|-------------|------------|----------------|------------------------|---------------------|---------------------|---------------------|----------------------------------------------------------|-------------|------|----------|--------------|------|
   |            | #000001 |          |                    | Check Missing Currency | ENTITY_ID INSTID INSTTYPE |             |              |             | Completeness         | PTF_RULE_01 |              | CONDITION      | Check Missing Currency | MISSING          | Currency            |             | 1           | 10000      | BUSINESS_RULES | CURRENCY               |                     | Missing             |                     | CURRENCY cannot be missing.                              |             |      |          |              |      |
   |            | #000002 |          |                    | Check Currency Length  | ENTITY_ID INSTID INSTTYPE |             | 3            |             | Accuracy & Integrity | PTF_RULE_02 |              | CONDITION      | Check Currency Length  | NOT_FIXED_LENGTH | Currency            |             | 1           | 10000      | BUSINESS_RULES | CURRENCY               |                     | Fixed Length        |                     | CURRENCY is not a 3-character variable.                  |             |      |          |              |      |
   |            | #000003 |          |                    | Check Collateral Flag  | ENTITY_ID INSTID INSTTYPE |             | "Y", "N"     |             | Accuracy & Integrity | PTF_RULE_03 |              | CONDITION      | Check Collateral Flag  | NOT_LIST         | Counterparty Status |             | 1           | 10000      | BUSINESS_RULES | COLLATERAL_SUPPORT_FLG |                     | Not In List         |                     | COLLATERAL_SUPPORT_FLG must be set to either "Y" or "N". |             |      |          |              |      |

   \ingroup rgfRestUtils

   \author  SAS Institute Inc.
   \date    2023
*/
%macro core_rest_get_rule_set(host =
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
                                , solution =
                                , filter =                                     /* Any other global filters to json attributes */
                                , start =
                                , limit = 1000
                                , ruleSetId =                                   /* ruleSet - objectId to filter, a simplier way to request for a single ruleset */
                                , outds_ruleSetInfo = work.ruleset_info         /* ruleset table metainfo */
                                , outds_ruleSetData = work.ruleset_data         /* table containig rules in rows format combiined with ruleset info */
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
   %let requestUrl = &baseUrl./&server./objects/ruleSets;


   /* Add filters to the request URL */
   %if (%sysevalf(%superq(ruleSetId) ne, boolean)) %then
      %let customFilter=eq(objectId,%27&ruleSetId%27);

   %core_set_rest_filter(key=&key., solution=&solution., filter=%superq(filter), customFilter=%superq(customFilter), start=&start., limit=&limit.); 

   filename respTabs temp;
   %let &outSuccess. = 0;
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
                    , fout = respTabs
                    , parser = sas.risk.cirrus.core_rest_parser.coreRestRuleSet
                    , outds = &outds_ruleSetInfo.
                    , arg1 = &outds_ruleSetData.
                    , outVarToken = &outVarToken.
                    , outSuccess = &outSuccess.
                    , outResponseStatus = &outResponseStatus.
                    , debug = &debug.
                    , logOptions = &oldLogOptions.
                    , restartLUA = &restartLUA.
                    , clearCache = &clearCache.
                    );

   %if ( (not &&&outSuccess..) or
      not(%rsk_dsexist(&outds_ruleSetInfo.)) or
      %rsk_attrn(&outds_ruleSetInfo., nobs) = 0 or
      not(%rsk_dsexist(&outds_ruleSetData.)) or
      %rsk_attrn(&outds_ruleSetData., nobs) = 0 )
   %then %do;
      %put ERROR: The request to get the ruleSet table failed.;
      %abort;
   %end;

   /* Clear references if we're not debugging */
   %if %upcase(&debug) ne TRUE %then %do;
      filename respTabs clear;
   %end;

%mend core_rest_get_rule_set;
