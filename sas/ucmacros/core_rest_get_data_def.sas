/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor core_rest_get_data_def

   \brief   Retrieve the Data Definition(s) registered in SAS Risk Cirrus Objects

   \param [in] host (Optional) Host url, including the protocol.
   \param [in] port (Optional) Server port.
   \param [in] server Name of the Web Application Server that provides the REST service (Default: riskCirrusObjects)
   \param [in] solution The solution short name from which this request is being made. This will get stored in the createdInTag and sharedWithTags attributes on the object (Default: 'blank').
   \param [in] username Username credentials
   \param [in] password Password credentials: it can be plain text or SAS-Encoded (it will be masked during execution)
   \param [in] authMethod: Authentication method (accepted values: BEARER). (Default: BEARER)
   \param [in] client_id The client id registered with the Viya authentication server. If blank, the internal SAS client id is used (only if GRANT_TYPE = password).
   \param [in] client_secret The secret associated with the client id.
   \param [in] key Instance key of the Cirrus object that is fetched with this REST request. If no Key is specified, the records are fetched using filter parameters
   \param [in] filter Filters to apply on the GET request when no value for key is specified (e.g. and(eq(objectId,'RMC_FX_CONVERSION%232022.11'),eq(createdBy,'sasadm')) ).
   \param [in] start Specify the starting point of the records to get. Start indicate the starting index of the subset. Start SHOULD be a zero-based index. The default start SHOULD be 0. Applicable only when filter is used.
   \param [in] limit Limit controls the maximum number of items to get from the start position (Default = 1000). Applicable only when filter is used.
   \param [in] debug True/False. If True, debugging informations are printed to the log (Default: false).
   \param [in] start Specify the starting point of the records to get. Start indicate the starting index of the subset. Start SHOULD be a zero-based index. The default start SHOULD be 0. Applicable only when a filter is used.
   \param [in] limit Limit controls the maximum number of items to get from the start position (Default = 1000). Applicable only when a filter is used.
   \param [in] logOptions Logging options (i.e. mprint mlogic symbolgen ...).
   \param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y).
   \param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y).
   \param [out] outds Name of the output table that contains the data defintion information summary (Default: work._tmp_dataDef_summary).
   \param [out] outds_columns Name of the output table that contains the data defintion columns information details (Default: work._tmp_dataDef_details).
   \param [out] outVarToken Name of the output macro variable which will contain the access token (Default: accessToken).
   \param [out] outSuccess Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess).
   \param [out] outResponseStatus Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus).

   \details
   This macro sends a GET request to <b><i>\<host\>:\<port\>/riskCirrusObjects/objects/dataDefinitions</i></b> and collects the results in the output table. \n
   See \link core_rest_request.sas \endlink for details about how to send GET requests and parse the response.


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

   2) Send a Http GET request and parse the JSON response into the output table WORK.data_definitions
   \code
      %let accessToken =;
      %core_rest_get_data_def(host =
                              , port =
                              , solution =
                              , logonHost =
                              , logonPort =
                              , username =
                              , password =
                              , authMethod = bearer
                              , client_id =
                              , client_secret =
                              , key = 30b7809f-cf98-4490-b6ce-dcb88d8e445c
                              , filter = %nrstr(&)key=30b7809f-cf98-4490-b6ce-dcb88d8e445c
                              , start =
                              , limit =
                              , outds = _tmp_dataDef_summary
                              , outds_columns = _tmp_dataDef_details
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

   <b>Sample output _tmp_dataDef_summary: </b>

   |name                  | objectId                          | creationTimeStamp        | sharedWithTags | sourceSystemCd | itemsCount  | key                                  | modifiedBy | createdInTag | schemaVersion | schemaName            | createdBy | modifiedTimeStamp          | description                 |
   |----------------------|-----------------------------------|--------------------------|----------------|----------------|-------------|--------------------------------------|------------|--------------|---------------|-----------------------|-----------|----------------------------|-----------------------------|
   | Portfolio Definition | CECL_CREDIT_PORTFOLIO#ecl.2022.11 | 2022-10-19T22:08:24.806Z |                | RCC            | 1           | fb80cb85-3564-4277-8fe5-45c258ae7fef | sasadm     | CORE         | ecl.2022.11   | CECL_CREDIT_PORTFOLIO | sasadm    | 2022-10-19T22:08:24.806Z   | Portfolio schema definition |


   <b>Sample output _tmp_dataDef_details: </b>

   | attributable |classification  | config_readOnly  | dataDefKey                           | dataDefName          | filterable | segmentationFlag | projectionFlag | format  | fx_var | informat | key | label          | mandatory_segmentation | name         | partition_flg          | primary_key_flg | projection | size | type  |
   |--------------|----------------|------------------|--------------------------------------|----------------------|------------|------------------|----------------|---------|--------|----------|-----|----------------|------------------------|--------------|------------------------|-----------------|------------|------|-------|
   |              |                |                  | fb80cb85-3564-4277-8fe5-45c258ae7fef | Portfolio Definition |            |                  |                | DATE9.9 |        |          |     | Reporting Date |                        | REPORTING_DT |                        |                 |            |      | FLOAT |

   \ingroup coreRestUtils

   \author  SAS Institute Inc.
   \date    2022
*/

%macro core_rest_get_data_def(host =
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
                                , outds = data_definitions
                                , outds_columns = data_definitions_columns
                                , outds_aggregation_config = datadef_aggregation_config
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
   %let requestUrl = &baseUrl./&server./objects/dataDefinitions;

   /* Add filters to the request URL */
   %core_set_rest_filter(key=&key., solution=&solution., filter=%superq(filter), start=&start., limit=&limit.);

   filename resp_def temp;

   /* Temporary disable mlogic and symbolgen options to avoid printing of userid/pwd to the log */
   option nomlogic nosymbolgen;
   /* Send the REST request */
   %core_rest_request(url = &requestUrl.
                     , method = GET
                     , logonHost = &logonHost.
                     , logonPort = &logonPort.
                     , username = &username.
                     , password = &password.
                     , headerOut = __hout_
                     , authMethod = &authMethod.
                     , client_id = &client_id.
                     , client_secret = &client_secret.
                     , fout = resp_def
                     , parser = sas.risk.cirrus.core_rest_parser.coreRestDataDefinition
                     , outds = &outds.
                     , arg1 = &outds_columns.
                     , arg2 = &outds_aggregation_config.
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
      %put ERROR: Unable to get the data definition information.;
      %abort;
   %end;

   /* Exit if there is more than one result */
   %if(%rsk_attrn(&outds., nobs) > 1) %then %do;
      %put ERROR: There is more than one Data Definition matching the same &schemaName and &schemaVersion..;
      %abort;
   %end;

   libname resp_def json fileref=resp_def noalldata nrm;

   %if %upcase(&debug) ne TRUE %then %do;
      filename resp_def CLEAR;
   %end;

%mend core_rest_get_data_def;
