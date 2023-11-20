/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor core_rest_get_config_table

   \brief   Retrieve the configuration table(s) registered in SAS Risk Cirrus Objects

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
   \param [in] configSetId Object Id filter to apply on the GET request when a value for key is specified.
   \param [in] filter Filters to apply on the GET request when no value for key is specified (e.g. eq(createdBy,'sasadm') | and(eq(name,'datastore_config'),eq(createdBy,'sasadm')) ).
   \param [in] configTableType Name of the table object used in type.
   \param [in] errorIfConfigTableNotFound (Y/N) If Y, and a configuration table is not found, then an error is thrown and processing stops. (Default: Y).
   \param [in] debug True/False. If True, debugging informations are printed to the log (Default: false).
   \param [in] start Specify the starting point of the records to get. Start indicate the starting index of the subset. Start SHOULD be a zero-based index. The default start SHOULD be 0. Applicable only when a filter is used.
   \param [in] limit Limit controls the maximum number of items to get from the start position (Default = 1000). Applicable only when a filter is used.
   \param [in] logOptions Logging options (i.e. mprint mlogic symbolgen ...)
   \param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y).
   \param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y).
   \param [out] outds_configTablesInfo Name of the output table that contains the schema info of 'datastore_config' (Default: config_tables_info).
   \param [out] outds_configTablesData Name of the output table data contains the schema of the analysis data structure (Default: config_tables_data).
   \param [out] outConfigTableFound Name of the output macro variable that indicates if the configuration table was found (&outConfigTableFound = 1) or not (&outConfigTableFound = 0). (Default: config_table_exists)
   \param [out] outVarToken Name of the output macro variable which will contain the access token (Default: accessToken).
   \param [out] outSuccess Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess).
   \param [out] outResponseStatus Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus).

   \details
   This macro sends a GET request to <b><i>\<host\>:\<port\>/riskCirrusObjects/objects/&collectionName./</i></b> and collects the results in the output table. \n
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

   2) Send a Http GET request and parse the JSON response into the output table work.configuration_tables
   \code
      %let accessToken =;
      %core_rest_get_config_table(host = <host>
                                  , port = <port>
                                  , server = riskCirrusObjects
                                  , logonHost =
                                  , logonPort =
                                  , username =
                                  , password =
                                  , authMethod = bearer
                                  , client_id =
                                  , client_secret =
                                  , solution = CORE
                                  , filter = eq(objectId,'datastore_config_2022_11')
                                  , start = 0
                                  , limit = 100
                                  , configSetId = ConfigSet-2022.11
                                  , configTableType = datastore_config
                                  , logSeverity = WARNING
                                  , outds_configTablesInfo = work.config_tables_info
                                  , outds_configTablesData = work.config_tables_data
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

   |      key                              |        name       |           versionNm        | createdInTag |   description    | statusCd | sourceSystemCd | sharedWithTags |           objectId          | type              |
   |---------------------------------------|-------------------|----------------------------|--------------|------------------|----------|----------------|----------------|-----------------------------|-------------------|
   | e7a2a66d-e326-4f1c-8ab7-28d12a1a5c67  | datastore_config  |            2022.11         | ECL          | datastore_config | TEST     | ECL            | ECL            | datastore_config_2022_11    | datastore_config  |

   <b>Sample output outds_configTablesData: </b>

   |analysisDataDesc              | analysisDataName                       | attributableVars   | businessCategoryCd  | constraintEnabledFlg    | datastoreGroupId    | dataCategoryCd  |  dataDefinitionDesc         | dataDefinitionName    | dataSubCategoryCd | dataType  |  filterableVars   | segmentationVars | projectionVars | indexList  | partitionVars    | primaryKey          | reportmartGroupId   | riskTypeCd |  schemaName          | schemaTypeCd | schemaVersion   | sourceLibref  | targetTableName    |
   |------------------------------|----------------------------------------|--------------------|---------------------|-------------------------|---------------------|-----------------|-----------------------------|-----------------------|-------------------|-----------|-------------------|------------------|----------------|------------|------------------|---------------------|---------------------|------------|----------------------|--------------|---------------- |---------------|--------------------|
   |Portfolio data for the base   | Portfolio <MONTH, 0, SAME, yymmddd10.> |                    | ECL                 |                         | Enrichment          | PORTFOLIO       | Portfolio schema definition | Portfolio Definition  |                   |           |                   |                  |                |            |                  | REPORTING_DT INSTID |                     | CREDIT     | ECL_CREDIT_PORTFOLIO | FLAT         | 2022.11         | ecl202211     | CREDIT_PORTFOLIO   |

   \ingroup rgfRestUtils

   \author  SAS Institute Inc.
   \date    2022
*/

%macro core_rest_get_config_table(host =
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
                                  , configSetId =                                /* configuration Sets - objectId to filter i.e objectId=ConfigSet-2022.11 */
                                  , configTableType =                            /* configuration Tables - name to filter i.e datastore_config */
                                  , logSeverity = WARNING
                                  , errorIfConfigTableNotFound = Y
                                  , outConfigTableFound = config_table_exists
                                  , outds_configTablesInfo = work.config_tables_info         /* configset table metainfo */
                                  , outds_configTablesData = work.config_tables_data         /* configtable structure metainfo + datainfo */
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
      linkTypeSSC
      respTabs
   ;

   /* Set the required log options */
   %if(%length(&logOptions.)) %then
      options &logOptions.;
   ;

   /* Get the current value of mlogic and symbolgen options */
   %local oldLogOptions;
   %let oldLogOptions = %sysfunc(getoption(mlogic)) %sysfunc(getoption(symbolgen));

   /* outConfigTableFound cannot be missing. Set a default value */
   %if(%sysevalf(%superq(outConfigTableFound) =, boolean)) %then
      %let outConfigTableFound = config_table_exists;

   /* Declare the output variable as global if it does not exist */
   %if(not %symexist(&outConfigTableFound.)) %then
      %global &outConfigTableFound.;

   %let &outConfigTableFound. = 0;

   /* Delete output table if it exists */
   %if (%rsk_dsexist(&outds_configTablesInfo.)) %then %do;
      proc sql;
         drop table &outds_configTablesInfo.;
      quit;
   %end;

   /* Delete output table if it exists */
   %if (%rsk_dsexist(&outds_configTablesData.)) %then %do;
      proc sql;
         drop table &outds_configTablesData.;
      quit;
   %end;

   %if (%sysevalf(%superq(configSetId) ne, boolean)) %then %do;

      %if (%sysevalf(%superq(configTableType) eq, boolean)) %then %do;
         %put ERROR: Parameter 'configTableType' is required if parameter configSetId is provided;
         %abort;
      %end;

   %end;

   /* Delete intermediate table if it exists */
   %if (%rsk_dsexist(work._tmp_link_type_)) %then %do;
      proc sql;
         drop table work._tmp_link_type_;
      quit;
   %end;

   /* Query the linkTypes for a given solution (if not found), look in RiskCirrusObjects, else abort the process since no linkType exist. Returns the macro variable 'linkTypeSSC' for the SourceSystemCd where the link is found. */
   %core_rest_get_link_types(host = &host.
                              , port = &port.
                              , server = riskCirrusObjects
                              , logonHost = &logonHost.
                              , logonPort = &logonPort.
                              , username = &username.
                              , password = &password.
                              , authMethod = &authMethod.
                              , client_id = &client_id.
                              , client_secret = &client_secret.
                              , solution = &solution.
                              , filter = eq(objectId,%27configurationSet_configurationTable%27)
                              , outds = work._tmp_link_type_
                              , outVarToken = &outVarToken.
                              , outSuccess = &outSuccess.
                              , outResponseStatus = &outResponseStatus.
                              , debug = &debug.
                              , logOptions = &logOptions.
                              , restartLUA = &restartLUA.
                              , clearCache = &clearCache.
                              );

   /* Get the linkType SSC */
   data _null_;
      set work._tmp_link_type_;
      call symputx("linkTypeSSC", upcase(sourceSystemCd), "L");
   run;

   /* Remove temporary data artefacts from the WORK */
   proc datasets library = work
                 memtype = (data)
                 nolist nowarn;
      delete _tmp_link_type_
             ;
   quit;

   /* Set the base request URL */
   %core_set_base_url(host=&host, server=&server., port=&port.);
   %let requestUrl = &baseUrl./&server./objects/configurationTables;

   /* Add filters to the request URL */
   %if (%sysevalf(%superq(configSetId) ne, boolean)) %then
      %let customFilter=and(hasObjectLinkToEq(%27&linkTypeSSC.%27,%27configurationSet_configurationTable%27,%27objectId%27,%27&configSetId.%27,0),eq(type,%27&configTableType.%27));
   %core_set_rest_filter(key=&key., solution=&solution., filter=%superq(filter), customFilter=%superq(customFilter), start=&start., limit=&limit.);

   /* Get a Unique fileref and assign a temp file to it */
   %let respTabs = %rsk_get_unique_ref(prefix = resp, engine = temp);

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
                    , fout = &respTabs.
                    , parser = sas.risk.cirrus.core_rest_parser.coreRestConfigTable
                    , outds = &outds_configTablesInfo.
                    , arg1 = &outds_configTablesData.
                    , outVarToken = &outVarToken.
                    , outSuccess = &outSuccess.
                    , outResponseStatus = &outResponseStatus.
                    , debug = &debug.
                    , logOptions = &oldLogOptions.
                    , restartLUA = &restartLUA.
                    , clearCache = &clearCache.
                    );

   /* Throw an error and exit if the configuration table request fails */
   %if ((not &&&outSuccess..) or not(%rsk_dsexist(&outds_configTablesInfo.))) %then %do;
      %put ERROR: The request to get the configurationTable failed.;
      %abort;
   %end;

   %if (%rsk_attrn(&outds_configTablesInfo., nlobs) gt 0 and %rsk_dsexist(&outds_configTablesData.)) %then %do;
      %if (%rsk_attrn(&outds_configTablesData., nlobs) gt 0) %then %do;
         %let &outConfigTableFound. = 1;
      %end;
   %end;

   /* Throw an error and exit if the configuration table was not found and the errorIfConfigTableNotFound input parameter was set to Y */
   %if ((not &&&outConfigTableFound..) and %upcase("&errorIfConfigTableNotFound.") eq "Y") %then %do;
      %put ERROR: Could not find any configuration table.;
      %abort;
   %end;

   /* Clear references if we're not debugging */
   %if %upcase(&debug) ne TRUE %then %do;
      filename &respTabs. clear;
   %end;

%mend core_rest_get_config_table;
