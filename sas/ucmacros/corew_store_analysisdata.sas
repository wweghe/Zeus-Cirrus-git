/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor corew_store_analysisdata

   \brief   Creates an analysis data instance in SAS Risk Cirrus objects and create an associated Data Definition (a wrapper to core_rest_create_analysisdata)

   \param [in] host (Optional) Host url, including the protocol.
   \param [in] port (Optional) Server port.
   \param [in] server Name that provides the REST service (Default: riskData).
   \param [in] logonHost (Optional) Host/IP of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app host/ip is the same as the host/ip in the url parameter.
   \param [in] logonPort (Optional) Port of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app port is the same as the port in the url parameter.
   \param [in] username (Optional) Username credentials
   \param [in] password (Optional) Password credentials: it can be plain text or SAS-Encoded (it will be masked during execution).
   \param [in] authMethod: Authentication method (accepted values: BEARER). (Default: BEARER).
   \param [in] client_id The client id registered with the Viya authentication server. If blank, the internal SAS client id is used (only if GRANT_TYPE = password).
   \param [in] client_secret The secret associated with the client id.
   \param [in] sourceSystemCd The source system code to assign to the object when registering it in Cirrus Objects (Default: 'blank').
   \param [in] solution The solution short name from which this request is being made. This will get stored in the createdInTag and sharedWithTags attributes on the object (Default: 'blank').
   \param [in] cycle_key Cycle key to use for the analysis data instance
   \param [in] analysis_run_key Analysis Run key to use for the analysis data instance
   \param [in] configSetId Object Id filter to apply on the GET request when a value for key is specified.
   \param [in] entity_id Entity Identifier.
   \param [in] base_dttm Base Datetime (SAS datetime value).
   \param [in] table_filter Filters to apply on datastore config table to subset the sample to run (e.g. upcase(datastoreGroupId) in ('ENRICHMENT')).
   \param [in] ovrDatastoreConfig customized datastore config table. It tends to have the same structure as the datastore config to register analysis data instances in Cirrus Core (e.g. work.datastore_config).
   \param [in] locationType The type of server location from which data will be imported. Currently, only DIRECTORY and LIBNAME are supported; support for other options is planned.
   \param [in] location The server location from which data will be imported. Interpretation of this parameter varies based on the value of locationType. When DIRECTORY, the filesystem path on the server where the import data is located. When LIBNAME, the name of the library in which the import data may be found.
   \param [in] fileName Name of the file or table from which data will be imported.
   \param [in] inputDataFilter Table with filters for each loaded schemaName to apply over input data filename. Or can use the 'whereClauseFilterInput' attribute in datastore_config for each schemaName and schemaVersion.
            Mandatory table columns: 'schemaName' e.g. schemaName="ECL_CREDIT_PORTFOLIO"
                           'schemaVersion' e.g. schemaVersion="<cadence>"
                           'input_data_filter' e.g. (reporting_dt = 21549) and (upcase(entity_id) = "SASBank_1").
   \param [in] enableResultAttr Enabels or disables a feature to add specific columns to result analysis data object (Default: 'N').
   \param [in] ovrAnalysisDataName Customized data name that overrides the datastore_config value.
   \param [in] ovrAnalysisDataDesc Customized data name description that overrides the datastore_config value.
   \param [in] ovrSourceLibref Customized source data libref that overrides the datastore_config value.
   \param [in] ovrSourceTableName Customized source data name that overrides the datastore_config value.
   \param [in] debug True/False. If True, debugging informations are printed to the log (Default: false).
   \param [in] logOptions Logging options (i.e. mprint mlogic symbolgen ...).
   \param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y).
   \param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y).
   \param [out] outds_configTablesInfo Name of the output table that contains the schema info of 'datastore_config' (Default: config_tables_info).
   \param [out] outds_configTablesData Name of the output table data contains the schema of the analysis data structure (Default: config_tables_data).
   \param [out] outds_dataStore_eligibleTables Name of the output table data contains the eligible tables after filters (if any exist) applied (Default: work.ds_config_data_eligible).
   \param [out] outVarAnalysisDataKey Name of the ouput macro variable that contains the new created analysis data key (Default: new_analysis_data_key)
   \param [out] outVarToken Name of the output macro variable which will contain the access token (Default: accessToken).
   \param [out] outSuccess Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess).
   \param [out] outResponseStatus Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus).

   \details
   This macro sends a POST request to <b><i>\<host\>:\<port\>/riskData/objects?locationType=&locationType.&location=&location.&fileName=&filename/</i></b> and collects the results in the output table. \n
   See \link core_rest_request.sas \endlink for details about how to send GET/POST requests and parse the response.

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

   2) Send a Http GET request and parse the JSON response into the outputs tables work.configuration_tables_info and work.configuration_tables_data
   \code

      %let accessToken =;
      %corew_store_analysisdata(host = <host>
                              , port = <port>
                              , server = riskData
                              , logonHost =
                              , logonPort =
                              , username =
                              , password =
                              , authMethod = bearer
                              , client_id =
                              , client_secret =
                              , solution = CORE
                              , sourceSystemCd = RCC
                              , cycle_key =
                              , analysis_run_key =
                              , configSetId = ConfigSet-2022.11
                              , entity_id =
                              , base_dttm =
                              , table_filter =
                              , ovrDatastoreConfig =
                              , locationType = LIBNAME
                              , location =
                              , fileName =
                              , inputDataFilter =
                              , enableResultAttr = N
                              , outds_configTablesInfo = work.ds_config_info
                              , outds_configTablesData = work.ds_config_data
                              , outds_dataStore_eligibleTables = work.ds_config_data_eligible
                              , outVarToken = accessToken
                              , outSuccess = httpSuccess
                              , outResponseStatus = responseStatus
                              , debug = false
                              , logOptions =
                              , restartLUA = Y
                              , clearCache = Y);
      %put &=httpSuccess;
      %put &=responseStatus;
   \endcode

   <b>Sample output outds_configTablesInfo: </b>

   |      key                              |        name       |           versionNm        | createdInTag |   description    | statusCd | sourceSystemCd | sharedWithTags |           objectId          | type              |
   |---------------------------------------|-------------------|----------------------------|--------------|------------------|----------|----------------|----------------|-----------------------------|-------------------|
   | e7a2a66d-e326-4f1c-8ab7-28d12a1a5c67  | datastore_config  |            2022.11         | ECL          | datastore_config | TEST     | ECL            | ECL            | datastore_config_2022_11    | datastore_config  |

   <b>Sample output outds_configTablesData: </b>

   |analysisDataDesc              | analysisDataName                       | attributableVars   | businessCategoryCd  | constraintEnabledFlg    | datastoreGroupId    | dataCategoryCd  |  dataDefinitionDesc         | dataDefinitionName    | dataSubCategoryCd | dataType  |  filterableVars   | segmentationVars | projectionVars | indexList  | partitionVars    | primaryKey          | reportmartGroupId   | riskTypeCd |  schemaName          | schemaTypeCd | schemaVersion   | sourceLibref  | targetTableName    |
   |------------------------------|----------------------------------------|--------------------|---------------------|-------------------------|---------------------|-----------------|-----------------------------|-----------------------|-------------------|-----------|-------------------|------------------|----------------|------------|------------------|---------------------|---------------------|------------|----------------------|--------------|---------------- |---------------|--------------------|
   |Portfolio data for the base   | Portfolio <MONTH, 0, SAME, yymmddd10.> |                    | ECL                 |                         | Enrichment          | PORTFOLIO       | Portfolio schema definition | Portfolio Definition  |                   |           |                   |                  |                |            |                  | REPORTING_DT INSTID |                     | CREDIT     | ECL_CREDIT_PORTFOLIO | FLAT         | 2022.11         | ecl202211     | CREDIT_PORTFOLIO   |

   <b>Sample output outds_dataStore_eligibleTables: </b>

   |analysisDataDesc              | analysisDataName                       | attributableVars   | businessCategoryCd  | constraintEnabledFlg    | datastoreGroupId    | dataCategoryCd  |  dataDefinitionDesc         | dataDefinitionName    | dataSubCategoryCd | dataType  |  filterableVars   | segmentationVars | projectionVars | indexList  | partitionVars    | primaryKey          | reportmartGroupId   | riskTypeCd |  schemaName          | schemaTypeCd | schemaVersion   | sourceLibref  | targetTableName    |
   |------------------------------|----------------------------------------|--------------------|---------------------|-------------------------|---------------------|-----------------|-----------------------------|-----------------------|-------------------|-----------|-------------------|------------------|----------------|------------|------------------|---------------------|---------------------|------------|----------------------|--------------|---------------- |---------------|--------------------|
   |Portfolio data for the base   | Portfolio <MONTH, 0, SAME, yymmddd10.> |                    | ECL                 |                         | Enrichment          | PORTFOLIO       | Portfolio schema definition | Portfolio Definition  |                   |           |                   |                  |                |            |                  | REPORTING_DT INSTID |                     | CREDIT     | ECL_CREDIT_PORTFOLIO | FLAT         | 2022.11         | ecl202211     | CREDIT_PORTFOLIO   |


   \ingroup rgfRestUtils

   \author  SAS Institute Inc.
   \date    2022
*/
%macro corew_store_analysisdata(host =
                                , port =
                                , server = riskData
                                , logonHost =
                                , logonPort =
                                , username =
                                , password =
                                , authMethod = bearer
                                , casHost =
                                , casPort =
                                , casSessionName =
                                , client_id =
                                , client_secret =
                                , solution = ECL
                                , sourceSystemCd = ECL
                                , cycle_key =
                                , analysis_run_key =
                                , configSetId =
                                , base_dttm =
                                , entity_id =
                                , table_filter =
                                , ovrDatastoreConfig =
                                , locationType = LIBNAME
                                , location = /* to fill during macro 'core_rest_get_config_table' execution - value stored in datastore_config table */
                                , fileName = /* to fill during macro 'core_rest_get_config_table' execution - value stored in datastore_config table */
                                , inputDataFilter =
                                , enableResultAttr = N
                                , ovrAnalysisDataName =
                                , ovrAnalysisDataDesc =
                                , ovrSourceLibref =
                                , ovrSourceTableName =
                                , outds_configTablesInfo = work.ds_config_info
                                , outds_configTablesData = work.ds_config_data
                                , outds_dataStore_eligibleTables = work.ds_config_data_eligible
                                , outVarAnalysisDataKey = new_analysis_data_key
                                , logSeverity = WARNING
                                , outVarToken = accessToken
                                , outSuccess = httpSuccess
                                , outResponseStatus = responseStatus
                                , debug = false
                                , logOptions =
                                , restartLUA = Y
                                , clearCache = Y);

   %local 
      httpSuccess
      responseStatus
      new_analysis_data_key_aux
   ;
   
   %if (%sysevalf(%superq(ovrDatastoreConfig) ne, boolean)) %then %do;
         %let outds_configTablesData = &ovrDatastoreConfig.;
   %end;
   %else %if (%sysevalf(%superq(configSetId) ne, boolean)) %then %do; /* Get configSet version */
         %let httpSuccess = 0;
         %let responseStatus =;
         /* Get configuration table information */
         %core_rest_get_config_table(host = &host.
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
                                    , configSetId = &configSetId.
                                    , configTableType = datastore_config
                                    , logSeverity = &logSeverity.
                                    , outds_configTablesInfo = &outds_configTablesInfo.
                                    , outds_configTablesData = &outds_configTablesData.
                                    , outVarToken = &outVarToken.
                                    , outSuccess = &outSuccess.
                                    , outResponseStatus = &outResponseStatus.
                                    , debug = &debug.
                                    , logOptions = &logOptions.
                                    , restartLUA = &restartLUA.
                                    , clearCache = &clearCache.
                                    );
         /* &&&outSuccess.. and &outds_ validations carried out in the macro call */

      %end; /* %if (%sysevalf(%superq(configSetId) ne, boolean)) */
      %else %do;
               %put ERROR: No ConfigSet or Customized datastore config available to use. ;
               %abort;
         %end;

   /* Add filters (if any) to subset the schema names to create DD and AD */
   data &outds_dataStore_eligibleTables.;

      length sourceLibref $200;

      set &outds_configTablesData.
      %if (%sysevalf(%superq(table_filter) ne, boolean)) %then %do;
         (where=(&table_filter.))
      %end;
      ;
      strategyType = "NONE";
      /* Override analysis data table fields for POST creation */
      %if (%sysevalf(%superq(ovrAnalysisDataName) ne, boolean)) %then %do;
         analysisDataName = "&ovrAnalysisDataName.";
      %end;
      %if (%sysevalf(%superq(ovrAnalysisDataDesc) ne, boolean)) %then %do;
         analysisDataDesc = "&ovrAnalysisDataDesc.";
      %end;
      %if (%sysevalf(%superq(ovrSourceLibref) ne, boolean)) %then %do;
         sourceLibref = "&ovrSourceLibref.";
      %end;
      call symputx(catx('_', 'sourceLibref', _N_), resolve(sourceLibref),'L');
      %if (%sysevalf(%superq(ovrSourceTableName) ne, boolean)) %then %do;
         targetTableName = "&ovrSourceTableName.";
      %end;
      call symputx(catx('_', 'sourceTableName', _N_), resolve(targetTableName),'L');

      call symputx(catx('_', 'schemaName', _N_), resolve(schemaName),'L');
      call symputx(catx('_', 'schemaVersion', _N_), resolve(schemaVersion),'L');

      %if ( %rsk_varexist(&outds_configTablesData., whereClauseFilterInput) > 0) %then %do;
         call symputx(catx('_', 'whereClauseFilterInput', _N_), resolve(whereClauseFilterInput),'L');
      %end;
      %else %do;
         call symputx(catx('_', 'whereClauseFilterInput', _N_),"",'L');
      %end;

      call symputx('tot_objs', _N_ , 'L');
   run;

   %if ( not %rsk_dsexist(&outds_dataStore_eligibleTables.) or %rsk_attrn(&outds_dataStore_eligibleTables., nobs) = 0) %then %do;
      %put ERROR: Filter: 'table_filter' is not producing the itended data subset.;
      %abort;
   %end;

   %do k=1 %to &tot_objs;

      %if(%sysevalf(%superq(inputDataFilter) ne, boolean)) %then %do;
         data _null_;
         set &inputDataFilter.;
            %if ( %rsk_varexist(&inputDataFilter., schemaName) > 0 and %rsk_varexist(&inputDataFilter., schemaVersion) > 0 and %rsk_varexist(&inputDataFilter., input_data_filter) > 0 ) %then %do;
               if schemaName = "&&schemaName_&k.." and schemaVersion = "&&schemaVersion_&k.." then do;
                  call symputx(catx('_', 'whereClauseFilterInput', &k.),input_data_filter,'L');
               end;
            %end;
            %else %do;
                     call symputx(catx('_', 'whereClauseFilterInput', &k.),"",'L');
                  %end;
         run;
      %end;

      %let httpSuccess = 0;
      %let responseStatus =;
      %core_rest_create_analysisdata(host = &host.
                                    , port = &port.
                                    , server = riskData
                                    , logonHost = &logonHost.
                                    , logonPort = &logonPort.
                                    , username = &username.
                                    , password = &password.
                                    , authMethod = &authMethod.
                                    , casHost = &casHost.
                                    , casPort = &casPort.
                                    , casSessionName = &casSessionName.
                                    , solution = &solution.
                                    , sourceSystemCd = &sourceSystemCd.
                                    , schemaName = &&schemaName_&k..
                                    , schemaVersion = &&schemaVersion_&k..
                                    , cycle_key = &cycle_key.
                                    , analysis_run_key = &analysis_run_key.
                                    , entity_id = &entity_id.
                                    , base_dttm = &base_dttm.
                                    , locationType = &locationType.
                                    , location = &&sourceLibref_&k..
                                    , fileName = &&sourceTableName_&k..
                                 %if (%sysevalf(%superq(whereClauseFilterInput_&k) ne, boolean)) %then %do;
                                    , inputDataFilter = &&whereClauseFilterInput_&k..
                                 %end;
                                    , enableResultAttr = &enableResultAttr.
                                    , ovrDatastoreConfig = &ovrDatastoreConfig.
                                    , outds_configTablesData = &outds_dataStore_eligibleTables.
                                    , outVarToken = &outVarToken.
                                    , outSuccess = &outSuccess.
                                    , outResponseStatus = &outResponseStatus.
                                    , outVarAnalysisDataKey = &outVarAnalysisDataKey.
                                    , debug = &debug.
                                    , logOptions = &logOptions.
                                    , restartLUA = &restartLUA.
                                    , clearCache = &clearCache.
                                    );

      %if "&&&outVarAnalysisDataKey." ne "" %then
         %if "&new_analysis_data_key_aux." ne "" %then
            %let new_analysis_data_key_aux = &new_analysis_data_key_aux. ,&&&outVarAnalysisDataKey.;
         %else
            %let new_analysis_data_key_aux = &&&outVarAnalysisDataKey.;
   %end;

   %let &outVarAnalysisDataKey. = &new_analysis_data_key_aux.;

%mend corew_store_analysisdata;