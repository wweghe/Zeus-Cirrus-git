/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor core_rest_create_analysisdata

   \brief   Create an analysis data instance in SAS Risk Cirrus objects, and create an associated Data Definition if none already exists with the given schema name and version

   \param [in] host (Optional) Host url, including the protocol.
   \param [in] port (Optional) Server port.
   \param [in] server Name that provides the REST service (Default: riskData).
   \param [in] logonHost (Optional) Host/IP of the sas-logon-app service or ingress. If blank, it is assumed that the sas-logon-app host/ip is the same as the host/ip in the url parameter.
   \param [in] logonPort (Optional) Port of the sas-logon-app service or ingress. If blank, it is assumed that the sas-logon-app port is the same as the port in the url parameter.
   \param [in] username (Optional) Username credentials
   \param [in] password (Optional) Password credentials: it can be plain text or SAS-Encoded (it will be masked during execution).
   \param [in] authMethod: Authentication method (accepted values: BEARER). (Default: BEARER).
   \param [in] casSessionName The name of a CAS session to use for local CAS actions. If one doesn't exist, a new session with this name will be created.
   \param [in] casHost (Optional) Host/IP of the sas-cas-server-default-client.
   \param [in] casPort (Optional) Port of the cas-server port.
   \param [in] casSessionName The name of a CAS session to use for local CAS actions.  If one doesn't exist, a new session with this name will be created.
   \param [in] client_id The client id registered with the Viya authentication server. If blank, the internal SAS client id is used (only if GRANT_TYPE = password).
   \param [in] client_secret The secret associated with the client id.
   \param [in] sourceSystemCd The source system code to assign to the object when registering it in Cirrus Objects (Default: <solution>)
   \param [in] solution The solution short name from which this request is being made. This will get stored in the createdInTag and sharedWithTags attributes on the object (Default: 'blank').
   \param [in] schemaName The schema name is the analysis data name.
   \param [in] schemaVersion The schema name is the analysis data name.
   \param [in] cycle_key Cycle key to use for the analysis data instance
   \param [in] analysis_run_key Analysis Run key to use for the analysis data instance
   \param [in] configSetId Object Id filter to apply on the GET request when a value for key is specified.
   \param [in] entity_id Entity Identifier. (eg. "SASBank_1")
   \param [in] base_dttm Base Datetime (SAS datetime value).
   \param [in] ovrDatastoreConfig customized datastore config table. It tends to have the same structure as the datastore config to register analysis data instances in Cirrus Core (e.g. work.datastore_config).
   \param [in] locationType The type of server location from which data will be imported. LIBNAME has tables in filename, FOLDER has csv files in filename and DIRECTORY has <tables>.sas7bdat in filename to be used through a libname.
   \param [in] location The server location from which data will be imported. Interpretation of this parameter varies based on the value of locationType. When DIRECTORY, the filesystem path on the server where the import data is located. When LIBNAME, the name of the library in which the import data may be found.
   \param [in] fileName Name of the file or table from which data will be imported.
   \param [in] inputDataFilter Generic filter to apply over input data filename. Use sas syntax to compose where clause. e.g. INSTID eq 'CI_00001' | RATING_GRADE in ('BBB' 'A').
   \param [in] enableResultAttr Enabels or disables a feature to add specific columns to result analysis data object (Default: 'N').
   \param [in] classification_point_key Key that identifies a specific Dimensional point to relates with a set of dimensional point paths. Accepts a set of key(s) for 'default' context. (e.g. "3a067c58-8022-4f72-9a4f-c7ae80e687dc" | "ec7e40f5-0045-49b6-9aef-2cb521668251","bd3d59c7-efb3-42db-8bc9-bd5ba4c11a2a" )
   \param [in] debug True/False. If True, debugging informations are printed to the log (Default: false).
   \param [in] logOptions Logging options (i.e. mprint mlogic symbolgen ...).
   \param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y).
   \param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y).
   \param [out] outVarAnalysisDataKey Name of the ouput macro variable that contains the new created analysis data key (Default: new_analysis_data_key)
   \param [out] outds_configTablesData Name of the output table data contains the schema of the analysis data structure (Default: config_tables_data).
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
      %core_rest_create_analysisdata(host = <host>
                              , port = <port>
                              , server = riskData
                              , logonHost =
                              , logonPort =
                              , username =
                              , password =
                              , authMethod = bearer
                              , client_id =
                              , client_secret =
                              , solution = ECL
                              , sourceSystemCd = ECL
                              , schemaName = ECL_CREDIT_PORTFOLIO
                              , schemaVersion = 2022.11
                              , cycle_key =
                              , analysis_run_key =
                              , configSetId = ConfigSet-2022.11
                              , entity_id =
                              , base_dttm =
                              , ovrDatastoreConfig =
                              , locationType = LIBNAME
                              , location =
                              , fileName =
                              , inputDataFilter =
                              , enableResultAttr = N
                              , classification_point_key =
                              , logSeverity = WARNING
                              , outds_configTablesData = work.config_tables_data
                              , outVarToken = accessToken
                              , outSuccess = httpSuccess
                              , outResponseStatus = responseStatus
                              , debug = true
                              , logOptions =
                              , restartLUA = Y
                              , clearCache = Y);
      %put &=httpSuccess;
      %put &=responseStatus;
   \endcode

   <b>Sample output outds_configTablesData: </b>

   |analysisDataDesc              | analysisDataName                       | attributableVars   | businessCategoryCd  | constraintEnabledFlg    | datastoreGroupId    | dataCategoryCd  |  dataDefinitionDesc         | dataDefinitionName    | dataSubCategoryCd | dataType  |  filterableVars   | segmentationVars | projectionVars | indexList  | partitionVars    | partitionByVars       | strategyType | partitionCount | primaryKey          | reportmartGroupId   | riskTypeCd |  schemaName          | schemaTypeCd | schemaVersion   | sourceLibref  | targetTableName    | allowDataAppend | allowDataDelete | martTableNm  | martLibraryNm |
   |------------------------------|----------------------------------------|--------------------|---------------------|-------------------------|---------------------|-----------------|-----------------------------|-----------------------|-------------------|-----------|-------------------|------------------|----------------|------------|------------------| --------------------- |--------------|----------------|---------------------|---------------------|------------|----------------------|--------------|---------------- |---------------|--------------------|-----------------|-----------------|--------------|---------------|
   |Portfolio data for the base   | Portfolio <MONTH, 0, SAME, yymmddd10.> |                    | ECL                 |                         | Enrichment          | PORTFOLIO       | Portfolio schema definition | Portfolio Definition  |                   |           |                   |                  |                |            |                  | REPORTING_DT INSTTYPE | BYVARS       | null           | REPORTING_DT INSTID |                     | CREDIT     | ECL_CREDIT_PORTFOLIO | FLAT         | 2022.11         | ecl202211     | CREDIT_PORTFOLIO   | true            | true            | ECL_..._VIEW | ECLReporting  |

   \ingroup rgfRestUtils

   \author  SAS Institute Inc.
   \date    2022
*/

%macro core_rest_create_analysisdata(host =
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
                              , solution =
                              , sourceSystemCd =
                              , schemaName =
                              , schemaVersion =
                              , cycle_key =
                              , analysis_run_key =
                              , configSetId =
                              , entity_id =
                              , base_dttm =
                              , ovrDatastoreConfig =
                              , locationType = LIBNAME
                              , location =
                              , fileName =
                              , inputDataFilter =
                              , enableResultAttr = N
                              , classification_point_key =
                              , logSeverity = WARNING
                              , outVarAnalysisDataKey = new_analysis_data_key
                              , outds_configTablesData = work.config_tables_data
                              , outVarToken = accessToken
                              , outSuccess = httpSuccess
                              , outResponseStatus = responseStatus
                              , debug = false
                              , logOptions =
                              , restartLUA = Y
                              , clearCache = Y) / minoperator;

   %local requestUrl
      alltogetherVars
      uniqueTogetherVars
      primaryKey
      filterableVars
      segmentationVars
      allocationVars
      projectionVars
      partitionByVars
      attributableVars
      dataDefinitionName
      dataDefinitionDesc
      targetTableName
      analysisDataName
      analysisDataDesc
      indexList
      constraintEnabledFlg
      dataCategoryCd
      dataType
      dataSubCategoryCd
      riskTypeCd
      schemaTypeCd
      businessCategoryCd
      i
      word
      contaVars
      analysisDataVarsScope
      data_definition_key
      no_filter_flg
      location_tmp
      base_dt
      currentDTTM
      objBaseDate
      analysisData_analysisRun_key
      analysisData_cycle_key
      classificationContext
      __classification_point_key__
      marked_to_delete
      log_file_fref
      marked_to_terminate
      casSessionTag
      cycle_name
      analysis_run_name
      genMVar
      genLen
      varsOfVars
      cas_table_exists
      promoted_filename
    ;

   /* Get part of cas session Id to use in potencial tables concurrency */
   %let casSessionTag = %scan(&_IOCASUUID_., 1,'-');

   /* Flag to terminate cas session */
   %let marked_to_terminate = N;
   /* Flag to mark when intermediate table must be deleted. i.e. CASUSER.<filename> */
   %let marked_to_delete = N;

   %if (%sysevalf(%superq(base_dttm) ne, boolean)) %then %do;
      %let base_dt = "%sysfunc(datepart(&base_dttm.),date9.)"d;
      %let objBaseDate = %sysfunc(putn(&base_dttm.,E8601DT25.)).000Z;
   %end;
   %else %do;
      %put WARNING: No Base Date has been provided. Execution Date will be used.;
      %let currentDTTM = %sysfunc(dhms(%sysfunc(today()), 0, 0, 0));
      %let base_dt = "%sysfunc(datepart(&currentDTTM.),date9.)"d;
      %let objBaseDate = %sysfunc(putn(&currentDTTM.,E8601DT25.)).000Z;
   %end;

   /* Set the required log options */
   %if(%length(&logOptions.)) %then
      options &logOptions.;
   ;

   /* Get the current value of mlogic and symbolgen options */
   %local oldLogOptions;
   %let oldLogOptions = %sysfunc(getoption(mlogic)) %sysfunc(getoption(symbolgen));

   %if (%sysevalf(%superq(solution) eq, boolean)) %then %do;
      %put ERROR: Parameter 'solution' is required.;
      %abort;
   %end;

   %if (%sysevalf(%superq(sourceSystemCd) eq, boolean)) %then %do;
      %let sourceSystemCd = &solution.;
   %end;

   %if (%sysevalf(%superq(schemaName) eq, boolean)) %then %do;
      %put ERROR: Parameter 'schemaName' is required.;
      %abort;
   %end;

   %if (%sysevalf(%superq(schemaVersion) eq, boolean)) %then %do;
      %put ERROR: Parameter 'schemaVersion' is required.;
      %abort;
   %end;

   %if (%sysevalf(%superq(locationType) eq, boolean)) %then %do;
      %put ERROR: The 'locationType' parameter is required. Available values : DIRECTORY, LIBNAME, FOLDER;
      %abort;
   %end;

   /* Declare the output variable as global if it does not exist */
   %if(not %symexist(&outVarAnalysisDataKey.)) %then
     %global &outVarAnalysisDataKey.;

   /* OutVarStatus cannot be missing. Set a default value */
   %if(%sysevalf(%superq(outVarAnalysisDataKey) =, boolean)) %then
      %let outVarAnalysisDataKey = new_analysis_data_key;


   /*****************************************************************************************************************************/
   /* NOTE: list of datastore_config variables that will be part of the DD|ADO payload                                          */
   /* once got here new variables is also needed to be added where payload is created. see steps in: 'file _body recfm=N;'.     */
   /*****************************************************************************************************************************/
   %let datastoreConfigVars = %lowcase(%rsk_getvarlist(&outds_configTablesData.));

   /* list of datastore_config columns that contain lists of variables */
   %do i=1 %to %sysfunc(countw("&datastoreConfigVars."," "));
      %let columnVarsExists=%trim(%scan(%superq(datastoreConfigVars),&i," "));
      %if ( %sysfunc(prxmatch(/(\w+)vars/i,&columnVarsExists.)) > 0 ) %then %do;
         %let varsOfVars= &varsOfVars. &columnVarsExists.;
      %end;
   %end;
   /*****************************************************************************************************************************/

   %if (%sysevalf(%superq(ovrDatastoreConfig) ne, boolean)) %then %do; /* (%sysevalf(%superq(ovrDatastoreConfig) ne, boolean)) */

      /* Build a step to load user table structure */
      %if ( not(%rsk_dsexist(&ovrDatastoreConfig.)) or %rsk_attrn(&ovrDatastoreConfig., nobs) = 0 ) %then %do;
         %put ERROR: Configuration table '&ovrDatastoreConfig.' for analysis data is required.;
         %abort;
      %end;

      %let ovrDatastoreConfigVars = "%sysfunc(prxchange(s/\s+/" "/,-1, %superq(datastoreConfigVars)))";
      /* The analysis data table should have least these 'countw("&ovrDatastoreConfigVars.")'' vars in scope to enable data definition */
      proc contents data=&ovrDatastoreConfig. out=work._tmp_out_contents_1 noprint; run;
      proc sql noprint;
         select count(*) into: contaVars
         from work._tmp_out_contents_1
         where lowcase(name) in (&ovrDatastoreConfigVars.)
      ;quit;

      /* Exit if all the required vars in scope are not present */
      %if &contaVars. ne %sysfunc(countw(&datastoreConfigVars.)) %then %do;
         %put ERROR: All the variables required in scope are not present.;
         %abort;
      %end;

      /* If customized table is meets the requirements */
      %let outds_configTablesData = &ovrDatastoreConfig.;
   %end;

   %if (%sysevalf(%superq(location) eq, boolean)) %then %do;
      %put ERROR: The 'location' parameter is required.;
      %abort;
   %end;

   %if (%sysevalf(%superq(fileName) eq, boolean)) %then %do;
      %put ERROR: The 'fileName' parameter is required.;
      %abort;
   %end;

   proc contents data=&outds_configTablesData. out=work._tmp_out_contents_2 noprint; run;

   proc sql noprint;
      select lowcase(strip(name)) into: varsFromTable separated by " "
   from work._tmp_out_contents_2
   ;quit;

   /*************************************************/
   /* Var generator for lengths and call symputs    */
   /*************************************************/
   %do i=1 %to %sysfunc(countw("&datastoreConfigVars."," "));
      %let columnExists=%trim(%scan(%lowcase(%superq(datastoreConfigVars)),&i," "));
      %if not %sysfunc(indexw("&varsFromTable.","&columnExists."," ")) %then %do;
         %let genLen= &genLen. length &columnExists. $100.%str(;);
      %end;
      %let genMVar= &genMVar. call symputx(%str(%")&columnExists.%str(%"),&columnExists.,%str(%")L%str(%"))%str(;);
   %end;

   %let varsOfVars = %sysfunc(prxchange(s/\s+/%str(,)/,-1,%superq(varsOfVars)));
   /* Get analysis data columns info to write analysisDefiniton and analysisData json _body */
   data _null_;

      &genLen.;

      set &outds_configTablesData.
         (where=(upcase(schemaName)=upcase("&schemaName.")
               and upcase(schemaVersion)=upcase("&schemaVersion."))
         );
      length alltogetherVars $10000.;
      /* added manually according to Var contents usage in solution by UI */
      alltogetherVars=upcase(catx(' ',primarykey,&varsOfVars.));
      call symputx("alltogetherVars",trim(alltogetherVars),"L");

      %unquote(&genMVar.);
   run;

   /* get a unique list of Var contents fileds to define data definitions payload */
   %let uniqueTogetherVars=;
   %do i=1 %to %sysfunc(countw(&alltogetherVars.,%str( )));
      %let word=%scan(&alltogetherVars.,&i,%str( ));
      %if not %sysfunc(indexw(&uniqueTogetherVars,&word,%str( ))) %then
         %let uniqueTogetherVars= &uniqueTogetherVars. &word.;
   %end;

   %if ( %sysevalf(%superq(cycle_key) ne, boolean) ) %then %do;
      %core_rest_get_cycle(host = &host.
                        , port = &port.
                        , logonHost = &logonHost.
                        , logonPort = &logonPort.
                        , username = &username.
                        , password = &password.
                        , authMethod = &authMethod.
                        , client_id = &client_id.
                        , client_secret = &client_secret.
                        , solution = &solution.
                        , key = &cycle_key.
                        , outds = _tmp_cycle
                        , outVarToken = &outVarToken.
                        , outSuccess = &outSuccess.
                        , outResponseStatus = &outResponseStatus.
                        , debug = &debug.
                        , logOptions = &logOptions.
                        , restartLUA = &restartLUA.
                        , clearCache = &clearCache.
                        );
      /* Check for errors */
      %if(not %rsk_dsexist(_tmp_cycle)) %then %do;
         %put ERROR: Unable to get the cycle information;
         %return;
      %end;
      /* Get the cycle name key */
      data _null_;
         set _tmp_cycle;
         call symputx("cycle_name", name, "L");
         call symputx("runTypeCd", runTypeCd, "L");
      run;
   %end;

   /* Get classification_point_key - 1st attempt to get a classification point */
   %if ( %sysevalf(%superq(classification_point_key) ne, boolean) ) %then %do;
      /* macro parameter */
      %let classification_point_key = &classification_point_key.;
      %let CLASSIFICATIONCONTEXT = "default": [&classification_point_key.];
   %end;

   %if ( %sysevalf(%superq(analysis_run_key) ne, boolean) ) %then %do;
      %core_rest_get_analysis_run( host = &host.
                                 , port = &port.
                                 , logonHost = &logonHost.
                                 , logonPort = &logonPort.
                                 , username = &username.
                                 , password = &password.
                                 , authMethod = &authMethod.
                                 , client_id = &client_id.
                                 , client_secret = &client_secret.
                                 , solution = &solution.
                                 , key = &analysis_run_key.
                                 , outds = _tmp_analysis_run
                                 , outVarToken = &outVarToken.
                                 , outSuccess = &outSuccess.
                                 , outResponseStatus = &outResponseStatus.
                                 , debug = &debug.
                                 , logOptions = &logOptions.
                                 , restartLUA = &restartLUA.
                                 , clearCache = &clearCache.
                                 );
      /* Check for errors */
      %if(not %rsk_dsexist(_tmp_analysis_run)) %then %do;
         %put ERROR: Unable to get the analysis run information;
         %return;
      %end;
      /* Get the analysis run name key */
      data _null_;
         set _tmp_analysis_run;
         call symputx("analysis_run_name", name, "L");
      run;

      /* Get classification_point_key - 2nd attempt to get a classification point */
      /* Pull the classification from the dim points from an analysisRun */
      %if (%sysevalf(%superq(classification_point_key) eq, boolean) ) %then %do;
         %core_rest_get_classific_point( host = &host.
                                       , port = &port.
                                       , logonHost = &logonHost.
                                       , logonPort = &logonPort.
                                       , username = &username.
                                       , password = &password.
                                       , authMethod = &authMethod.
                                       , client_id = &client_id.
                                       , client_secret = &client_secret.
                                       , key = &analysis_run_key.
                                       , restPath = analysisRuns
                                       , outds = work._tmp_ar_classification_points
                                       , outVarToken = &outVarToken.
                                       , outSuccess = &outSuccess.
                                       , outResponseStatus = &outResponseStatus.
                                       , debug = &debug.
                                       , logOptions = &logOptions.
                                       , restartLUA = &restartLUA.
                                       , clearCache = &clearCache.
                                       );
         %if ( %rsk_dsexist(work._tmp_ar_classification_points) > 0 ) %then %do;
            %if ( %rsk_attrn(work._tmp_ar_classification_points, nobs) ne 0 and %rsk_varexist(work._tmp_ar_classification_points, classificationKey) ne 0 ) %then %do;
               %let classificationContext=;
               %let __classification_point_key__=;
               %let classification_point_key=;
               proc sort data=work._tmp_ar_classification_points;
                  by classificationField;
               run;
               /* this step builds the json content to characterize the set of classification points keys */
               data _null_;
                  set work._tmp_ar_classification_points;
                  by classificationField;
                  length classificationContext $1000 __classification_point_key__ classification_point_key $1000;
                  retain __classification_point_key__ classification_point_key classificationContext "";
                  if first.classificationField then do;
                     __classification_point_key__ = "";
                     if classificationContext = "" then do;
                        classificationContext = cats('"',classificationField,'"');
                     end;
                     else do;
                           classificationContext = cats(classificationContext,',"',classificationField,'"');
                        end;
                     if not missing(classificationKey) then do;
                        __classification_point_key__ = cats('"',classificationKey,'"');
                        classification_point_key = cats('"',classificationKey,'"');
                     end;
                  end;
                  else do;
                     if not missing(classificationKey) then do;
                        __classification_point_key__ = cats(__classification_point_key__,',"',classificationKey,'"');
                        classification_point_key = cats(classification_point_key,',"',classificationKey,'"');
                     end;
                  end;
                  if last.classificationField then do;
                     classificationContext = cats(classificationContext,' : [',__classification_point_key__,']');
                     output;
                     if classification_point_key eq "" then do;
                        classificationContext = cats('"default" : []');
                     end;
                  end;
                  call symput("classificationContext",classificationContext);
                  call symput("classification_point_key",classification_point_key);
               run;
            %end;
         %end;
      %end;
   %end; /* %if ( %sysevalf(%superq(analysis_run_key) ne, boolean) ) */

   /* Get classification_point_key - 3rd attempt to get a classification point */
   /* Pull the classification from a dataDefinition and the data_definition_key to rest POST */
   %let query_filter = and(eq(schemaName,%27&schemaName.%27),eq(schemaVersion,%27&schemaVersion.%27));
   %core_rest_get_data_def(host = &host.
                           , port = &port.
                           , logonHost = &logonHost.
                           , logonPort = &logonPort.
                           , username = &username.
                           , password = &password.
                           , authMethod = &authMethod.
                           , client_id = &client_id.
                           , client_secret = &client_secret.
                           , solution = &solution.
                           , filter = %superq(query_filter)
                           , limit = 1000
                           , outds = _tmp_dataDef_summary
                           , outds_columns = _tmp_dataDef_details
                           , outVarToken = &outVarToken.
                           , outSuccess = &outSuccess.
                           , outResponseStatus = &outResponseStatus.
                           , debug = &debug.
                           , logOptions = &logOptions.
                           , restartLUA = &restartLUA.
                           , clearCache = &clearCache.
                           );

   %let data_definition_key =;
   data _tmp_dataDef_summary;
      set _tmp_dataDef_summary;
      %if( %sysevalf(%superq(schemaVersion) eq, boolean) ) %then %do;
         /* When schemaVersion is blank, the REST request above might return multiple objects (since we only filtered on schemaName).
         We need to apply an additional filter on the result table to check on the schemaVersion */
         where missing(schemaVersion);
         %put WARNING: Need to apply an additional filter to check on the schemaVersion..;
         %return;
      %end;
      call symputx("data_definition_key", key, "L");
   run;
   %if (%sysevalf(%superq(classification_point_key) eq, boolean)) %then %do;
      %if (%sysevalf(%superq(data_definition_key) ne, boolean)) %then %do;
         %let &outSuccess. = 0;
         %core_rest_get_classific_point(host = &host.
                                       , port = &port.
                                       , logonHost = &logonHost.
                                       , logonPort = &logonPort.
                                       , username = &username.
                                       , password = &password.
                                       , authMethod = &authMethod.
                                       , client_id = &client_id.
                                       , client_secret = &client_secret.
                                       , solution = &solution.
                                       , key = &data_definition_key.
                                       , restPath = dataDefinitions
                                       , outds = work._tmp_dd_classification_points
                                       , outVarToken = &outVarToken.
                                       , outSuccess = &outSuccess.
                                       , outResponseStatus = &outResponseStatus.
                                       , debug = &debug.
                                       , logOptions = &logOptions.
                                       , restartLUA = &restartLUA.
                                       , clearCache = &clearCache.
                                       );
         %if ( %rsk_dsexist(work._tmp_dd_classification_points) > 0 ) %then %do;
               %if ( %rsk_attrn(work._tmp_dd_classification_points, nobs) ne 0 and %rsk_varexist(work._tmp_dd_classification_points, classificationKey) ne 0 ) %then %do;
                  %let classificationContext=;
                  %let __classification_point_key__=;
                  %let classification_point_key=;
                  proc sort data=WORK._tmp_dd_classification_points;
                     by classificationField;
                  run;
                  /* this step builds the json content to characterize the set of classification points keys */
                  data _null_;
                     set WORK._tmp_dd_classification_points;
                     by classificationField;
                     length classificationContext $1000 __classification_point_key__ classification_point_key $1000;
                     retain __classification_point_key__ classification_point_key classificationContext "";
                     if first.classificationField then do;
                        __classification_point_key__ = "";
                        if classificationContext = "" then do;
                           classificationContext = cats('"',classificationField,'"');
                        end;
                        else do;
                              classificationContext = cats(classificationContext,',"',classificationField,'"');
                           end;
                        if not missing(classificationKey) then do;
                           __classification_point_key__ = cats('"',classificationKey,'"');
                           classification_point_key = cats('"',classificationKey,'"');
                        end;
                     end;
                     else do;
                        if not missing(classificationKey) then do;
                           __classification_point_key__ = cats(__classification_point_key__,',"',classificationKey,'"');
                           classification_point_key = cats(classification_point_key,',"',classificationKey,'"');
                        end;
                     end;
                     if last.classificationField then do;
                        classificationContext = cats(classificationContext,' : [',__classification_point_key__,']');
                        output;
                          if classification_point_key eq "" then do;
                           classificationContext = cats('"default" : []');
                        end;
                     end;
                     call symput("classificationContext",classificationContext);
                     call symput("classification_point_key",classification_point_key);
                  run;
               %end;
         %end;
      %end; /* %if (%sysevalf(%superq(data_definition_key) ne, boolean)) */
   %end; /* %if (%sysevalf(%superq(classification_point_key) eq, boolean)) */

   /* In any case and in the absence of classifications from parent objects the default classification [] is assigned. */
   %if (%sysevalf(%superq(classification_point_key) eq, boolean)) %then %do;
      %let classification_point_key =;
      %put WARNING: No classification point has been provided. A Default classification point will be placed.;
      %let CLASSIFICATIONCONTEXT = "default": [ ];
   %end;

   /* Process date directives */
   %let analysisDataName = %core_process_date_directive(name = %superq(analysisDataName));
   %let analysisDataDesc = %core_process_date_directive(name = %superq(analysisDataDesc));
   %let dataDefinitionName = %core_process_date_directive(name = %superq(dataDefinitionName));
   %let dataDefinitionDesc = %core_process_date_directive(name = %superq(dataDefinitionDesc));

   /* Get the analysisData_analysisRun_out link type key, if needed */
   %if(%sysevalf(%superq(analysis_run_key) ne, boolean)) %then %do;
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
                                 , filter = eq(objectId,%27analysisData_analysisRun_out%27)
                                 , outds = _tmp_link_type_
                                 , outVarToken = &outVarToken.
                                 , outSuccess = &outSuccess.
                                 , outResponseStatus = &outResponseStatus.
                                 , debug = &debug.
                                 , logOptions = &LogOptions.
                                 , restartLUA = &restartLUA.
                                 , clearCache = &clearCache.
                                 );
      /* Exit in case of errors */
      %if(not &&&outSuccess.. or not %rsk_dsexist(_tmp_link_type_)) %then %do;
         %put ERROR: Unable to get the analysisData_analysisRun_out link type;
         %abort;
      %end;

      /* Get the linkType key */
      data _null_;
         set _tmp_link_type_;
         call symputx("analysisData_analysisRun_key", key, "L");
      run;

   %end;

   /* Get the analysisData_cycle link type key, if needed */
   %if(%sysevalf(%superq(cycle_key) ne, boolean)) %then %do;

      %core_rest_get_link_types(host = &host
                                 , port = &port.
                                 , solution = &solution.
                                 , logonHost = &logonHost.
                                 , logonPort = &logonPort.
                                 , username = &username.
                                 , password = &password.
                                 , authMethod = &authMethod.
                                 , client_id = &client_id.
                                 , client_secret = &client_secret.
                                 , filter = eq(objectId,%27analysisData_cycle%27)
                                 , outds = _tmp_link_type_
                                 , outVarToken = &outVarToken.
                                 , outSuccess = &outSuccess.
                                 , outResponseStatus = &outResponseStatus.
                                 , debug = &debug.
                                 , logOptions = &LogOptions.
                                 , restartLUA = &restartLUA.
                                 , clearCache = &clearCache.
                                 );

      /* Exit in case of errors */
      %if(not &&&outSuccess.. or not %rsk_dsexist(_tmp_link_type_)) %then %do;
         %put ERROR: Unable to get the analysisData_cycle link type;
         %abort;
      %end;

      /* Get the linkType key */
      data _null_;
         set _tmp_link_type_;
         call symputx("analysisData_cycle_key", key, "L");
      run;

   %end;

   filename _body temp;

   data _null_;
      file _body recfm=N;
      length str var _attr_ $1000;
      put '{';
      put '"dataDefinition": {';
      %if (%sysevalf(%superq(data_definition_key) eq, boolean)) %then %do;
         str = '"name":"'||strip("&dataDefinitionName.")||'"'; put str;
         str = ',"description":"'||strip("&dataDefinitionDesc.")||'"'; put str;
         str = ',"sourceSystemCd":"'||upcase(strip("&sourceSystemCd."))||'"'; put str;
         str = ',"createdInTag":"'||upcase(strip("&solution."))||'"'; put str;
         put ',"customFields": {';
         str = '"schemaName":"'||strip("&schemaName.")||'"'; put str;
         str = ',"schemaVersion":"'||strip("&schemaVersion.")||'"'; put str;
         str = ',"defaultIndexList":"'||strip("&defaultIndexList.")||'"'; put str;
         str = ',"dataCategoryCd":"'||strip("&dataCategoryCd.")||'"'; put str;
         str = ',"dataType":"'||strip("&dataType.")||'"'; put str;
         str = ',"dataSubCategoryCd":"'||strip("&dataSubCategoryCd.")||'"'; put str;
         str = ',"riskTypeCd":"'||strip("&riskTypeCd.")||'"'; put str;
         str = ',"schemaTypeCd":"'||strip("&schemaTypeCd.")||'"'; put str;
         str = ',"businessCategoryCd":"'||strip("&businessCategoryCd.")||'"'; put str;
      %if( %upcase("&allowDataAppend.") eq "TRUE" ) %then %do;
         str = ',"allowDataAppend": true'; put str;
      %end;
      %else %do;
               str = ',"allowDataAppend": false'; put str;
            %end;
      %if( %upcase("&allowDataDelete.") eq "TRUE" ) %then %do;
         str = ',"allowDataDelete": true'; put str;
      %end;
      %else %do;
               str = ',"allowDataDelete": false'; put str;
            %end;
         str = ',"martTableNm":"'||strip("&martTableNm.")||'"'; put str;
         str = ',"martLibraryNm":"'||strip("&martLibraryNm.")||'"'; put str;
         put ',"columnInfo": [';

         %if(%sysevalf(%superq(uniqueTogetherVars) eq, boolean)) %then %do;
            %put NOTE: Empty variables for columnInfo scope in Data Definition: &analysisDataVarsScope.;
         %end;
         %else %do;
            i = 1;
            do while (strip(scan(strip("&uniqueTogetherVars."),i,' ')) ne '');
               var = strip(scan(strip("&uniqueTogetherVars."),i,' '));
               put '{';
               str = '"name": "'||strip(var)||'"'; put str;

               _attr_=strip("&allocationVars.");
               if indexw(_attr_,var) gt 0 then
                  put ', "allocation": true';

               _attr_=strip("&primaryKey.");
               if indexw(_attr_,var) gt 0 then
                  put ',"primaryKeyFlag": true';

               _attr_=strip("&filterableVars.");
               if indexw(_attr_,var) gt 0 then
                  put ',"filterable": true';

               _attr_=strip("&segmentationVars.");
               if indexw(_attr_,var) gt 0 then
                  put ',"segmentationFlag": true';

               _attr_=strip("&projectionVars.");
               if indexw(_attr_,var) gt 0 then
                  put ',"projectionFlag": true';

               _attr_=strip("&partitionByVars.");
               if indexw(_attr_,var) gt 0 then
                  put ',"partitionFlag": true';

               _attr_=strip("&attributableVars.");
               if indexw(_attr_,var) gt 0 then
                  put ',"attributable": true';

               i=i+1;
               if strip(scan(strip("&uniqueTogetherVars."),i,' ')) ne "" then do;
                  str = '},'; put str;
               end;
               else do;
                  str = '}'; put str;
               end;
            end;
         %end;
         put ']';
         put '}';
         put ',"classification": {';
         put "%superq(CLASSIFICATIONCONTEXT)";
         put '}}';
      %end; /* (%sysevalf(%superq(data_definition_key) eq, boolean)) */
      %else %do;
         str = '"key":"'||strip("&data_definition_key.")||'"'; put str;
         put '}';
      %end;

      put ',"analysisData": {';
      str = '"name":"'||strip("&analysisDataName.")||'"'; put str;
      str = ',"description":"'||strip("&analysisDataDesc.")||'"'; put str;
      str = ',"sourceSystemCd":"'||upcase(strip("&sourceSystemCd."))||'"'; put str;
      str = ',"createdInTag":"'||upcase(strip("&solution."))||'"'; put str;
      put ',"customFields": {';
      str = '"baseDttm":"'||"&objBaseDate."||'"'; put str;
      str = ',"baseDt":"'||"%substr(&objBaseDate.,1,10)"||'"'; put str;
      str = ',"strategyType":"'||upcase(strip("&strategyType."))||'"'; put str;
      str = ',"dataCategoryCd":"'||upcase(strip("&dataCategoryCd."))||'"'; put str;
      %if (%upcase(&partitionCount.) ne .) %then %do;
         str = ',"partitionCount": '||upcase(strip("&partitionCount.")); put str;
      %end;
      %if (%upcase(&constraintEnabledFlg.) eq Y) %then %do;
         %if (%sysevalf(%superq(indexList) ne, boolean)) %then %do;
            /* validate the constraint_enabled_flg field to provide the index list */
            str = ',"indexList":"'||strip("&indexList.")||'"'; put str;
         %end;
      %end;
      put '}';
      put ',"classification": {';
      put "%superq(CLASSIFICATIONCONTEXT)";
      put '}';

      /* Add objectLinks to the request body, if needed */
      %if(%sysevalf(%superq(analysis_run_key) ne, boolean)) or (%sysevalf(%superq(cycle_key) ne, boolean)) %then %do;
         put ',"objectLinks": [';
            %if(%sysevalf(%superq(analysis_run_key) ne, boolean)) %then %do;
               put '{';
               str = '"sourceSystemCd":"'||upcase(strip("&sourceSystemCd."))||'"'; put str;
               str = ',"linkType":"'||strip("&analysisData_analysisRun_key.")||'"'; put str;
               str = ',"businessObject2":"'||strip("&analysis_run_key.")||'"'; put str;
               put '}';
            %end;
            %if(%sysevalf(%superq(cycle_key) ne, boolean)) %then %do;
               %if(%sysevalf(%superq(analysis_run_key) ne, boolean)) %then %do;
                  put ',{';
               %end;
               %else %do;
                  put '{';
               %end;
               str = '"sourceSystemCd":"'||upcase(strip("&sourceSystemCd."))||'"'; put str;
               str = ',"linkType":"'||strip("&analysisData_cycle_key.")||'"'; put str;
               str = ',"businessObject2":"'||strip("&cycle_key.")||'"'; put str;
               put '}';
            %end;
         put ']';
      %end;
      put '}';
      put '}';
   run;

   /* Create a CAS session if needed */
   %if (%sysevalf(%superq(casSessionName) ne, boolean)) %then %do;
      %if not %sysfunc(sessfound(&casSessionName.)) %then %do;
         /* start a cas session, if needed */
         %core_cas_initiate_session(cas_host = &casHost.
                                 , cas_port = &casPort.
                                 , cas_session_name = &casSessionName.
                                 , cas_session_options = casdatalimit=ALL
                                 , cas_assign_librefs = Y);

         %let marked_to_terminate = Y;
      %end;
   %end;

   /* get filename info about, locationType and location */
   %if ( %upcase("&locationType.") eq "LIBNAME" ) %then %do;

      /* assign a temporary libref to CAS due to 8 characters limitation - to allow execution of datastep in the script further ahead */
      %if ( %length(&location.) > 8 or %index(%superq(location),%str( )) > 0 or %sysfunc(findc(%superq(location),' ',kn)) > 0 ) %then %do; /* CAS Lib if location > 8 or < 8 but with spaces */
         libname tmpcas cas caslib="&location.";
         %let location_tmp = tmpcas;
      %end;
      %else %do; /* keep the same 'location' once it's not an issue for datastep execution */
         %let location_tmp = &location.;
      %end;
      %if ( not %rsk_dsexist(&location_tmp.."&filename."n) ) %then %do;
         %put ERROR: 'filename' is not available in this 'location'.;
         %abort;
      %end;

      %if ( %rsk_get_lib_engine(&location_tmp.) ne CAS ) %then %do; /* libname BASE v9 */
         %if (%sysevalf(%superq(inputDataFilter) ne, boolean) or %upcase("&enableResultAttr.") eq "Y") %then %do;
            proc sql noprint;
               create view WORK."_view_&filename."n as
               select *
                  %if (%upcase("&enableResultAttr.") eq "Y" and (%upcase("&dataCategoryCd.") eq "RESULTS")) %then %do;
                     , "&cycle_key." as CYCLE_ID length=100
                     , "&cycle_name." as CYCLE_NAME length=256
                     , "&analysis_run_key." as ANALYSIS_RUN_ID length=100
                     , "&analysis_run_name." as ANALYSIS_RUN_NAME length=256
                     , "&runTypeCd." as CYCLE_RUN_TYPE length=100
                  %end;
               from &location_tmp.."&filename."n
                  %if (%sysevalf(%superq(inputDataFilter) ne, boolean)) %then %do;
                     where (&inputDataFilter.)
                  %end;
               using libname _tmp_ "%sysfunc(pathname(&location_tmp.))";
            quit;

            %if ( %rsk_attrn(WORK."_view_&filename."n, nobs) = 0) %then %do;
               %put WARNING: No data retrieved after filter applied on "&location.._view_&filename.".;
            %end;

            %let locationType = &locationType.;
            %let location = WORK;
            %let filename = _view_&filename.;
         %end;
      %end; /* %if ( %rsk_get_lib_engine(&location.) ne CAS ) */
      %else %do; /* libname CAS */

         /* See if the &location..&filename. CAS table is session-level scope.  If it is, promote it for riskData to see it. */
         %rsk_dsexist_cas(cas_lib=&location.,cas_table=&filename., cas_session_name=&casSessionName., out_var=cas_table_exists);
         %if &cas_table_exists.=1 %then %do;
            proc casutil;
               promote inCaslib="&location."  casData="&filename."
                        outCaslib="CASUSER" casOut="&filename._&casSessionTag.";
               run;
            quit;
            %let promoted_filename = &filename._&casSessionTag.;
            %let filename = &filename._&casSessionTag.;
            %let location = CASUSER;
            %let marked_to_delete = Y;
         %end;

         %if (%sysevalf(%superq(inputDataFilter) ne, boolean) or %upcase("&enableResultAttr.") eq "Y") %then %do;
            /* subset the input data source and upload */
            proc cas;
               session &casSessionName.;
               /* create a view with prior filename to add new columns */
               table.view /
                  caslib="CASUSER" name="_view_cas_riskdata_&casSessionTag." promote=TRUE
                  tables = { { name="&filename."
                              caslib="&location."
                           %if (%upcase("&enableResultAttr.") eq "Y" and (%upcase("&dataCategoryCd.") eq "RESULTS")) %then %do;
                              computedVars={
                                    {name="CYCLE_ID"}
                                    {name="CYCLE_NAME"}
                                    {name="ANALYSIS_RUN_ID"}
                                    {name="ANALYSIS_RUN_NAME"}
                                    {name="CYCLE_RUN_TYPE"}
                                    }
                              computedVarsProgram="length CYCLE_ID ANALYSIS_RUN_ID VARCHAR(100) CYCLE_NAME ANALYSIS_RUN_NAME VARCHAR(256);
                                       CYCLE_ID = '&cycle_key.';
                                       CYCLE_NAME = '&cycle_name.';
                                       ANALYSIS_RUN_ID = '&analysis_run_key.';
                                       ANALYSIS_RUN_NAME = '&analysis_run_name.';
                                       CYCLE_RUN_TYPE = '&runTypeCd.'"
                           %end;
                           %if (%sysevalf(%superq(inputDataFilter) ne, boolean)) %then %do;
                              where = "%superq(inputDataFilter)"
                           %end;
                     } };
            run;quit;
            %let locationType = &locationType.;
            %let location = CASUSER;
            %let filename = _view_cas_riskdata_&casSessionTag.;
            %let marked_to_delete = Y;
         %end;
      %end;
   %end;

   %if ( %upcase("&locationType.") in ("DIRECTORY") ) %then %do;
      %if (%sysevalf(%superq(inputDataFilter) ne, boolean) or %upcase("&enableResultAttr.") eq "Y") %then %do;
         /* turn into libname|table in order to enable filters over input data */
         %rsk_libname(STMT=libname _tmp_ base "&location.");
         /* verify if input source has the filter variables */
         /* querying the same table TODO: to be replaced in future with dataDefinition attribute: srcWhereStmt */
         proc sql noprint;
            create view _tmp_._view_&filename. as
            select *
               %if (%upcase("&enableResultAttr.") eq "Y" and (%upcase("&dataCategoryCd.") eq "RESULTS")) %then %do;
                  , "&cycle_key." as CYCLE_ID length=100
                  , "&cycle_name." as CYCLE_NAME length=256
                  , "&analysis_run_key." as ANALYSIS_RUN_ID length=100
                  , "&analysis_run_name." as ANALYSIS_RUN_NAME length=256
                  , "&runTypeCd." as CYCLE_RUN_TYPE length=100
               %end;
            from _tmp_.&filename.
            %if (%sysevalf(%superq(inputDataFilter) ne, boolean)) %then %do;
               where (&inputDataFilter.)
            %end;
            using libname _tmp_ "&location."
         ;quit;
         %if ( %rsk_attrn(_tmp_."_view_&filename."n, nobs) = 0) %then %do;
            %put WARNING: No data retrieved after filter applied on "&location.&filename.".;
         %end;
         %let locationType = &locationType.;
         %let location = &location.;
         %let filename = _view_&filename.;
      %end;
   %end;

   %if ( %upcase("&locationType.") in ("FOLDER") ) %then %do;
      /* only csv files accepted */
      %if %sysfunc(prxmatch(/.*\.csv$/i, %bquote(&filename.))) %then %do;
         filename flnm filesrvc folderPath="&location." filename="&filename.";
         %if &SYSFILRC. = 1 %then %do;
            %put ERROR: The "&location.&filename." does not exist.;
            %abort;
         %end;
      %end;
      %else %do;
         %put ERROR: The "&filename." is not a .csv file.;
         %abort;
      %end;
      %let locationType = &locationType.;
      %let location = &location.;
      %let filename = &filename.;
   %end;

   /* URL encoded to REST request */
   %let location=%sysfunc(urlencode(%bquote(&location.)));
   %let fileName=%sysfunc(urlencode(%bquote(&fileName.)));
   %core_set_base_url(host=&host, server=&server., port=&port.);
   %let requestUrl = &baseUrl./&server./objects?locationType=&locationType.%nrstr(&)location=&location.%nrstr(&)fileName=&fileName.;

   filename _resp temp;

   /* Temporary disable mlogic and symbolgen options to avoid printing of userid/pwd to the log */
   option nomlogic nosymbolgen;
   /* Send the REST request */
   %core_rest_request(url = &requestUrl.
                     , method = POST
                     , logonHost = &logonHost.
                     , logonPort = &logonPort.
                     , username = &username.
                     , password = &password.
                     , authMethod = &authMethod.
                     , headerIn = Accept:application/json
                     , body = _body
                     , contentType = application/json
                     , fout = _resp
                     , outds = rest_request_post_response
                     , outVarToken = &outVarToken.
                     , outSuccess = &outSuccess.
                     , outResponseStatus = &outResponseStatus.
                     , debug = &debug.
                     , logOptions = &oldLogOptions.
                     , restartLUA = &restartLUA.
                     , clearCache = &clearCache.
                     );

   libname _resp json fileref=_resp noalldata;

   /* Exit in case of errors */
   %if( (not &&&outSuccess..) or not(%rsk_dsexist(rest_request_post_response)) or %rsk_attrn(rest_request_post_response, nobs) = 0 ) %then %do;
      %put ERROR: Unable to accomplish the service: Create Analysis Data;
      data _null_;
         set _resp.root(keep=message);
         call symputx("resp_message",message);
      run;
      %put ERROR: &resp_message.;
      %abort;
   %end; /* (not &&&outSuccess..) */
   %else %do;
      /* Get Job Id for monitoring the execution status */
      data _null_;
         set _RESP.ANALYSISDATA_CUSTOMFIELDS(keep=importJobId);
         call symputx("importJobId",importJobId);
      run;

      %let jobState=;
      %core_rest_wait_job_execution(jobID = &importJobId.
                                    , wait_flg = Y
                                    , pollInterval = 1
                                    , maxWait = 3600
                                    , timeoutSeverity = ERROR
                                    , outJobStatus = jobState
                                    , outVarToken = accessToken
                                    , outSuccess = httpSuccess
                                    , outResponseStatus = responseStatus
                                    , debug = false
                                    );

      %if (not &&&outSuccess..) %then
         %PUT ERROR: Could not get the status of the job execution process (&importJobId.);

      data _null_;
         set _resp.analysisdata;
         call symputx("new_ad_key", key, "L");
      run;

      %let &outVarAnalysisDataKey. = &new_ad_key.;

      %if "&jobState." ne "COMPLETED" %then %do; /* send log path to execution main log */

         %put NOTE: Analysis Data will be deleted if exists!;

         %let &outVarAnalysisDataKey. = ;

         /* delete analysis data object */
         %if ( %rsk_dsexist(_resp.analysisdata) > 0 ) %then %do;
            %if ( %rsk_attrn(_resp.analysisdata, nobs) ne 0 and %rsk_varexist(_resp.analysisdata, Key) ne 0 ) %then %do;
               /* delete analysis data object */

               %core_rest_delete_analysisdata(server = &server.
                                                   , key = &new_ad_key.
                                                   , outds = _tmp_del_ad
                                                   , outVarToken = accessToken
                                                   , outSuccess = httpSuccess
                                                   , outResponseStatus = responseStatus
                                                   );
               %if (not &&&outSuccess..) %then %do;
                  data _null_;
                     set work._tmp_del_ad(keep=response);
                     call symputx("resp_message",response);
                  run;
                  %put ERROR: &resp_message.;
               %end;
            %end;
         %end;
         /* Delete the analysis data object and data definition (if new) if the compute import job fails */
         %if (%sysevalf(%superq(data_definition_key) eq, boolean)) %then %do;
            %if ( %rsk_dsexist(_resp.datadefinition) > 0 ) %then %do;
               %if ( %rsk_attrn(_resp.datadefinition, nobs) ne 0 and %rsk_varexist(_resp.datadefinition, Key) ne 0 ) %then %do;
                  /* delete data definition object */
                  data _null_;
                     set _resp.datadefinition;
                     call symputx("new_data_definition_key", key, "L");
                  run;
                  %core_rest_delete_data_def(server = riskCirrusObjects
                                                   , key = &new_data_definition_key.
                                                   , outds = _tmp_del_dd
                                                   , outVarToken = accessToken
                                                   , outSuccess = httpSuccess
                                                   , outResponseStatus = responseStatus
                                                   );
                  %if (not &&&outSuccess..) %then %do;
                     data _null_;
                        set work._tmp_del_dd(keep=response);
                        call symputx("resp_message",response);
                     run;
                     %put ERROR: &resp_message.;
                  %end;
               %end;
            %end;
         %end;

         %abort;
      %end; /* %if "&jobState." ne "COMPLETED" */

      %if ( &marked_to_delete. eq Y and %upcase("&locationType.") eq "LIBNAME" ) %then %do;
         /* Delete table in CASUSER if exists - clean intermediate tables */
         %core_cas_drop_table(cas_session_name = &casSessionName.
                           , cas_libref = CASUSER
                           , cas_table = &filename.
                           );
         %core_cas_drop_table(cas_session_name = &casSessionName.
                           , cas_libref = CASUSER
                           , cas_table = &promoted_filename.
                           );
      %end;

      %if ( %upcase("&locationType.") eq "DIRECTORY" ) %then %do;
         %rsk_delete_ds(_tmp_.&filename.);
         libname _tmp_ clear;
      %end;

   %end;

   /* Terminate the cas session if we created it */
   %if (&marked_to_terminate. eq Y) %then
   %core_cas_terminate_session(cas_session_name = &casSessionName.);

%mend core_rest_create_analysisdata;
