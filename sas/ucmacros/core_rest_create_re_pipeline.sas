/*
 Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor core_rest_create_re_pipeline

   \brief   Create a new, restricted pipeline from a locked project in Risk Engine.

   \param [in] host (optional) Host url, including the protocol
   \param [in] server Name of the REST service (Default: riskPipeline)
   \param [in] solution Solution identifier (Source system code) for Cirrus Core content packages (Default: currently blank)
   \param [in] port (optional) Server port
   \param [in] logonHost (Optional) Host/IP of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app host/ip is the same as the host/ip in the url parameter
   \param [in] logonPort (Optional) Port of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app port is the same as the port in the url parameter
   \param [in] username (optional) Username credentials
   \param [in] password (optional) Password credentials: it can be plain text or SAS-Encoded (it will be masked during execution).
   \param [in] authMethod: Authentication method (accepted values: BEARER). (Default: BEARER).
   \param [in] client_id The client id registered with the Viya authentication server. If blank, the internal SAS client id is used (only if GRANT_TYPE = password).
   \param [in] client_secret The secret associated with the client id.
   \param [in] analysisRunKey (Optional) The key of the analysis run this macro is being called in. If none is provided, no links will be created.
   \param [in] reProjectKey Key of the locked Risk Pipeline project whose main (or specific) pipeline will act as the baseline for the new pipeline and under which the new pipeline will be created.
   \param [in] rePipelineKey (optional) Key of the specific Risk Pipeline to act as baseline for the new pipeline. If not provided, the main pipeline will act as the baseline for the new pipeline.
   \param [in] name Name of the new Risk Pipeline.
   \param [in] inputCasLib Name of the CAS library to which the pipeline's input data will be passed.
   \param [in] outputCasLib Name of the CAS library to which the pipeline's results data will be passed.
   \param [in] asOfDate Date for the pipeline. Expected format: yyyy-mm-dd
   \param [in] reSaveOutputTables Flag (true/false).  If true, save the pipeline's output tables to disk (Override - Default: false or value in main pipeline)
   \param [in] rePromoteOutputTables Flag (true/false).  If true, promote the pipeline's output tables (Override - Default: true or value in main pipeline)
   \param [in] rePromoteOutputTablesLifetime.  The number of hours to keep the pipeline's promoted tables in memory. (Override - Default: no limit or value in main pipeline)
   \param [in] saveEnvironment Flag (Y/N). Determines whether the new pipeline will have a Save Environment node or not (Default: Y)
   \param [in] reSaveEnvOutputTables Flag (true/false).  If true, save the output tables in the "Save Environment" node to disk (Override - Default: false or value in main pipeline)
   \param [in] rePromoteEnvOutputTables Flag (true/false).  If true, promote the output tables in the "Save Environment" node (Override - Default: true or value in main pipeline)
   \param [in] rePromoteEnvOutputTablesLifetime.  The number of hours to keep the promoted tables from the "Save Environment" node in memory. (Override - Default: no limit or value in main pipeline)
   \param [in] reRegisterEnv Flag (true/false).  If true, registers the Risk Environment in the "Save Environment" node (Override - Default: false or value in main pipeline)
   \param [in] reScenarioDrawCount Sets the "Number of draws for stochastic methods" property of the Evaluate Portfolio node in the new pipeline
   \param [in] reMarketDataType "Input format" of the Market Data node.  Valid options are "separated" or "combined".
   \param [in] reInterval "Interval" input of the Market Data node, if reMarketDataType is "combined"
   \param [in] reIntervalAlignment "Interval alignment" input of the Market Data node, if reMarketDataType is "combined"
   \param [in] reOutputCurrency Output currency code
   \param [in] rePortfolioTable "Simple instrument data" input of the Portfolio Data node.
   \param [in] reCounterpartyTable "Counterparty data" input of the Portfolio Data node.
   \param [in] reMitigantTable "Mitigation data" input of the Portfolio Data node.
   \param [in] reQuadraticTable "Quadratic instrument data" input of the Portfolio Data node.
   \param [in] reCurrentMarketTable "Current market data" input of the Market Data node, if reMarketDataType is "separated"
   \param [in] reHistoricalMarketTable "Historical market data" input of the Market Data node, if reMarketDataType is "separated"
   \param [in] reFutureMarketTable "Scenario perturbations" input of the Market Data node, if reMarketDataType is "separated"
   \param [in] reCombinedTables CSV separated list of "Combined data" inputs of the Market Data node, if reMarketDataType is "combined"
   \param [in] reCombinedHorizonCount number of horizons for combined market data, required if reMarketDataType is "combined"
   \param [in] rePreCode Fileref pointing to the SAS or Python code to be executed in the pipeline before any action nodes
   \param [in] rePostCode Fileref pointing to the SAS or Python code to be executed in the pipeline after any action nodes
   \param [in] reCustomCode Comma separated list of Custom Code to fileref mapping.  Syntax: <Custom Code Node ID>:<Fileref with Code>[, <Custom Code Node ID>:<Fileref with Code>]
   \param [in] reParameterMatrices Comma separated list of parameter matrices. Syntax: <Pmx Name>:<Dataset Name>[, <Pmx Name>:<Dataset Name>]
   \param [in] reCashflows Comma separated list of cashflow objects. Syntax: <Cashflow Object Name>:<Dataset Name>[, <Cashflow Object Name>:<Dataset Name>]
   \param [in] reValueData Comma separated list of value data objects. Syntax: <ValueData Name>:<Dataset Name>[, <ValueData Name>:<Dataset Name>]
   \param [in] reFunctionSets Comma separated list of function sets. Syntax: <Function Set Name>:<Function Set ID>[, <Function Set Name>:<Function Set ID>]
   \param [in] reCrossClassVars Comma separated list of cross-classification variables
   \param [in] reqnQueryType Risk Engine Query Node Query Type parameter override
   \param [in] reqnOutputTables Risk Engine Query Node Output Tables parameter override
   \param [in] reqnRiskMethods Risk Engine Query Node Risk Methods parameter override
   \param [in] reqnHorizons Risk Engine Query Node Horizons parameter override
   \param [in] reqnFilter Risk Engine Query Node Filter parameter override
   \param [in] reqnOutputVariables Risk Engine Query Node Output Variables parameter override
   \param [in] reqnAggregations Risk Engine Query Node Aggregation Levels parameter override
   \param [in] reqnStatistics Risk Engine Query Node Statistics parameter override
   \param [in] debug True/False. If True, debugging informations are printed to the log (Default: false)
   \param [in] logOptions Logging options (i.e. mprint mlogic symbolgen ...)
   \param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y)
   \param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y)
   \param [out] ds_out Name of the output table that contains the pipeline's results
   \param [out] fout_code Optional. Fileref for the model's score code. A temporary fileref is created is missing
   \param [out] outVarToken Name of the output macro variable which will contain the Viya Service Ticket (Default: accessToken)
   \param [out] outSuccess Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess)
   \param [out] outResponseStatus Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus)

   \details
   This macro sends a Post request to <b><i><host>/riskPipeline/riskPipelines</i></b> \n
   See \link core_rest_request.sas \endlink for details about how to send GET requests and parse the response.

   <b>Example:</b>

   1) Set up the environment (set SASAUTOS and required LUA libraries).  Assumes the spre folder is under /riskcirruscore/core/code_libraries/release-core-2022.11
   \code
      %let core_root_path=/riskcirruscore/core/code_libraries/release-core-2022.11;
      option insert = (
         SASAUTOS = (
            "&core_root_path./spre/sas/ucmacros"
            )
         );
      filename LUAPATH ("&core_root_path./spre/lua");
   \endcode

   2) Send a Http GET request and parse the JSON response into the output table WORK.new_pipeline
   \code
     %let accessToken =;
     %core_rest_create_re_pipeline(reProjectKey = 386107d7-1fab-413d-b1e6-804f9a48a4f7
                             , name = RMC_20181231_10000
                             , inputCasLib = PUBLIC
                             , asOfDate = 2018-12-31
                             , saveEnvironment = Y
                             , reScenarioDrawCount = 1
                             , reOutputCurrency = USD
                             , ds_out = new_pipeline
                             , outVarToken = accessToken
                             , outSuccess = httpSuccess
                             , outResponseStatus = responseStatus
                             , debug = false
                             );
     %put &=accessToken;
     %put &=httpSuccess;
     %put &=responseStatus;
   \endcode

   \ingroup rgfRestUtils

   \author  SAS Institute Inc.
   \date   2021
*/
%macro core_rest_create_re_pipeline(host =
                                 , server = riskPipeline
                                 , solution =
                                 , port =
                                 , logonHost =
                                 , logonPort =
                                 , username =
                                 , password =
                                 , authMethod = bearer
                                 , client_id =
                                 , client_secret =
                                 , analysisRunKey =
                                 , reProjectKey =
                                 , rePipelineKey =
                                 , name =
                                 , inputCasLib =
                                 , outputCasLib =
                                 , asOfDate =
                                 , reSaveOutputTables = false
                                 , rePromoteOutputTables = true
                                 , rePromoteOutputTablesLifetime =
                                 , saveEnvironment = Y
                                 , reSaveEnvOutputTables = false
                                 , rePromoteEnvOutputTables = true
                                 , rePromoteEnvOutputTablesLifetime =
                                 , reRegisterEnv = false
                                 , reMethodTrace =
                                 , reScenarioDrawCount =
                                 , reMarketDataType = separated
                                 , reInterval =
                                 , reIntervalAlignment =
                                 , reOutputCurrency =
                                 , rePortfolioTable =
                                 , reCounterpartyTable =
                                 , reMitigantTable =
                                 , reQuadraticTable =
                                 , reCurrentMarketTable =
                                 , reHistoricalMarketTable =
                                 , reFutureMarketTable =
                                 , reCombinedTables =
                                 , reCombinedHorizonCount =
                                 , rePreCode =
                                 , rePostCode =
                                 , reCustomCode =
                                 , reParameterMatrices =
                                 , reCashflows =
                                 , reValueData =
                                 , reFunctionSets =
                                 , reCrossClassVars =
                                 , reAdvOptsMarketData =
                                 , reAdvOptsScoreCounterparties =
                                 , reAdvOptsEvalPort =
                                 , reAdvOptsQueryResults =
                                 , reAdvOptsSaveEnvironment =
                                 , reqnQueryType =
                                 , reqnOutputTables =
                                 , reqnRiskMethods =
                                 , reqnHorizons =
                                 , reqnFilter =
                                 , reqnOutputVariables =
                                 , reqnAggregations =
                                 , reqnStatistics =
                                 , ds_out =
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
      oldLogOptions
      new_name
      unique_name
      suffix
      rePipelineKeyResult
      combinedTable
      objectMap
      rePreCodeExtension
      rePostCodeExtension
      rePreCodeLanguage
      rePostCodeLanguage
      i
      rePipelineKeyCreated
   ;

   /* Set the required log options */
   %if(%length(&logOptions.)) %then
      options &logOptions.;
   ;

   /* Get the current value of mlogic and symbolgen options */
   %let oldLogOptions = %sysfunc(getoption(mlogic)) %sysfunc(getoption(symbolgen));

   %core_set_base_url(host=&host, server=&server., port=&port.);
   %let requestUrl = &baseUrl./&server./riskPipelines;

   /********************************/
   /* Get the Risk Engines project */
   /********************************/

   filename vre_out1 temp;
   data _null_;
      file vre_out1;
      put;
   run;

   /* Temporary disable mlogic and symbolgen options to avoid printing of userid/pwd to the log */
   option nomlogic nosymbolgen;
   /* Send the REST request */
   %core_rest_request(url = &requestUrl.?project.id=&reProjectKey.
                     , method = GET
                     , logonHost = &logonHost.
                     , logonPort = &logonPort.
                     , username = &username.
                     , password = &password.
                     , authMethod = bearer
                     , client_id = &client_id.
                     , client_secret = &client_secret.
                     , parser =
                     , outds =
                     , fout = vre_out1
                     , printResponse = N
                     , outVarToken = &outVarToken.
                     , outSuccess = &outSuccess.
                     , outResponseStatus = &outResponseStatus.
                     , debug = &debug.
                     , logOptions = &oldLogOptions.
                     , restartLUA = &restartLUA.
                     , clearCache = &clearCache.
                     );

   libname vre_lib1 json fileref=vre_out1;

   %let rePipelineKeyResult=;
   %if ("&rePipelineKey." eq %str("")) %then %do;

      %if %rsk_varexist(vre_lib1.items, type) %then %do;
         data _null_;
            set vre_lib1.items;
            where type = 'main';
            call symputx('rePipelineKeyResult', id, 'L');
         run;
      %end;

      %if ("&rePipelineKeyResult." eq %str("")) %then %do;
         %put ERROR: Failed to find the main pipeline in Risk Engines project with key &reProjectKey.;
         %abort;
      %end;

   %end;
   %else %do;

      %if %rsk_varexist(vre_lib1.items, id) %then %do;
         data _null_;
            set vre_lib1.items;
            where id = "&rePipelineKey.";
            call symputx('rePipelineKeyResult', id, 'L');
         run;
      %end;

      %if ("&rePipelineKeyResult." eq %str("")) %then %do;
         %put ERROR: Failed to find the pipeline with key &rePipelineKey. in Risk Engines project with key &reProjectKey.;
         %abort;
      %end;

   %end;

   /* Ensure a unique pipeline name is chosen... add "_<number>" to the end of the name as needed */
   %let new_name = &name.;
   %let unique_name = 0;
   %let suffix = 1;
   %do %while (&unique_name. = 0);
      data _tmp_existing_pipelines_w_name;
         set vre_lib1.items;
         where name = "&new_name.";
      run;

      %if %rsk_attrn(_tmp_existing_pipelines_w_name, nlobs) %then
         %let new_name = &name._&suffix.; /* the pipeline name is not unique... keep looking for a unique name */
      %else
         %let unique_name = 1; /* the pipeline name is unique... use this name for the new pipeline */
      %let suffix = %eval(&suffix. + 1);
   %end;

   filename vre_out1 clear;
   libname vre_lib1 clear;


   /*********************************/
   /* Get the Risk Engines pipeline */
   /*********************************/

   filename vre_fout temp;
   data _null_;
      file vre_fout;
      put;
   run;

   /* Temporary disable mlogic and symbolgen options to avoid printing of userid/pwd to the log */
   option nomlogic nosymbolgen;
   /* Send the REST request */
   %core_rest_request(url = &requestUrl./&rePipelineKeyResult.
                     , method = GET
                     , logonHost = &logonHost.
                     , logonPort = &logonPort.
                     , username = &username.
                     , password = &password.
                     , authMethod = &authMethod.
                     , client_id = &client_id.
                     , client_secret = &client_secret.
                     , parser =
                     , outds =
                     , fout = vre_fout
                     , printResponse = N
                     , outVarToken = &outVarToken.
                     , outSuccess = &outSuccess.
                     , outResponseStatus = &outResponseStatus.
                     , debug = &debug.
                     , logOptions = &oldLogOptions.
                     , restartLUA = &restartLUA.
                     , clearCache = &clearCache.
                     );

   /* Exit in case of errors */
   %if(not &&&outSuccess..) %then %do;
      %put ERROR: the request to retrieve data from the Risk Pipeline with key &rePipelineKeyResult. failed.;
      %abort;
   %end;


   /************************************************************************ */
   /* Update the JSON from the base pipeline to create the new pipeline JSON */
   /************************************************************************ */

   /* Modifications to pipeline configurations */
   data re_configuration_inputs;
      format type $32. parameter $64. value $32000.;

      type="configuration";
      parameter="asOfDate";                  value="&asOfDate.";                       output;
      parameter="inputCaslib";               value="&inputCasLib.";                    output;
      parameter="outputCaslib";              value="&outputCasLib.";                   output;
      parameter="outputCurrency";            value="&reOutputCurrency.";               output;
      parameter="reSaveTables";              value=lowcase("&reSaveOutputTables.");    output;
      parameter="rePromoteTables";           value=lowcase("&rePromoteOutputTables."); output;
      parameter="rePromoteTablesLifetime";   value="&rePromoteOutputTablesLifetime.";  output;

      type="valueData";
      %do i=1 %to %sysfunc(countw("&reValueData", ","));
         %let objectMap = %sysfunc(scan("&reValueData.", &i., ","));
         parameter=strip(scan("&objectMap.", 1, ":")); value=strip(scan("&objectMap.", 2, ":")); output;
      %end;

      type="pmx";
      %do i=1 %to %sysfunc(countw("&reParameterMatrices", ","));
         %let objectMap = %sysfunc(scan("&reParameterMatrices.", &i., ","));
         parameter=strip(scan("&objectMap.", 1, ":")); value=strip(scan("&objectMap.", 2, ":")); output;
      %end;

      type="cashflow";
      %do i=1 %to %sysfunc(countw("&reCashflows", ","));
         %let objectMap = %sysfunc(scan("&reCashflows.", &i., ","));
         parameter=strip(scan("&objectMap.", 1, ":")); value=strip(scan("&objectMap.", 2, ":")); output;
      %end;

      type="functionSet";
      %do i=1 %to %sysfunc(countw("&reFunctionSets", ","));
         %let objectMap = %sysfunc(scan("&reFunctionSets.", &i., ","));
         parameter=strip(scan("&objectMap.", 1, ":")); value=strip(scan("&objectMap.", 2, ":")); output;
      %end;

   run;

   /* Modifications to data nodes */
   data work.re_data_node_inputs;
      format type $32. parameter $64. value $32000.;
      type="portfolioDataTable";
      parameter="instrument";              value="&rePortfolioTable.";              output;
      parameter="counterparty";            value="&reCounterpartyTable.";           output;
      parameter="mitigation";              value="&reMitigantTable.";               output;
      parameter="quadratic";               value="&reQuadraticTable.";              output;

      type="marketDataTable";

      %if %lowcase("&reMarketDataType.") = "separated" %then %do;
         parameter="current";                 value="&reCurrentMarketTable.";           output;
         parameter="historical";              value="&reHistoricalMarketTable.";        output;
         parameter="scenario";                value="&reFutureMarketTable.";            output;
      %end;
      %else %if %lowcase("&reMarketDataType.") = "combined" %then %do;
         %do i=1 %to %sysfunc(countw("&reCombinedTables", ","));
            %let combinedTable = %sysfunc(scan("&reCombinedTables.", &i., ","));
            parameter="combined&i.";          value="&combinedTable.";  output;
         %end;
      %end;

      type="marketData";
      parameter="reMarketDataType";       value="&reMarketDataType.";               output;
      parameter="reInterval";             value="&reInterval.";                     output;
      parameter="reIntervalAlignment";    value="&reIntervalAlignment.";            output;
      parameter="reCombinedHorizonCount"; value="&reCombinedHorizonCount.";         output;
      parameter="reAdvancedOptions";      value="&reAdvOptsMarketData.";            output;
   run;

   /* Modifications to pipeline action nodes */

   /* RE fails if the query filter has double-quotes in it (even from the UI).  In most cases, it will work
   to convert the double-quotes to single-quotes */
   %let reqnFilter=%sysfunc(prxchange(s/(%str(%"))/%str(%')/, -1, %bquote(&reqnFilter.)));

   /* Pre and post code language is determined by the file extension type (.sas or .py).  Temp filerefs
   do not have an extension, so will assume .sas.  To be safe, pass in filerefs to non-temporary files that have the extension */
   %if "&rePreCode." ne "" %then %do;
      %let rePreCodeExtension=%scan(%sysfunc(pathname(&rePreCode.)), 2, .);
      %if "&rePreCodeExtension." = "sas" or "&rePreCodeExtension." = "" %then
         %let rePreCodeLanguage=sas;
      %else %if "&rePreCodeExtension." = "py" %then
         %let rePreCodeLanguage=python;
      %else %do;
         %put ERROR: Pre-code is an unsupported language - only SAS and python are supported;
         %abort;
      %end;
   %end;

   %if "&rePostCode." ne "" %then %do;
      %let rePostCodeExtension=%scan(%sysfunc(pathname(&rePostCode.)), 2, .);
      %if "&rePostCodeExtension." = "sas" or "&rePostCodeExtension." = "" %then
         %let rePostCodeLanguage=sas;
      %else %if "&rePostCodeExtension." = "py" %then
         %let rePostCodeLanguage=python;
      %else %do;
         %put ERROR: Post-code is an unsupported language - only SAS and python are supported;
         %abort;
      %end;
   %end;

   data re_action_node_inputs;
      format type $32. parameter $64.  value $32000.;

      type="query";
      parameter="reQueryType";         value="&reqnQueryType.";         output;
      parameter="reHorizons";          value=strip("&reqnHorizons.");   output;
      parameter="reOutputTables";      value="&reqnOutputTables.";      output;
      parameter="reRiskMethods";       value="&reqnRiskMethods.";       output;
      parameter="reFilter";            value="%superq(reqnFilter)";     output;
      parameter="reOutputVariables";   value="&reqnOutputVariables.";   output;
      parameter="reAggregationLevels"; value="&reqnAggregations.";      output;
      parameter="reStatistics";        value="&reqnStatistics.";        output;
      parameter="reAdvancedOptions";   value="&reAdvOptsQueryResults."; output;

      type="scoreCounterparties";
      parameter="reMethodTrace";       value=lowcase("&reMethodTrace.");         output;
      parameter="reScenarioDrawCount"; value="&reScenarioDrawCount.";            output;
      parameter="reAdvancedOptions";   value="&reAdvOptsScoreCounterparties.";   output;

      type="evaluatePortfolio";
      parameter="reMethodTrace";          value=lowcase("&reMethodTrace.");   output;
      parameter="reScenarioDrawCount";    value="&reScenarioDrawCount.";      output;
      parameter="reAdvancedOptions";      value="&reAdvOptsEvalPort.";        output;
      parameter="reCrossClassVariables";  value="&reCrossClassVars.";         output;

      type="saveEnvironment";
      parameter="reSaveTables";              value=lowcase("&reSaveEnvOutputTables.");    output;
      parameter="rePromoteTables";           value=lowcase("&rePromoteEnvOutputTables."); output;
      parameter="rePromoteTablesLifetime";   value="&rePromoteEnvOutputTablesLifetime.";  output;
      parameter="reRegisterEnv";             value=lowcase("&reRegisterEnv.");            output;
      parameter="reAdvancedOptions";         value="&reAdvOptsSaveEnvironment.";          output;

      type="customCode";
      parameter="rePreCode";           value="&rePreCode.";             output;
      parameter="rePreCodeLanguage";   value="&rePreCodeLanguage.";     output;
      parameter="rePostCode";          value="&rePostCode.";            output;
      parameter="rePostCodeLanguage";  value="&rePostCodeLanguage.";    output;
      %do i=1 %to %sysfunc(countw("&reCustomCode", ","));
         %let objectMap = %sysfunc(scan("&reCustomCode.", &i., ","));
         parameter=strip(scan("&objectMap.", 1, ":")); value=strip(scan("&objectMap.", 2, ":"));  output;
      %end;
   run;

   %put NOTE: Building new pipeline body using Lua module sas.risk.re.re_rest_builder.buildPipelineBody;
   filename vre_new temp;
   %core_rest_build_re_pipeline_body(from_pipeline_json_fref = vre_fout
                                    , new_pipeline_name = &new_name.
                                    , action_nodes_changes_ds = work.re_action_node_inputs
                                    , data_nodes_changes_ds = work.re_data_node_inputs
                                    , configuration_changes_ds = work.re_configuration_inputs
                                    , save_environment = &saveEnvironment.
                                    , new_pipeline_json_fref = vre_new
                                    , restartLua = &restartLUA.
                                    , debug = &debug.);


   /*************************************************/
   /* Create the new pipeline with the updated JSON */
   /*************************************************/

   data _null_;
      file vre_fout;
      put;
   run;

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
                     , client_id = &client_id.
                     , client_secret = &client_secret.
                     , headerIn = Accept: application/json;charset=utf-8
                     , body = vre_new
                     , contentType = application/json;charset=utf-8
                     , parser =
                     , outds =
                     , fout = vre_fout
                     , printResponse = N
                     , outVarToken = &outVarToken.
                     , outSuccess = &outSuccess.
                     , outResponseStatus = &outResponseStatus.
                     , debug = &debug.
                     , logOptions = &oldLogOptions.
                     , restartLUA = &restartLUA.
                     , clearCache = &clearCache.
                     );

   /* Exit in case of errors */
   %if(not &&&outSuccess..) %then %do;
      %put ERROR: the request to create a new pipeline based off of the Risk Pipeline with key &rePipelineKeyResult. failed.;
      %abort;
   %end;

   filename vre_new clear;

   libname _tmp_vre json fileref=vre_fout;
   filename vre_fout clear;

   data &ds_out.;
      set _tmp_vre.root;
      set _tmp_vre.project (keep=name rename=(name=projectName));
   run;
   libname _tmp_vre clear;

   /* Create link from analysis run to risk pipeline */
   %if %sysevalf(%superq(analysisRunKey)^=,boolean) %then %do;
      /* Get the key of the risk pipeline that was created */
      %let rePipelineKeyCreated=;
      data _null_;
         set &ds_out.;
         call symputx("rePipelineKeyCreated", id, "L");
      run;

      /* Get the keys required to build a link */
      %let keyPrefixAnalysisRun = %substr(&analysisRunKey., 1, 7);
      %let keyPrefixRiskPipeline = %substr(&rePipelineKeyCreated., 1, 7);

      /* Send request to create a link */
      %let &outSuccess. = 0;
      %core_rest_create_link_inst(host = &host.
                                , port = &port.
                                , logonHost = &logonHost.
                                , logonPort = &logonPort.
                                , username = &username.
                                , password = &password.
                                , authMethod = &authMethod.
                                , client_id = &client_id.
                                , client_secret = &client_secret.
                                , link_instance_id = analysisRun_riskPipeline_&keyPrefixAnalysisRun._&keyPrefixRiskPipeline.
                                , linkSourceSystemCd = &solution.
                                , link_type = analysisRun_riskPipeline
                                , solution = &solution.
                                , business_object1 = &analysisRunKey.
                                , business_object2 = &rePipelineKeyCreated.
                                , collectionObjectKey = &analysisRunKey.
                                , collectionName = analysisRuns
                                , outds = link_instance
                                , outVarToken = &outVarToken.
                                , outSuccess = &outSuccess.
                                , outResponseStatus = &outResponseStatus.
                                , debug = &debug.
                                , logOptions = &oldLogOptions.
                                , restartLUA = &restartLUA.
                                , clearCache = &clearCache.
                                );

      /* Exit in case of errors */
      %if(not &&&outSuccess..) %then %do;
         %put ERROR: Unable to create link between analysis run: &analysisRunKey. and risk pipeline: &rePipelineKeyCreated.;
         %abort;
      %end;
   %end;

%mend;