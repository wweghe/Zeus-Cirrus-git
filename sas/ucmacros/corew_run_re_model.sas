%macro corew_run_re_model(host =
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
                        , reNewPipelineName =
                        , reParametersDs =
                        , inPortfolio =
                        , inCounterparty =
                        , inCollateral =
                        , inMitigant =
                        , inScenarios =
                        , inScenarioSet =
                        , scenWeightCalcFlg =
                        , weightedCalcOutputVars =
                        , outResults =
                        , sasPreCodeFileRef =
                        , rePreCodeFileRef =
                        , rePostCodeFileRef =
                        , sasPostCodeFileRef =
                        , allocPostRiskCodeFileRef =
                        , reDeletePipelineInputs =
                        , reDeletePipelineOutputs =
                        , outReParametersDs =
                        , asOfDate =
                        , casTablesTag =
                        , inModelCasLib =
                        , outModelCasLib =
                        , casSessionName =
                        , maxWait =
                        , outVarToken = accessToken
                        , debug = false
                        );

   %local
      inCasLibref outCasLibref
      rePipelineName rePipelineKey pipeline_qvals_table
      asOfDateFmt outHistorical outCurrent outFuture
      ds_in_portfolio ds_in_counterparty ds_in_mitigant ds_in_collateral ds_in_cashflow ds_in_synth_position
      ds_in_scenario ds_in_scen_info ds_out_model_result
      reSaveOutputTables
      rePromoteOutputTables
      rePromoteOutputTablesLifetime
      reOutputCurrency
      reCrossClassVariables
      reFunctionSets
      reParameterMatrices
      reCashflows
      reValueData
      reMethodTrace
      reScenarioDrawCount
      reQueryType
      reOutputTables
      reRiskMethods
      reHorizons
      reFilter
      reAggregationLevels
      reOutputVariables
      reStatistics
      reCustomCode
      reDeletePipelineInputs
      reDeletePipelineOutputs
      reAdvOptsMarketData
      reAdvOptsScoreCounterparties
      reAdvOptsEvalPort
      reAdvOptsQueryResults
      reAdvOptsSaveEnvironment
      reSaveEnvOutputTables
      rePromoteEnvOutputTables
      rePromoteEnvOutputTablesLifetime
      reRegisterEnv
      rePipelineEnvCasLib
      rePipelineEnvCasTable
      reCreatePipelineFlag
      reRunPipelineFlag
      re_output_variables_ignore
      reOutputTableWhereClause
      _tmp_scenario_name_
      ;

   /* Read in the RE macrovariables */
   data _null_;
      set &reParametersDs.;
      call symputx(name, value, "L");
   run;
   
   /* Check and set RE macrovariables: reCreatePipelineFlag reRunPipelineFlag */
   %let reRunPipelineFlag = %sysfunc(coalescec(&reRunPipelineFlag., Y));
   %let reCreatePipelineFlag=%sysfunc(coalescec(&reCreatePipelineFlag., Y));

   %if "&reRunPipelineFlag." = Y %then
      %let reCreatePipelineFlag = Y;

   %let asOfDateFmt=%sysfunc(putn(&asOfDate., yymmddn8.));

   %let inCasLibref = %rsk_get_unique_ref(type = lib, engine = cas, args = caslib="&inModelCaslib." sessref=&casSessionName.);
   %let outCasLibref = %rsk_get_unique_ref(type = lib, engine = cas, args = caslib="&outModelCaslib." sessref=&casSessionName.);

   /**********************************/
   /* Run the SAS pre-execution code */
   /**********************************/
   %if "&sasPreCodeFileRef." ne "" %then %do;
      %put Note: Running the credit risk model pre-Model code;
      %include &sasPreCodeFileRef. / source2 lrecl = 32000;
      filename &sasPreCodeFileRef. clear;
   %end;
   
   /*Resolve three macros to grab active values*/
   proc sort data=sashelp.vmacro  (where=(name in ("REVALUEDATA","REPARAMETERMATRICES","RECASHFLOWS") and scope = "COREW_RUN_RE_MODEL")) out=tmp_hld;
      by name offset;
   run;

   data tmp_hld_2;
      length finalValue $10000;
      set tmp_hld;
      retain finalValue "";
      by name;
      if first.name then
      finalValue=value;
      if not first.name then
         finalValue=catt(finalValue, value);
      if last.name then output;
   run;

   data _null_;
      set tmp_hld_2;
      finalValue=resolve(finalValue);
      call symputx(name, finalValue, "L");
   run;
 
   /*******************************************/
   /* Process the Risk Engines macrovariables */
   /*******************************************/
   /* reCrossClassVariables - if not specified, set it to all character variables in the portfolio */
   /* if it is specified, ensure that it contains instid */
   %if "&reCrossClassVariables." = "" %then %do;
      %let reCrossClassVariables=%sysfunc(prxchange(s/\s+/%str(,)/, -1, %rsk_getvarlist(&inCasLibref.."&inPortfolio."n, type=C)));
   %end;
   %else %if not %sysfunc(prxmatch(/\binstid\b/i, %bquote(&reCrossClassVariables.))) %then %do;
      %let reCrossClassVariables = %bquote(instid,&reCrossClassVariables);
   %end;

   /* add _inst_scenario_forecast_time_ (numeric) to the cross-class vars if it is in the portfolio */
   %if %rsk_varexist(&inCasLibref.."&inPortfolio."n, _inst_scenario_forecast_time_) %then
      %let reCrossClassVariables = %bquote(&reCrossClassVariables,_inst_scenario_forecast_time_);

   /* reAggregationLevels must contain instid */
   %if "&reAggregationLevels." = "" %then %do;
      %let reAggregationLevels=instid;
   %end;
   %else %if not %sysfunc(prxmatch(/\binstid\b/i, %bquote(&reAggregationLevels.))) %then %do;
      %let reAggregationLevels = %bquote(instid,&reAggregationLevels);
   %end;

   /* if reQueryType is not provided or is set to noaggregate, remove any aggregation levels
         -noaggregate: we are not doing aggregation in the query, so aggregation levels are not needed.
         (If aggregation levels are provided RE will not use them but will throw a warning.)
         -our default is noaggregate, since the typical query will be at the most detail level.  RE has
         signficantly better performance when noaggregate is used.
   */
   %if "&reQueryType." = "" or %lowcase("&reQueryType.") = "noaggregate" %then
      %let reAggregationLevels=;

   /* reOutputTables must contain VALUES */
   %if "&reOutputTables." = "" %then %do;
      %let reOutputTables=VALUES;
   %end;
   %else %if not %sysfunc(prxmatch(/\bVALUES\b/i, %bquote(&reOutputTables.))) %then %do;
      %let reOutputTables = %bquote(VALUES,&reOutputTables);
   %end;

   /* Resolve the reHorizons horizon list */
   %core_resolve_re_horizons(inHorizonsList = %bquote(&reHorizons.)
                           , outHorizonsVar = reHorizons
                           , inScenariosCasLib = &inModelCasLib.
                           , inScenariosTable = &inScenarios.
                           , horizonVarName = horizon
                           , casSessionName = &casSessionName.
                           );

   /* If the portfolio table has the _effectiveDate_, _ineffectiveDate_, or _inst_scenario_name_ columns,
   then it is using dynamic positions.  In that case, we need to set handleDynamicPositions = TRUE
   in the evaluatePortfolio and scoreCounterparties nodes of the Risk Pipeline */
   %let reOutputTableWhereClause=1=1;
   %if %rsk_varexist(&inCasLibref.."&inPortfolio."n, _effectiveDate_)
       or %rsk_varexist(&inCasLibref.."&inPortfolio."n, _ineffectiveDate_)
       or %rsk_varexist(&inCasLibref.."&inPortfolio."n, _inst_scenario_name_) %then %do;

      %if %superq(reAdvOptsEvalPort) ne %then
         %let reAdvOptsEvalPort = %superq(reAdvOptsEvalPort)|handleDynamicPositions:TRUE|addDynamicPositionOnHorizon:TRUE;
      %else
         %let reAdvOptsEvalPort = handleDynamicPositions:TRUE|addDynamicPositionOnHorizon:TRUE;

      %if %superq(reAdvOptsScoreCounterparties) ne %then
         %let reAdvOptsScoreCounterparties = %superq(reAdvOptsScoreCounterparties)|handleDynamicPositions:TRUE|addDynamicPositionOnHorizon:TRUE;
      %else
         %let reAdvOptsScoreCounterparties = handleDynamicPositions:TRUE|addDynamicPositionOnHorizon:TRUE;

      /* The RE dynamic features position will output results for all scenarios for a given instid+_inst_scenario_name_ row in the portfolio.
         The results for the other scenarios not matching _inst_scenario_name_ are all 0, but are present.

         For example, say there are 2 scenarios, Adverse and Basecase, and you have the following instruments in the RE input portfolio:
            -CI_0001 is a normal (backbook) instid, so has
               synthetic_instrument_flg=N and _inst_scenario_name_=""
            -synth_01 is a synthetic instrument generated for the Adverse scenario, so has:
               synthetic_instrument_flg=Y for _inst_scenario_name_=Adverse
            -CI_0002 is a normal instid that was eliminated in the Adverse scenario, so has:
               synthetic_instrument_flg=N for _inst_scenario_name_=Basecase
               synthetic_instrument_flg=E for _inst_scenario_name_=Adverse

         The RE query results would then look like this:

         instid      synthetic_instrument_flg   _inst_scenario_name_    AnalysisName      _horizon_      ECL      <include_in_results> <filter>
         CI_0001     N                                                  Basecase          372            #        Y                    _inst_scenario_name_ = ""
         CI_0001     N                                                  Adverse           372            #        Y                    _inst_scenario_name_ = ""
         synth_01    Y                          Adverse                 Basecase          372            0        N
         synth_01    Y                          Adverse                 Adverse           372            #        Y                    _inst_scenario_name_ =AnalaysisName
         CI_0002     N                          Basecase                Basecase          372            #        Y                    _inst_scenario_name_ =AnalaysisName
         CI_0002     N                          Basecase                Adverse           372            0        N
         CI_0002     E                          Adverse                 Basecase          372            0        N
         CI_0002     E                          Adverse                 Adverse           372            #        Y                    _inst_scenario_name_ =AnalaysisName

         These extra rows will cause downstream calculation issues and primary key violations.  To avoid, we only want to keep the rows above with
         <include_in_results> set to Y by using the corresponding <filter> */
      %if %rsk_varexist(&inCasLibref.."&inPortfolio."n, _inst_scenario_name_) %then %do;
         %let reOutputTableWhereClause = _inst_scenario_name_ eq '' or AnalysisName = _inst_scenario_name_;
         %if not %sysfunc(prxmatch(/\b_inst_scenario_name_\b/i, %bquote(&reCrossClassVariables.))) %then
            %let reCrossClassVariables = %bquote(&reCrossClassVariables,_inst_scenario_name_);
         %if not %sysfunc(prxmatch(/\bsynthetic_instrument_flg\b/i, %bquote(&reCrossClassVariables.))) %then
            %let reCrossClassVariables = %bquote(&reCrossClassVariables,synthetic_instrument_flg);
      %end;

   %end;

   %if %upcase(&reSaveEnvOutputTables.) eq TRUE %then %do;
      %if %superq(reAdvOptsSaveEnvironment) ne %then
         %let reAdvOptsSaveEnvironment = %superq(reAdvOptsSaveEnvironment)|compress:TRUE;
      %else
         %let reAdvOptsSaveEnvironment = compress:TRUE;
   %end;


   /****************************************************/
   /* Prepare scenarios to be consumed by the pipeline */
   /****************************************************/
   %let outHistorical = historical_&asOfDateFmt._&casTablesTag.;
   %let outCurrent = current_&asOfDateFmt._&casTablesTag.;
   %let outFuture = future_&asOfDateFmt._&casTablesTag.;
   %corew_prepare_re_scenarios(inScenarios = &inScenarios.
                              , inCasLib = &inModelCasLib.
                              , outHistorical = &outHistorical.
                              , outCurrent = &outCurrent.
                              , outFuture = &outFuture.
                              , outCasLib = &inModelCasLib.
                              , casSessionName = &casSessionName.
                              , asOfDate = &asOfDate.
                              );

   /*************************************************************************************/
   /* Determine final values of re* parameters used in corew_run_re_model, if requested */
   /*************************************************************************************/

   %if "&outReParametersDs." ne "" %then %do;

      /*Work around for REPARAMETERMATRICES due to length*/
      proc sort data=sashelp.vmacro (where=((substr(name, 1,2) eq "RE") and scope = "COREW_RUN_RE_MODEL")) out=temp_repmx;
         by name offset;
      run;

      data temp_repmx2 (drop=value);
         length finalValue $10000;
         set temp_repmx;
         retain finalValue "";
         by name;
         if first.name then
            finalValue=value;
         if not first.name then
            finalValue=catt(finalValue, value);
         if last.name then output;
      run;

      data &inModelCasLib..&outReParametersDs.(keep = name value);
         length value $10000;
	 set temp_repmx2;
	 value=resolve(finalValue);
	 call symputx(name, value, "L");
      run;

   %end;

   %if "&reCreatePipelineFlag." eq "N" %then %do;
      %put NOTE: The reCreatePipelineFlag has been set to &reCreatePipelineFlag.. The model execution has been stopped before the creation of the RE pipeline.;
      %return;
   %end;
   /*************************************/
   /* Create the Risk Engines pipeline  */
   /*************************************/
   %core_rest_create_re_pipeline(host = &host.
                              , server = &server.
                              , solution = &solution.
                              , port = &port.
                              , logonHost = &logonHost.
                              , logonPort = &logonPort.
                              , username = &username.
                              , password = &password.
                              , authMethod = &authMethod.
                              , client_id = &client_id.
                              , client_secret = &client_secret.
                              , analysisRunKey = &analysisRunKey.
                              , reProjectKey = &reProjectKey.
                              , name = &reNewPipelineName.
                              , inputCasLib = &inModelCasLib.
                              , outputCasLib = &outModelCasLib.
                              , asOfDate = %sysfunc(putn(&asOfDate., YYMMDD10.))
                              , reSaveOutputTables = %sysfunc(coalescec(&reSaveOutputTables.,false))
                              , rePromoteOutputTables = %sysfunc(coalescec(&rePromoteOutputTables.,true))
                              , rePromoteOutputTablesLifetime = &rePromoteOutputTablesLifetime.
                              , saveEnvironment = Y
                              , reSaveEnvOutputTables = %sysfunc(coalescec(&reSaveEnvOutputTables.,false))
                              , rePromoteEnvOutputTables = %sysfunc(coalescec(&rePromoteEnvOutputTables.,true))
                              , rePromoteEnvOutputTablesLifetime = &rePromoteEnvOutputTablesLifetime.
                              , reRegisterEnv = %sysfunc(coalescec(&reRegisterEnv.,false))
                              , reMethodTrace = %sysfunc(coalescec(&reMethodTrace.,false))
                              , reScenarioDrawCount = &reScenarioDrawCount.
                              , reOutputCurrency = &reOutputCurrency.
                              , rePortfolioTable = &inPortfolio.
                              , reCounterpartyTable = &inCounterparty.
                              , reMitigantTable = &inMitigant.
                              , reCurrentMarketTable = &outCurrent.
                              , reHistoricalMarketTable = &outHistorical.
                              , reFutureMarketTable = &outFuture.
                              , rePreCode = &rePreCodeFileRef.
                              , rePostCode = &rePostCodeFileRef.
                              , reCustomCode = %bquote(&reCustomCode.)
                              , reParameterMatrices = %bquote(&reParameterMatrices.)
                              , reCashflows = %bquote(&reCashflows.)
                              , reValueData = %bquote(&reValueData.)
                              , reFunctionSets = %bquote(&reFunctionSets.)
                              , reCrossClassVars = %bquote(&reCrossClassVariables.)
                              , reAdvOptsMarketData = %superq(reAdvOptsMarketData)
                              , reAdvOptsScoreCounterparties = %superq(reAdvOptsScoreCounterparties)
                              , reAdvOptsEvalPort = %superq(reAdvOptsEvalPort)
                              , reAdvOptsQueryResults = %superq(reAdvOptsQueryResults)
                              , reAdvOptsSaveEnvironment = %superq(reAdvOptsSaveEnvironment)
                              , reqnQueryType = %sysfunc(coalescec(&reQueryType.,noaggregate))
                              , reqnOutputTables = %bquote(&reOutputTables.)
                              , reqnRiskMethods = %bquote(&reRiskMethods.)
                              , reqnHorizons = %bquote(&reHorizons.)
                              , reqnFilter = %bquote(&reFilter.)
                              , reqnOutputVariables = %bquote(&reOutputVariables.)
                              , reqnAggregations = %bquote(&reAggregationLevels.)
                              , reqnStatistics = %bquote(&reStatistics.)
                              , ds_out = pipeline_create_results
                              , outVarToken = &outVarToken.
                              , debug = &debug.
                              );

   /* Exit in case of errors */
   %if(not &httpSuccess.) %then %do;
      %put ERROR: Failed to create Risk Pipeline &reNewPipelineName. in project &reProjectKey.;
      %abort;
   %end;

   data _null_;
      set pipeline_create_results;
      call symputx("rePipelineName", name, "L");
      call symputx('rePipelineKey', id, 'L');
   run;
   %let pipeline_qvals_table=&rePipelineName._qvals;


   %if "&reRunPipelineFlag." eq "N" %then %do;
      %put NOTE: The reRunPipelineFlag has been set to &reRunPipelineFlag.. The model execution has been stopped before running of the RE pipeline.;
      %return;
   %end;
   /********************************************************/
   /* Run the Risk Engines pipeline and monitor its status */
   /********************************************************/
   %core_rest_run_re_pipeline(host = &host.
                           , server = &server.
                           , port = &port.
                           , logonHost = &logonHost.
                           , logonPort = &logonPort.
                           , username = &username.
                           , password = &password.
                           , authMethod = &authMethod.
                           , client_id = &client_id.
                           , client_secret = &client_secret.
                           , rePipelineKey = &rePipelineKey.
                           , outQvals = &pipeline_qvals_table.
                           , outEnvTableInfo = environment_table_info
                           , outCasLib = &outModelCasLib.
                           , casSessionName = &casSessionName.
                           , maxWait = &maxWait.
                           , outVarToken = &outVarToken.
                           , debug = &debug.
                           );

   /* Exit if the pipeline execution failed */
   %if(not &httpSuccess.) %then %do;
      %put ERROR: Failed to execute Risk Pipeline &reNewPipelineName. in project &reProjectKey.;
      %abort;
   %end;

   /* Exit if the pipeline failed to produce the expected query values results */
   %rsk_dsexist_cas(cas_lib=%superq(outModelCasLib),cas_table=%superq(pipeline_qvals_table), cas_session_name=&casSessionName.);
   %if (not &cas_table_exists.) %then %do;
      %put ERROR: The Risk Pipeline &reNewPipelineName. in project &reProjectKey. failed to produce query results;
      %abort;
   %end;

   /* Set the rePipelineEnvCasLib and rePipelineEnvCasTable macrovariables, which point to the pipeline's environment
   table.  The pipeline's environment table is needed to query the pipeline results again in post-SAS code, among other things */
   data _null_;
      set environment_table_info;
      call symputx("rePipelineEnvCasLib", caslibName, "L");
      call symputx("rePipelineEnvCasTable", tableName, "L");
   run;

   /* Update reOutputVariables to be the list of output variables actually used in the RE pipeline */
   proc cas;
      session &casSessionName.;
      table.view /
         caslib="&rePipelineEnvCasLib." name="re_env_table" replace=TRUE
         tables = { { caslib="&rePipelineEnvCasLib." name="&rePipelineEnvCasTable." } }
      ;
   run;

   %let envCasLibref = %rsk_get_unique_ref(type = lib, engine = cas, args = caslib="&rePipelineEnvCasLib." sessref=&casSessionName.);
   proc sql noprint;
      select distinct NAME into :reOutputVariables separated by ' '
      from &envCasLibref..re_env_table
      where upcase(category)="VARIABLE" and upcase(subcategory)="PRICE" and prxmatch('/"dropped"\s*:\s*true\b/i', attributes) = 0;
   quit;
   libname &envCasLibref. clear;

   /******************************************/
   /* Weight calculation                     */
   /******************************************/
   %if(%sysevalf(%superq(scenWeightCalcFlg) ne, boolean)) %then %do;
      %if(&scenWeightCalcFlg. = Y) %then %do;

         /* keep only the weighted output variables were also pipeline output variables */
         %let weightedCalcOutputVars = %rsk_filter_mvar_list(mvar_list=&weightedCalcOutputVars., filter_mvar_list=&reOutputVariables.);

         %corew_weights_calculation(inResults = &pipeline_qvals_table.
                                    , CASLib = &outModelCaslib.
                                    , inScenarios = &inScenarioSet.
                                    , resultsFilter = &reOutputTableWhereClause.
                                    , classAggregVars = _DATE_ _HORIZON_ FORECAST_TIME INSTID
                                    , varWeight = weight
                                    , weightedCalcOutputVars = &weightedCalcOutputVars.
                                    , scenarioAggName = Weighted
                                    , casSessionName = &casSessionName.
                                    , outSumResults = &outResults._sum
                                    );

         %if ( %rsk_dsexist(&outCasLibref..&outResults._sum) < 1 ) %then %do;
            %put ERROR: None of the weighted variables selected were found in the pipeline%str(%')s results. Weighted scenarios calculation were not generated.;
            %return;
         %end;

         /* get the RE pipeline output variables that are NOT weighted variables */
         %let re_output_variables_ignore=%rsk_filter_mvar_list(mvar_list=&reOutputVariables.
                                                               , filter_mvar_list=&weightedCalcOutputVars.
                                                               , filter_method=drop);
      %end;
   %end;

   /******************************************/
   /* Join the RE results onto the portfolio */
   /******************************************/
   /* get the distinct horizons that result from RE model output table */
   data _null_;
      set &outCasLibref.."&inScenarioSet."n (obs=1);
      call symputx("_tmp_scenario_name_",scenarioName,'L');
   run;
   data &outCasLibref.."&ds_out_model_result."n(rename=(AnalysisName=scenario_name _horizon_=horizon)
                                                   drop=_horidx_ _date_ scenarioName);

     /* Set internal variables for tracking movement changes */
     length MOVEMENT_ID 8. MOVEMENT_DESC varchar(100);
     retain
        MOVEMENT_ID 1
        MOVEMENT_DESC "01. Model Output" ;
      merge
         &inCasLibref.."&inPortfolio."n
         %if(%sysevalf(%superq(reFilter) ne, boolean)) %then %do;
              ( where=(&reFilter.) );
           %end;
         &outCasLibref.."&pipeline_qvals_table."n (where=(&reOutputTableWhereClause.))
         ;
      by instid;

         if _N_ = 0 then
            set &inCasLibref.."&inScenarioSet."n (keep=scenarioName forecast_time);

         if _N_ = 1 then do;
            declare hash hFT(dataset: "&inCasLibref..'&inScenarioSet.'n");
            hFT.defineKey("scenarioName");
            hFT.defineData("forecast_time");
            hFT.defineDone();
         end;

         /* Lookup the scenario's forecast_time */
         drop __rcFT__;
         call missing(forecast_time);
         scenarioName=AnalysisName;
         __rcFT__ = hFT.find();
         forecast_time = coalesce(forecast_time, 0);
         output;

      /* if weighted table exists generate new rows per each horizon available, which relates to each scenario */
      %if ( %rsk_dsexist(&outCasLibref..&outResults._sum) > 0 ) %then %do;
         if scenarioName = "&_tmp_scenario_name_." then do;
               /* Each row will have at this point the same info for all fields coming from Portfolio and a new 'scenario_name(AnalysisName)' = "Weighted" with a speceific horizon */
               AnalysisName = "Weighted";
               /* ignore by setting to missing all the other variables not selected to scenario weighting coming from RE model execution */
               %sysfunc(prxchange(s/(\w+)/$1=.%str(;)/, -1, &re_output_variables_ignore.));
               output;
         end;
      %end;
   run;

   /* if weighted table exists merge previous table with this scenario weighting calculation */
   %if ( %rsk_dsexist(&outCasLibref..&outResults._sum) > 0 ) %then %do;
      data &outCasLibref.."&ds_out_model_result."n;
         merge
            &outCasLibref.."&ds_out_model_result."n
            &outCasLibref..&outResults._sum (drop= _date_ _name_
               rename=(analysisName=scenario_name _horizon_=horizon ))
         ;
         by instid scenario_name horizon FORECAST_TIME;
      run;
   %end;

   %core_cas_drop_table(cas_session_name = &casSessionName.
                     , cas_libref = &outModelCasLib.
                     , cas_table = &outResults._sum);


   /*******************************************/
   /* Run the allocation model post-risk code */
   /*******************************************/
   %if "&allocPostRiskCodeFileRef." ne "" %then %do;
      %if %sysfunc(fexist(&allocPostRiskCodeFileRef.)) %then %do;
         %put Note: Running the allocation model post-risk code;
         %include &allocPostRiskCodeFileRef. / source2 lrecl = 32000;
         filename &allocPostRiskCodeFileRef. clear;
      %end;
   %end;


   /***********************************/
   /* Run the SAS post-execution code */
   /***********************************/
   %if "&sasPostCodeFileRef." ne "" %then %do;
      %put Note: Running the credit risk model post-Model code;
      %include &sasPostCodeFileRef. / source2 lrecl = 32000;
      filename &sasPostCodeFileRef. clear;
   %end;


   /*******************************************************************/
   /* Delete the Input/Output Risk Engines pipeline data if requested */
   /*******************************************************************/
   %if "&reDeletePipelineInputs." eq "Y" or "&reDeletePipelineOutputs." eq "Y" %then %do;

      %core_rest_delete_re_pipeline(host = &host.
                                 , server = &server.
                                 , port = &port.
                                 , logonHost = &logonHost.
                                 , logonPort = &logonPort.
                                 , username = &username.
                                 , password = &password.
                                 , authMethod = &authMethod.
                                 , client_id = &client_id.
                                 , client_secret = &client_secret.
                                 , pipeline_key = &rePipelineKey.
                                 , del_inputs = &reDeletePipelineInputs.
                                 , del_pipeline = N
                                 , del_results = &reDeletePipelineOutputs.
                                 , outds = re_deleted_pipeline
                                 , casSessionName = &casSessionName.
                                 , outVarToken = &outVarToken.
                                 , debug = &debug.
                                 );

         %if(not &httpSuccess.) %then %do;
            %put ERROR: Failed to delete inputs/outputs for Risk Pipeline &reNewPipelineName. in project &reProjectKey.;
            %abort;
         %end;

         /* The QVALS table might have been created by the the pipeline's post-execution program, so need to delete it separately */
         %if("&reDeletePipelineOutputs." eq "Y") %then %do;

            /* Delete the CAS table and source file if exist */
            %core_cas_drop_table(cas_session_name = &casSessionName.
                                 , cas_libref = &outModelCasLib.
                                 , cas_table = &pipeline_qvals_table.);

         %end;

   %end;

   libname &inCasLibref. clear;
   libname &outCasLibref. clear;

%mend corew_run_re_model;