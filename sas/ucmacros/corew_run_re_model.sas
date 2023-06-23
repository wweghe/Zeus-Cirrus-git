%macro corew_run_re_model(host =
                        , server = riskPipeline
                        , port =
                        , logonHost =
                        , logonPort =
                        , username =
                        , password =
                        , authMethod = bearer
                        , client_id =
                        , client_secret =
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
                        , reDeletePipelineInputs =
                        , reDeletePipelineOutputs =
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
      inst_scenario_name_col_exists
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
      re_output_variables_ignore
      single_var
      ;

   /* Read in the RE macrovariables */
   data _null_;
      set &reParametersDs.;
      call symputx(name, value, "L");
   run;

   %let asOfDateFmt=%sysfunc(putn(&asOfDate., yymmddn8.));

   %let inCasLibref = %rsk_get_unique_ref(type = lib, engine = cas, args = caslib="&inModelCaslib." sessref=&casSessionName.);
   %let outCasLibref = %rsk_get_unique_ref(type = lib, engine = cas, args = caslib="&outModelCaslib." sessref=&casSessionName.);

   /**********************************/
   /* Run the SAS pre-execution code */
   /**********************************/
   %if "&sasPreCodeFileRef." ne "" %then %do;
      %include &sasPreCodeFileRef. / source2 lrecl = 32000;
      filename &sasPreCodeFileRef. clear;
   %end;

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

   /* If reHorizons is MAX or empty, set it to the max horizon number in the scenario data */
   /* Currently assuming all scenarios have the same number of horizons.  If that might not be the case, this
   will need updated to group by scenario */
   /* If reHorizons is ALL, use all horizons */
   %if %upcase("&reHorizons.")="MAX" or "&reHorizons."="" %then %do;
      proc sql noprint;
         select max(horizon) into :reHorizons
         from &inCasLibref.."&inScenarios."n
         ;
      quit;
   %end;
   %else %if %upcase("&reHorizons.")="ALL" %then %let reHorizons=;

   /* If the portfolio table has the _effectiveDate_ or _inst_scenario_name_ columns,
   then it is using dynamic positions.  In that case, we need to set handleDynamicPositions = TRUE
   in the evaluatePortfolio and scoreCounterparties nodes of the Risk Pipeline */
   %let inst_scenario_name_col_exists=%rsk_varexist(&inCasLibref.."&inPortfolio."n, _inst_scenario_name_);
   %if (%rsk_varexist(&inCasLibref.."&inPortfolio."n, _effectiveDate_)) or &inst_scenario_name_col_exists. %then %do;

      %if %superq(reAdvOptsEvalPort) ne %then
         %let reAdvOptsEvalPort = %superq(reAdvOptsEvalPort)|handleDynamicPositions:TRUE|addDynamicPositionOnHorizon:TRUE;
      %else
         %let reAdvOptsEvalPort = handleDynamicPositions:TRUE|addDynamicPositionOnHorizon:TRUE;

      %if %superq(reAdvOptsScoreCounterparties) ne %then
         %let reAdvOptsScoreCounterparties = %superq(reAdvOptsScoreCounterparties)|handleDynamicPositions:TRUE|addDynamicPositionOnHorizon:TRUE;
      %else
         %let reAdvOptsScoreCounterparties = handleDynamicPositions:TRUE|addDynamicPositionOnHorizon:TRUE;

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


   /*************************************/
   /* Create the Risk Engines pipeline  */
   /*************************************/
   %core_rest_create_re_pipeline(host = &host.
                              , server = &server.
                              , port = &port.
                              , logonHost = &logonHost.
                              , logonPort = &logonPort.
                              , username = &username.
                              , password = &password.
                              , authMethod = &authMethod.
                              , client_id = &client_id.
                              , client_secret = &client_secret.
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

   /******************************************/
   /* Weight calculation                     */
   /******************************************/
   %if(%sysevalf(%superq(scenWeightCalcFlg) ne, boolean)) %then %do;
      %if(&scenWeightCalcFlg. = Y) %then %do;
      
         proc cas;
            session &casSessionName.;
            table.view /
               caslib="&rePipelineEnvCasLib." name="re_env_table" replace=TRUE
               tables = { { caslib="&rePipelineEnvCasLib." name="&rePipelineEnvCasTable." } }
            ;
         run;

         %let envCasLibref = %rsk_get_unique_ref(type = lib, engine = cas, args = caslib="&rePipelineEnvCasLib." sessref=&casSessionName.);
         proc sql noprint;
            select distinct NAME into :re_pipeline_output_variables separated by ' '
            from &envCasLibref..re_env_table
            where category="VARIABLE" and subcategory="PRICE";
         quit;
         libname &envCasLibref. clear;

         %macro filter_variables(var_list, value_list);
            %let filtered_list = ;
            %let var_count = %sysfunc(countw(&var_list., " "));
            %let val_count = %sysfunc(countw(&value_list., " "));
            
            %do i = 1 %to &var_count.;
               %let current_var = %scan(&var_list., &i., " ");
               
               %do j = 1 %to &val_count.;
                  %let current_val = %scan(&value_list., &j., " ");
                  
                  %if "&current_var." = "&current_val." %then %do;
                     %let filtered_list = &filtered_list. &current_var.;
                     %goto leave;
                  %end;
                  %else %do;
                  /* Variable was included that is not used */
                  %put WARNING: Variable &current_var. is not used in the Risk Engine pipeline. It will not be included in the results.;
                  %end;
               %end;
               %leave:
            %end;
            
            %substr(%bquote(&filtered_list.), 1)
         %mend;
      
         %let weightedCalcOutputVars = %filter_variables(%bquote(&weightedCalcOutputVars.), %bquote(&re_pipeline_output_variables.));

         %corew_weights_calculation(inResults = &pipeline_qvals_table.
                                    , CASLib = &outModelCaslib.
                                    , inScenarios = &inScenarioSet.
                                    , classAggregVars = _DATE_ _HORIZON_ FORECAST_TIME INSTID
                                    , varWeight = weight
                                    , weightedCalcOutputVars = &weightedCalcOutputVars.
                                    , scenarioAggName = Weighted
                                    , casSessionName = &casSessionName.
                                    , outSumResults = &outResults._sum
                                    );
         %if ( %rsk_dsexist(&outCasLibref..&outResults._sum) < 1 ) %then %do;
            %put ERROR: Weighted scenarios calculation were not generated.;
            %return;
         %end;


         %if(%sysevalf(%superq(re_pipeline_output_variables) eq, boolean)) %then
            %let re_pipeline_output_variables=;
         /* remove calculated variables from full output variables list */
         %do i=1 %to %sysfunc(countw(&re_pipeline_output_variables.,%str( )));
            %let single_var=%scan(&re_pipeline_output_variables.,&i,%str( ));
            %if not %sysfunc(indexw(&weightedCalcOutputVars.,&single_var,%str( ))) %then
               %let re_output_variables_ignore= &re_output_variables_ignore. &single_var.;
         %end;
      %end;
   %end;

   %let weighted_vars=&weightedCalcOutputVars.;
   %let weighted_vars_rename=%sysfunc(prxchange(s/(\w+)/$1=$1_w/, -1, &weighted_vars.));
   %let weighted_vars_reassign=%sysfunc(prxchange(s/(\w+)/$1=$1_w%str(;)/, -1, &weighted_vars.));
   %let weighted_vars_drop=%sysfunc(prxchange(s/(\w+)/$1_w/, -1, &weighted_vars.));

   /******************************************/
   /* Join the RE results onto the portfolio */
   /******************************************/
   data &outCasLibref.."&ds_out_model_result."n (rename=(AnalysisName=scenario_name _horizon_=horizon) 
                                                drop=scenarioName);

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
         &outCasLibref.."&pipeline_qvals_table."n
      %if ( %rsk_dsexist(&outCasLibref..&outResults._sum) > 0 ) %then %do;
         &outCasLibref..&outResults._sum (drop=_name_ 
            rename=(analysisName=analysisNameWeighted
                  &weighted_vars_rename. ) )
      %end;
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


      %if ( %rsk_dsexist(&outCasLibref..&outResults._sum) > 0 ) %then %do;
         if first.instid = 1 then do;
            output;
            analysisName=analysisNameWeighted;
            &weighted_vars_reassign.;;
            /* set variable by variable to '0' when scenario weighted - all the others */
            %sysfunc(prxchange(s/(\w+)/$1=.%str(;)/, -1, &re_output_variables_ignore.));
            output;
         end; /* if first.instid = 1 */
         else do;
      %end;
         /* Lookup the scenario's forecast_time */
         drop __rcFT__;
         call missing(forecast_time);
         scenarioName=AnalysisName;
         __rcFT__ = hFT.find();
         forecast_time = coalesce(forecast_time, 0);

         %if &inst_scenario_name_col_exists. %then %do;
            if synthetic_instrument_flg ne "Y" or _inst_scenario_name_ = scenarioName then
            output;
            %end;
         %else %do;
            output;
         %end;
      %if ( %rsk_dsexist(&outCasLibref..&outResults._sum) > 0 ) %then %do;
            drop analysisNameWeighted &weighted_vars_drop.;;
         end; /* if first.instid ne 1 */
      %end;

   run;

   %core_cas_drop_table(cas_session_name = &casSessionName.
                     , cas_libref = &outModelCasLib.
                     , cas_table = &outResults._sum);


   /***********************************/
   /* Run the SAS post-execution code */
   /***********************************/
   %if "&sasPostCodeFileRef." ne "" %then %do;
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
         %if("&reDeletePipelineOutputs." ne "Y") %then %do;

            /* Delete the CAS table and source file if exist */
            %core_cas_drop_table(cas_session_name = &casSessionName.
                                 , cas_libref = &outModelCasLib.
                                 , cas_table = &pipeline_qvals_table.);

         %end;

   %end;

   libname &inCasLibref. clear;
   libname &outCasLibref. clear;

%mend corew_run_re_model;