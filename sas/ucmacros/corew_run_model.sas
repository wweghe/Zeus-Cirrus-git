%macro corew_run_model(solution =
                       , host =
                       , port =
                       , logonHost =
                       , logonPort =
                       , username =
                       , password =
                       , authMethod = bearer
                       , client_id =
                       , client_secret =
                       , analysisRunKey =
                       , inPortfolio =
                       , inCounterparty =
                       , inCollateral =
                       , inMitigant =
                       , inCashflow =
                       , inSynthPosition =
                       , inScenarios =
                       , inScenarioSet =
                       , outResults =
                       , asOfDate =
                       , scenWeightCalcFlg =
                       , weightedCalcOutputVars =
                       , casTablesTag =
                       , inModelCasLib =
                       , outModelCasLib =
                       , keepModelData =
                       , casSessionName = casauto
                       , maxWait =
                       , outVarToken = accessToken
                       , debug = false
                       );

   %local
      inCasLibref
      asOfDateFmt casTablesTag
      num_linked_models modelKey engine reProjectKey modelCodeAttachmentNames fileRefList
      modelParamName
      forecastTimeFlag
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
      inPortfolio_base
      ;

   /* Tables, RE pipelines, etc created in this macro will be tagged with "_YYYYMMDD_<arTag>",
   where <arTag> is passed in the casTables Tag parameter (usually the first 7 characters of the analysis run key) */
   %let asOfDateFmt=%sysfunc(putn(&asOfDate., yymmddn8.));

   %let inCasLibref = %rsk_get_unique_ref(type = lib, engine = cas, args = caslib="&inModelCaslib." sessref=&casSessionName.);

   %let inPortfolio_base = &inPortfolio.; /* just to ensure generic removal at the end of execution */

   /********************************************/
   /* Get the model linked to the analysis run */
   /********************************************/
   /* Get the linked model's key */
   %core_rest_get_link_instances(solution = &solution.
                                 , host = &host.
                                 , port = &port.
                                 , logonHost = &logonHost.
                                 , logonPort = &logonPort.
                                 , username = &username.
                                 , password = &password.
                                 , authMethod = &authMethod.
                                 , client_id = &client_id.
                                 , client_secret = &client_secret.
                                 , objectKey = &analysisRunKey.
                                 , objectType = analysisRuns
                                 , linkType = analysisRun_param_models
                                 , linkInstanceFilter =
                                 , outds = ar_model_link_instance
                                 , outSuccess = httpSuccess
                                 , outVarToken = &outVarToken.
                                 , debug = &debug.
                                 );

   /* Exit in case of errors */
   %if(not &httpSuccess.) %then %do;
      %put ERROR: Failed to get the model linked to analysis run &analysisRunKey.;
      %abort;
   %end;

   /* Exit if we don't find a model parameter */
   %let num_linked_models=%rsk_getattr(ar_model_link_instance, NOBS);
   %if &num_linked_models.=0 %then %do;
      %put ERROR: There were no model object parameters found for analysis run &analysisRunKey.;
      %abort;
   %end;
   /*%else %if &num_linked_models.>1 %then %do;
      %put ERROR: 2 or more model object parameters were found for analysis run &analysisRunKey.;
      %abort;
   %end;*/

   data _null_;
      set ar_model_link_instance;
      call symputx("modelKey", businessObject2, "L");
   run;

   /* Get the model's engine and Risk-Engines project ID/parameters (if applicable) from the
   resolved parameter expression on the analysis run */
   %core_rest_get_analysis_run(solution = &solution.
                              , host = &host.
                              , port = &port.
                              , logonHost = &logonHost.
                              , logonPort = &logonPort.
                              , username = &username.
                              , password = &password.
                              , authMethod = &authMethod.
                              , client_id = &client_id.
                              , client_secret = &client_secret.
                              , key = &analysisRunKey.
                              , outds = analysis_run
                              , outds_params = analysis_run_params
                              , outSuccess = httpSuccess
                              , outVarToken = &outVarToken.
                              , debug = &debug.
                              );

   /* Exit in case of errors */
   %if(not &httpSuccess.) %then %do;
      %put ERROR: Failed to get the model (&modelKey.) engine from analysis run &analysisRunKey.;
      %abort;
   %end;

   /* Model object fields are stored in encoded customFields string. */
   /* To read, write the customFields string to a file and read in using the JSON libname engine */
   filename _frefMod temp;
   data _null_;
      file _frefMod;
      set analysis_run_params;

      if objectKey = "&modelKey." then do;
         call symputx("modelParamName", name, "L");
         put objectCustomFields;
      end;
   run;

   libname _lrefMod json fileref=_frefMod NOALLDATA NRM;
   data _null_;
      set _lrefMod.root;
      call symputx("engine", engineCd, "L");
      if engineCd="RE" then
         call symputx("reProjectKey", scan(engineModelUri, -1, '/'), "L");
   run;

   filename _frefMod clear;
   libname _lrefMod clear;


   /*******************************************/
   /* Get the analysis run's code attachments */
   /*******************************************/
   %let modelCodeAttachmentNames =  &modelParamName._modelCode.sas|
                                    &modelParamName._preModelCode.sas|
                                    &modelParamName._preReActionsCode|
                                    &modelParamName._postReActionsCode|
                                    &modelParamName._postModelCode.sas;
   %core_rest_get_file_attachment(solution = &solution.
                           , host = &host.
                           , port = &port.
                           , logonHost = &logonHost.
                           , logonPort = &logonPort.
                           , username = &username.
                           , password = &password.
                           , authMethod = &authMethod.
                           , client_id = &client_id.
                           , client_secret = &client_secret.
                           , objectKey = &analysisRunKey.
                           , objectType = analysisRuns
                           , attachmentNames = &modelCodeAttachmentNames.
                           , outFileRefs = sasCode|sasPre|rePre|rePost|sasPost
                           , errorIfFileNotFound = N
                           , getAttachmentContent = Y
                           , outds = analysis_run_attachments
                           , outSuccess = httpSuccess
                           , outVarToken = &outVarToken.
                           , debug = &debug.
                           );

   /* Exit in case of errors */
   %if(not &httpSuccess.) %then %do;
      %put ERROR: Failed to retrieve code attachments for analysis run &analysisRunKey.;
      %abort;
   %end;

   %let fileRefList=;
   proc sql noprint;
      select fileref into :fileRefList separated by '|'
      from analysis_run_attachments
      ;
   quit;

   /**********************************************/
   /* Create the full portfolio table, if needed */
   /**********************************************/
   %if(%sysevalf(%superq(inSynthPosition) ne, boolean)) %then %do;

      %if(%rsk_varexist(&inCasLibref.."&inSynthPosition."n, _inst_scenario_name_))  %then %do;

         data &inCasLibref..scenario_set_map (keep=scenarioName _inst_scenario_name_ _inst_scenario_forecast_time_);
            set &inCasLibref.."&inScenarioSet."n end=last;
            rename synthetic_scenario_name=_inst_scenario_name_ forecast_time=_inst_scenario_forecast_time_;
            if last then
               call symputx("forecastTimeFlag", forecast_time_flag, "L");
         run;

         %if (not %rsk_varexist(&inCasLibref.."&inSynthPosition."n, _inst_scenario_forecast_time_)) and &forecastTimeFlag. %then %do;
            %put ERROR: _inst_scenario_forecast_time_ is required in the synthetic positions.;
            %abort;
         %end;

         /* Create the final portfolio table */
         data &inCasLibref..port_full_&asOfDateFmt._&casTablesTag. (
                  drop=scenarioName _inst_scenario_forecast_time_ _inst_scenario_name_orig_ __rc__
                  promote=yes
               )
               &inCasLibref..synth_exceps_&asOfDateFmt._&casTablesTag. (
                  keep=instid _inst_scenario_name_ _inst_scenario_forecast_time_ __rc__
               );

            length _inst_scenario_name_orig_ varchar(200) synthetic_instrument_flg varchar(3);
            set &inCasLibref.."&inPortfolio."n &inCasLibref.."&inSynthPosition."n (in=b);

            /* Add the scenario_name column from the lookup table */
            if _N_ = 0 then set &inCasLibref..scenario_set_map;

            /* Create the scenario lookup hash */
            if _N_ = 1 then do;
               declare hash hScenLookup(dataset: "&inCasLibref..scenario_set_map", multidata: "yes");
               hScenLookup.defineKey("_inst_scenario_name_", "_inst_scenario_forecast_time_");
               hScenLookup.defineData("scenarioName");
               hScenLookup.defineDone();
            end;

            /* If forecastTimeFlag is false, just map by scenario */
            %if not &forecastTimeFlag. %then %do;
               _inst_scenario_forecast_time_=.;
            %end;

            /* Perform the scenario lookup and overwrite _inst_scenario_name_ with the full scenario name value found */
            if b then do;

               synthetic_instrument_flg="Y";

               call missing(scenarioName);
               __rc__ = hScenLookup.find();

               if __rc__ ne 0 then
                  output &inCasLibref..synth_exceps_&asOfDateFmt._&casTablesTag.;
               else do;
                  do while(__rc__ eq 0);
                     _inst_scenario_name_orig_ = _inst_scenario_name_;
                     _inst_scenario_name_ = scenarioName;
                     output &inCasLibref..port_full_&asOfDateFmt._&casTablesTag.;
                     _inst_scenario_name_ = _inst_scenario_name_orig_;
                     call missing(scenarioName);
                     __rc__ = hScenLookup.find_next();
                  end;
               end;

            end;
            else do;
               synthetic_instrument_flg="N";
               output &inCasLibref..port_full_&asOfDateFmt._&casTablesTag.;
            end;

         run;

         /* Throw warnings if any synth-scenario mappings failed.  We don't error out here since, for example,
         the user might want to run with a subset of scenarios */
         %if %rsk_attrn(&inCasLibref..synth_exceps_&asOfDateFmt._&casTablesTag., nobs)>0 %then %do;

            %put WARNING: Could not find a matching scenario in scenario set &inScenarioSet. for 1 or more synthetic instruments;
            %put WARNING: These synthetic instruments will not be included in the analysis.;
            %put WARNING: See exceptions in table &inModelCasLib..synth_exceps_&asOfDateFmt._&casTablesTag.;

            data &inCasLibref..synth_exceps_&asOfDateFmt._&casTablesTag. (promote=yes);
               set &inCasLibref..synth_exceps_&asOfDateFmt._&casTablesTag.;
            run;

         %end;

      %end;
      %else %do;
         /* If _inst_scenario_name_ is not in the synthetic instrument data, just append the synthetic instrument
         data to the portfolio.  This will add any new columns to the portfolio (such as _effectiveDate_) */
         data &inCasLibref..port_full_&asOfDateFmt._&casTablesTag. (promote=yes);
            set &inCasLibref.."&inPortfolio."n &inCasLibref.."&inSynthPosition."n;
         run;
      %end;

      /* Update inPortfolio to point to the final portfolio table */
      %let inPortfolio = port_full_&asOfDateFmt._&casTablesTag.;

   %end;

   /* Set the table names - these were the names expected in Stratum solutions, so setting again here for compatibility.
   These can be referred to in model pre/post-sas code. */
   data ds_params;
      length name $ 200 value $ 32000;
      name="ds_in_portfolio"; value="&inPortfolio."; output;
      name="ds_in_counterparty"; value="&inCounterparty."; output;
      name="ds_in_mitigant"; value="&inMitigant."; output;
      name="ds_in_collateral"; value="&inCollateral."; output;
      name="ds_in_cashflow"; value="&inCashflow."; output;
      name="ds_in_synth_position"; value="&inSynthPosition."; output;
      name="ds_in_scenario"; value="&inScenarios."; output;
      name="ds_in_scen_info"; value="&inScenarioSet."; output;
      name="ds_out_model_result"; value="&outResults."; output;
   run;


   /*****************/
   /* Run the model */
   /*****************/

   /* Delete the results if they already exist */
   %core_cas_drop_table(cas_session_name = &casSessionName.
                        , cas_libref = &outModelCasLib.
                        , cas_table = &outResults.);

   /* Run the model, depending on the engine */
   %if(&engine. = SAS) %then %do;

      /*********************/
      /* Run the SAS model */
      /*********************/
      %include sasCode / source2 lrecl = 32000;
      filename sasCode clear;

   %end;
   %else %if(&engine. = RE) %then %do;

      /******************************/
      /* Run the Risk Engines model */
      /******************************/

      /* Set the special engine parameter macrovars which are available in pre/post SAS code:
         1. ds_* macrovariables set in this macro: These refer to CAS tables (for backwards compatibility with Stratum)
         2. re* macrovariables set from any analysis run parameters: These configure the RE pipeline that is created and run.
      */
      data re_params;
         set   ds_params (in=a)
               analysis_run_params (in=b) end=last;

         if a then output;
         if b then do;
            name = prxchange('s/^[^_]+_//', 1, name);
            /* Risk Engines parameters */
            if prxmatch('/^re[A-Z].*/', name) then output;
         end;

      run;

      /* Run the RE model */
      %corew_run_re_model(host = &host.
                        , port = &port.
                        , logonHost = &logonHost.
                        , logonPort = &logonPort.
                        , username = &username.
                        , password = &password.
                        , authMethod = &authMethod.
                        , client_id = &client_id.
                        , client_secret = &client_secret.
                        , reProjectKey = &reProjectKey.
                        , reNewPipelineName = %upcase(&solution.)_&asOfDateFmt._&casTablesTag.
                        , reParametersDs = re_params
                        , inPortfolio = &inPortfolio.
                        , inCounterparty = &inCounterparty.
                        , inCollateral = &inCollateral.
                        , inMitigant = &inMitigant.
                        , inScenarios = &inScenarios.
                        , inScenarioSet = &inScenarioSet.
                        , scenWeightCalcFlg = &scenWeightCalcFlg.
                        , weightedCalcOutputVars = &weightedCalcOutputVars.
                        , outResults = &outResults.
                        , sasPreCodeFileRef = %if %sysfunc(find(&fileRefList.,sasPre)) ne 0 %then sasPre;
                        , rePreCodeFileRef = %if %sysfunc(find(&fileRefList.,rePre)) ne 0 %then rePre;
                        , rePostCodeFileRef = %if %sysfunc(find(&fileRefList.,rePost)) ne 0 %then rePost;
                        , sasPostCodeFileRef = %if %sysfunc(find(&fileRefList.,sasPost)) ne 0 %then sasPost;
                        , reDeletePipelineInputs = %if ("&keepModelData." = "Y") %then N; %else Y;
                        , reDeletePipelineOutputs = %if ("&keepModelData." = "Y") %then N; %else Y;
                        , asOfDate = &asOfDate.
                        , casTablesTag = &casTablesTag.
                        , inModelCasLib = &inModelCasLib.
                        , outModelCasLib = &outModelCasLib.
                        , casSessionName = &casSessionName.
                        , maxWait = &maxWait.
                        , outVarToken = &outVarToken.
                        , debug = &debug.
                        );
   %end; /* End RE model execution */

   /* Check if the session-level results table has been created */
   %rsk_dsexist_cas(cas_lib=%superq(outModelCasLib),cas_table=%superq(outResults));
   %if(not &cas_table_exists.) %then %do;
      %put ERROR: The model did not create required output table &outModelCasLib..&outResults.;
      %abort;
   %end;

   /* Promote the results table */
   proc casutil;
        promote inCaslib="&outModelCasLib."   casData="&outResults."
                outCaslib="&outModelCasLib." casOut="&outResults.";
      run;
   quit;

   %if ( "&keepModelData." ne "Y" ) %then %do;
      /* remove all other unecessary CAS tables */
      %core_cas_drop_table(cas_session_name = &casSessionName.
                        , cas_libref = &inModelCaslib.
                        , cas_table = &inPortfolio_base.);
      %core_cas_drop_table(cas_session_name = &casSessionName.
                        , cas_libref = &inModelCaslib.
                        , cas_table = &inPortfolio.);
      %if(%sysevalf(%superq(inCounterparty) ne, boolean)) %then %do;
         %core_cas_drop_table(cas_session_name = &casSessionName.
                           , cas_libref = &inModelCaslib.
                           , cas_table = &inCounterparty.);
      %end;
      %if(%sysevalf(%superq(inCollateral) ne, boolean)) %then %do;
         %core_cas_drop_table(cas_session_name = &casSessionName.
                           , cas_libref = &inModelCaslib.
                           , cas_table = &inCollateral.);
      %end;
      %if(%sysevalf(%superq(inMitigant) ne, boolean)) %then %do;
         %core_cas_drop_table(cas_session_name = &casSessionName.
                           , cas_libref = &inModelCaslib.
                           , cas_table = &inMitigant.);
      %end;
      %if(%sysevalf(%superq(inCashflow) ne, boolean)) %then %do;
      %core_cas_drop_table(cas_session_name = &casSessionName.
                        , cas_libref = &inModelCaslib.
                        , cas_table = &inCashflow.);
      %end;
      %if(%sysevalf(%superq(inSynthPosition) ne, boolean)) %then %do;
         %core_cas_drop_table(cas_session_name = &casSessionName.
                           , cas_libref = &inModelCaslib.
                           , cas_table = &inSynthPosition.);
      %end;
      %core_cas_drop_table(cas_session_name = &casSessionName.
                        , cas_libref = &inModelCaslib.
                        , cas_table = &inScenarios.);
      %core_cas_drop_table(cas_session_name = &casSessionName.
                        , cas_libref = &inModelCaslib.
                        , cas_table = &inScenarioSet.);
   %end;

   libname &inCasLibref. clear;

%mend corew_run_model;