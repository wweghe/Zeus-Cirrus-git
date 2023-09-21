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
                       , inElimPosition =
                       , inScenarios =
                       , inScenarioSet =
                       , outResults =
                       , asOfDate =
                       , scenWeightCalcFlg =
                       , weightedCalcOutputVars =
                       , allocPostRiskCodeFileRef =
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
      httpSuccess
      asOfDateFmt casTablesTag
      modelKey engine reProjectKey modelCodeAttachmentNames fileRefList
      modelParamName
      forecastTimeFlag
      synth_table_exists elim_table_exists
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

   /***********************************/
   /* Get the analysis run parameters */
   /***********************************/
   data object_param_output_tables;
      length objectType $64 outputDs $128;
      objectType="models"; outputDs="work.model_summary";   /* Get all model object parameters in the analysis run into table work.model_summary */
   run;

   /* Get the analysis run's parameters and their values */
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
                              , outObjParamsConfig = work.object_param_output_tables
                              , outSuccess = httpSuccess
                              , outVarToken = &outVarToken.
                              , debug = &debug.
                              );

   /* Exit in case of errors */
   %if(not &httpSuccess.) %then %do;
      %put ERROR: Failed to get the parameters for analysis run &analysisRunKey.;
      %abort;
   %end;

   /* Exit if no model parameters were found for this analysis run */
   %if not %rsk_dsexist(work.model_summary) or %rsk_attrn(work.model_summary, nobs) = 0 %then %do;
      %put ERROR: Failed to find any model parameters for analysis run &analysisRunKey.;
      %abort;
   %end;

   /* Get the credit risk model parameter (type=Analysis) key and engine information */
   data _null_;
      set model_summary;
      if execTypeCd in ("", "ANA") then do;
         call symputx("modelKey", key, "L");
         call symputx("engine", engineCd, "L");
         if engineCd="RE" then
            call symputx("reProjectKey", scan(engineModelUri, -1, '/'), "L");
      end;
   run;

   /* Exit if no model parameters of type=ANA (or type='') were found for this analysis run */
   %if "&modelKey." = "" %then %do;
      %put ERROR: Failed to find a credit risk model (model with type=Analysis) parameter in analysis run &analysisRunKey.;
      %abort;
   %end;

   /* Get the name of the credit risk model (type=Analysis) parameter */
   data _null_;
      set analysis_run_params (where=(objectRestPath="models" and objectKey = "&modelKey."));
      call symputx("modelParamName", name, "L");
   run;


   /******************************************************************/
   /* Get the analysis run's model (execTypeCd=ANA) code attachments */
   /******************************************************************/
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
   %let synth_table_exists=0;
   %let elim_table_exists=0;
   %if (%sysevalf(%superq(inSynthPosition) ne, boolean)) %then
      %rsk_dsexist_cas(cas_lib=&inModelCaslib.,cas_table=&inSynthPosition., cas_session_name=&casSessionName., out_var=synth_table_exists);
   %if (%sysevalf(%superq(inElimPosition) ne, boolean)) %then
      %rsk_dsexist_cas(cas_lib=&inModelCaslib.,cas_table=&inElimPosition., cas_session_name=&casSessionName., out_var=elim_table_exists);

   %if &synth_table_exists. or &elim_table_exists.  %then %do;

      /* Create the full portfolio:
            1. Synthetic positions are appended to the portfolio
            2. Eliminated positions are merged with the portfolio by instid (generally will be multiple matches if the same instid) */
      data &inCasLibref..port_full_&asOfDateFmt._&casTablesTag. (promote=yes);
         length synthetic_instrument_flg varchar(3);
         merge &inCasLibref.."&inPortfolio."n
               %if &synth_table_exists. %then %do;
                  &inCasLibref.."&inSynthPosition."n (in=synth)
               %end;
               %if &elim_table_exists. %then %do;
                  &inCasLibref.."&inElimPosition."n (in=elim)
               %end;
               ;
         by instid;
         synthetic_instrument_flg=ifc(synth, "Y", ifc(elim, coalescec(synthetic_instrument_flg,"E"), "N"));
      run;

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
                        , allocPostRiskCodeFileRef = &allocPostRiskCodeFileRef.
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
      %if(%sysevalf(%superq(inElimPosition) ne, boolean)) %then %do;
         %core_cas_drop_table(cas_session_name = &casSessionName.
                           , cas_libref = &inModelCaslib.
                           , cas_table = &inElimPosition.);
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