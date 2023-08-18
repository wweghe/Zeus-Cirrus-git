%macro corew_weights_calculation(inResults =
                              , CASLib =
                              , inScenarios =
                              , classAggregVars =           /*e.g.: _DATE_ _HORIZON_ FORECAST_TIME FORECAST_PERIOD INSTID */
                              , varWeight = weight          /* column name with weights */
                              , weightedCalcOutputVars =    /*e.g.: ECL_12M ECL_Lifetime PD_12M PD_Lifetime */
                              , scenarioAggName = Weighted  /* Name for scenario. e.g.: Weighted */
                              , casSessionName =
                              , outSumResults =
                              );

   %local
      bep_csv
      bep_keep
      bep_lookup
      weight_keep
      weight_lookup
      weight_csv
      scenarioName_col_exists
      scenario_name_col_exists
      AnalysisName_col_exists
   ;

   %let casLibref = %rsk_get_unique_ref(type = lib, engine = cas, args = caslib="&CASLib." sessref=&casSessionName.);

   /* Check parameter weightedOutputVars */
   %if %sysevalf(%superq(weightedCalcOutputVars) =, boolean) %then %do;
      %put WARNING: Weight calculation flagged but 'WEIGHTED OUTPUT VARS' is missing. Please select Output Variables in the Risk Engine Model UI to perform scenario weighting. Skipping execution..;
      %return;
   %end;

   %let query_weightedCalcOutputVars = &weightedCalcOutputVars.;
   %let query_weightedCalcOutputVars="%sysfunc(prxchange(s/\s+/" "/,-1,&query_weightedCalcOutputVars.))";

   %if %sysevalf(%superq(classAggregVars) =, boolean) %then %do;
      %put WARNING: Weight calculation flagged but 'CLASSIFICATION OUTPUT VARS' is missing. Please select Output Variables in the Risk Engine Model UI to perform scenario weighting. Skipping execution..;
      %return;
   %end;

   %let query_classAggregVars = &classAggregVars.;
   /* AnalysisName field added to classification variables because contains the value 'Weigthed' for scenario_name */
   %let query_classAggregVars="AnalysisName" "%sysfunc(prxchange(s/\s+/" "/,-1,&query_classAggregVars.))";

   /* Check if we have the scenario info */
   %if not(%rsk_dsexist(&casLibref..&inScenarios.)) %then %do;
      %put ERROR: Input dataset DS_IN_SCEN_INFO has not been provided or does not exist. Please provide Scenario info to perform scenario weighting. Skipping execution..;
      %return;
   %end;

   %if not %rsk_varexist(&casLibref..&inResults., INSTID) %then %do;
      %put ERROR: You must specify Instrument ID variable (INSTID) in the query criteria code either in the SAS Risk Engine pipeline or in the Risk Engine Post-Execution Program code to perform scenario weighting. Skipping execution..;
      %return;
   %end;

   /* Check if variable bepName is available in the scenario info table (only for solutions that use the Business Evolution Plan component) */
   %if %rsk_varexist(&casLibref..&inScenarios., bepName) %then %do;
      %let bep_keep = bepName;
      %let bep_lookup = , "&bep_keep.";
      %let bep_csv = , &bep_keep.;
   %end;

   %let weight_keep = &varWeight.;
   %let weight_lookup = , "&weight_keep.";
   %let weight_csv = , &weight_keep.;

   %let scenarioName_col_exists=%rsk_varexist(&casLibref.."&inResults."n, scenarioName);
   %let scenario_name_col_exists=%rsk_varexist(&casLibref.."&inResults."n, scenario_name);
   %let AnalysisName_col_exists=%rsk_varexist(&casLibref.."&inResults."n, analysisName);

   /* Get Scenario Info */
   data &casLibref.._tmp_model_result_scen_info_;
      set &casLibref..&inResults.;

      if _N_ = 0 then
         set &casLibref..&inScenarios.(keep = forecast_time scenarioName &bep_keep &weight_keep.);

      /* Define the lookup */
      if _N_ = 1 then do;
         declare hash hScen(dataset: "&casLibref..&inScenarios.");
         hScen.defineKey("scenarioName");
         hScen.defineData("forecast_time" &bep_lookup. &weight_lookup.);
         hScen.defineDone();
         call missing(forecast_time &bep_csv. &weight_csv.);
      end;

      /* Perform lookup */
      drop __rc__;
      __rc__ = hScen.find(key:
               %if &scenarioName_col_exists. %then scenarioName;
               %if &scenario_name_col_exists. %then scenario_name;
               %if &AnalysisName_col_exists. %then AnalysisName;
               );

      if __rc__ = 0 then do;
         AnalysisName = "&scenarioAggName.";
         forecast_time = sum(forecast_time, 0);
         output;
      end;
   run;

   /* Perform scenario weighting */
   proc cas;
      session &casSessionName.;
      simple.summary result=r status=s /
         inputs={&query_weightedCalcOutputVars.},
         subSet={"MEAN"},
         table={
               caslib="&CASLib.",
               name="_TMP_MODEL_RESULT_SCEN_INFO_",
               groupBy={&query_classAggregVars.},
               groupByMode="NOSORT",
               singlePass=TRUE
               }
         casout={caslib="&CASLib.", name="_tmp_model_result_weighted_", replace=True}
   weight="&varWeight.";
   run;quit;
   proc cas;
      session &casSessionName.;
      transpose.transpose /
         table={
            caslib="&CASLib.",
            name="_tmp_model_result_weighted_",
            groupBy={&query_classAggregVars.}
         },
         id={"_Column_"},
         casOut={caslib="&CASLib.", name="&outSumResults.", replace=true}, 
         transpose={"_MEAN_"} ;
   run;quit;

   %core_cas_drop_table(cas_session_name = &casSessionName.
                     , cas_libref = &CASLib.
                     , cas_table = _tmp_model_result_scen_info_);
   %core_cas_drop_table(cas_session_name = &casSessionName.
                     , cas_libref = &CASLib.
                     , cas_table = _tmp_model_result_weighted_);

%mend corew_weights_calculation;