%macro corew_prepare_attribution(host =
                              , port =
                              , solution =
                              , logonHost =
                              , logonPort =
                              , username =
                              , password =
                              , authMethod = bearer
                              , client_id =
                              , client_secret =
                              , inAttributionTemplate =
                              , inSasAttributionTablesConfig =
                              , inSasScenarioMap =
                              , inModelCasLib =
                              , outModelCasLib =
                              , analysisRunKey =
                              , asOfDate =
                              , casTablesTag =
                              , keepModelData =
                              , solutionRootFolder =
                              , ddlSubPath =
                              , scenario_selection =
                              , mart_table_name =
                              , outSasAttributionConfig =
                              , outSasAttributionRunConfig =
                              , outScenarioMap =
                              , casSessionName = casauto
                              , outVarToken = accessToken
                              , debug = false
                              );

   %local
      inCasLibref outCasLibref
      inAttributionTable asOfDateFmt
      out_scenarios_prev out_scenario_sets_prev out_scenarios_current out_scenario_sets_current
      out_portfolio_prev out_portfolio_current out_counterparty_prev out_counterparty_current
      out_mitigant_prev out_mitigant_current out_results_prev out_results_current
      from_scenario_name
      primary_key piped_primary_key classification_vars classification_vars_csv
      prev_analysis_run_key curr_analysis_run_key
      prev_result_key curr_result_key
      prev_portfolio_key curr_portfolio_key
      prev_counterparty_key curr_counterparty_key
      prev_mitigant_key curr_mitigant_key
      prev_scenario_set_ids curr_scenario_set_ids
      prev_scenario_ft_map curr_scenario_ft_map
      ddlPath
      ;

   %if(%sysevalf(%superq(inAttributionTemplate) eq, boolean)) %then %do;
      %put ERROR: inAttributionTemplate is required.;
      %abort;
   %end;

   %if(%sysevalf(%superq(inSasAttributionTablesConfig) eq, boolean)) %then %do;
      %put ERROR: inSasAttributionTablesConfig is required.;
      %abort;
   %end;

   %let keepModelData=%sysfunc(coalescec(%upcase(&keepModelData.), N));

   /* Read the previous/current period tables into local macrovariables */
   data _null_;
      set &inSasAttributionTablesConfig.;
      call symputx(config_name, config_value, "L");
   run;

   %let inCasLibref = %rsk_get_unique_ref(type = lib, engine = cas, args = caslib="&inModelCaslib." sessref=&casSessionName.);
   %let outCasLibref = %rsk_get_unique_ref(type = lib, engine = cas, args = caslib="&outModelCaslib." sessref=&casSessionName.);

   /* Tables, RE pipelines, etc created in this macro will be tagged with "_YYYYMMDD_<arTag>",
   where <arTag> is the first 7 characters of the analysis run key */
   %let asOfDateFmt=%sysfunc(putn(&asOfDate, yymmddn8.));

   /* Get the attribution template if a Cirrus Objects AA template key is provided */
   %if %index("&inAttributionTemplate.", -) > 0 %then %do;
      %put Note: "&inAttributionTemplate." contains '-', assuming it is a uuid key for a Cirrus attribution template object.;

      %let tmp_attribution_table = work.tmp_attribution_table;

      /* Delete &tmp_attribution_table. table if it exists */
      %if (%rsk_dsexist(&tmp_attribution_table.)) %then %do;
         proc sql;
            drop table &tmp_attribution_table.;
         quit;
      %end;

      /* GET the attribution template --> &inAttributionTable. */
      %core_rest_get_attribution(host = &host.
                                 , port = &port.
                                 , solution = &solution.
                                 , logonHost = &logonHost.
                                 , logonPort = &logonPort.
                                 , username = &username.
                                 , password = &password.
                                 , authMethod = &authMethod.
                                 , client_id = &client_id.
                                 , client_secret = &client_secret.
                                 , key = &inAttributionTemplate.
                                 , details_flg = Y
                                 , outds = &tmp_attribution_table.
                                 , outVarToken = &outVarToken.
                                 , debug = &debug.
                                 );

      /* Throw an error if the attribution table was not produced or has no information */
      %if (not %rsk_dsexist(&tmp_attribution_table.) or %rsk_attrn(&tmp_attribution_table., nlobs) eq 0) %then %do;
         %put ERROR: No information was found for the requested attribution template (uuid key= &inAttributionTemplate.);
         %abort;
      %end;

      %let inAttributionTable = &tmp_attribution_table.;
   %end;
   %else %do;
      %put Note: "&inAttributionTemplate." does not contain '-', assuming it is a SAS dataset.;
      %let inAttributionTable = &inAttributionTemplate.;
   %end;

   %let out_scenarios_prev = prev_scenarios_&asOfDateFmt._&casTablesTag.;
   %let out_scenario_sets_prev = prev_scen_set_&asOfDateFmt._&casTablesTag.;
   %let out_scenarios_current = curr_scenarios_&asOfDateFmt._&casTablesTag.;
   %let out_scenario_sets_current = curr_scen_set_&asOfDateFmt._&casTablesTag.;
   %let out_portfolio_prev = prev_port_&asOfDateFmt._&casTablesTag.;
   %let out_portfolio_current = curr_port_&asOfDateFmt._&casTablesTag.;
   %let out_counterparty_prev = %sysfunc(ifc("&prev_counterparty_key." ne "", prev_cpty_&asOfDateFmt._&casTablesTag.,));
   %let out_counterparty_current = %sysfunc(ifc("&curr_counterparty_key." ne "", curr_cpty_&asOfDateFmt._&casTablesTag.,));
   %let out_mitigant_prev = %sysfunc(ifc("&prev_mitigant_key." ne "", prev_mit_&asOfDateFmt._&casTablesTag.,));
   %let out_mitigant_current = %sysfunc(ifc("&curr_mitigant_key." ne "", curr_mit_&asOfDateFmt._&casTablesTag.,));
   %let out_results_prev = prev_results_&asOfDateFmt._&casTablesTag.;
   %let out_results_current = curr_results_&asOfDateFmt._&casTablesTag.;


   /***********************************************************************/
   /* Export the prior and current period scenario set scenarios into CAS */
   /***********************************************************************/
   %corew_prepare_scenarios(host = &host.
                           , port = &port.
                           , logonHost = &logonHost.
                           , logonPort = &logonPort.
                           , username = &username.
                           , password = &password.
                           , authMethod = &authMethod.
                           , client_id = &client_id.
                           , client_secret = &client_secret.
                           , scenarioSetIds = %bquote(&prev_scenario_set_ids.)
                           , inSasScenarioFtMap = &prev_scenario_ft_map.
                           , outScenarios = _tmp_scenarios_prev
                           , outScenarioSet = &out_scenario_sets_prev.
                           , promoteScenarios = N
                           , outCasLib = &inModelCasLib.
                           , casSessionName = &casSessionName.
                           , outVarToken = &outVarToken.
                           , debug = &debug.
                           );

   %corew_prepare_scenarios(host = &host.
                           , port = &port.
                           , logonHost = &logonHost.
                           , logonPort = &logonPort.
                           , username = &username.
                           , password = &password.
                           , authMethod = &authMethod.
                           , client_id = &client_id.
                           , client_secret = &client_secret.
                           , scenarioSetIds = %bquote(&curr_scenario_set_ids.)
                           , inSasScenarioFtMap = &curr_scenario_ft_map.
                           , outScenarios = &out_scenarios_current.
                           , outScenarioSet = &out_scenario_sets_current.
                           , outCasLib = &inModelCasLib.
                           , casSessionName = &casSessionName.
                           , outVarToken = &outVarToken.
                           , debug = &debug.
                           );

   /* Apply "aging" of the prior period scenarios to the current period scenarios asOfadte, since this is always required
      in the first attribution factor run.  This includes:
      1. Updating the horizon to be relative to the current period scenario asOfDate.  (Ex: h=1 in prior becomes h=0)
      2. At the new "basedate" horizon (h=1 is now h=0), only output 1 row (per scenario variable) with scenario_name missing.
      3. The new "basedate" horizon scenario data is the current scenario data at h=0 (the "realized" data for the prior scenario for its h=1)
   */
   data _null_;
      set &inCasLibref.."&out_scenario_sets_prev."n (in=a) &inCasLibref.."&out_scenario_sets_current."n (in=b);
      if a then do;
         call symputx("prev_asOfDate", input(asOfDate, YYMMDD10.), "L");
         call symputx("prev_scenarioName", scenarioName, "L");
      end;
      else do;
         call symputx("curr_asOfDate", input(asOfDate, YYMMDD10.), "L");
         call symputx("interval", periodType, "L");
      end;
   run;

   data &inCasLibref..curr_scenarios_curr_horizon;
      set &inCasLibref.."&out_scenarios_current."n (where=(horizon=0));
   run;

   /* Delete the previous period scenarios CAS table if it already exists */
   %core_cas_drop_table(cas_session_name = &casSessionName.
                        , cas_libref = &inModelCaslib.
                        , cas_table = &out_scenarios_prev.);


   data &inCasLibref.."&out_scenarios_prev."n (promote=yes);
      set &inCasLibref.._tmp_scenarios_prev;

      if _N_=1 then do;
         /* Create the h=0 current scenario override hash */
         declare hash hScenOverrideCurr(dataset: "&inCasLibref..curr_scenarios_curr_horizon");
         hScenOverrideCurr.defineKey("scenario_name", "horizon", "variable_name");
         hScenOverrideCurr.defineData("change_type", "change_value");
         hScenOverrideCurr.defineDone();
      end;

      horizon=intck("&interval.", "&curr_asOfDate.", date);

      if horizon<0 then interval="";

      if horizon=0 then do;
         if scenario_name="&prev_scenarioName." then do;
            scenario_name="";
            call missing(change_type, change_value);
            _rc_ = hScenOverrideCurr.find();
            drop _rc_;
            output;
         end;
      end;
      else output;

   run;

   /* Create the final prior-to-current period scenario map */
   data &inCasLibref.."&outScenarioMap."n;
      set "&inSasScenarioMap."n;
   run;

   proc fedsql sessref=&casSessionName;
      create table "&inModelCasLib."."&outScenarioMap." {options replace=true} as
      select   prev.scenarioName as fromScenarioName,
               curr.scenarioName as toScenarioName
      from "&inModelCasLib."."&outScenarioMap." as scenMap
      left join "&inModelCasLib."."&out_scenario_sets_prev." as prev
         on index(upper(prev.scenarioName), strip(upper(scenMap.fromScenarioName)))>0
      left join "&inModelCasLib."."&out_scenario_sets_current." as curr
         on index(upper(curr.scenarioName), strip(upper(scenMap.toScenarioName)))>0
      ;
   quit;


   /******************************************************************/
   /* Export the prior and current period input analysis data to CAS */
   /******************************************************************/
   %corew_prepare_input_data(host = &host.
                     , port = &port.
                     , logonHost = &logonHost.
                     , logonPort = &logonPort.
                     , username = &username.
                     , password = &password.
                     , authMethod = &authMethod.
                     , client_id = &client_id.
                     , client_secret = &client_secret.
                     , inTableList = &prev_portfolio_key. &curr_portfolio_key.
                                       &prev_counterparty_key. &curr_counterparty_key. &prev_mitigant_key. &curr_mitigant_key.
                     , outTableList = &out_portfolio_prev. &out_portfolio_current.
                                       &out_counterparty_prev. &out_counterparty_current. &out_mitigant_prev. &out_mitigant_current.
                     , outCasLib = &inModelCasLib.
                     , outCasTablesScope = global
                     , casSessionName = &casSessionName.
                     , outVarToken = &outVarToken.
                     , debug = &debug.
                     );

   /*******************************************************************/
   /* Export the prior and current period output analysis data to CAS */
   /*******************************************************************/
   %corew_prepare_input_data(host = &host.
                           , port = &port.
                           , logonHost = &logonHost.
                           , logonPort = &logonPort.
                           , username = &username.
                           , password = &password.
                           , authMethod = &authMethod.
                           , client_id = &client_id.
                           , client_secret = &client_secret.
                           , inTableList = &prev_result_key. &curr_result_key.
                           , outTableList = _tmp_results_prev _tmp_results_current
                           , outCasLib = &outModelCasLib.
                           , outCasTablesScope = session
                           , casSessionName = &casSessionName.
                           , outVarToken = &outVarToken.
                           , debug = &debug.
                           );

   proc fedsql sessref=&casSessionName;
      create table "&outModelCasLib.".max_horizon_prev {options replace=true} as
      select max(horizon) as max_horizon
      from "&outModelCasLib."._tmp_results_prev
      ;
      create table "&outModelCasLib.".max_horizon_curr {options replace=true} as
      select max(horizon) as max_horizon
      from "&outModelCasLib."._tmp_results_current
      ;
   quit;

   data _null_;
      set &outCasLibref..max_horizon_prev;
      call symputx("prevMaxHorizon", max_horizon, "L");
   run;

   data _null_;
      set &outCasLibref..max_horizon_curr;
      call symputx("currMaxHorizon", max_horizon, "L");
   run;

   /* Create the results structure from the DDL file */
   %let dtfmt=yymmdd10.;
   %let pctfmt=percent8.2;
   %let commafmt=comma25.2;
   %let input_flg=N;
   %let libref=&outCasLibref.;
   %let solutionCadence=%sysget(SAS_RISK_CIRRUS_CADENCE);
   %let ddlPath = &solutionRootFolder./&solutionCadence.;
   %if (%sysevalf(%superq(ddlSubPath) ne, boolean)) %then %do;
      %let ddlPath = &ddlPath./&ddlSubPath.;
   %end;
   filename ddl filesrvc folderPath="&ddlPath." filename="&mart_table_name..sas";
   %include ddl / lrecl=64000;

   /* Get a list of classification variables from the mart's DDL - this is used to create a hash of the classification var values */
   %let primary_key = REPORTING_DT INSTID SCENARIO_NAME FORECAST_TIME orig_movement_id movement_type_cd;
   %let piped_primary_key = %sysfunc(prxchange(s/\s+/|/i, -1, %sysfunc(strip(&primary_key.))));
   %let classification_vars = %rsk_getvarlist(&outCasLibref.."&mart_table_name."n, type = C,
      pattern = ^((?!\b(&piped_primary_key.|entity_id|movement_desc|movement_type|movement_category)\b).)*$);
   %let classification_vars_csv=%sysfunc(prxchange(s/\s+/%str(,)/, -1, &classification_vars.));

   /* Delete the previous period results CAS table if it already exists */
   %core_cas_drop_table(cas_session_name = &casSessionName.
                        , cas_libref = &outModelCaslib.
                        , cas_table = &out_results_prev.);

   /* Create the final previous results CAS table */
   data
      &outCasLibref..&out_results_prev.
         %if &keepModelData. eq Y %then %do;
            (promote=yes)
         %end;
      &outCasLibref.._scen_map_failure_
      ;

      length class_var_hash varchar(200);

      set &outCasLibref.._tmp_results_prev (where=(
         horizon=&prevMaxHorizon.
         %if "&scenario_selection." ne "" %then %do;
            and scenario_name="&scenario_selection."
         %end;
         %else %do;
            and scenario_name ne "Weighted"
         %end;
         ));

      if _N_ = 0 then
         set &inCasLibref.."&outScenarioMap."n;

      if _N_ = 1 then do;
         /* Create the scenario map lookup hash */
         declare hash hScenMap(dataset: "&inCasLibref..'&outScenarioMap.'n");
         hScenMap.defineKey("fromScenarioName");
         hScenMap.defineData("toScenarioName");
         hScenMap.defineDone();
      end;

      /* Overwrite the prior period scenario name with the corresponding current period scenario name
         - so that the merge in corew_calculate_attribution matches */
      if scenario_name ne "Weighted" then do;
         fromScenarioName = scenario_name;
         call missing(toScenarioName);
         _rc_scen_map_ = hScenMap.find();
         if _rc_scen_map_=0 then
            scenario_name = toScenarioName;
         else do;
            output &outCasLibref.._scen_map_failure_;
            stop;
         end;
      end;
      drop fromScenarioName toScenarioName _rc_scen_map_;

      /* Set the reporting_dt to the current period's asOfDate - so that the merge in corew_calculate_attribution matches */
      reporting_dt = &curr_asOfDate.;

      /* create a hash variable representing this row's class vars */
      class_var_hash=hashing('md5', cats(&classification_vars_csv.));

      output &outCasLibref..&out_results_prev.;

   run;

   %let from_scenario_name=;
   data _null_;
      set &outCasLibref.._scen_map_failure_;
      call symputx("from_scenario_name", fromScenarioName, "L");
   run;

   %if "&from_scenario_name." ne "" %then %do;
      %put ERROR: Failed to map previous scenario &from_scenario_name. to a current scenario.;
      %put ERROR: Check scenario mappings to ensure that all previous scenarios are correctly mapped to current scenarios.;
      %abort;
   %end;

   /* Delete the previous period results CAS table if it already exists */
   %core_cas_drop_table(cas_session_name = &casSessionName.
                        , cas_libref = &outModelCaslib.
                        , cas_table = &out_results_current.);

   /* Create the final current results CAS table */
   data &outCasLibref.."&out_results_current."n
      %if &keepModelData. eq Y %then %do;
         (promote=yes)
      %end;
      ;
      length class_var_hash varchar(200);
      set &outCasLibref.._tmp_results_current (where=(
         horizon=&currMaxHorizon.
         %if "&scenario_selection." ne "" %then %do;
            and scenario_name="&scenario_selection."
         %end;
         %else %do;
            and scenario_name ne "Weighted"
         %end;
         )
      );

      /* Set the reporting_dt to the current period's asOfDate - so that the merge in corew_calculate_attribution matches */
      reporting_dt = &curr_asOfDate.;

      /* create a hash variable representing this row's class vars */
      class_var_hash=hashing('md5', cats(&classification_vars_csv.));

   run;

   /* Make sure that the reporting_dt is the same format (for the merge in corew_calculation_attribution) */
   proc cas;
      session &casSessionName.;
      table.alterTable /
         caslib="&outModelCaslib."
         name="&out_results_prev."
         columns={
            {name="reporting_dt" format="&dtfmt."}
         }
      ;
      run;
      table.alterTable /
         caslib="&outModelCaslib."
         name="&out_results_current."
         columns={
            {name="reporting_dt" format="&dtfmt."}
         }
      ;
      run;
   quit;


   /*******************************************************************************************************/
   /* Create the attribution config and attribution run config tables from the input attribution template */
   /*******************************************************************************************************/
   proc sort data = &inAttributionTable. out=tmp_attribution_config;
      by attributionGroupNo;
   run;

   data &outSasAttributionConfig.
      &outSasAttributionRunConfig. (drop = attributeName attributionType);
      length
         attributionKey                $100.
         attributionGroupNo            8.
         run_sequence_no               8.
         runFlg                        $1.
         previous_analysis_run_key     $100.
         current_analysis_run_key      $100.
         previous_result_table         $100.
         current_result_table          $100.
         previous_portfolio_table      $100.
         current_portfolio_table       $100.
         portfolio_switch_flg          $1.
         portfolio_override_varlist    $10000.
         scenario_switch_flg           $1.
         scenario_override_varlist     $10000.
         model_switch_flg              $1.
         previous_counterparty_table   $100.
         current_counterparty_table    $100.
         previous_mitigant_table       $100.
         current_mitigant_table        $100.
         previous_scenarios_table      $100.
         current_scenarios_table       $100.
         previous_scenario_sets_table  $100.
         current_scenario_sets_table   $100.
         current_reporting_dt          8.
         previous_scen_weight_flg      $1.
         current_scen_weight_flg       $1.
         previous_weight_output_vars   $10000.
         current_weight_output_vars    $10000.
      ;
      set tmp_attribution_config;
      by attributionGroupNo;

      retain
         portfolio_switch_flg "N"
         scenario_switch_flg  "N"
         model_switch_flg     "N"
      ;

      retain
         runFlg
         portfolio_override_varlist
         scenario_override_varlist
      ;

      drop cum_sequence_no;
      retain cum_sequence_no;

      /* Set static parameters */
      attributionKey = "&analysisRunKey.";
      previous_analysis_run_key = "&prev_analysis_run_key.";
      current_analysis_run_key = "&curr_analysis_run_key.";
      previous_result_table = "&out_results_prev.";
      current_result_table = "&out_results_current.";
      previous_portfolio_table = "&out_portfolio_prev.";
      current_portfolio_table = "&out_portfolio_current.";
      previous_counterparty_table = ifc("&prev_counterparty_key." ne "", "&out_counterparty_prev.", "");
      current_counterparty_table = ifc("&curr_counterparty_key." ne "", "&out_counterparty_current.", "");
      previous_mitigant_table = ifc("&prev_mitigant_key." ne "", "&out_mitigant_prev.", "");
      current_mitigant_table = ifc("&curr_mitigant_key." ne "", "&out_mitigant_current.", "");
      previous_scenarios_table = "&out_scenarios_prev.";
      current_scenarios_table = "&out_scenarios_current.";
      previous_scenario_sets_table = "&out_scenario_sets_prev.";
      current_scenario_sets_table = "&out_scenario_sets_current.";
      current_reporting_dt = &curr_asOfDate.;
      previous_scen_weight_flg = "&prev_scen_weight_flg.";
      current_scen_weight_flg = "&curr_scen_weight_flg.";
      previous_weight_output_vars = "&prev_weight_output_vars.";
      current_weight_output_vars = "&curr_weight_output_vars.";

      /* Reset RunFlg */
      if(first.attributionGroupNo) then
         runFlg = "N";

      /* Flag which changes require a model run */
      if(attributionType in ("PortfolioAging" "PortfolioAttribute" "PortfolioSwitch" "ScenarioRiskFactor" "ScenarioSwitch" "Model" "FX")) then
         runFlg = "Y";

      if(attributionType = "PortfolioSwitch") then do;
         portfolio_switch_flg = "Y";
         call missing(portfolio_override_varlist);
      end;

      if(attributionType = "ScenarioSwitch") then do;
         scenario_switch_flg = "Y";
         call missing(scenario_override_varlist);
      end;

      if(attributionType = "Model") then
         model_switch_flg = "Y";


      /* Keep track of all portfolio attributes being changed (cumulative override) */
      if(attributionType = "PortfolioAttribute" and portfolio_switch_flg = "N") then
         portfolio_override_varlist = catx(" ", portfolio_override_varlist, attributeName);

      /* Keep track of all portfolio attributes being changed (cumulative override) */
      if(attributionType = "ScenarioRiskFactor" and scenario_switch_flg = "N") then
         scenario_override_varlist = catx(" ", scenario_override_varlist, attributeName);


      /* No need to run anything if we have switched Portflio, Scenarios and Model (-> this is the current period result) */
      if(portfolio_switch_flg = "Y" and scenario_switch_flg = "Y" and model_switch_flg = "Y") then
         runFlg = "N";

      if last.attributionGroupNo and runFlg = "Y" then do;
         cum_sequence_no + 1;
         run_sequence_no = cum_sequence_no;
         output &outSasAttributionRunConfig.;
      end;

      output &outSasAttributionConfig.;

   run;

   libname &inCasLibref. clear;
   libname &outCasLibref. clear;

%mend;