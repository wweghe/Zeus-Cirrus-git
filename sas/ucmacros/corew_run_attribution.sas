%macro corew_run_attribution(solution =
                              , host =
                              , port =
                              , logonHost =
                              , logonPort =
                              , username =
                              , password =
                              , authMethod = bearer
                              , client_id =
                              , client_secret =
                              , attributionType = CIRRUS          /* CIRRUS or RE */
                              , inSasAttributionRunConfig =
                              , inScenarioMap =
                              , ds_in_map_movement =
                              , ds_in_cardinality =
                              , keepModelData =
                              , scenario_selection =
                              , inModelCasLib =
                              , outModelCasLib =
                              , analysisRunKey =
                              , asOfDate =
                              , casTablesTag =
                              , pollInterval = 5
                              , maxWait = 3600
                              , casSessionName = casauto
                              , solutionRootFolder =
                              , ddlSubPath =
                              , mart_table_name =
                              , outVarToken = accessToken
                              , debug = false
                              );

   %local   inCasLibref outCasLibref
            i row_list seq_no Totrows asOfDateFmt scen_override_varlist
            from_scenario_name
      ;

   %let attributionType=%sysfunc(coalescec(%upcase(&attributionType.), CIRRUS));
   %if &attributionType. ne CIRRUS and &attributionType. ne RE %then %do;
      %put ERROR: Attribution type "&attributionType." is not supported.  (Supported types: CIRRUS, RE);
      %abort;
   %end;

   %if(%sysevalf(%superq(inSasAttributionRunConfig) eq, boolean)) %then %do;
      %put ERROR: inSasAttributionRunConfig is required.;
      %abort;
   %end;

   %if(%sysevalf(%superq(inScenarioMap) eq, boolean)) %then %do;
      %put ERROR: inScenarioMap is required.;
      %abort;
   %end;

   %let keepModelData=%sysfunc(coalescec(%upcase(&keepModelData.), N));

   /* Tables, RE pipelines, etc created in this macro will be tagged with "_YYYYMMDD_<arTag>",
   where <arTag> is the first 7 characters of the analysis run key */
   %let asOfDateFmt=%sysfunc(putn(&asOfDate, yymmddn8.));

   %let inCasLibref = %rsk_get_unique_ref(type = lib, engine = cas, args = caslib="&inModelCaslib." sessref=&casSessionName.);
   %let outCasLibref = %rsk_get_unique_ref(type = lib, engine = cas, args = caslib="&outModelCaslib." sessref=&casSessionName.);

   /* Find out the total number of partitions */
   data _null_;
      set &ds_in_cardinality.;
      call symputx("partition_no", partition_no, "L");
      call symputx("n_partitions", n_partitions, "L");
   run;

   /* Find out which rows of the attribution run table this macro call will process */
   %let Totrows = %rsk_attrn(&inSasAttributionRunConfig., nobs);
   %let row_list=;
   %do i=&partition_no. %to &TotRows. %by &n_partitions.;
      %let row_list=&row_list. &i.;
   %end;

   data __in_attribution_run_config__;
      set &inSasAttributionRunConfig.;
      if _N_ in (&row_list.);
   run;

   /* Create a macrovariable array from the filtered attribution run configuration table */
   %let TotRuns = 0;
   data _null_;

      /* Subset the records for the current partition */
      set __in_attribution_run_config__ end = last;

      /* Set all macro variables */
      call symputx(cats("run_sequence_no_", put(_N_, 8.)), run_sequence_no, "L");

      /* Set portfolio parameters */
      if(portfolio_switch_flg = "Y") then do;
         /* Use current period portfolio */
         call symputx(cats("portfolio_table_", put(_N_, 8.)), current_portfolio_table, "L");

         /* Set the previous portfolio as override but with no vars in order to force a inner join of the two portfolios */
         call symputx(cats("portfolio_override_table_", put(_N_, 8.)), previous_portfolio_table, "L");
         call symputx(cats("portfolio_override_varlist_", put(_N_, 8.)), " ", "L");

         /* Use current period Counterparty and Mitigant data if available
            If the previous model requires counterparty and the current model doesn't, the current_counterparty_key will be missing:
               In such a case, if the PortfolioSwitch happens before the ModelSwitch we need to have a counterparty/mitigant table to run the previous model,
               so we will use previous period data as a fallback */
         call symputx(cats("counterparty_table_", put(_N_, 8.)), coalescec(current_counterparty_table, previous_counterparty_table), "L");
         call symputx(cats("mitigant_table_", put(_N_, 8.)), coalescec(current_mitigant_table, previous_mitigant_table), "L");
      end;
      else do;
         /* Use previous period portfolio and override variables */
         call symputx(cats("portfolio_table_", put(_N_, 8.)), previous_portfolio_table, "L");
         call symputx(cats("portfolio_override_table_", put(_N_, 8.)), current_portfolio_table, "L");
         call symputx(cats("portfolio_override_varlist_", put(_N_, 8.)), portfolio_override_varlist, "L");

         /* Use previous period Counterparty and Mitigant data */
         call symputx(cats("counterparty_table_", put(_N_, 8.)), previous_counterparty_table, "L");
         call symputx(cats("mitigant_table_", put(_N_, 8.)), previous_mitigant_table, "L");
      end;

      /* Set scenario parameters */
      if(scenario_switch_flg = "Y") then do;
         /* Use current period scenarios */
         call symputx(cats("scen_override_varlist_", put(_N_, 8.)), " ", "L");
         call symputx(cats("scen_override_table_", put(_N_, 8.)), " ", "L");
         call symputx(cats("scen_table_", put(_N_, 8.)), current_scenarios_table, "L");
         call symputx(cats("scen_set_table_", put(_N_, 8.)), current_scenario_sets_table, "L");
      end;
      else do;
         /* Use previous period scenarios and override variables */
         call symputx(cats("scen_override_varlist_", put(_N_, 8.)), scenario_override_varlist, "L");
         call symputx(cats("scen_override_table_", put(_N_, 8.)), current_scenarios_table, "L");
         call symputx(cats("scen_table_", put(_N_, 8.)), previous_scenarios_table, "L");
         call symputx(cats("scen_set_table_", put(_N_, 8.)), previous_scenario_sets_table, "L");
      end;

      /* Check if the attribution analysis requires scenario mapping */
      if(scenario_switch_flg = "Y" or not missing(scenario_override_varlist)) then
         call symputx("required_scenarios_flg", "Y", "L");

      /* Set model parameters */
      if(model_switch_flg = "Y") then do;
         call symputx(cats("analysis_run_key_", put(_N_, 8.)), current_analysis_run_key, "L");
         call symputx(cats("model_switch_flg_", put(_N_, 8.)), "Y", "L");
         call symputx(cats("scen_weight_flg_", put(_N_, 8.)), current_scen_weight_flg, "L");
         call symputx(cats("weight_output_vars_", put(_N_, 8.)), current_weight_output_vars, "L");
      end;
      else do;
         call symputx(cats("analysis_run_key_", put(_N_, 8.)), previous_analysis_run_key, "L");
         call symputx(cats("model_switch_flg_", put(_N_, 8.)), "N", "L");
         call symputx(cats("scen_weight_flg_", put(_N_, 8.)), previous_scen_weight_flg, "L");
         call symputx(cats("weight_output_vars_", put(_N_, 8.)), previous_weight_output_vars, "L");
      end;

      if last then do;
         /* Set general parameters */
         call symputx("attribution_key", attributionKey, "L");
         call symputx("curr_reporting_dt", current_reporting_dt, "L");
         /* Total number of records processed */
         call symputx("TotRuns", _N_, "L");
      end;

   run;

   /* Perform the intermediate attribution runs (for this session).  This includes doing the following for each attribution run:
      1. Scenario overrides --> intermediate run scenario data
      2. Portfolio overrides --> intermediate run portfolio data
      3. corew_run_model (run the intermediate model - uses all intermediate run input data)
      4. corew_model_post_process (post-process the intermediate model) --> intermediate run results
   */
   %do i=1 %to &TotRuns.;

      %let seq_no = &&run_sequence_no_&i..;

      /***********************************/
      /* Perform Scenario Data overrides */
      /***********************************/

      /* Delete the scenario CAS table if it already exists */
      %core_cas_drop_table(cas_session_name = &casSessionName.
                           , cas_libref = &inModelCaslib.
                           , cas_table = scenarios_&asOfDateFmt._&casTablesTag._&seq_no.);

      %let scen_override_varlist = %upcase(%sysfunc(prxchange(s/(\w+)/"$1"/i, -1, &&scen_override_varlist_&i..)));
      data  &inCasLibref..scenarios_&asOfDateFmt._&casTablesTag._&seq_no. (promote=yes)
            &inCasLibref.._scen_map_failure_;

         set &inCasLibref..&&scen_table_&i..;

         /* If we've switched to the current period, just copy the current period scenario table.  If we're still
         using the prior period scenario:
            1. Override the prior period scenario names with the corresponding current period scenario names
            2. Apply scenario variable overrides (if any)
         */
         %if "&&scen_override_table_&i.." ne "" %then %do;

            if _N_ = 0 then
               set &inCasLibref.."&inScenarioMap."n;

            if _N_ = 1 then do;

               /* Create the scenario map lookup hash */
               declare hash hScenMap(dataset: "&inCasLibref..'&inScenarioMap.'n");
               hScenMap.defineKey("fromScenarioName");
               hScenMap.defineData("toScenarioName");
               hScenMap.defineDone();

               /* Create the scenario override hash */
               %if %sysevalf(%superq(scen_override_varlist) ne, boolean) %then %do;
                  declare hash hScenOverride(dataset: "&inCasLibref..&&scen_override_table_&i..");
                  hScenOverride.defineKey("scenario_name", "horizon", "variable_name");
                  hScenOverride.defineData("change_type", "change_value");
                  hScenOverride.defineDone();
               %end;

            end;

            /* Overrides only need done for future data (h>0).  Current/historical data is realized so is the same between the 2 scenarios */
            /* Only keep rows meeting at least 1 of these conditions:
                  -the scenario was found in the current period as well.
                  -the scenario row is historical data (h<0), since we need to include history
            */
            if horizon>0 then do;

               /* Lookup the current period scenario name corresponding to this prior period scenario name */
               fromScenarioName = scenario_name;
               call missing(toScenarioName);
               _rc_scen_map_ = hScenMap.find();

               /* Map on the current period override vars by scenario_name, date, and horizon */
               if _rc_scen_map_ = 0 then do;
                  scenario_name = toScenarioName;

                  %if %sysevalf(%superq(scen_override_varlist) ne, boolean) %then %do;
                     if upcase(variable_name) in (&scen_override_varlist.) then
                        hScenOverride.find();
                  %end;
               end;
               else do;
                  output &inCasLibref.._scen_map_failure_;
                  stop;
               end;

               if _rc_scen_map_=0 or horizon < = 0 then output &inCasLibref..scenarios_&asOfDateFmt._&casTablesTag._&seq_no.;

               drop fromScenarioName toScenarioName _rc_scen_map_;

            end;
            else output &inCasLibref..scenarios_&asOfDateFmt._&casTablesTag._&seq_no.;

         %end;
         %else %do;
            output &inCasLibref..scenarios_&asOfDateFmt._&casTablesTag._&seq_no.;
         %end;

      run;

      %let from_scenario_name=;
      data _null_;
         set &inCasLibref.._scen_map_failure_;
         call symputx("from_scenario_name", fromScenarioName, "L");
      run;

      %if "&from_scenario_name." ne "" %then %do;
         %put ERROR: Failed to map previous scenario &from_scenario_name. to a current scenario.;
         %put ERROR: Check scenario mappings to ensure that all previous scenarios are correctly mapped to current scenarios.;
         %abort;
      %end;

      /* Delete the scenario set CAS table if it already exists */
      %core_cas_drop_table(cas_session_name = &casSessionName.
                           , cas_libref = &inModelCaslib.
                           , cas_table = scen_set_&asOfDateFmt._&casTablesTag._&seq_no.);

      data  &inCasLibref..scen_set_&asOfDateFmt._&casTablesTag._&seq_no. (promote=yes);
         set &inCasLibref..&&scen_set_table_&i..;

         %if "&&scen_override_table_&i.." ne "" %then %do;

            if _N_ = 0 then
               set &inCasLibref.."&inScenarioMap."n;

            if _N_ = 1 then do;
               /* Create the scenario map lookup hash */
               declare hash hScenMap(dataset: "&inCasLibref..'&inScenarioMap.'n");
               hScenMap.defineKey("fromScenarioName");
               hScenMap.defineData("toScenarioName");
               hScenMap.defineDone();
            end;

            /* Lookup the current period scenario name corresponding to this prior period scenario name */
            fromScenarioName = scenarioName;
            call missing(toScenarioName);
            _rc_scen_map_ = hScenMap.find();
            scenarioName = toScenarioName;
            drop fromScenarioName toScenarioName _rc_scen_map_;

         %end;

      run;

      /************************************/
      /* Perform Portfolio Data overrides */
      /************************************/
      /* Delete the portfolio CAS table if it already exists */
      %core_cas_drop_table(cas_session_name = &casSessionName.
                           , cas_libref = &inModelCaslib.
                           , cas_table = portfolio_&asOfDateFmt._&casTablesTag._&seq_no.);

      /* Always join the prior and current together and keep only the common instruments (non-common instruments are handled in
         the "Expired Instruments" and "New Originations" attribution factors separately).  If this attribution factor run includes
         portfolio override columns, then we overwrite those columns as well */
      data &inCasLibref..portfolio_&asOfDateFmt._&casTablesTag._&seq_no. (promote=yes);
         merge
            &inCasLibref.."&&portfolio_table_&i.."n (in=a)
            &inCasLibref.."&&portfolio_override_table_&i.."n (in=b keep=instid
               %if %sysevalf(%superq(portfolio_override_varlist_&i.) ne, boolean) %then %do;
                  &&portfolio_override_varlist_&i..
               %end;
            )
            ;
         by instid;

         /* Keep only common records between the portfolio and the override table */
         if a and b then output;
      run;

      /* For Cirrus attribution, run corew_run_model and corew_model_post_process */
      %if &attributionType. eq CIRRUS %then %do;

         %let ATTRIBUTION_FLG=Y;

         %corew_run_model(host = &host.
                           , port = &port.
                           , logonHost = &logonHost.
                           , logonPort = &logonPort.
                           , username = &username.
                           , password = &password.
                           , authMethod = &authMethod.
                           , client_id = &client_id.
                           , client_secret = &client_secret.
                           , solution = &solution.
                           , analysisRunKey = &&analysis_run_key_&i..
                           , inPortfolio = portfolio_&asOfDateFmt._&casTablesTag._&seq_no.
                           , inCounterparty = &&counterparty_table_&i..
                           , inMitigant = &&mitigant_table_&i..
                           /*, inCollateral =
                           , inCashflow = */
                           , inScenarios = scenarios_&asOfDateFmt._&casTablesTag._&seq_no.
                           , inScenarioSet = scen_set_&asOfDateFmt._&casTablesTag._&seq_no.
                           , outResults = results_&asOfDateFmt._&casTablesTag._&seq_no.
                           %if("&&scen_weight_flg_&i.." = "Y") %then %do;
                              , scenWeightCalcFlg = &&scen_weight_flg_&i..
                              , weightedCalcOutputVars = &&weight_output_vars_&i..
                           %end;
                           , asOfDate = &curr_reporting_dt.
                           , casTablesTag = &casTablesTag._&seq_no.
                           , inModelCasLib = &inModelCasLib.
                           , outModelCasLib = &outModelCasLib.
                           , keepModelData = &keepModelData.
                           , maxWait = &maxWait.
                           , casSessionName = &casSessionName.
                           , outVarToken = &outVarToken.
                           , debug = &debug.
                           );

         /* Get only the max horizon in the results - we don't support attribution on multiple horizons */
         proc fedsql sessref=&casSessionName.;
            create table "&outModelCasLib.".max_horizon {options replace=true} as
            select max(horizon) as max_horizon
            from "&outModelCasLib."."results_&asOfDateFmt._&casTablesTag._&seq_no."
            ;
         quit;

         data _null_;
            set &outCasLibref..max_horizon;
            call symputx("maxHorizon", max_horizon, "L");
         run;

         /************************************************/
         /* Perform post-processing on the model results */
         /************************************************/
         %corew_model_post_process(outModelCasLib = &outModelCasLib.
                                 , inResults = results_&asOfDateFmt._&casTablesTag._&seq_no.
                                 , ds_in_map_movement = &ds_in_map_movement.
                                 , outResults = results_&asOfDateFmt._&casTablesTag._&seq_no.
                                 , solutionRootFolder = &solutionRootFolder.
                                 , ddlSubPath = &ddlSubPath.
                                 , mart_table_name = &mart_table_name.
                                 , asOfDate = &curr_reporting_dt.
                                 , casSessionName = &casSessionName.
                                 , customCode = if horizon=&maxHorizon.
                                    %if "&scenario_selection." ne "" %then %do;
                                       and scenario_name="&scenario_selection."
                                    %end;
                                    %else %do;
                                       and scenario_name ne "Weighted"
                                    %end;
                                 );

      %end; /* End Cirrus Attribution */
      %else %do;
         /* TODO: RE attribution */
      %end;

   %end; /* End Totruns loop */

   /* Cleanup some remaining CAS tables, if we aren't keeping CAS data */
   %if &keepModelData. ne Y %then %do;

      %do i=1 %to &TotRuns.;

         %core_cas_drop_table(cas_session_name = &casSessionName.
                           , cas_libref = &inModelCaslib.
                           , cas_table = &&portfolio_table_&i..);

         %core_cas_drop_table(cas_session_name = &casSessionName.
                           , cas_libref = &inModelCaslib.
                           , cas_table = &&scen_set_table_&i..);

         %core_cas_drop_table(cas_session_name = &casSessionName.
                           , cas_libref = &inModelCaslib.
                           , cas_table = &&scen_table_&i..);

         %if "&&counterparty_table_&i.." ne "" %then %do;
            %core_cas_drop_table(cas_session_name = &casSessionName.
                              , cas_libref = &inModelCaslib.
                              , cas_table = &&counterparty_table_&i..);
         %end;

         %if "&&mitigant_table_&i.." ne "" %then %do;
            %core_cas_drop_table(cas_session_name = &casSessionName.
                              , cas_libref = &inModelCaslib.
                              , cas_table = &&mitigant_table_&i..);
         %end;

      %end;

   %end;

   libname &inCasLibref. clear;
   libname &outCasLibref. clear;

%mend;
