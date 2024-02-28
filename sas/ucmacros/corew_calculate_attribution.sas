%macro corew_calculate_attribution(solution =
                              , host =
                              , port =
                              , logonHost =
                              , logonPort =
                              , username =
                              , password =
                              , authMethod = bearer
                              , client_id =
                              , client_secret =
                              , inSasAttributionConfig =
                              , ds_in_map_movement =
                              , outAttributionResults =
                              , mart_table_name =
                              , keepModelData =
                              , epsilon = 1e-10
                              , outModelCasLib =
                              , analysisRunKey =
                              , asOfDate =
                              , casTablesTag =
                              , instrument_key_vars = instid
                              , stage_var = ECL_STAGE
                              , ecl_12m_var = ECL_12M
                              , ecl_lifetime_var = ECL_LIFETIME
                              , ecl_var = ECL
                              , stage_attribution_method = Stage
                              , solutionRootFolder =
                              , ddlSubPath =
                              , casSessionName = casauto
                              , outVarToken = accessToken
                              , debug = false
                              );

   %local TotRunAttr asOfDateFmt instrument_key_vars_qcsv
            transferFrom_no transferTo_no transferFrom_label transferTo_label
            derecognition_no derecognition_label origination_no origination_label
            synth_derecognition_no synth_derecognition_label synth_origination_no synth_origination_label synth_instrument_flg
            ia_no ia_label qf_adjustment_no qf_adjustment_label manual_adjustment_no manual_adjustment_label
            model_no model_label model_is_last_flg
            other_no other_label
            primary_key piped_primary_key primary_key_qcsv
            dt_fmts dttm_fmts tm_fmts dt_vars
            exclusion_pattern
            reset_vars
            rename_stmt
            TotAttrVars TotClassificationVars
            class_exclude_vars classification_vars classification_vars_csv classification_vars_sql classification_vars_piped classification_vars_csv_qtd
            cnt
            outCasLibref
            ddlPath
            i
            tot_dependent_vars
            compute_stage_delta_flg
            model_is_last_flg
      ;

   %if(%sysevalf(%superq(inSasAttributionConfig) eq, boolean)) %then %do;
      %put ERROR: inSasAttributionConfig is required.;
      %abort;
   %end;

   %let keepModelData=%sysfunc(coalescec(%upcase(&keepModelData.), N));

   /* Tables, RE pipelines, etc created in this macro will be tagged with "_YYYYMMDD_<arTag>",
   where <arTag> is the first 7 characters of the analysis run key */
   %let asOfDateFmt=%sysfunc(putn(&asOfDate, yymmddn8.));

   %let outCasLibref = %rsk_get_unique_ref(type = lib, engine = cas, args = caslib="&outModelCaslib." sessref=&casSessionName.);

   /* Convert the list of Instrument Key variables into a quoted comma separated values */
   %let instrument_key_vars_qcsv = %sysfunc(prxchange(s/\s+/%str(, )/i, -1, &instrument_key_vars.));
   %let instrument_key_vars_qcsv = %sysfunc(prxchange(s/(\w+)/"$1"/i, -1, %bquote(&instrument_key_vars_qcsv.)));

   /* Assign default value for stage_attribution_method if missing */
   %if %sysevalf(%superq(stage_attribution_method) =, boolean) %then
      %let stage_attribution_method = STAGE;
   %else
      %let stage_attribution_method = %upcase(&stage_attribution_method.);

   /* Create the final results structure from the DDL file */
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

   /* *********************************************************** */
   /* Process attribution configuration table                     */
   /* *********************************************************** */
   %let compute_stage_delta_flg = N;
   %let model_is_last_flg = Y;
   %let TotRunAttr = 0;
   data _null_;
      set &inSasAttributionConfig. end = last;

      /* *********************************************************** */
      /* Process attribution entries that do not require a model run */
      /* *********************************************************** */
      select(attributionType);
         when("Derecognition") do;
            call symputx("derecognition_no", attributionGroupNo, "L");
            call symputx("derecognition_label", attributionGroupName, "L");
         end;
         when("Origination") do;
            call symputx("origination_no", attributionGroupNo, "L");
            call symputx("origination_label", attributionGroupName, "L");
         end;
         when("SynthDerecognition") do;
            call symputx("synth_derecognition_no", attributionGroupNo, "L");
            call symputx("synth_derecognition_label", attributionGroupName, "L");
         end;
         when("SynthOrigination") do;
            call symputx("synth_origination_no", attributionGroupNo, "L");
            call symputx("synth_origination_label", attributionGroupName, "L");
         end;
         when("Model") do;
            call symputx("model_no", attributionGroupNo, "L");
            call symputx("model_label", attributionGroupName, "L");
         end;
         when("IndividualAdjustment") do;
            call symputx("ia_no", attributionGroupNo, "L");
            call symputx("ia_label", attributionGroupName, "L");
         end;
         when("StageAllocation") do;
            call symputx("compute_stage_delta_flg", "Y", "L");
            call symputx("stage_allocation_no", attributionGroupNo, "L");
            call symputx("stage_allocation_label", attributionGroupName, "L");
         end;
         when("QFactor") do;
            call symputx("qf_adjustment_no", attributionGroupNo, "L");
            call symputx("qf_adjustment_label", attributionGroupName, "L");
         end;
         when("ManAdjust") do;
            call symputx("manual_adjustment_no", attributionGroupNo, "L");
            call symputx("manual_adjustment_label", attributionGroupName, "L");
         end;
         when("Other") do;
            call symputx("other_no", attributionGroupNo, "L");
            call symputx("other_label", attributionGroupName, "L");
         end;
         otherwise;
      end;

      /* *********************************************************** */
      /* Process attribution entries that require a model run        */
      /* *********************************************************** */
      if not missing(run_sequence_no) then do;
         call symputx(cats("attrib_result_", put(run_sequence_no, 8.)), cats("results_&asOfDateFmt._&casTablesTag._", put(run_sequence_no, 8.)), "L");
         call symputx(cats("run_group_no_", put(run_sequence_no, 8.)), attributionGroupNo, "L");
         call symputx(cats("run_group_label_", put(run_sequence_no, 8.)), attributionGroupName, "L");

         /* Check if the Model Logic was part of the run sequence */
         if(attributionType = "Model") then
            /* Model Logic is not the last */
            call symputx("model_is_last_flg", "N", "L");

         /* Update total count of model runs */
         call symputx("TotRunAttr", run_sequence_no, "L");
      end;

      if last then do;
         call symputx("prev_result_table", previous_result_table, "L");
         call symputx("curr_result_table", current_result_table, "L");
         call symputx("attribution_vars", attributionVars, "L");
         call symputx("transferFrom_label", transferFromLabel, "L");
         call symputx("transferTo_label", transferToLabel, "L");

         call symputx("tables_to_drop", catx(" ",
	      previous_portfolio_table, current_portfolio_table,
	      previous_counterparty_table, current_counterparty_table,
	      previous_mitigant_table, current_mitigant_table,
	      previous_scenarios_table, current_scenarios_table,
              previous_scenario_sets_table, current_scenario_sets_table), "L");

      end;
   run;

   %if &keepModelData. ne Y %then %do;

      /* Clean up tables */
      %local TotTblsToDrop dtable_nm;

      %let TotTblsToDrop = %sysfunc(countw(&tables_to_drop., %str( )));

      %do dcnt = 1 %to &TotTblsToDrop.;

         %let dtable_nm=%scan(&tables_to_drop., &dcnt., %str( ));

         %core_cas_drop_table(cas_session_name = &casSessionName.
                          , cas_libref = &outModelCaslib.
                          , cas_table = &dtable_nm.);

      %end;

   %end;

   %let TotAttrVars = %sysfunc(countw(&attribution_vars., %str( )));

   /**********************/
   /* Set default labels */
   /**********************/

   /* Set default values if the macro variables were not set by the datastep above */
   %let transferFrom_no = 1;
   %if %sysevalf(%superq(transferFrom_label) =, boolean) %then
      %let transferFrom_label = Reclassified from other categories;

   /* Set default values if the macro variables were not set by the datastep above */
   %let transferTo_no = 1;
   %if %sysevalf(%superq(transferTo_label) =, boolean) %then
      %let transferTo_label = Reclassified to other categories;

   /* Set default values if the macro variables were not set by the datastep above */
   %if %sysevalf(%superq(other_label) =, boolean) %then %do;
      %let other_no = 99;
      %let other_label = Other;
   %end;

   /* Unless otherwise specified in the attribution configuration, the following delta will be mapped to Other */
   %if %sysevalf(%superq(derecognition_label) =, boolean) %then %do;
      %let derecognition_no = &other_no.;
      %let derecognition_label = &other_label.;
   %end;

   /* Unless otherwise specified in the attribution configuration, the following delta will be mapped to Other */
   %if %sysevalf(%superq(origination_label) =, boolean) %then %do;
      %let origination_no = &other_no.;
      %let origination_label = &other_label.;
   %end;

   /* Unless otherwise specified in the attribution configuration, the following delta will be mapped to Other */
   %if %sysevalf(%superq(ia_label) =, boolean) %then %do;
      %let ia_no = &other_no.;
      %let ia_label = &other_label.;
   %end;

   /* Unless otherwise specified in the attribution configuration, the following delta will be mapped to Other */
   %if %sysevalf(%superq(stage_allocation_label) =, boolean) %then %do;
      %let stage_allocation_no = &other_no.;
      %let stage_allocation_label = &other_label.;
   %end;

   /* Unless otherwise specified in the attribution configuration, the following delta will be mapped to Other */
   %if %sysevalf(%superq(qf_adjustment_label) =, boolean) %then %do;
      %let qf_adjustment_no = &other_no.;
      %let qf_adjustment_label = &other_label.;
   %end;

   /* Unless otherwise specified in the attribution configuration, the following delta will be mapped to Other */
   %if %sysevalf(%superq(manual_adjustment_label) =, boolean) %then %do;
      %let manual_adjustment_no = &other_no.;
      %let manual_adjustment_label = &other_label.;
   %end;

   /* Unless otherwise specified in the attribution configuration, the following delta will be mapped to Other */
   %if %sysevalf(%superq(model_label) =, boolean) %then %do;
      %let model_no = &other_no.;
      %let model_label = &other_label.;
   %end;

   /**********************************/
   /* Set variable-related macrovars */
   /**********************************/
   /*Note: The primary_key is hardcoded here.  This is to give the customer freedom to add partition variables to their credit risk detail
   configuration table - partition variables have to also be given as primary key variables.  So the customer might add classification variables
   to the primary key, which we do not want to use here.  The columns here are required for any credit risk detail data definition. */
   %let primary_key = REPORTING_DT INSTID SCENARIO_NAME FORECAST_TIME orig_movement_id movement_type_cd;
   %let piped_primary_key = %sysfunc(prxchange(s/\s+/|/i, -1, %sysfunc(strip(&primary_key.))));
   %let primary_key_qcsv = %sysfunc(prxchange(s/\s+/%str(,)/i, -1, %sysfunc(strip(&primary_key.))));
   %let primary_key_qcsv = %sysfunc(prxchange(s/(\w+)/"$1"/i, -1, %bquote(&primary_key_qcsv.)));

   %if(&compute_stage_delta_flg. = Y) %then %do;

      %let stage_attribution_vars = &ecl_12m_var. &ecl_lifetime_var. &ecl_var.;
      %let tot_dependent_vars = 0;

      /* Check if the var dependency dataset was provided */
      %if %sysevalf(%superq(ds_in_var_dependency) ne, boolean) %then %do;
         proc sort data = &ds_in_var_dependency.
                   out = ecl_var_dependency;
            by order_no;
            where
               lowcase(schema_name) = "&schema_name."
               and resolve(schema_version) = "&schema_version."
               and upcase(trigger_var_name) = "%upcase(&ecl_var.)"
            ;
         run;

         data _null_;
            set ecl_var_dependency end = last;
            length dependent_var_list $32000.;
            retain dependent_var_list;
            /* Add the dependent_var_name to the list if it has not been already added */
            if(not prxmatch(cats("/\b(", dependent_var_name, ")\b/i"), dependent_var_list)) then
               dependent_var_list = catx(" ", dependent_var_list, dependent_var_name);
            call symputx(cats("dependent_var_", put(_N_, 8.)), dependent_var_name, "L");
            call symputx(cats("dependent_expr_", put(_N_, 8.)), expression_txt, "L");
            if last then do;
               call symputx("dependent_var_list", dependent_var_list, "L");
               call symputx("tot_dependent_vars", _N_, "L");
            end;
         run;

         %let stage_attribution_vars = &stage_attribution_vars. &dependent_var_list.;

         %end;

         /* Convert the list to pipe separated */
         %let piped_stage_attribution_vars = %sysfunc(prxchange(s/\s+/|/i, -1, &stage_attribution_vars.));

         /* Make sure that the stage attribution vars are included in the list of attribution_vars (without duplicate entries) */

         %let attribution_vars = %sysfunc(prxchange(s/\b(&piped_stage_attribution_vars.)\b//i, -1, &attribution_vars.)) &stage_attribution_vars.;

         /* Update the variable counter */
         %let TotAttrVars = %sysfunc(countw(&attribution_vars., %str( )));

      /*%end;*/

   %end;

   /* Create list of date/time/datetime formats - needed to know which numeric vars to keep */
   %let dt_fmts = %rsk_get_dtm_formats(type = date);
   %let dttm_fmts = %rsk_get_dtm_formats(type = datetime);
   %let tm_fmts = %rsk_get_dtm_formats(type = time);

   /* Get list of date vars */
   %let dt_vars = %rsk_getvarlist(&outCasLibref.."&mart_table_name."n, format = &dt_fmts.|&dttm_fmts.|&tm_fmts.);

   /* List of variables to exclude from the call missing statement */
   %let exclusion_pattern = movement_id|horizon|&piped_primary_key.|%sysfunc(prxchange(s/\s+/|/i, -1, &attribution_vars.));
   %if %sysevalf(%superq(dt_vars) ne, boolean) %then
      %let exclusion_pattern = &exclusion_pattern.|%sysfunc(prxchange(s/\s+/|/i, -1, &dt_vars.));

   /* Retrieve the list of numeric variables excluding the primary_key and attribution vars. These variables must be reset to missing across movements */
   %let reset_vars = %rsk_getvarlist(&outCasLibref.."&mart_table_name."n, type = N, pattern = ^((?!\b(&exclusion_pattern.)\b).)*$);

   /* Retrieve the list of Classification variables (all characters vars) excluding the primary_key and the stage var (if required) */
   %let class_exclude_vars = entity_id|movement_desc|movement_type|movement_category|synthetic_instrument_flg;

   %if(&compute_stage_delta_flg. = Y) %then %do;
      %let classification_vars = %rsk_getvarlist(&outCasLibref.."&mart_table_name."n, type = C, pattern = ^((?!\b(&piped_primary_key.|&class_exclude_vars.|&stage_var.)\b).)*$);
   %end;
   %else %do;
      %let classification_vars = %rsk_getvarlist(&outCasLibref.."&mart_table_name."n, type = C, pattern = ^((?!\b(&piped_primary_key.|&class_exclude_vars.)\b).)*$);
   %end;

   %let classification_vars_csv=%sysfunc(prxchange(s/\s+/%str(,)/, -1, &classification_vars.));
   %let classification_vars_sql=%sysfunc(prxchange(s/(\w+)/b.$1/, -1, %bquote(&classification_vars_csv.)));
   %let classification_vars_piped=%sysfunc(prxchange(s/\s+/%str(|)/, -1, &classification_vars.));
   %let classification_vars_csv_qtd=%sysfunc(prxchange(s/(\w+)/"$1"/, -1, %bquote(&classification_vars_csv.)));

   /* Total list of classification variables */
   %let TotClassificationVars = %sysfunc(countw(&classification_vars., %str( )));

   /* Create a class variables lookup table so that we don't need to include class variables in our DATA step merge below.
   Create on key variable that is a hash based on the value of all classification variable values for that row */
   proc fedsql sessref=&casSessionName.;
      create table "&outModelCaslib.".class_vars_lookup_table {options replace=true} as
      select distinct &classification_vars_csv., class_var_hash
      from (
         select &classification_vars_csv., class_var_hash
         from "&outModelCaslib.".&prev_result_table.
         union
         select &classification_vars_csv., class_var_hash
         from "&outModelCaslib.".&curr_result_table.
      ) as a
      ;
   quit;

   %let synth_instrument_flg=N;
   %if(%rsk_varexist(&outCasLibref.."&mart_table_name."n, synthetic_instrument_flg))  %then
      %let synth_instrument_flg=Y;

   /* Move the SAS mapping tables into CAS so that the big merge below runs in CAS */
   %if %sysevalf(%superq(ds_in_map_movement) ne, boolean) %then %do;

      /* Note - these are charcter columns in SAS but need to be VARCHAR column in CAS (to match the DDL definition) */
      data &outCasLibref..ds_in_map_movement (rename=
         (new_movement_type_cd=movement_type_cd new_movement_type=movement_type new_movement_category=movement_category)
      );
         length new_movement_type_cd varchar(32) new_movement_type new_movement_category varchar(100);
         set &ds_in_map_movement.;

         new_movement_type_cd=strip(movement_type_cd);
         new_movement_type=strip(movement_type);
         new_movement_category=strip(movement_category);

         drop movement_type_cd movement_type movement_category;
      run;
   %end;
   %else %do;
      data &outCasLibref..ds_in_map_movement;
         length movement_type_cd varchar(32) movement_type movement_category varchar(100);
         movement_type_cd="CR_MODEL";
         movement_type="00. Opening Balance";
         movement_category="00. Opening Balance";
      run;
   %end;

   /* Delete the attribution results CAS table if it already exists */
   %core_cas_drop_table(cas_session_name = &casSessionName.
                        , cas_libref = &outModelCaslib.
                        , cas_table = &outAttributionResults.);

   /*******************************************/
   /* Merge and Calculate Attribution Results */
   /*******************************************/

   data &outCasLibref.."&outAttributionResults."n (
         keep=%rsk_getvarlist(&outCasLibref.."&mart_table_name."n, pattern = ^((?!\b(&classification_vars_piped.)\b).)*$) class_var_hash
      );

      length class_var_hash varchar(200);

      merge
         /* Prior Period results (all movements) */
         /* Drop class vars.  Rename Attribution variables (s0 suffix) */
         &outCasLibref.."&prev_result_table."n (
            in=_prev_
            drop=&classification_vars.
            rename = (
               movement_id = orig_movement_id class_var_hash = class_var_hash_prev

               /* Rename Attribution vars <Attribution Var> -> tmp_attrib_var_<cnt>_s0 */
               %do cnt = 1 %to &TotAttrVars.;
                  %scan(&attribution_vars., &cnt., %str( )) = tmp_attrib_var_&cnt._s0
               %end;

               /* Rename Stage variable <Stage Var> -> tmp_stage_var_s0 */
               %if(&compute_stage_delta_flg. = Y) %then %do;
                  &stage_var. = tmp_stage_var_s0
               %end;
            )
         )

         /* Current Period results (all movements) */
         /* Drop class vars.  Rename Attribution variables (s99 suffix) */
         &outCasLibref.."&curr_result_table."n (
            in=_curr_
            drop=&classification_vars.
            rename = (
               movement_id = orig_movement_id class_var_hash = class_var_hash_curr

               /* Rename Attribution vars <Attribution Var> -> tmp_attrib_var_<cnt>_s99 */
               %do cnt = 1 %to &TotAttrVars.;
                  %scan(&attribution_vars., &cnt., %str( )) = tmp_attrib_var_&cnt._s99
               %end;

               /* Rename Stage variable <Stage Var> -> tmp_stage_var_s99 */
               %if(&compute_stage_delta_flg. = Y) %then %do;
                  &stage_var. = tmp_stage_var_s99
               %end;
            )
         )

         /* Intermediate Run Results (movement_id=1 only) */
         /* Keep only primary key and attribution variables.  Rename attribution variables (s<i> suffix) */
         %do i = 1 %to &TotRunAttr.;
            &outCasLibref..&&attrib_result_&i.. (
               keep= %sysfunc(prxchange(s/orig_movement_id/movement_id/i, -1, &primary_key.)) &attribution_vars.
               rename=(
                  movement_id=orig_movement_id

                  /* Rename Attribution vars <Attribution Var> -> tmp_attrib_var_<cnt>_s<i> */
                  %do cnt = 1 %to &TotAttrVars.;
                     %scan(&attribution_vars., &cnt., %str( )) = tmp_attrib_var_&cnt._s&i.
                  %end;
               )
            )
         %end;
      ;

      by &primary_key.;
      retain movement_id;

      if _N_ = 1 then do;
         /* Lookup to retrieve the movement types */
         declare hash hMvmt(dataset: "&outCasLibref..ds_in_map_movement");
         hMvmt.defineKey("movement_type_cd");
         hMvmt.defineData("movement_type", "movement_category");
         hMvmt.defineDone();

      end;

      /* if synthetic_instrument_flg is not part of the results DDL, synthetic instruments are not being used.
      set it to "N" for all instruments here so it can be used in the rest of the data step in either case */
      %if &synth_instrument_flg.=N %then %do;
         synthetic_instrument_flg="N";
      %end;
      synthetic_instrument_flg = upcase(synthetic_instrument_flg);


      /* Reset retained variables at the beginning of each by-group (across movements: third PK variable from the right) */
      if first.%scan(&primary_key., -3, %str( )) then
         movement_id = 0;

      /* Lookup Movement Type and Category for the original movement code */
      _rc_mvmt = hMvmt.find();

      /* Copy original movement_type_cd: it will be overridden below */
      orig_movement_type_cd = movement_type_cd;
      /* Copy the pre-existing movement_type into the movement_desc field (for drill-down purpose) */
      movement_desc = movement_type;

      /* As long as there is an entry from the previous period we have to generate the Opening Balance entry */
      if _prev_ then do;

         /* Reclassify this movement as Opening Balance */
         movement_type_cd = "OB";
         /* Lookup Movement Type and Category for the new code */
         _rc_mvmt = hMvmt.find();

         /* Set values for the classification Variables hash to the previous period */
         class_var_hash = class_var_hash_prev;

         %if(&compute_stage_delta_flg. = Y) %then %do;
            &stage_var. = tmp_stage_var_s0;
         %end;

         /* Set values for the attribution variables from previous period result (<attribution_var> = tmp_attrib_var_<cnt>_S0;) */
         %do cnt = 1 %to &TotAttrVars.;
            %scan(&attribution_vars., &cnt., %str( )) = tmp_attrib_var_&cnt._s0;
         %end;

         /* Increment movement_id */
         movement_id + 1;
         /* Always write the Opening Balance record, even if they are zero: this is to avoid having less Opening Balance instruments than what we had in the closing balance of the previous period. */
         output;

         /* Reset all numeric variables that are not involved in the attribution delta */
         %if %sysevalf(%superq(reset_vars) ne, boolean) %then
            call missing(of &reset_vars.);
         ;

         /* Check if the instrument is in the current period as well (only for non-synthetic instruments) */
         if _curr_ and synthetic_instrument_flg ne "Y" then do;

            /* Set the movement type and category (movement_desc is retained from above) */
            movement_type_cd = "ATTRIBUTION";
            movement_type = "Attribution";

            /* Generate transfer movements if any of the classification Variables has changed */
            if( class_var_hash_prev ne class_var_hash_curr ) then do;

               /* One or more classification attributes have changed from one period to the next: we need to generate two movements:
               - Transfer-Out: Use previous period classification variables hash and revert the previous period amount (-<attribution_var>_S0)
               - Transfer-In: Use current period classification variables hash and set the previous period amount (<attribution_var>_S0)
               */

               /* Revert the OB movement value for the attribution variables from previous period result (<attribution_var> = -tmp_attrib_var_<cnt>_S0;) */
               %do cnt = 1 %to &TotAttrVars.;
                  %scan(&attribution_vars., &cnt., %str( )) = -tmp_attrib_var_&cnt._s0;
               %end;

               /* Set the movement type and category (movement_desc is retained from above) */
               movement_category = "%sysfunc(putn(&transferTo_no., z2.)). &transferTo_label.";
               /* Increment movement_id */
               movement_id + 1;
               /* Write Transfer-Out record */
               output;

               /* Set values for the classification Variables to the current period hash */
               class_var_hash = class_var_hash_curr;

               %if(&compute_stage_delta_flg. = Y) %then %do;
                  &stage_var. = tmp_stage_var_s99;
               %end;

               /* Set previous period values for the attribution variables as the Transfer-In value (<attribution_var> = tmp_attrib_var_<cnt>_S0;) */
               %do cnt = 1 %to &TotAttrVars.;
                  %scan(&attribution_vars., &cnt., %str( )) = tmp_attrib_var_&cnt._s0;
               %end;

               /* Set the movement type and category (movement_desc is retained from above) */
               movement_category = "%sysfunc(putn(&transferFrom_no., z2.)). &transferFrom_label.";
               /* Increment movement_id */
               movement_id + 1;
               /* Write Transfer-In record */
               output;

            end; /* End transfer movements */

            /* Model related attribution (orig_movement_id = 1) */
            if(orig_movement_id = 1) then do;

               /* Initialize the value of the cumulated variables. These variables hold the cumulative delta effect that results from a change of stage across each run */
               %if(&compute_stage_delta_flg. = Y) %then %do;
                  cum_stage_ecl_delta = 0;
                  %do j = 1 %to &tot_dependent_vars.;
                     tmp_cum_&j. = 0;
                  %end;
               %end;

               /* Attribution across the various runs */
               %do i = 1 %to &TotRunAttr.;

                  /* Compute delta for the attribution variables (<attribution_var> = sum(<attribution_var>_S<i>, - <attribution_var>_S<i-1>) */
                  %do cnt = 1 %to &TotAttrVars.;
                     %scan(&attribution_vars., &cnt., %str( )) = sum(tmp_attrib_var_&cnt._s&i., -tmp_attrib_var_&cnt._s%eval(&i.-1));
                  %end;

                  /* At this point, each of the attribution variables store the delta between run i and run i-1 */
                  %if(&compute_stage_delta_flg. = Y) %then %do;
                     /* Check if there was a change in stage between run i and run i-1 */
                     if(tmp_stage_var_s&i. ne tmp_stage_var_s%eval(&i.-1)) then do;
                        /* We have to decompose the ECL delta in two portions:
                           - the delta due to change of stage
                           - the delta due to the model output
                           Case 1: Stage 1 --> Stage 2/3
                              we currently have
                              - ECL  = ECL_LIFETIME_S(i) - ECL_12M_S(i-1)
                              - ECL_12M = ECL_12M_S(i) - ECL_12M_S(i-1)
                              - ECL_LIFETIME = ECL_LIFETIME_S(i) - ECL_LIFETIME_S(i-1)
                              The delta due to change of stage is then
                                 - STAGE_DELTA = ECL - ECL_LIFETIME
                                 - ECL = ECL_LIFETIME
                           Case 2: Stage 2/3 --> Stage 1
                              we currently have
                              - ECL  = ECL_12M_S(i) - ECL_LIFETIME_S(i-1)
                              - ECL_12M = ECL_12M_S(i) - ECL_12M_S(i-1)
                              - ECL_LIFETIME = ECL_LIFETIME_S(i) - ECL_LIFETIME_S(i-1)
                              The delta due to change of stage is then
                                 - STAGE_DELTA = ECL - ECL_12M
                                 - ECL = ECL_12M
                        */
                        %if(&stage_attribution_method. = STAGE) %then %do;
                           /* Stage First: Stage Change -> Model Change. Use current stage var */
                           cum_stage_ecl_delta = sum(cum_stage_ecl_delta, &ecl_var., -ifn(tmp_stage_var_s&i. = "Stage 1", &ecl_12m_var., &ecl_lifetime_var.));
                           &ecl_var. = ifn(tmp_stage_var_s&i. = "Stage 1", &ecl_12m_var., &ecl_lifetime_var.);
                        %end;
                        %else %do;
                           /* Model First: Model Change -> Stage Change. Use previous stage var */
                           cum_stage_ecl_delta = sum(cum_stage_ecl_delta, &ecl_var., -ifn(tmp_stage_var_s%eval(&i.-1) = "Stage 1", &ecl_12m_var., &ecl_lifetime_var.));
                           &ecl_var. = ifn(tmp_stage_var_s%eval(&i.-1) = "Stage 1", &ecl_12m_var., &ecl_lifetime_var.);
                        %end;
                        /* Update all dependent variables */
                        %do j = 1 %to &tot_dependent_vars.;
                           tmp_cum_&j. = sum(tmp_cum_&j., &&dependent_var_&j.., -(&&dependent_expr_&j..));
                           &&dependent_var_&j.. = &&dependent_expr_&j..;
                        %end;
                     end;
                  %end;

                  /* Map the delta to the specified label */
                  movement_category = "%sysfunc(putn(&&run_group_no_&i.., z2.)). &&run_group_label_&i..";

                  /* Write the records if any of the attribution variable is non-zero */
                  if(0 %sysfunc(prxchange(s/(\w+)/or abs($1) > &epsilon./i, -1, &attribution_vars.))) then do;
                     /* Increment movement_id */
                     movement_id + 1;
                     /* Write the delta */
                     output;
                  end;

               %end; /* End attribution for factors that were run */

               /* Compute delta for the last ("Model" or "Other") bucket (<attribution_var> = sum(<attribution_var>_S99, - <attribution_var>_S<TotRunAttr>) */
               %do cnt = 1 %to &TotAttrVars.;
                  %scan(&attribution_vars., &cnt., %str( )) = sum(tmp_attrib_var_&cnt._s99, -tmp_attrib_var_&cnt._s&TotRunAttr.);
               %end;

               %if(&compute_stage_delta_flg. = Y) %then %do;
                  /* Check if there was a change in stage between run i and run i-1 */
                  if(tmp_stage_var_s99 ne tmp_stage_var_s&TotRunAttr.) then do;
                     %if(&stage_attribution_method. = STAGE) %then %do;
                        /* Stage First: Stage Change -> Model Change. Use current stage var */
                        cum_stage_ecl_delta = sum(cum_stage_ecl_delta, &ecl_var., -ifn(tmp_stage_var_s99 = "Stage 1", &ecl_12m_var., &ecl_lifetime_var.));
                        &ecl_var. = ifn(tmp_stage_var_s99 = "Stage 1", &ecl_12m_var., &ecl_lifetime_var.);
                     %end;
                     %else %do;
                        /* Model First: Model Change -> Stage Change. Use previous stage var */
                        cum_stage_ecl_delta = sum(cum_stage_ecl_delta, &ecl_var., -ifn(tmp_stage_var_s&TotRunAttr. = "Stage 1", &ecl_12m_var., &ecl_lifetime_var.));
                        &ecl_var. = ifn(tmp_stage_var_s&TotRunAttr. = "Stage 1", &ecl_12m_var., &ecl_lifetime_var.);
                     %end;
                     /* Update all dependent variables */
                     %do j = 1 %to &tot_dependent_vars.;
                        tmp_cum_&j. = sum(tmp_cum_&j., &&dependent_var_&j.., -(&&dependent_expr_&j..));
                        &&dependent_var_&j.. = &&dependent_expr_&j..;
                     %end;
                  end;
               %end;

               %if(&model_is_last_flg. = Y) %then %do;
                  /* Map the delta to the specified label for "Model" */
                  movement_category = "%sysfunc(putn(&model_no., z2.)). &model_label.";
               %end;
               %else %do;
                  /* Map the delta to the specified label for "Other" */
                  movement_category = "%sysfunc(putn(&other_no., z2.)). &other_label.";
               %end;

               /* Write the records if any of the attribution variable is non-zero */
               if(0 %sysfunc(prxchange(s/(\w+)/or abs($1) > &epsilon./i, -1, &attribution_vars.))) then do;
                  /* Increment movement_id */
                  movement_id + 1;
                  /* Write the delta */
                  output;
               end;

               %if(&compute_stage_delta_flg. = Y) %then %do;
                  /* Reset all numeric variables */
                  call missing(of &reset_vars. &attribution_vars.);
                  /* Set ECL = Cumulative sum of all the stage related deltas */
                  &ecl_var. = cum_stage_ecl_delta;
                  /* Update all dependent variables */
                  %do j = 1 %to &tot_dependent_vars.;
                     &&dependent_var_&j.. = tmp_cum_&j.;
                  %end;
                  /* Map the delta to the specified label for "Stage Allocation" */
                  movement_category = "%sysfunc(putn(&stage_allocation_no., z2.)). &stage_allocation_label.";
                  /* Write the records if any of the attribution variable is non-zero */
                  if(0 %sysfunc(prxchange(s/(\w+)/or abs($1) > &epsilon./i, -1, &attribution_vars.))) then do;
                     /* Increment movement_id */
                     movement_id + 1;
                     /* Write the delta */
                     output;
                  end;
               %end;

            end; /* if(orig_movement_id = 1) */
            else do; /* Non-Model related attribution (orig_movement_id = 2). See macro irmc_aggregate_mart_movements.sas for details */

               /* Compute delta for the Non-Model adjustments (<attribution_var> = sum(<attribution_var>_S99, - <attribution_var>_S0) */
               %do cnt = 1 %to &TotAttrVars.;
                  %scan(&attribution_vars., &cnt., %str( )) = sum(tmp_attrib_var_&cnt._s99, -tmp_attrib_var_&cnt._s0);
               %end;

               /* Non-Model related attribution */
               select(orig_movement_type_cd);
                  /* Management Adjustments delta */
                  when("QF_ADJ") do;
                     /* Map the delta to the specified label for the Q-Factor Adjustments */
                     movement_category = "%sysfunc(putn(&qf_adjustment_no., z2.)). &qf_adjustment_label.";
                  end;
                  when("CR_ADJ", "MGM_ACTION") do;
                     /* Map the delta to the specified label for the Manual Adjustments */
                     movement_category = "%sysfunc(putn(&manual_adjustment_no., z2.)). &manual_adjustment_label.";
                  end;
                  /* Individual Assessment delta */
                  when("IA_ADJ") do;
                     /* Map the delta to the specified label for the Individual Assessment */
                     movement_category = "%sysfunc(putn(&ia_no., z2.)). &ia_label.";
                  end;
                  /* Unknown movement type cd: Map to Other */
                  otherwise do;
                     /* Map the delta to the specified label for the Individual Assessment */
                     movement_category = "%sysfunc(putn(&other_no., z2.)). &other_label.";
                  end;
               end;

               /* Write the records if any of the attribution variable is non-zero */
               if(0 %sysfunc(prxchange(s/(\w+)/or abs($1) > &epsilon./i, -1, &attribution_vars.))) then do;
                  /* Increment movement_id */
                  movement_id + 1;
                  /* Write the delta */
                  output;
               end;

            end; /* Non-Model related attribution (orig_movement_id = 2) */

         end; /* if _curr_ */
         else do; /* Derecognition: instruments in the previous period that are not in the current period */
            /* Revert the OB movement value for the attribution variables from previous period result (<attribution_var> = -<attribution_var>_S0) */
            %do cnt = 1 %to &TotAttrVars.;
               %scan(&attribution_vars., &cnt., %str( )) = -tmp_attrib_var_&cnt._s0;
            %end;

            /* If the attribution template had attributionType="SynthDerecognition" and this is a synthetic instrument,
            put its derecognition into the SYNTHETIC_DERECOGNITION movement_type_cd. Otherwise, the derecognition goes into the
            DERECOGNITION movement_type_cd */

            /* Set the movement type and category (movement_desc is retained from above) */
            movement_type_cd = "DERECOGNITION";
            movement_type = "Derecognition";
            movement_category = "%sysfunc(putn(&derecognition_no., z2.)). &derecognition_label.";

            %if "&synth_derecognition_no." ne "" %then %do;
               if synthetic_instrument_flg = "Y" then do;
                  /* Set the movement type and category (movement_desc is retained from above) */
                  movement_type_cd = "SYNTHETIC_DERECOGNITION";
                  movement_type = "Synthetic Derecognition";
                  movement_category = "%sysfunc(putn(&synth_derecognition_no., z2.)). &synth_derecognition_label.";
               end;
            %end;

            /* Increment movement_id */
            movement_id + 1;
            /* Write Opening Balance record */
            output;

         end;
      end; /* if _prev_ */
      else do;/* The instrument exists only in the current period */

         /* Set values for the classification Variables hash to the current period */
         class_var_hash = class_var_hash_curr;

         %if(&compute_stage_delta_flg. = Y) %then %do;
            &stage_var. = tmp_stage_var_s99;
         %end;

         /* Set values for the attribution variables from current period result (<attribution_var> = <attribution_var>_S99) */
         %do cnt = 1 %to &TotAttrVars.;
            %scan(&attribution_vars., &cnt., %str( )) = tmp_attrib_var_&cnt._s99;
         %end;

         /* If the attribution template had attributionType="SynthOrigination" and this is a synthetic instrument,
         put its origination into the SYNTHETIC_ORIGINATION movement_type_cd. Otherwise, the origination goes into the
         ORIGINATION movement_type_cd */

         /* Set the movement type and category (movement_desc is retained from above) */
         movement_type_cd = "ORIGINATION";
         movement_type = "Origination";
         movement_category = "%sysfunc(putn(&origination_no., z2.)). &origination_label.";


         %if "&synth_origination_no." ne "" %then %do;
            if synthetic_instrument_flg = "Y" then do;
               /* Set the movement type and category (movement_desc is retained from above) */
               movement_type_cd = "SYNTHETIC_ORIGINATION";
               movement_type = "Synthetic Origination";
               movement_category = "%sysfunc(putn(&synth_origination_no., z2.)). &synth_origination_label.";
            end;
         %end;

         /* Increment movement_id */
         movement_id + 1;
         /* Write New Origination record */
         output;

      end; /* The instrument exists only in the current period */

   run;

   /* Add the classification variables onto the results using the classification variables lookup */
   data &outCasLibref.."&outAttributionResults."n (drop=_rc_ class_var_hash promote=yes);
      set &outCasLibref.."&outAttributionResults."n;
      if _N_=0 then
         set &outCasLibref..class_vars_lookup_table;

      if _N_=1 then do;
         declare hash hClassVars(dataset: "&outCasLibref..class_vars_lookup_table");
         hClassVars.defineKey("class_var_hash");
         hClassVars.defineData(&classification_vars_csv_qtd.);
         hClassVars.defineDone();
      end;

      _rc_=hClassVars.find();
   run;

   /* Cleanup some remaining CAS tables, if we aren't keeping CAS data */
   %if &keepModelData. ne Y %then %do;

      %do i = 1 %to &TotRunAttr.;

         %core_cas_drop_table(cas_session_name = &casSessionName.
                             , cas_libref = &outModelCaslib.
                             , cas_table = &&attrib_result_&i..);

      %end;

      %core_cas_drop_table(cas_session_name = &casSessionName.
                        , cas_libref = &outModelCaslib.
                        , cas_table = &prev_result_table.);

      %core_cas_drop_table(cas_session_name = &casSessionName.
                        , cas_libref = &outModelCaslib.
                        , cas_table = &curr_result_table.);
   %end;

   libname &outCasLibref. clear;

%mend;
