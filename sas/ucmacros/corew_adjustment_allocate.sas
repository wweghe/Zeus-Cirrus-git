%macro corew_adjustment_allocate(
                                 ds_in_allocation_config =
                                 , ds_in_aggregation_config =
                                 , ds_in_dependency_config =
                                 , ds_in_map_movement =
                                 , dr_libref = WORK
                                 , ds_out_alloc_rules_summary = WORK.alloc_rules_summary
                                 , ds_out_modified_delta = WORK.alloc_modified_delta
                                 , ds_out_alloc = WORK.tmp_out_alloc
                                 , ds_out_rules_info = WORK.tmp_rules_info
                                 , ds_out_rules_data = WORK.tmp_rules_data
                                 , ds_out_analysis_data_view = WORK.tmp_analysis_data_view
                                 , ds_out_data_def_link_instances = WORK.tmp_datadef_link_instances
                                 , ds_out_data_def_summary = WORK.tmp_datadef_summary
                                 , ds_out_data_def_columns = WORK.tmp_datadef_columns
                                 , ovr_primary_key =
                                 , ovr_rule_table_libref = WORK
                                 , ovr_rule_table =
                                 , solution =
                                 , epsilon = 1e-10
                                 );

   %local
      ds_in_mart
      ruleSet_type
      schema_name
      schema_version
      reportmart_group_id
      primary_key
      movement_id
      max_movement_id
      filterable_vars
      append_where_clause
      tot_aggr_vars
      dt_fmts
      dttm_fmts
      tm_fmts
      i

      data_definition_key

      accessToken
      httpSuccess
      responseStatus

   ;
   /* Reset syscc variable */
   %let syscc = 0;


   /* *********************************************** */
   /*         Read config                             */
   /* *********************************************** */

   /* Make sure we have any options to process */
   %if(%rsk_attrn(&ds_in_allocation_config., nobs) < 2) %then %do;
      %put ERROR: Input dataset &ds_in_allocation_config. does not contain at least 2 options. analysis_data_key and ruleset_key are required. Skipping execution..;
      %abort;
   %end;

   data _null_;
      set &ds_in_allocation_config.;
      call symputx(CONFIG_NAME, CONFIG_VALUE, "L");
   run;

   %put _user_;

   /* *********************************************** */
   /*         Get rule set                            */
   /* *********************************************** */

   %if not %symexist(ds_out_rules_info) %then %do;
      %let ds_out_rules_info = &dr_libref..tmp_rules_info;
   %end;
   %if not %symexist(ds_out_rules_data) %then %do;
      %let ds_out_rules_data = &dr_libref..tmp_rules_data;
   %end;

   %if %sysevalf(%superq(ds_out_rules_info) eq, boolean) %then %do;
      %let ds_out_rules_info = &dr_libref..tmp_rules_info;
   %end;
   %if %sysevalf(%superq(ds_out_rules_data) eq, boolean) %then %do;
      %let ds_out_rules_data = &dr_libref..tmp_rules_data;
   %end;

   /* Handle rule data override */
   %if %sysevalf(%superq(ovr_rule_table) ne, boolean) %then %do;
      %let ds_out_rules_data = &ovr_rule_table_libref..&ovr_rule_table.;
      
      /* Make sure we have any rules to process */
      %if(%rsk_attrn(&ds_out_rules_data., nobs) = 0) %then %do;
         %put WARNING: Input dataset &ds_out_rules_data. is empty. Skipping execution..;
         %abort;
      %end;
   %end;
   %else %do;
      %let accessToken=;
      %let httpSuccess=;
      %let responseStatus=;

      %core_rest_get_rule_set(
                              ruleSetId = &ruleset_key
                              , outds_ruleSetInfo = &ds_out_rules_info.
                              , outds_ruleSetData = &ds_out_rules_data.
                              , solution = &solution
                              , outVarToken = accessToken
                              , outSuccess = httpSuccess
                              , outResponseStatus = responseStatus
                              );

      /* Exit in case of errors */
      %if(not &httpSuccess. or not %rsk_dsexist(&ds_out_rules_data.)) %then %do;
         %put ERROR: Cannnot get ruleset data for the ruleSetId = &ruleset_key. Skipping execution.;
         %abort;
      %end;

      /* *********************************************** */
      /*         Validate RuleSet Configuration          */
      /* *********************************************** */

      /* Make sure we have any rules to process */
      %if(%rsk_attrn(&ds_out_rules_data., nobs) = 0) %then %do;
         %put WARNING: Input dataset &ds_out_rules_data. is empty. Skipping execution..;
         %abort;
      %end;

      /* Get the ruleset type */
      data _null_;
         set &ds_out_rules_data.(obs = 1);
         call symputx("ruleSet_type", ruleSetType, "L");
      run;

      /* Make sure it is an allocation/Qfactor rule set */
      %if(%sysevalf(%superq(ruleSet_type) ne ALLOCATION_RULES, boolean)
         and %sysevalf(%superq(ruleSet_type) ne QFACTOR_RULES, boolean)
         ) %then %do;
         %put ERROR: Only Allocation/Q-Factor Rule Sets are supported.;
         %abort;
      %end;

   %end;
   /* *********************************************** */
   /*         Validate Primary Key Override           */
   /* *********************************************** */

   %if not %symexist(analysis_data_key) %then %do;
      %put ERROR: The config does not contain analysis_data_key.;
      %abort;
   %end;

   %if %sysevalf(%superq(ovr_primary_key) ne, boolean) %then %do;
         %let primary_key = &ovr_primary_key.;
   %end;
   %else %do;
      %let accessToken=;
      %core_rest_get_link_instances(
                                 objectType = analysisData
                                 , objectKey = &analysis_data_key.
                                 , linkType = analysisData_dataDefinition
                                 , outds = &ds_out_data_def_link_instances.
                                 , solution = &solution
                                 , outVarToken = accessToken
                                 , outSuccess = httpSuccess
                                 , outResponseStatus = responseStatus
                                 );

      %if(not &httpSuccess. or not %rsk_dsexist(&ds_out_data_def_link_instances.)) %then %do;
         %put ERROR: Cannot get analysisData_dataDefinition link for analysis data: &analysis_data_key..;
         %abort;
      %end;

      data _null_;
         set &ds_out_data_def_link_instances.;
         call symputx("data_definition_key", businessObject2, "L");
      run;

      %if %sysevalf(%superq(data_definition_key) eq, boolean) %then %do;
         %put ERROR: Cannot get data definition key for analysis data: &analysis_data_key..;
         %abort;
      %end;
      %core_rest_get_data_def(
                              key = &data_definition_key.
                              , outds = &ds_out_data_def_summary.
                              , outds_columns = &ds_out_data_def_columns.
                              , outVarToken = accessToken
                              , outSuccess = httpSuccess
                              , outResponseStatus = responseStatus
                              , debug = true
                              );
      %if(not &httpSuccess. or not %rsk_dsexist(&ds_out_data_def_columns.)) %then %do;
         %put ERROR: Cannot get data definition columns data set for: &data_definition_key..;
         %abort;
      %end;

      proc sql noprint;
         select name into :primary_key separated by ' '
         from &ds_out_data_def_columns.
         where primaryKeyFlag="true"
      ;
      quit;

      %if %sysevalf(%superq(primary_key) eq, boolean) %then %do;
         %put ERROR: No primary key columns defined for the data definition: &data_definition_key..;
         %abort;
      %end;
   %end;


   /* *********************************************** */
   /*         Get the analysis data view              */
   /* *********************************************** */

   %if not %symexist(ds_out_analysis_data_view) %then %do;
      %let ds_out_analysis_data_view = &dr_libref..tmp_analysis_data_view;
   %end;
   %if %sysevalf(%superq(ds_out_analysis_data_view) eq, boolean) %then %do;
      %let ds_out_analysis_data_view = &dr_libref..tmp_analysis_data_view;
   %end;

   %let accessToken=;
   %let httpSuccess=;
   %let responseStatus=;

   %core_rest_get_analysis_data_view(key = &analysis_data_key.
                                    , outview = &ds_out_analysis_data_view.
                                    , solution = &solution
                                    , outVarToken = accessToken
                                    , outSuccess = httpSuccess
                                    , outResponseStatus = responseStatus
                                    );

   /* Exit in case of errors */
   %if(not &httpSuccess. or not %rsk_dsexist(&ds_out_analysis_data_view.)) %then %do;
      %put ERROR: Cannot create view &ds_out_analysis_data_view. for input analysis data: key = &analysis_data_key.. Skipping execution.;
      %abort;
   %end;

   %let ds_in_mart = &ds_out_analysis_data_view.;

   /* *********************************************** */
   /*         Create empty output tables              */
   /* *********************************************** */

   /* Allocation summary */
   data &ds_out_alloc_rules_summary.;
      attrib
         ruleSetKey                 length = $200      label = "Ruleset Key"
         rule_id                    length = $32.      label = "Rule Id"
         rule_desc                  length = $4096.    label = "Rule Description"
         rule_condition             length = $10000.   label = "Rule Condition"
         adjustment_value           length = $32000.   label = "Adjustment Value"
         measure_var_nm             length = $150.     label = "Measure Variable Name"
         adjustment_type            length = $150.     label = "Adjustment Type"
         allocation_method          length = $150.     label = "Allocation Method"
         aggregation_method         length = $32.      label = "Aggregation Method"
         weight_var_nm              length = $150.     label = "Weight Variable Name"
         weighted_aggregation_flg   length = $3.       label = "Weighted Aggregation Flag"
         affected_row_cnt           length = 8.        label = "Affected Row Count"
         total_row_cnt              length = 8.        label = "Total Row Count"
      ;
      stop;
   run;

   /* Modified Delta */
   data &ds_out_modified_delta.;
      attrib
         %rsk_get_attrib_def(ds_in = &ds_in_mart.)
      ;
      stop;
   run;


   /* *********************************************** */
   /*       Retrieve variable dependency rules        */
   /* *********************************************** */

   data WORK.var_dependency_config;
      attrib
         SCHEMA_NAME          length = $150.
         SCHEMA_VERSION       length = $50.
         TRIGGER_VAR_NAME     length = $32.
         ORDER_NO             length = 8.
         DEPENDENT_VAR_NAME   length = $32.
         EXPRESSION_TXT       length = $4096.
      ;
      stop;
   run;

   %if %symexist(ds_in_dependency_config) %then %do;
      %if %sysevalf(%superq(ds_in_dependency_config) ne, boolean) %then %do;
         proc append base=WORK.var_dependency_config data=&ds_in_dependency_config. force;
         run;
      %end;
   %end;

   /* *********************************************** */
   /*         Get Movement Information                */
   /* *********************************************** */

   /* Check if movement_id exists in the input table */

   %if(%core_get_vartype(&ds_in_mart., movement_id) = N) %then %do;
      /* Get the current movement version */
      %let movement_id = .;
      proc sql noprint;
         select
            max(movement_id)
               into :movement_id
         from
            &ds_in_mart.
         ;
      quit;
   %end;
   %else %do;
      %put WARNING: There are no numerical movement_id column in &ds_in_mart..;
      %let movement_id = 1;
   %end;


      %if(%sysevalf(%superq(movement_id) =., boolean)) %then %do;
         %put ERROR: There are no records with not missing movement_id in &ds_in_mart..;
         %abort;
      %end;

      data WORK.__aggregation_config__;
         attrib
            SCHEMA_NAME          length = $150.
            SCHEMA_VERSION       length = $50.
            VARIABLE_NAME        length = $32.
            AGGREGATION_METHOD   length = $32.
            WEIGHT_VAR           length = $200.
         ;
         stop;
      run;

      %if %symexist(ds_in_aggregation_config) %then %do;
         %if %sysevalf(%superq(ds_in_aggregation_config) ne, boolean) %then %do;
            proc append base=WORK.__aggregation_config__ data=&ds_in_aggregation_config. nowarn force;
            run;
         %end;
      %end;
      %else %do;
         %put WARNING: There is no aggregation config provided.;
      %end;

      /* Retrieve mart aggregation rules */
      data _null_;
         set WORK.__aggregation_config__;
         call symputx(cats("aggr_var_name_", _N_), variable_name, "L");
      run;

      %let tot_aggr_vars = %rsk_attrn(__aggregation_config__, nobs);

      /* If this is not the first adjustment ever made to this mart table and there are info available about how to aggregate the mart variables */
      %if(&movement_id. > 1 and &tot_aggr_vars. > 0) %then %do;

         /* Aggregate all data by the primary key */
         proc summary data = &ds_in_mart. missing nway;
            /* Remove movement_id from the primary key so we get aggregated results across all adjustments */
            class
               %sysfunc(prxchange(s/movement_id//i, -1, &primary_key.))
            ;
            var
               %do i = 1 %to &tot_aggr_vars.;
                  &&aggr_var_name_&i..
               %end;
            ;
            output
               out = _tmp_mart_aggr_ (drop = _type_ _freq_)
               sum =
            ;
         run;

         /* Convert list of primarykey variables to comma separated quoted list of variables (needed for lookup later on) */
         %let var_list = %sysfunc(prxchange(s/movement_id//i, -1, &primary_key.));

         /* Recreate a full mart with all columns */
         data aggregated_input_mart;
            merge &ds_in_mart.(in=a where = (movement_id = 1)) _tmp_mart_aggr_(in=b);
            by &var_list.;
            if a then output;
         run;

         %let ds_in_mart = aggregated_input_mart;
         /* Redirect macro variable */
         
      %end; 
      
      %if (&movement_id. > 1 and &tot_aggr_vars. = 0) %then %do;
         %put NOTE: No aggregation vars were provided so will use the original base values to apply adjustments without aggregating the existing movements; 
         data aggregated_input_mart;
            set &ds_in_mart.(where = (movement_id = 1));
         run;
         
         %let ds_in_mart = aggregated_input_mart;
      %end;
   /* *********************************************** */
   /*           Run Allocation Rules                  */
   /* *********************************************** */

   %let compress_option = %sysfunc(getoption(compress));
   options compress = yes;

   /* Run Allocation Rules */
   %core_apply_allocation_rules(ds_in = &ds_in_mart.
                               , rule_def_ds = &ds_out_rules_data.
                               , ds_in_var_dependency = var_dependency_config
                               , exclude_filter_vars = ruleSetKey
                               , custom_filter_var = filter_exp
                               , ds_out = &ds_out_alloc.
                               , ds_out_audit = modified_delta
                               , ds_out_rule_summary = rule_summary
                               );

   /* Exit in case of errors */
   %if(&syserr. > 4 or &syscc. > 4) %then
      %abort;

   /* Append rules_summary to the output summary table */
   proc append base = &ds_out_alloc_rules_summary.
               data = rule_summary nowarn force;
   run;

   /* *********************************************** */
   /*  Post-processing: create delta movements table  */
   /* *********************************************** */

      /* Sort data for transposing */
      proc sort
         data = modified_delta
                  (keep = &primary_key.
                   measure_name
                   measure_var_type
                   sequence_no
                   current_txt_value
                   current_value
                   previous_value
                   previous_txt_value
                   delta_value
                   rule_id
                   rule_desc
                   )
         out = modified_delta_srt
         ;
         /* process only numeric variables with non-zero delta */
         where
            measure_var_type = "N"
            and delta_value is not missing
            and abs(delta_value) > &epsilon.
         ;
         by
            &primary_key
            sequence_no
         ;
      run;

      proc sort data=modified_delta;
         by &primary_key. SEQUENCE_NO;
      run;

      /* Check if we have any records to transpose */
      %if(%rsk_attrn(modified_delta_srt, nobs)) %then %do;
         /* Transpose the delta values */
         proc transpose data = modified_delta_srt
                        out = modified_delta_trsp (drop = _name_);
            by
               &primary_key.
               sequence_no
            ;
            id measure_name;
            var delta_value;
         run;
      %end;
      %else %do;
         /* There is nothing to transpose, just create an empty table */
         data modified_delta_trsp;
            set modified_delta_srt (keep = &primary_key. sequence_no);
            stop;
         run;
      %end;

      /* Create list of date/time/datetime formats - needed to know which numeric vars to keep */
      %let dt_fmts = %rsk_get_dtm_formats(type = date);
      %let dttm_fmts = %rsk_get_dtm_formats(type = datetime);
      %let tm_fmts = %rsk_get_dtm_formats(type = time);


      /* Finalize Delta Movement table */
      %let max_movement_id = 0;
      data &ds_out_modified_delta.;
         attrib
            %rsk_get_attrib_def(ds_in = &ds_in_mart.)

            %if(%core_get_vartype(&ds_in_mart., RULE_ID) =) %then %do;
               rule_id                 length = $32.      label = "Rule Id"
            %end;
            %if(%core_get_vartype(&ds_in_mart., RULE_DESC) =) %then %do;
               rule_desc               length = $4096.    label = "Rule Description"
            %end;
            %if(%core_get_vartype(&ds_in_mart., MOVEMENT_TYPE_CD) =) %then %do;
               MOVEMENT_TYPE_CD       length = $32.       label = "Movement Type Code"
            %end;
            %if(%core_get_vartype(&ds_in_mart., MOVEMENT_TYPE) =) %then %do;
               MOVEMENT_TYPE          length = $100.      label = "Movement Type"
            %end;
            %if(%core_get_vartype(&ds_in_mart., MOVEMENT_CATEGORY) =) %then %do;
               MOVEMENT_CATEGORY      length = $100.      label = "Movement Category"
            %end;
         ;

         merge modified_delta_trsp(in=a) modified_delta(in=b keep=&primary_key. RULE_ID RULE_DESC SEQUENCE_NO %rsk_getvarlist(&ds_in_mart., type = C) %rsk_getvarlist(&ds_in_mart., format = &dt_fmts.|&dttm_fmts.|&tm_fmts.)) end = __last__;
         by &primary_key. sequence_no;
         drop
            sequence_no
            /* table_id
            project_id */
            __max_movement_id__
            __new_movement_id__
            _rc_
         ;
         retain __max_movement_id__ &movement_id.;

         if _N_ = 1 then do;
            /* Declare lookup for retrieving the movement type and category */
            declare hash hMvmt(dataset: "&ds_in_map_movement.");
            hMvmt.defineKey("movement_type_cd");
            hMvmt.definedata("movement_type", "movement_category");
            hMvmt.defineDone();

            /* Declare lookup used to remap the combination (movement_id, sequence_no) -> __new_movement_id__. This is needed to preserve uniqueness of the primary key */
            declare hash hMvmtId;
            hMvmtId = _new_ hash();
            hMvmtId.defineKey("movement_id", "sequence_no");
            hMvmtId.defineData("__new_movement_id__");
            hMvmtId.defineDone();
         end;

         /* Get the new movement_id */
         call missing(__new_movement_id__);
         if(hMvmtId.find() ne 0) then do;
            /* No match was found. Compute the next movement id */
            __max_movement_id__ + 1;
            __new_movement_id__ = __max_movement_id__;
            /* Push the new mapped value to the loopkup */
            hMvmtId.replace();
         end;

         /* Set Run-specific attributes */
         analysis_run_id = "&analysis_run_id.";
         /* Keep the initial analysis name for all the adjustments so they can be filtered together in VA by Analysis Name */

         /* Set the movement id */
         movement_id = __new_movement_id__;

         /* Override processed_dttm column if exists */
         %if(%rsk_varexist(&ds_in_mart., processed_dttm)) %then %do;
            processed_dttm = "%sysfunc(datetime(), datetime21.)"dt;
         %end;

         /* Set the movement code description */
         movement_type_cd = "&movement_type_cd.";
         movement_desc = catx(". ", put(movement_id, z2.), "&movement_desc.");
         /* Lookup movement type and category */
         _rc_ = hMvmt.find();

         if __last__ then
            call symputx("max_movement_id", __max_movement_id__, "L");

         if a then output;
      run;

      proc contents data= &ds_out_modified_delta. out=out_content noprint;
      quit;

      proc sql noprint;
         select name into :other_sel_vars separated by ','
         from out_content
         where strip(upcase(name)) not in (select strip(upcase(variable_name)) as name from WORK.__aggregation_config__ );
      quit;

      /* Aggregate all data by the primary key */
      proc sql;
         create table &ds_out_modified_delta. as
         select &other_sel_vars.
               %do i = 1 %to &tot_aggr_vars.;
                 , SUM(&&aggr_var_name_&i..) as &&aggr_var_name_&i..
               %end;
         from &ds_out_modified_delta.
         group by &other_sel_vars.;
      quit;

      options compress = &compress_option.;
%mend;
