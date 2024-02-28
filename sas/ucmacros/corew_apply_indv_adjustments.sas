%macro corew_apply_indv_adjustments(sourceSystemCd =
                                    , solution =
                                    , host =
                                    , port =
                                    , logonHost =
                                    , logonPort =
                                    , username =
                                    , password =
                                    , authMethod = bearer
                                    , client_id =
                                    , client_secret =
                                    , ds_in_key =
                                    , ds_in =                     /* Dataset of the analysis data instance to which adjustments will be applied */
                                    , ds_in_adjustments =         /* Dataset of the analysis data instance containing the adjustments */
                                    , ds_out =                    /* Output dataset containing adjustment records */
                                    , ds_out_ia_exceptions =      /* Output dataset containing adjustment records that were not matched to input data records */
                                    , exclude_vars_list =         /* Vars to exclude from 'ds_in_adjustments' */
                                    , movement_desc =             /* Movement description for VA report (single description for all adjustments so they can be grouped together in the report) */
                                    , analysis_run_key =          /* Analysis Run key for output table */
                                    , ds_in_map_movement =
                                    , ds_in_var_dependency =
                                    , ds_in_aggregation_cfg =
                                    /* next two below params will only be used if ds_in_key cannot be provided. */
                                    , ds_in_datastore_cfg =
                                    , reportmart_group_id =
                                    , epsilon = 1e-10
                                    , outVarToken = accessToken
                                    , ds_in_adj_key =             /* Key of the dataset of the analysis data instance containing the adjustments */
                                    , outModelCasLib =
                                    , casSessionName = casauto
                                    , debug = false
                                    );

   %local
      merge_by_vars
      movement_id
      max_movement_id
      work_path
      current_date
      current_time
      exception_file_name
      i
      data_definition_key
      httpSuccess
      responseStatus
      schema_name
      schema_version
      primary_key
      ds_out_data_def_link_instances
      targ_folder
      attachmentName
      attachmentDesc
      outCasLibref
      TOT_AGGR_VARS
      aggr_var_round
   ;

   %let outCasLibref = %rsk_get_unique_ref(type = lib, engine = cas, args = caslib="&outModelCaslib." sessref=&casSessionName.);

   /* Reset syscc variable */
   %let syscc = 0;

   /********************************/
   /* get primary key for CRD data */
   /********************************/
   %let primary_key=;
   %let schema_name=;
   %let schema_version=;

   %if (%sysevalf(%superq(ds_in_key) ne, boolean)) %then %do;
      %let httpSuccess=;
      %let responseStatus=;
      %let accessToken=;
      %core_rest_get_link_instances(
                                 objectType = analysisData
                                 , server = riskCirrusObjects
                                 , objectKey = &ds_in_key.
                                 , linkType = analysisData_dataDefinition
                                 , outds = _data_def_link_instances_
                                 , solution = &solution.
                                 , outVarToken = accessToken
                                 , outSuccess = httpSuccess
                                 , outResponseStatus = responseStatus
                                 );

      %if(not &httpSuccess. or not %rsk_dsexist(_data_def_link_instances_)) %then %do;
         %put ERROR: Cannot get analysisData_dataDefinition link for analysis data: &analysis_data_key..;
         %abort;
      %end;

      data _null_;
         set _data_def_link_instances_;
         call symputx("data_definition_key", businessObject2, "L");
      run;

      %if %sysevalf(%superq(data_definition_key) eq, boolean) %then %do;
         %put ERROR: Cannot get data definition key for analysis data: &analysis_data_key..;
         %abort;
      %end;
      %core_rest_get_data_def(
                              key = &data_definition_key.
                              , outds = _data_def_summary_
                              , outds_columns = _data_def_columns_
                              , outVarToken = accessToken
                              , outSuccess = httpSuccess
                              , outResponseStatus = responseStatus
                              , debug = true
                              );
      %if(not &httpSuccess. or not %rsk_dsexist(_data_def_columns_)) %then %do;
         %put ERROR: Cannot get data definition columns data set for: &data_definition_key..;
         %abort;
      %end;

      proc sql noprint;
         select name into 
               :primary_key separated by ' '
         from _data_def_columns_
         where primaryKeyFlag="true";

         select distinct schemaName, schemaVersion into 
               :schema_name,
               :schema_version
         from _data_def_summary_
      ;
      quit;
   %end;
   %else %do;
            data _null_;
               set &ds_in_datastore_cfg.;
               where lowcase(datastoreGroupId) = lowcase("&reportmart_group_id.")
               ;
               call symputx("schema_name", schemaName, "L");
               call symputx("schema_version", schemaVersion, "L");
               call symputx("primary_key", primaryKey, "L");
            run;
         %end;

   %if %sysevalf(%superq(primary_key) eq, boolean) %then %do;
      %put ERROR: No primary key columns defined for the data definition: &data_definition_key..;
      %abort;
   %end;

   /* Get the current movement version */
   %let movement_id = .;
   proc fedsql sessref=&casSessionName.;
      create table "&outModelCaslib.".ia_max_movement {options replace=true} as
      select max(movement_id) as movement_id
      from "&outModelCaslib.".&ds_in.
      ;
   quit;
   proc sql noprint;
      select
         movement_id into :movement_id
      from &outCasLibref..ia_max_movement;
   ;
   quit;

   %if(%sysevalf(%superq(movement_id) =., boolean)) %then %do;
      %put ERROR: There are no data for the selected Credit Risk Detail.;
      %return;
   %end;

   %let tot_aggr_vars = 0;

   %if (%rsk_dsexist(&ds_in_aggregation_cfg.)) %then %do;
      %if %sysevalf(%superq(ds_in_aggregation_cfg) ne, boolean) %then %do;

         data aggregation_config;
            set &ds_in_aggregation_cfg.;
            where
               lowcase(schema_name) = lowcase("&schema_name.")
               and lowcase(schema_version) = lowcase("&schema_version.")
            ;
            call symputx(cats("aggr_var_name_", _N_), variable_name, "L");
         run;

         %let tot_aggr_vars = %rsk_attrn(aggregation_config, nobs);
      %end;
   %end;
   %else %do;
            %put WARNING: There is no aggregation config provided.;
         %end;

   /* If this is not the first adjustment ever made to this mart table and there are info available about how to aggregate the mart variables */
   %if(&movement_id. > 1 and &tot_aggr_vars. > 0) %then %do;

      /* Aggregate all data by the primary key */
      proc summary data = &outCasLibref..&ds_in. missing nway;
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

      %core_cas_upload_and_convert(inLib = work
                                 , inTable = _tmp_mart_aggr_
                                 , encoding_adjustment = Y
                                 , outLib = &outModelCaslib.
                                 , outTable = _tmp_mart_aggr_
                                 , casSessionName = &casSessionName.
                                 );

      /* Convert list of primarykey variables to comma separated quoted list of variables (needed for lookup later on) */
      %let quoted_var_list = %sysfunc(prxchange(s/movement_id//i, -1, &primary_key.));
      %let quoted_var_list = %sysfunc(prxchange(s/\s+/%str(, )/i, -1, &quoted_var_list.));
      %let quoted_var_list = %sysfunc(prxchange(s/(\w+)/"$1"/i, -1, %bquote(&quoted_var_list.)));

      /* round aggregated vars after summary to ensure correct comparison when running step 'corew_apply_indv_adjs_rules' */
      %let aggr_var_round=;
      %do i = 1 %to &tot_aggr_vars.;
         %let aggr_var_round= &aggr_var_round. &&aggr_var_name_&i.. = round(&&aggr_var_name_&i..,&epsilon.)%str(;);
      %end;
      /* Recreate a full mart with all columns */
      data &outCasLibref..aggregated_input_mart;
         set &outCasLibref..&ds_in. (where = (movement_id = 1));
         if _N_ = 1 then do;
            declare hash hAggrMeas(dataset: "&outCasLibref.._tmp_mart_aggr_");
            hAggrMeas.defineKey(&quoted_var_list.);
            hAggrMeas.defineData(all: "yes");
            hAggrMeas.defineDone();
         end;
         drop _rc_;
         _rc_ = hAggrMeas.find();

         &aggr_var_round.;
      run;

      /* Redirect macro variable */
      %let ds_in = aggregated_input_mart;
   %end; 
   
   %if (&movement_id. > 1 and &tot_aggr_vars. = 0) %then %do;
      %put NOTE: No aggregation vars were provided so will use the original base values to apply adjustments without aggregating the existing movements; 
      data &outCasLibref..aggregated_input_mart;
         set &outCasLibref..&ds_in.(where = (movement_id = 1));
      run;

      /* Redirect macro variable */
      %let ds_in = aggregated_input_mart;
   %end;

   /* Create list of variables to merge by */
   %let merge_by_vars=;
   %do i = 1 %to %sysfunc(countw(&primary_key., %str( )));
      /* Exclude any primary key variables that are not found in the adjustment dataset */
      %let curr_var = %scan(&primary_key., &i., %str( ));
      %if(%rsk_varexist(&outCasLibref..&ds_in_adjustments., &curr_var.)) %then %do;
         %let merge_by_vars = &merge_by_vars. &curr_var.;
      %end;
   %end;

   /* Run Allocation Rules */
   %corew_apply_indv_adjs_rules(ds_in = &ds_in.
                               , ds_in_adj = &ds_in_adjustments.
                               , epsilon = &epsilon.
                               , merge_by_vars = &merge_by_vars.
                               %if(%rsk_dsexist(&ds_in_var_dependency.)) %then %do;
                                  , ds_in_var_dependency = &ds_in_var_dependency.
                               %end;
                               , exclude_vars_list = &exclude_vars_list.
                               , ds_out_delta = modified_delta
                               , ds_out_exceptions = &ds_out_ia_exceptions.
                               , outModelCasLib = &outModelCasLib.
                               , casSessionName = &casSessionName.
                               );

   /* Exit in case of errors */
   %if(&syserr. > 4 or &syscc. > 4) %then
      %return;

   /* *********************************************** */
   /*  Attach the IA exceptions table (if not empty)  */
   /* *********************************************** */

   %if(%rsk_getattr(&outCasLibref..&ds_out_ia_exceptions., NOBS) > 0) %then %do;

      %let targ_folder=/tmp;
      %if %sysfunc(libref(rptlib))=0 %then %do;
         %let targ_folder=%sysfunc(pathname(rptlib));
         /*Remove any quotes or parenthesys */
         %let targ_folder = %sysfunc(translate(%superq(targ_folder), %str( ), %str(""()'')));
      %end;
      %else %do;
            %if %symexist(__CORE_AR_DIR__) %then %do;
               %let targ_folder = &__CORE_AR_DIR__.;
            %end;
         %end;

      %let current_date = %sysfunc(date(), DATE9.);
      %let current_time = %sysfunc(tranwrd(%sysfunc(time(), hhmm8.2), :, .));
      %let exception_file_name = IA_Exceptions_&current_date._&current_time..xlsx;

      /* Export IA exceptions table to excel */
      proc export
         data = &outCasLibref..&ds_out_ia_exceptions.
         dbms = xlsx
         outfile = "&targ_folder./&exception_file_name."
         replace;
      run;

      %let attachmentName = &exception_file_name.;
      %let attachmentDesc = IA_Exceptions_&current_date._&current_time.;

      %if (&syscc. > 4) %then %do;
         %let attachmentName = ERROR - &attachmentName.;
         %let attachmentDesc = ERROR - &attachmentDesc.;
      %end;

      /*********************************/
      /* Attach IA Exceptions Report to the Analysis */
      /*********************************/
      %core_rest_create_file_attachment(server = riskCirrusObjects
                                       , solution = &solution.
                                       , host = &host.
                                       , port = &port.
                                       , logonHost = &logonHost.
                                       , logonPort = &logonPort.
                                       , username = &username.
                                       , password = &password.
                                       , authMethod = &authMethod.
                                       , client_id = &client_id.
                                       , client_secret = &client_secret.
                                       , objectKey = &analysis_run_key.
                                       , objectType = analysisRuns
                                       , file = &targ_folder./&exception_file_name.
                                       , attachmentSourceSystemCd = &sourceSystemCd.
                                       , attachmentName = &attachmentName.
                                       , attachmentDisplayName = &attachmentName.
                                       , attachmentDesc = &attachmentDesc.
                                       , attachmentGrouping = report_attachments
                                       , replace = Y
                                       , outds = object_file_attachments
                                       , outVarToken = &outVarToken.
                                       , outSuccess = httpSuccess
                                       , outResponseStatus = responseStatus
                                       , debug = &debug.
                                       );


      /* Write a warning to the log about the number of IA exceptions */
      %put WARNING: There were %rsk_getattr(&outCasLibref..&ds_out_ia_exceptions., NOBS) records from the individual adjustment table that did not match any records on the credit risk detail table.;
      %put These exceptions will not be processed.  These exceptions can be viewed in the IA_Exceptions attachment on the Analysis Run.;

   %end;

   /* *********************************************** */
   /*  Post-processing: create delta movements table  */
   /* *********************************************** */

   /* Finalize Delta Movement table */
   data &outCasLibref..&ds_out.;

      set &outCasLibref..modified_delta end = __last__;

      drop
         _rc_
      ;

      if _N_ = 0 then
         set &outCasLibref..&ds_in_map_movement.(keep = MOVEMENT_TYPE_CD MOVEMENT_TYPE MOVEMENT_CATEGORY);

      if _N_ = 1 then do;

         /* Declare lookup for retrieving the movement type and category */
         declare hash hMvmt(dataset: "&outCasLibref..&ds_in_map_movement.");
         hMvmt.defineKey("movement_type_cd");
         hMvmt.definedata("movement_type", "movement_category");
         hMvmt.defineDone();

      end;

      /* Set the movement id */
      movement_id = %eval(&movement_id + 1);

      /* Override processed_dttm column if exists */
      %if(%rsk_varexist(&outCasLibref..&ds_in., processed_dttm)) %then %do;
         processed_dttm = "%sysfunc(datetime(), datetime21.)"dt;
      %end;

      /* Set the movement code description */
      movement_desc = catx(". ", put(movement_id, z2.), "&movement_desc.");
      /* Lookup movement type and category */
      _rc_ = hMvmt.find();

      if __last__ then
         call symputx("max_movement_id", movement_id, "L");
   run;

   /* remove unnecessary tables */
   %core_cas_drop_table(cas_session_name = &casSessionName.
                     , cas_libref = &outModelCasLib.
                     , cas_table = _tmp_mart_aggr_);
   %core_cas_drop_table(cas_session_name = &casSessionName.
                     , cas_libref = &outModelCasLib.
                     , cas_table = aggregated_input_mart);
   %core_cas_drop_table(cas_session_name = &casSessionName.
                     , cas_libref = &outModelCasLib.
                     , cas_table = modified_delta);

   libname &outCasLibref. clear;

%mend corew_apply_indv_adjustments;