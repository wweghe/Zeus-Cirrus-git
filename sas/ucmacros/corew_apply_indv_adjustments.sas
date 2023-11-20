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
                                    , ds_in =                     /* Dataset of the analysis data instance to which adjustments will be applied */
                                    , ds_in_adjustments =         /* Dataset of the analysis data instance containing the adjustments */
                                    , ds_out =                    /* Output dataset containing adjustment records */
                                    , ds_out_ia_exceptions =      /* Output dataset containing adjustment records that were not matched to input data records */
                                    , exclude_vars_list =         /* Vars to exclude from 'ds_in_adjustments' */
                                    , movement_desc =             /* Movement description for VA report (single description for all adjustments so they can be grouped together in the report) */
                                    , analysis_run_key =          /* Analysis Run key for output table */
                                    , ds_in_map_movement =
                                    , ds_in_var_dependency =
                                    , epsilon = 1e-10
                                    , ds_out_datastore_table_nm =
                                    , ds_out_datastore_libref =
                                    , reportmart_group_id =
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
      ia_primary_key
      ds_out_data_def_link_instances
      targ_folder
      attachmentName
      attachmentDesc
      outCasLibref
   ;

   %let outCasLibref = %rsk_get_unique_ref(type = lib, engine = cas, args = caslib="&outModelCaslib." sessref=&casSessionName.);

   /* Reset syscc variable */
   %let syscc = 0;

   /* *********************************************** */
   /*         Create empty output tables              */
   /* *********************************************** */

   %let httpSuccess=;
   %let responseStatus=;
   %let ia_primary_key=;
   %let ds_out_data_def_link_instances = WORK.tmp_datadef_link_instances;

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
                              , objectType = analysisData
                              , objectKey = &ds_in_adj_key.
                              , linkType = analysisData_dataDefinition
                              , outds = &ds_out_data_def_link_instances.
                              , outVarToken = &outVarToken.
                              , outSuccess = httpSuccess
                              , outResponseStatus = responseStatus
                              , debug = &debug.
                              );

   %if(not &httpSuccess. or not %rsk_dsexist(&ds_out_data_def_link_instances.)) %then %do;
      %put ERROR: Cannot get analysisData_dataDefinition link for analysis data: &ds_in_adj_key..;
      %abort;
   %end;

   data _null_;
      set &ds_out_data_def_link_instances.;
      call symputx("data_definition_key", businessObject2, "L");
   run;

   %if %sysevalf(%superq(data_definition_key) eq, boolean) %then %do;
      %put ERROR: Cannot get data definition key for analysis data: &ds_in_adj_key..;
      %abort;
   %end;

   /*****************************/

   %core_rest_get_data_def(solution = &solution.
                           , host = &host.
                           , port = &port.
                           , logonHost = &logonHost.
                           , logonPort = &logonPort.
                           , username = &username.
                           , password = &password.
                           , authMethod = &authMethod.
                           , client_id = &client_id.
                           , client_secret = &client_secret.
                           , key = &data_definition_key.
                           , outds = _tmp_dataDef_summary_IA
                           , outds_columns = _tmp_dataDef_details_IA
                           , outVarToken = &outVarToken.
                           , outSuccess = httpSuccess
                           , outResponseStatus = responseStatus
                           , debug = &debug.
                           );

   %if(not &httpSuccess. or not %rsk_dsexist(_tmp_dataDef_details_IA)) %then %do;
      %put ERROR: Cannot get data definition columns data set for: &data_definition_key.;
      %abort;
   %end;

   proc sql noprint;
      select name into :ia_primary_key separated by ' '
      from _tmp_dataDef_details_IA
      where primaryKeyFlag="true"
   ;
   quit;

   %if %sysevalf(%superq(ia_primary_key) eq, boolean) %then %do;
      %put ERROR: No primary key columns defined for the data definition: &data_definition_key.;
      %abort;
   %end;

   %let merge_by_vars = &ia_primary_key.;

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

   /* Get the current movement version */
   %let movement_id = .;
   proc sql noprint;
      select
         max(movement_id)
            into :movement_id
      from &outCasLibref..&ds_in.;
   ;
   quit;

   %if(%sysevalf(%superq(movement_id) =., boolean)) %then %do;
      %put ERROR: There are no data for the selected Credit Risk Detail.;
      %return;
   %end;

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

   libname &outCasLibref. clear;

%mend corew_apply_indv_adjustments;