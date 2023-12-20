 %macro corew_model_post_process(sourceSystemCd =
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
                                , analysisRunKey =
                                , inResults =
                                , ds_in_map_movement =
                                , ds_in_aggregation_config = aggregation_config
                                , ds_in_dependency_config =
                                , ds_in_datastore_config = datastore_config
                                , outResults =
                                , outModelCasLib =
                                , asOfDate =
                                , solutionRootFolder =
                                , ddlSubPath =
                                , mart_table_name =
                                , mart_movement_type_cd = CR_MODEL
                                , promoteResults = Y
                                , casSessionName = casauto
                                , customCode =
                                , ia_data_key =
                                , ia_adjust_exclude_vars_list = REPORTING_DT HORIZON FORECAST_TIME
                                , outVarToken = accessToken
                                , debug = false
                                );

   %local dtfmt input_flg libref outCasLibref solutionCadence ddlSubPath;

   %if(%sysevalf(%superq(mart_table_name) eq, boolean)) %then %do;
      %put ERROR: mart_table_name is required.;
      %abort;
   %end;

   %let outCasLibref = %rsk_get_unique_ref(type = lib, engine = cas, args = caslib="&outModelCaslib." sessref=&casSessionName.);

   %if(%sysevalf(%superq(ds_in_map_movement) ne, boolean)) %then %do;
      /* Move the SAS mapping tables into CAS so that the DATA step below runs in CAS */
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

   /*********************************/
   /* Case: reRunPipelineFlag was N */
   %rsk_dsexist_cas(cas_lib=%superq(outModelCasLib),cas_table=%superq(inResults), cas_session_name=&casSessionName.);

   %if not &cas_table_exists. %then %do;
      data &outCasLibref..temp_ir;
         set &outCasLibref..&mart_table_name.;
      run;
      %let inResults = temp_ir;
   %end;

   /*********************************/

   /* Append the results to the DDL structure */
   data &outCasLibref..
      %if %upcase("&promoteResults.") eq "Y" %then %do;
         "&mart_table_name."n
      %end;
      %else %do;
         "&outResults."n
      %end;
      (keep=%rsk_getvarlist(&outCasLibref.."&mart_table_name."n));

      set &outCasLibref.."&mart_table_name."n &outCasLibref.."&inResults."n;
      %if(%sysevalf(%superq(ds_in_map_movement) ne, boolean)) %then %do;
         if _N_ = 1 then do;
            /* Lookup to retrieve the movement types */
            declare hash hMvmt(dataset: "&outCasLibref..ds_in_map_movement");
            hMvmt.defineKey("movement_type_cd");
            hMvmt.defineData("movement_type", "movement_category");
            hMvmt.defineDone();
         end;

         reporting_dt = &asOfDate.;
         movement_type_cd = "&mart_movement_type_cd.";
         _rc_mvmt = hMvmt.find();
      %end;
      %else %do;
         reporting_dt = &asOfDate.;
         movement_type_cd = "CR_MODEL";
         movement_type = "00. Opening Balance";
         movement_category = "00. Opening Balance";
      %end;

      /* Run custom code */
      %if(%sysevalf(%superq(customCode) ne, boolean)) %then %do;
         %unquote(&customCode.);
      %end;

   run;

   /* ************************************************** */
   /*  Apply individual adjustments (if provided)        */
   /* ************************************************** */

   /* Check if any individual adjustments were provided */
   %if %sysevalf(%superq(ia_data_key) ne, boolean) AND &cas_table_exists. %then %do;

      %local ds_out_ia_data; /* Dataset name that contains ia data */

      %if not %symexist(ds_out_ia_data) %then %do;
         %let ds_out_ia_data = tmp_ia_data;
      %end;
      %if %sysevalf(%superq(ds_out_ia_data) eq, boolean) %then %do;
         %let ds_out_ia_data = tmp_ia_data;
      %end;

      %corew_prepare_input_data(host = &host.
                                , port = &port.
                                , logonHost = &logonHost.
                                , logonPort = &logonPort.
                                , username = &username.
                                , password = &password.
                                , authMethod = &authMethod.
                                , client_id =  &client_id.
                                , client_secret = &client_secret.
                                , inTableList = &ia_data_key.
                                , outTableList = &ds_out_ia_data.
                                , outCasLib = &outModelCasLib.
                                , casSessionName = &casSessionName.
                                , outVarToken = &outVarToken.
                                , debug = &debug.
                                );

      data &outCasLibref..&ds_out_ia_data.;
        set &outCasLibref..&ds_out_ia_data.;
        format REPORTING_DT &dtfmt.;
      run;

      %if %upcase("&promoteResults.") eq "Y" %then %do;
         %let name_ds_in= "&mart_table_name."n;
      %end;
      %else %do;
         %let name_ds_in= "&outResults."n;
      %end;

      /* Dataset to contain the IA movements*/
      %let ds_out_ia_movements=IA_movements;

      /* Dataset to contain the IA exceptions (if applicable)*/
      %let ds_out_ia_exceptions=IA_exceptions;

      /* Apply the individual adjustments */
      %corew_apply_indv_adjustments(solution = &solution.
                                    , host = &host.
                                    , port = &port.
                                    , logonHost = &logonHost.
                                    , logonPort = &logonPort.
                                    , username = &username.
                                    , password = &password.
                                    , authMethod = &authMethod.
                                    , client_id =  &client_id.
                                    , client_secret = &client_secret.
                                    , ds_in = &name_ds_in.
                                    , ds_in_adjustments = &ds_out_ia_data.
                                    , ds_out = &ds_out_ia_movements.
                                    , ds_out_ia_exceptions = &ds_out_ia_exceptions.
                                    , exclude_vars_list = &ia_adjust_exclude_vars_list.
                                    , movement_desc = Individual Adjustments
                                    , analysis_run_key = &analysisRunKey.
                                    , ds_in_map_movement = ds_in_map_movement  /*This is the converted ds*/
                                    %if(%rsk_dsexist(&ds_in_dependency_config.)) %then %do;
                                       , ds_in_var_dependency = &ds_in_dependency_config.
                                    %end;
                                    , epsilon = 1e-10
                                    , ds_out_datastore_table_nm = &mart_table_name.
                                    , ds_out_datastore_libref = work
                                    , reportmart_group_id = Credit Risk
                                    , outVarToken = &outVarToken.
                                    , ds_in_adj_key = &ia_data_key.
                                    , outModelCasLib = &outModelCasLib.
                                    , casSessionName = &casSessionName.
                                    , debug = &debug.
                                    );

      /* Append the IA movements to the mapped model output */

      %if(%rsk_attrn(&outCasLibref..&ds_out_ia_movements., nobs)) %then %do;
         /* These two next steps are necessary to align */
         proc contents data=&outCasLibref..&ds_out_ia_movements. out=&outCasLibref..DS_IN_CONTENTS noprint; run;
         %let new_vars=;
         %let list_vars=;
         data _null_;
            length new_vars list_vars $10000;
            set &outCasLibref..DS_IN_CONTENTS(keep=name length type) end=last;
            retain new_vars "" list_vars "";

            if type = 1 then
               new_vars = compbl(new_vars||name||" 8. ");
            else new_vars = compbl(new_vars||name||" VARCHAR("||length||")");

            list_vars = compbl(list_vars||name);

            if last then do;
               call symputx("new_vars",new_vars,'L');
               call symputx("list_vars",list_vars,'L');
            end;
         run;
         %let list_vars_rename=%sysfunc(prxchange(s/(\w+)/$1=$1_n/, -1, &list_vars.));
         %let list_vars_reassign=%sysfunc(prxchange(s/(\w+)/$1=$1_n%str(;)/, -1, &list_vars.));
         %let list_vars_drop=%sysfunc(prxchange(s/(\w+)/$1_n/, -1, &list_vars.));

         data &outCasLibref.._&ds_out_ia_movements._;
            length &new_vars.;
            set &outCasLibref..&ds_out_ia_movements.(rename=(&list_vars_rename.));
               drop &list_vars_drop.
               /* Override processed_dttm column if exists */
               %if( not(%rsk_varexist(&outCasLibref..&name_ds_in., processed_dttm)) ) %then %do;
                  processed_dttm;
               %end;
               ;
               &list_vars_reassign.;
               ;
         run;

         data &outCasLibref.."&mart_table_name."n;
               set &outCasLibref.."&mart_table_name."n &outCasLibref.._&ds_out_ia_movements._;
         run;
      %end;

   %end;
   %else %do;
      %put NOTE: There were no Individual Adjustments data provided.;
   %end;

   /*****************************************************/

   %if %upcase("&promoteResults.") eq "Y" %then %do;

      /* Delete the results CAS table if it already exists */
      %core_cas_drop_table(cas_session_name = &casSessionName.
                           , cas_libref = &outModelCasLib.
                           , cas_table = &outResults.);
      %if %symexist(ds_out_ia_data) %then %do;
         %if %sysevalf(%superq(ds_out_ia_data) ne, boolean) %then %do;
            %core_cas_drop_table(cas_session_name = &casSessionName.
                                 , cas_libref = &outModelCasLib.
                                 , cas_table = &ds_out_ia_data.);
         %end;
      %end;
      %if %symexist(ds_out_ia_movements) %then %do;
         %if %sysevalf(%superq(ds_out_ia_movements) ne, boolean) %then %do;
            %core_cas_drop_table(cas_session_name = &casSessionName.
                                 , cas_libref = &outModelCasLib.
                                 , cas_table = _&ds_out_ia_movements._);
         %end;
      %end;

      /* Promote the results CAS table */
      proc casutil;
         promote inCaslib="&outModelCasLib."   casData="&mart_table_name."
                  outCaslib="&outModelCasLib." casOut="&outResults.";
         run;
      quit;

   %end;

   libname &outCasLibref. clear;

%mend;