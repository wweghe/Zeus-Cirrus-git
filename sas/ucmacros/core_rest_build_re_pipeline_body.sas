%macro core_rest_build_re_pipeline_body(from_pipeline_json_fref = vre_fout
                                       , new_pipeline_name =
                                       , action_nodes_changes_ds =
                                       , data_nodes_changes_ds =
                                       , configuration_changes_ds =
                                       , save_environment = Y
                                       , new_pipeline_json_fref = vre_new
                                       , restartLua = Y
                                       , debug = false);

   %local out_json_ds out_json_ds_var out_json_ds_var_length product;

   %let out_json_ds = work.raw_json_text;
   %let out_json_ds_var = json_text;
   %let out_json_ds_var_length = 32000;

   %if %upcase("&save_environment.") = "Y" %then
      %let save_environment = true;
   %else
      %let save_environment = false;

   /* Call the Lua function to build the new request body */
   %let product = IRM;
   %rsk_call_riskexec(entrypoint_module        =   sas.risk.re.re_rest_builder
                     , entrypoint_function     =   buildPipelineBody
                     , restartLUA              =   &restartLUA.
                     , arg1                    =   &from_pipeline_json_fref.           /* original pipeline JSON body fileref */
                     , arg2                    =   &new_pipeline_name.                 /* new pipeline name */
                     , arg3                    =   &action_nodes_changes_ds.           /* new pipeline action node modifications */
                     , arg4                    =   &data_nodes_changes_ds.             /* new pipeline data node inputs */
                     , arg5                    =   &configuration_changes_ds.          /* new pipeline configuration modifications */
                     , arg6                    =   &save_environment.                  /* new pipeline - should we save the risk environment */
                     , arg7                    =   &out_json_ds.                       /* new pipeline JSON body output dataset */
                     , arg8                    =   &out_json_ds_var.                   /* new pipeline JSON body output dataset column name */
                     , arg9                    =   &out_json_ds_var_length.            /* new pipeline JSON body output dataset column name length */
                     , arg10                   =   &debug.
                     );

   %if (not %rsk_dsexist(&out_json_ds.) or %rsk_attrn(&out_json_ds., nobs) eq 0) %then %do;
      %put ERROR: Failed to create the new pipeline JSON request body;
      %abort;
   %end;

   /* Writing to a file in PROC LUA has issues:
      1. the io module is disabled, so we can't use it
      2. we can use sas.submit to run a data step to write it, but it is filled with PROC LUA bugs.  (For example, it errors out if the text has "%mend" in it)
         -this is fairly likely to happen with RE pre/post code
   So, instead, in buildPipelineBody (LUA), we write 32K chars at a time to a column in the &out_json_ds. table.
   We then loop over it here to write to the output file
   */
   data _null_;
      set &out_json_ds.;
      retain i 1;
      file &new_pipeline_json_fref.;
      put @(i) &out_json_ds_var. @@;
      i=i+&out_json_ds_var_length.;
   run;

%mend;