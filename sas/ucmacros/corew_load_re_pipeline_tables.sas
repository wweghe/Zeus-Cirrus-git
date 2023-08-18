%macro corew_load_re_pipeline_tables(host =
                                    , port =
                                    , server = riskPipeline
                                    , logonHost =
                                    , logonPort =
                                    , username =
                                    , password =
                                    , authMethod = bearer
                                    , client_id =
                                    , client_secret =
                                    , rePipelineKey =
                                    , casSessionName =
                                    , loadInputTables = Y
                                    , loadOuputTables = Y
                                    , outEnvTable =
                                    , outds = work.re_pipeline_tables
                                    , outVarToken = accessToken
                                    , outSuccess = httpSuccess
                                    , outResponseStatus = responseStatus
                                    , debug = false
                                    , logOptions =
                                    , restartLUA = Y
                                    , clearCache = Y
                                    );

   %local
      outputCaslib
      env_table_uri
      execution_state
      i
      oldLogOptions
      rc_load
      rePipelineEnvCasLib
      rePipelineEnvCasTable
      rePipelineEnvSourceTable
      TotTables
   ;
   
   %if(%sysevalf(%superq(rePipelineKey) eq, boolean)) %then %do;
      %put ERROR: Pipeline key was not provided.;
      %return;
   %end;
   
   /* Set the required log options */
   %if(%length(&logOptions.)) %then
      options &logOptions.;
   ;
   
   /* Get the current value of mlogic and symbolgen options */
   %let oldLogOptions = %sysfunc(getoption(mlogic)) %sysfunc(getoption(symbolgen));
   
   /* Determine the base url */
   %core_set_base_url(host=&host, server=&server., port=&port.);
   
   %macro __loadTable__(name=, caslib=, path=, casSessionName=, suffix=, ds_out=);
      %local
         rc_load_&suffix.
      ;
      
      /* Check if CAS table exists */
      %rsk_dsexist_cas(cas_lib=%superq(caslib),cas_table=%superq(name), cas_session_name=&casSessionName., out_var=cas_table_exists1_&suffix.);
      
      %if not &&cas_table_exists1_&suffix.. %then %do;
         %put NOTE: CAS table &caslib..&name. not loaded. Loading;
         
         /* Load CAS table */
         proc cas;
            session &casSessionName.;
            table.loadTable status=rc /
               caslib="&caslib."
               path="&path."
               casOut={caslib="&caslib." name="&name." promote=TRUE}
            ;
            symputx("rc_load_&suffix.", rc.severity, "L"); /* rc=0 if successful load */
            run;
         quit;
         
         /* Check if CAS table exists */
         %rsk_dsexist_cas(cas_lib=%superq(caslib),cas_table=%superq(name), cas_session_name=&casSessionName., out_var=cas_table_exists2_&suffix.);
         
         %if &&rc_load_&suffix.. or not &&cas_table_exists2_&suffix.. %then %do;
            %put ERROR: Failed to load the CAS table &caslib..&name.;
            %return;
         %end;
         %else %do;
            %put NOTE: Successfully loaded the CAS table &caslib..&name.;
         %end;
         
      %end; /* %if not &&cas_table_exists1_&suffix.. */
      %else %do;
         %put NOTE: CAS table &caslib..&name. already loaded.;
      %end;
      
      %if %sysevalf(%superq(ds_out)^=,boolean) %then %do;
         %if (%rsk_dsexist(&ds_out.)) %then %do;
            proc sql;
               drop table &ds_out.;
            quit;
         %end;
         data &ds_out.;
            caslibName = "&caslib.";
            tableName = "&name.";
            sourceTableName = "&path.";
            output;
         run;
      %end;
   %mend __loadTable__;
   
   /* Delete output table if it exists */
   %if (%rsk_dsexist(&outds.)) %then %do;
      proc sql;
         drop table &outds.;
      quit;
   %end;
   
   /* Create empty output table */
   data &outds.;
      length
         caslibName        $256.
         tableName         $256.
         sourceTableName   $256.
         ;
      stop;
   run;
   
   %if (%rsk_dsexist(work.__risk_pipeline_summary__)) %then %do;
      proc sql;
         drop table work.__risk_pipeline_summary__;
      quit;
   %end;
   
   %if (%rsk_dsexist(work.__risk_pipeline_results__)) %then %do;
      proc sql;
         drop table work.__risk_pipeline_results__;
      quit;
   %end;
   
   /*****************************************/
   /* Get the Risk Engines pipeline details */
   /*****************************************/
   %core_rest_get_re_pipeline(host = &host.
                            , port = &port.
                            , server = &server.
                            , logonHost = &logonHost.
                            , logonPort = &logonPort.
                            , username = &username.
                            , password = &password.
                            , authMethod = &authMethod.
                            , client_id = &client_id.
                            , client_secret = &client_secret.
                            , rePipelineKey = &rePipelineKey.
                            , outds = work.__risk_pipeline_summary__
                            , outds_execution_results = work.__risk_pipeline_results__
                            , outVarToken = &outVarToken.
                            , outSuccess = &outSuccess.
                            , outResponseStatus = &outResponseStatus.
                            , debug = &debug.
                            , logOptions = &oldLogOptions.
                            , restartLUA = &restartLUA.
                            , clearCache = &clearCache.
                            );
                            
   /* Exit in case of errors */
   %if(not &&&outSuccess..) %then %do;
      %put ERROR: Failed to get the risk pipeline details.;
      %return;
   %end;

   data _null_;
      set work.__risk_pipeline_summary__;
      call symputx('execution_state', execution_state, 'L');
      call symputx('outputCaslib', configuration_outputCaslib, 'L');
   run;

   %if "&execution_state." = "completed" %then %do;
   
      /*****************************************************/
      /* Get the Risk Engines pipeline's environment table */
      /*****************************************************/
      data _null_;
         set work.__risk_pipeline_results__;
         where results_links_type = 'application/vnd.sas.risk.common.cas.table' and
               results_links_uri like '%results/environment';
         call symputx('env_table_uri', results_links_uri, 'L');
      run;
      
      %if %sysevalf(%superq(env_table_uri) ne, boolean) %then %do;
         %put Note: Pipeline has SAS Risk Engine pipeline environment table. Retrieving;
         
         %if (%rsk_dsexist(work.__environment_table_info__)) %then %do;
            proc sql;
               drop table work.__environment_table_info__;
            quit;
         %end;
         
         /* Temporary disable mlogic and symbolgen options to avoid printing of userid/pwd to the log */
         option nomlogic nosymbolgen;
         /* Send the REST request */
         %core_rest_request(url = &baseUrl.&env_table_uri.
                           , method = GET
                           , logonHost = &logonHost.
                           , logonPort = &logonPort.
                           , username = &username.
                           , password = &password.
                           , authMethod = &authMethod.
                           , client_id = &client_id.
                           , client_secret = &client_secret.
                           , printResponse = N
                           , parser = coreRestPlain
                           , outds = work.__environment_table_info__
                           , outVarToken = &outVarToken.
                           , outSuccess = &outSuccess.
                           , outResponseStatus = &outResponseStatus.
                           , debug = &debug.
                           , logOptions = &oldLogOptions.
                           , restartLUA = &restartLUA.
                           , clearCache = &clearCache.
                           );
                           
         /* Throw an error if the table request fails */
         %if(not &&&outSuccess..) %then %do;
            %put ERROR: Failed to retrieve the SAS Risk Engine pipeline environment table info from &env_table_uri. (Pipeline ID= &rePipelineKey.);
            %return;
         %end;
         
         /* Throw an error if the environment info table was not produced or has no information about the environment */
         %if (not %rsk_dsexist(work.__environment_table_info__) or %rsk_attrn(work.__environment_table_info__, nlobs) eq 0) %then %do;
            %put ERROR: No information was found for the SAS Risk Engine pipeline environment table from &env_table_uri. (Pipeline ID= &rePipelineKey.);
            %return;
         %end;
         
      %end; /* %if %sysevalf(%superq(env_table_uri) ne, boolean) */
      %else %do;
         /* Throw an error if the environment info table was not produced by a Manage Environment node */
         %put ERROR: No information was found for the SAS Risk Engine pipeline environment table. (Pipeline ID= &rePipelineKey.);
         %return;
      %end;
      
      data _null_;
         set work.__environment_table_info__;
         call symputx("rePipelineEnvCasLib", caslibName, "L");
         call symputx("rePipelineEnvCasTable", tableName, "L");
         call symputx("rePipelineEnvSourceTable", sourceTableName, "L");
      run;
      
      %__loadTable__(name=&rePipelineEnvCasTable., caslib=&rePipelineEnvCasLib., path=&rePipelineEnvSourceTable., casSessionName=&casSessionName., suffix=env);
      
      %if (%rsk_dsexist(work.__environment_table__)) %then %do;
         proc sql;
            drop table work.__environment_table__;
         quit;
      %end;
      
      /* Handle caslib and cas table names longer than SAS 9.4 max length limit */
      proc cas;
         source originTables;
            data casuser.__environment_table__;
               set &rePipelineEnvCasTable.(caslib=&rePipelineEnvCasLib.);
            run;
         endsource;
         dataStep.runCode / code=originTables;
      quit;
      data work.__environment_table__;
         set casuser.__environment_table__;
      run;
      
      %if(%sysevalf(%superq(outEnvTable) ne, boolean)) %then %do;
         /* Delete output table if it exists */
         %if (%rsk_dsexist(&outEnvTable.)) %then %do;
            proc sql;
               drop table &outEnvTable.;
            quit;
         %end;
         
         /* Create output table */
         data &outEnvTable.;
            set work.__environment_table__;
         run;
      %end;
      
      %if (%rsk_dsexist(work.__tableref__)) %then %do;
         proc sql;
            drop table work.__tableref__;
         quit;
      %end;
      
      data work.__tableref__;
         set work.__environment_table__ end=last;
         where tableref ne "";
         if last then call symputx("TotTables", _N_, "L");
      run;
      
      /*******************/
      /* Load CAS tables */
      /*******************/
      %do i = 1 %to &TotTables.;
         %local
            caslib_&i.
            name_&i.
         ;
         
         data _null_;
            p = &i;
            set work.__tableref__ point=p;
            call symputx("caslib_&i.", scan(tableref, 1, "."), "L");
            call symputx("name_&i.", scan(tableref, 2, "."), "L");
            stop;
         run;
         
         %if (("&outputCaslib." eq "&&caslib_&i.." and "%upcase(&loadOuputTables.)" eq "Y")
            or ("&outputCaslib." ne "&&caslib_&i.." and "%upcase(&loadInputTables.)" eq "Y")) %then %do;
            %__loadTable__(name=&&name_&i.., caslib=&&caslib_&i.., path=&&name_&i...sashdat, casSessionName=&casSessionName., suffix=&i., ds_out=work.__tmp&i.__);
         %end;
         
         /* Append data */
         %rsk_append(base = &outds.
                     , data = work.__tmp&i.__
                     , length_selection = longest);
                     
         /* Remove temporary data artefacts from the WORK */
         proc datasets library = work
                       memtype = (data)
                       nolist nowarn;
            delete __tmp&i.__
                   ;
         quit;
         
      %end; /* %do i = 1 %to &TotTables. */
      
      /* Remove temporary data artefacts from the WORK */
      proc datasets library = work
                    memtype = (data)
                    nolist nowarn;
         delete __risk_pipeline_summary__
                __risk_pipeline_results__
                __environment_table_info__
                __environment_table__
                __tableref__
                ;
      quit;
      
   %end; /* %if "&execution_state." = "completed" */
   %else %do;
      %put ERROR: The SAS Risk Engine pipeline with id &rePipelineKey. has the following status: &execution_state., but status completed is required.;
      %return;
   %end;
   
%mend;