%macro corew_run_attribution_wrapper(solution =
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
                                    , maxParallelRuns =
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
                                    , outSuccess = attrSuccess
                                    , debug = false
                                    , log_level = 1
                                    );

   %local   Totrows num_sessions sessionPrefix sessionWorkLibref s;

   %if(%sysevalf(%superq(inSasAttributionRunConfig) eq, boolean)) %then %do;
      %put ERROR: inSasAttributionRunConfig is required.;
      %abort;
   %end;

   /* OutSuccess cannot be missing. Set a default value */
   %if(%sysevalf(%superq(outSuccess) =, boolean)) %then
      %let outSuccess = attrSuccess;

   /* Declare the output variable as global if it does not exist */
   %if(not %symexist(&outSuccess.)) %then
      %global &outSuccess.;

   %let &outSuccess. = 0;

   /* Determine the number of sessions to run in parallel */
   %let Totrows = %rsk_attrn(&inSasAttributionRunConfig., nobs);
   %if &Totrows.=0 %then %do;
      %put Note: No intermediate attribution runs were found in the attribution template.;
      %return;
   %end;

   %let num_sessions = %sysfunc(coalescec(&maxParallelRuns.,&Totrows.));
   %if &num_sessions. <= 0 or &num_sessions.> &Totrows. %then
      %let num_sessions = &Totrows.;

   %let sessionPrefix=sess;
   %let sessionWorkLibref=rwork;

   /* If any tables are in WORK, we need to update the libref to the work libref used in the child sesssion.
      This is necessary because the WORK libref in the child session points to its own work space, and cannot be changed.
      Ex: work.my_table --> rwork.my_table */
   %let inSasAttributionRunConfig = %sysfunc(prxchange(s/^work\./&sessionWorkLibref../i, -1, &inSasAttributionRunConfig));
   %if not %sysfunc(find(&inSasAttributionRunConfig., %str(.))) %then
      %let inSasAttributionRunConfig=&sessionWorkLibref..&inSasAttributionRunConfig.;

   %if(%sysevalf(%superq(ds_in_map_movement) ne, boolean)) %then %do;
      %let ds_in_map_movement = %sysfunc(prxchange(s/^work\./&sessionWorkLibref../i, -1, &ds_in_map_movement));
      %if not %sysfunc(find(&ds_in_map_movement., %str(.))) %then
         %let ds_in_map_movement=&sessionWorkLibref..&ds_in_map_movement.;
   %end;

   /* If any tables are in CAS but are session-level, they must be promoted for the CAS sessions in the child sessions to
   see them.  This is necessary because separate CAS sessions must be created in each child sessions (if they all connect
   to the parent CAS session, there will be concurrency errors with CAS actions/data steps */
   %core_cas_drop_table(cas_session_name = &casSessionName.
                        , cas_libref = &inModelCasLib.
                        , cas_table = &inScenarioMap._&casTablesTag.);

   proc casutil;
        promote inCaslib="&inModelCasLib."   casData="&inScenarioMap."
                outCaslib="&inModelCasLib." casOut="&inScenarioMap._&casTablesTag."
                keep;
      run;
   quit;


   /*************************************************/
   /* Perform attribution runs in parallel sessions */
   /*************************************************/
   %do s=1 %to &num_sessions.;

      /***********************************/
      /* Start and setup a child session */
      /***********************************/
      %core_prepare_sessions(numSessions = 1
                           , sessionNumStart = &s.
                           , sessionPrefix = &sessionPrefix.
                           , casSessionName = &casSessionName.
                           , inheritLibs = work=&sessionWorkLibref.
                           , log_level = &log_level.
                           , outSuccess = &outSuccess.
                           );

      %if not &&&outSuccess.. %then
         %goto EXIT;

      /* Create the ds_in_cardinality dataset for the macros in the child sessions to use */
      data attr_cardinality_&s.;
         length partition_no n_partitions 8.;
         partition_no=&s.;
         n_partitions=&num_sessions.;
      run;

      /* Pass this macro's local macrovariables to the child session */
      %syslput _LOCAL_ / remote=&sessionPrefix.&s.;

      rsubmit &sessionPrefix.&s. wait=NO log=keep cmacvar = rc_session&s.;

         %nrstr(%put Note: Running in child session &sessionPrefix.&s.;)

         /* Kick off attribution in this child session */
         %corew_run_attribution(solution = &solution.
                                 , host = &host.
                                 , port = &port.
                                 , logonHost = &logonHost.
                                 , logonPort = &logonPort.
                                 , username = &username.
                                 , password = &password.
                                 , authMethod = &authMethod.
                                 , client_id = &client_id.
                                 , client_secret = &client_secret.
                                 , attributionType = &attributionType.
                                 , inSasAttributionRunConfig = &inSasAttributionRunConfig.
                                 , inScenarioMap = &inScenarioMap._&casTablesTag.
                                 , ds_in_map_movement = &ds_in_map_movement.
                                 , ds_in_cardinality = &sessionWorkLibref..attr_cardinality_&s.
                                 , keepModelData = &keepModelData.
                                 , scenario_selection = &scenario_selection.
                                 , inModelCasLib = &inModelCasLib.
                                 , outModelCasLib = &outModelCasLib.
                                 , analysisRunKey = &analysisRunKey.
                                 , asOfDate = &asOfDate.
                                 , casTablesTag = &casTablesTag.
                                 , pollInterval = &pollInterval.
                                 , maxWait = &maxWait.
                                 , casSessionName = &casSessionName.
                                 , solutionRootFolder = &solutionRootFolder.
                                 , ddlSubPath = &ddlSubPath.
                                 , mart_table_name = &mart_table_name.
                                 , outVarToken = &outVarToken.
                                 , debug = &debug.
                                 );

      endrsubmit;

   %end;

   %do s=1 %to &num_sessions.;

      /* Wait for each child session's code to complete */
      waitfor _all_ &sessionPrefix.&s. timeout=&maxWait.;

      /* Verify each child session's setup code submitted (not executed) successfully */
      %if &&rc_session&s.. ne 0 %then %do;
         %put ERROR: Failed to submit code to child session &sessionPrefix.&s.. Return code is &&rc_session&s...;
         %goto EXIT;
      %end;

   %end;

   %let &outSuccess. = 1;

   %EXIT:

   /********************************/
   /* Terminate each child session */
   /********************************/
   %do s=1 %to &num_sessions.;
      signoff &sessionPrefix.&s.;
   %end;

   /* Remove any session-level tables we promoted from global scope */
   %core_cas_drop_table(cas_session_name = &casSessionName.
                        , cas_libref = &inModelCasLib.
                        , cas_table = &inScenarioMap._&casTablesTag.);

%mend;