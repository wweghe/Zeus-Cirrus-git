%macro core_prepare_sessions(numSessions=
                        , sessionNumStart=1
                        , sessionPrefix = sess
                        , casSessionName=
                        , casHost=
                        , casPort=
                        , inheritLibs=
                        , log_level=
                        , signOnMaxWait = 60
                        , setupMaxWait = 60
                        , outSuccess=
                     );

   %local start_dttm current_dttm casSessionId sasautos luapaths casHost casPort s;

   /* outSuccess cannot be missing. Set a default value */
   %if(%sysevalf(%superq(numSessions) =, boolean)) %then %do;
      %put ERROR: numSessions is required.;
      %abort;
   %end;

   %let sessionNumStart=%sysfunc(coalescec(&sessionNumStart., 1));
   %let sessionNumEnd=%eval(&sessionNumStart.+&numSessions.-1);
   %let sessionPrefix=%sysfunc(coalescec(&sessionPrefix., sess));
   %if %length(&sessionPrefix.)>=8 %then %do;
      %put ERROR: Session prefix must be 7 characters or shorter.  Value is &sessionPrefix.;
      %abort;
   %end;

   /* If not provided, set signOnMaxWait to 60 seconds */
   %let signOnMaxWait=%sysfunc(coalescec(&signOnMaxWait., 60));

   /* If not provided, set setupMaxWait to 60 seconds */
   %let setupMaxWait=%sysfunc(coalescec(&setupMaxWait., 60));

   /* outSuccess cannot be missing. Set a default value */
   %if(%sysevalf(%superq(outSuccess) =, boolean)) %then
      %let outSuccess = rsubmitSuccess;

   /* Declare the output variable as global if it does not exist */
   %if(not %symexist(&outSuccess.)) %then
      %global &outSuccess.;

   %let &outSuccess.=0;

   /* Capture the CAS host of the parent compute session, if not provided */
   %if(%sysevalf(%superq(casHost) eq, boolean)) %then
      %let casHost = %sysfunc(getoption(CASHOST));

   /* Capture the CAS host of the parent compute session, if not provided */
   %if(%sysevalf(%superq(casPort) eq, boolean)) %then
      %let casPort = %sysfunc(getoption(CASPORT));

   /* Capture the parent compute session's SASAUTOS and LUAPATH options into macrovariables */
   %let sasautos = %sysfunc(getoption(SASAUTOS));
   %let luapaths = %sysfunc(prxchange(s/[%str(%')()]//, -1, %sysfunc(PATHNAME(LUAPATH)))); /* ' */
   %let luapaths = %sysfunc(prxchange(s/([^\s]+)/'$1'/, -1, &luapaths.));


   /*****************************************/
   /* Kick off signon to each child session */
   /*****************************************/
   %do s=&sessionNumStart. %to &sessionNumEnd.;
      /* Start a child compute session using the SAS command */
      signon &sessionPrefix.&s. sascmd="!sascmd" cmacvar = rc_signon&s. SIGNONWAIT=NO inheritlib=(&inheritLibs.);
   %end;

   /* Wait for all child session signons to complete */
   %do s=&sessionNumStart. %to &sessionNumEnd.;

      %let start_dttm = %sysfunc(datetime());
      %do %while(&&rc_signon&s.. = 3);

         %let current_dttm = %sysfunc(datetime());
         %if (%sysevalf(&current_dttm. - &start_dttm. > &signOnMaxWait.)) %then %do;
            %put ERROR: Sign on to session &sessionPrefix.&s. timed out.;
            %goto EXIT;
         %end;

         data _null_;
            call sleep(0.5,1);
         run;
      %end;

      %if(&&rc_signon&s.. = 1) %then %do;
         %put ERROR: Sign on to session &sessionPrefix.&s. failed.  Return code is: &&rc_signon&s...;
         %goto EXIT;
      %end;

   %end;


   /***********************************************/
   /* Submit the setup code in each child session */
   /***********************************************/
   %do s=&sessionNumStart. %to &sessionNumEnd.;

      /* Pass this macro's local macrovariables to the child session */
      %syslput _LOCAL_ / remote=&sessionPrefix.&s.;

      /* Submit setup code to each session to run. */
      rsubmit &sessionPrefix.&s. wait=NO log=KEEP cmacvar = rc_prep&s.;

         %nrstr(%put Note: Setting up child session &sessionPrefix.&s.;)

         /* Set SASAUTOS/LUAPATH to their values from the parent session */
         %if(%sysevalf(%superq(sasautos) ne, boolean)) %then %do;
            options sasautos=&sasautos.;
         %end;

         %if(%sysevalf(%superq(luapaths) ne, boolean)) %then %do;
            filename LUAPATH (&luapaths.);
         %end;

         /* Set the same logging options as the parent compute session */
         %rsk_set_logging_options (outDebugVar = debug);

         /* Start a new CAS session against the same CAS server as the parent compute session */
         %if(%sysevalf(%superq(casSessionName) ne, boolean)) %then %do;
            %core_cas_initiate_session(cas_host = &casHost.
                                 , cas_port = &casPort.
                                 , cas_session_name = &casSessionName.
                                 , cas_session_options = casdatalimit=ALL
                                 , cas_assign_librefs = Y);
         %end;

      endrsubmit;

   %end;

   /* Wait for the child session's code to complete */
   waitfor _all_ %do s=&sessionNumStart. %to &sessionNumEnd; &sessionPrefix.&s. %end; timeout=&setupMaxWait.;

   /* Verify each child session's setup code submitted (not executed) successfully */
   %do s=&sessionNumStart. %to &sessionNumEnd;
      %if &&rc_prep&s.. ne 0 %then %do;
         %put ERROR: Failed to submit setup code to child session &sessionPrefix.&s.. Return code is &&rc_prep&s...;
         %goto EXIT;
      %end;
   %end;

   %let &outSuccess.=1;

   %EXIT:

%mend;
