%macro corew_model_post_process(inResults =
                              , ds_in_map_movement =
                              , outResults =
                              , outModelCasLib =
                              , asOfDate =
                              , solutionRootFolder =
                              , mart_table_name =
                              , mart_movement_type_cd = CR_MODEL
                              , promoteResults = Y
                              , casSessionName = casauto
                              , customCode =
                              );

   %local dtfmt input_flg libref outCasLibref solutionCadence;

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
   filename ddl filesrvc folderPath="&solutionRootFolder./&solutionCadence./ddl" filename="&mart_table_name..sas";
   %include ddl / lrecl=64000;

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

   %if %upcase("&promoteResults.") eq "Y" %then %do;

      /* Delete the results CAS table if it already exists */
      %core_cas_drop_table(cas_session_name = &casSessionName.
                           , cas_libref = &outModelCasLib.
                           , cas_table = &outResults.);

      /* Promote the results CAS table */
      proc casutil;
         promote inCaslib="&outModelCasLib."   casData="&mart_table_name."
                  outCaslib="&outModelCasLib." casOut="&outResults.";
         run;
      quit;

   %end;

   libname &outCasLibref. clear;

%mend;