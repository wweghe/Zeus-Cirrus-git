%macro corew_prepare_input_data(host =
                        , server = riskData
                        , port =
                        , logonHost =
                        , logonPort =
                        , username =
                        , password =
                        , authMethod = bearer
                        , client_id =
                        , client_secret =
                        , inCasLib =
                        , inSasLib =
                        , inTableList =
                        , outTableList =
                        , outCasLib =
                        , outSasLib =
                        , outCasTablesScope = global
                        , casSessionName =
                        , outVarToken = accessToken
                        , debug = false
                        );

   %local   inCasLibref outCasLibref
            location locationType engine cas_table_exists
            httpSuccess responseStatus inTable outTable i
            ;

   %if "&inCasLib." ne "" %then
      %let inCasLibref = %rsk_get_unique_ref(type = lib, engine = cas, args = caslib="&inCasLib." sessref=&casSessionName.);
   %if "&outCasLib." ne "" %then
      %let outCasLibref = %rsk_get_unique_ref(type = lib, engine = cas, args = caslib="&outCasLib." sessref=&casSessionName.);

   %if "&outSasLib." ne "" %then %do;
      %let location=%sysfunc(pathname(&outSasLib.));
      %let locationType=DIRECTORY;
      %let engine=SAS;
      %let outCasTablesScope=;
   %end;
   %else %do;
      %let location=%sysfunc(coalescec(&outCasLib., Public));
      %let locationType=LIBNAME;
      %let engine=CAS;
   %end;

   /* Loop over all input tables and produce the corresponding output table in CAS with global scope*/
   %do i=1 %to %sysfunc(countw(%superq(inTableList), %str( )));

      %let inTable = %scan(%superq(inTableList), &i., %str( ));
      %let outTable = %scan(%superq(outTableList), &i., %str( ));

      /* Check if the table parameter is an Analysis Data key */
      %if %sysfunc(index("&inTable.", -)) > 0 %then %do;

         /* It is a key - retrieve the table's data from the risk-data service into SAS or CAS */
         %let httpSuccess = 0;
         %let responseStatus =;
         %core_rest_export_analysis_data(host = &host.
                                       , server = &server.
                                       , port = &port.
                                       , logonHost = &logonHost.
                                       , logonPort = &logonPort.
                                       , username = &username.
                                       , password = &password.
                                       , authMethod = &authMethod.
                                       , client_id = &client_id.
                                       , client_secret = &client_secret.
                                       , key = &inTable.
                                       , locationType = &locationType.
                                       , location = &location.
                                       , fileName = &outTable.
                                       , replace = true
                                       , casSessionName = &casSessionName.
                                       , casScope = &outCasTablesScope.
                                       , outVarToken = &outVarToken.
                                       , outSuccess = httpSuccess
                                       , outResponseStatus = responseStatus
                                       , debug = &debug.
                                       );

         /* Exit in case of errors */
         %if not &httpSuccess. %then %do;
            %put ERROR: The request to export table &outTable. to &location. failed;
            %abort;
         %end;

         /* Exit in case the output table wasn't created or isn't available in the current session */
         %if &engine.=SAS %then %do;
            %if not %rsk_dsexist(&outSasLib..&outTable.) %then %do;
               %put ERROR: The output table &outSasLib..&outTable. does not exist in the current session;
               %abort;
            %end;
         %end;
         %if &engine.=CAS %then %do;
            %rsk_dsexist_cas(cas_lib=%superq(location),cas_table=%superq(outTable),cas_session_name=&casSessionName.);
            %if not &cas_table_exists. %then %do;
               %put ERROR: The output table &location..&outTable. does not exist in the current session;
               %abort;
            %end;
         %end;

      %end;
      %else %do;

         /* If the input table is not a key and exists, move it from the input location to the output location */
         %if &engine.=SAS %then %do;
            %if %rsk_dsexist(&inSasLib..&inTable.) %then %do;
               data &outSasLib..&outTable.;
                  set &inSasLib..&inTable.;
               run;
            %end;
            %else %do;
               %put ERROR: Input parameter inTableList is invalid. The specified table (&inSasLib..&inTable.) does not exist in the current session.;
               %abort;
            %end;
         %end;

         %if &engine.=CAS %then %do;

            %rsk_dsexist_cas(cas_lib=%superq(inCasLib),cas_table=%superq(inTable),cas_session_name=&casSessionName.);
            %if &cas_table_exists. %then %do;
               data &outCasLibref..&outTable. (
                  %if %upcase("&outCasTablesScope.") = "GLOBAL" %then %do;
                     promote=yes
                  %end;
               );
                  set &inCasLibref..&inTable.;
               run;
            %end;
            %else %do;
               %put ERROR: Input parameter inTableList is invalid. The specified table (&inCasLib..&inTable.) does not exist in the current session.;
               %abort;
            %end;
         %end;

      %end;

   %end; /* End loop over input tables */

   %if "&inCasLibref." ne "" %then %do;
      libname &inCasLibref. clear;
   %end;
   %if "&outCasLibref." ne "" %then %do;
      libname &outCasLibref. clear;
   %end;

%mend;

