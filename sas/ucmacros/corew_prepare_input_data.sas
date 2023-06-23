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
                        , inCasLib = Public
                        , inTableList =
                        , outTableList =
                        , outCasLib = Public
                        , outCasTablesScope = global
                        , casSessionName =
                        , outVarToken = accessToken
                        , debug = false
                        );

   %local   inCasLibref outCasLibref
            httpSuccess responseStatus inTable
            outTable outTableCasLib outTableCasTable;

   %let inCasLibref = %rsk_get_unique_ref(type = lib, engine = cas, args = caslib="&inCasLib." sessref=&casSessionName.);
   %let outCasLibref = %rsk_get_unique_ref(type = lib, engine = cas, args = caslib="&outCasLib." sessref=&casSessionName.);

   /* Loop over all input tables and produce the corresponding output table in CAS with global scope*/
   %do i=1 %to %sysfunc(countw(%superq(inTableList), %str( )));

      %let inTable = %scan(%superq(inTableList), &i., %str( ));
      %let outTable = %scan(%superq(outTableList), &i., %str( ));

      %let outTableCasLib = &outCasLib.;
      %let outTableCasTable = &outTable.;

      /* Check if the table parameter is an Analysis Data key */
      %if %sysfunc(index("&inTable.", -)) > 0 %then %do;

         /* It is a key - retrieve the table's data from the risk-data service into CAS with global scope */
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
                                       , locationType = LIBNAME
                                       , location = &outTableCasLib.
                                       , fileName = &outTableCasTable.
                                       , replace = true
                                       , casSessionName = &casSessionName.
                                       , casScope = &outCasTablesScope.
                                       , outVarToken = &outVarToken.
                                       , outSuccess = httpSuccess
                                       , outResponseStatus = responseStatus
                                       , debug = &debug.
                                       );

         /* Exit in case of errors */
         %rsk_dsexist_cas(cas_lib=%superq(outTableCasLib),cas_table=%superq(outTableCasTable),cas_session_name=&casSessionName.);

         %if(not &httpSuccess. or not &cas_table_exists.) %then
            %abort;

      %end;
      %else %if %rsk_dsexist(&inTable.) %then %do;
         /* It is not a key - move the input table to the output location and promote to global scope*/
         data &outCasLibref..&outTable. (promote=yes);
            set &inCasLibref..&inTable.;
         run;
      %end;
      %else %do;
         /* The specified input table does not exist */
         %put ERROR: Input parameter inTableList is invalid. The specified table (&inTable.) does not exist.;
         %abort;
      %end;

   %end; /* End loop over input tables */

%mend;

