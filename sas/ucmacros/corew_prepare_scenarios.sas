%macro corew_prepare_scenarios(host =
                              , server = riskScenarios
                              , port =
                              , logonHost =
                              , logonPort =
                              , username =
                              , password =
                              , authMethod = bearer
                              , client_id =
                              , client_secret =
                              , scenarioSetIds =
                              , baselineId =
                              , dateBasedFormat = false
                              , includeScenarioHistory = true
                              , outScenarios =
                              , outScenarioSet = scenario_set
                              , promoteScenarios = Y
                              , promoteScenarioSet = Y
                              , outCasLib = Public
                              , outSasLib =
                              , casSessionName = casauto
                              , outVarToken = accessToken
                              , debug = false
                              );

   %local   outLibref httpSuccess responseStatus cas_table_exists
            locationType outEngine outScenariosLib outScenariosTable outScenarioSetTable scenExportTag scenFileRef scenarioNameLength
            currScenario interval asOfDate scenarioSetId num_scenario_sets oldIds i;

   %if(%sysevalf(%superq(outScenarios) eq, boolean)) %then %do;
      %put ERROR: outScenarios is required;
      %abort;
   %end;

   %if (%sysevalf(%superq(scenarioSetIds) eq, boolean)) %then %do;
      %put ERROR: no eligible scenarios to prepare Credit Risk Analysis.;
      %abort;
   %end;

   %let scenExportTag=%substr(%sysfunc(uuidgen()), 1, 7);
   %if "&outSasLib." ne "" %then %do;
      %let outLibref=&outSasLib.;
      %let locationType=FOLDER;
      %let outEngine=SAS;

      %let outScenariosLib = &outSasLib.;
      %let outScenariosTable = &outScenarios.;
      %let outScenarioSetTable = &outScenarioSet.;
      %let scenarioNameLength=$200;
   %end;
   %else %do;
      %let outCasLib = %sysfunc(coalescec(&outCasLib., Public));
      %let outLibref = %rsk_get_unique_ref(type = lib, engine = cas, args = caslib="&outCasLib." sessref=&casSessionName.);
      %let locationType=LIBNAME;
      %let outEngine=CAS;

      %let outScenariosLib = &outCasLib.;
      %let outScenariosTable = &outScenarios.;
      %let outScenarioSetTable = &outScenarioSet.;
      %let scenarioNameLength=varchar(200);
   %end;

   %if(%sysevalf(%superq(dateBasedFormat) eq, boolean)) %then
      %let dateBasedFormat = false;

   %if &dateBasedFormat.=true and "&outSasLib." ne "" %then %do;
      %put ERROR: Exporting to SAS is only supported with  &dateBasedFormat.=false;
      %abort;
   %end;

   /********************************************************************/
   /* Export all of the scenarios in the scenario sets to a CAS table. */
   /********************************************************************/

   /* Loop over all scenario set IDs provided */
   %let num_scenario_sets = %sysfunc(countw(%superq(scenarioSetIds), %str( )));
   %do i=1 %to &num_scenario_sets.;

      /* Export the scenario set information into a SAS table and the scenarios themselves into a CAS table */
      %let scenarioSetId = %scan(%superq(scenarioSetIds), &i., %str( ));
      %let httpSuccess=0;
      %let responseStatus = Not Set - Before Scenario Set Get Request;
      %core_rest_export_rsm_scen_set(host = &host.
                                    , server = &server.
                                    , port = &port.
                                    , logonHost = &logonHost.
                                    , logonPort = &logonPort.
                                    , username = &username.
                                    , password = &password.
                                    , authMethod = &authMethod.
                                    , client_id = &client_id.
                                    , client_secret = &client_secret.
                                    , scenarioSetId = &scenarioSetId.
                                    , baselineId = &baselineId.
                                    , includeScenarioHistory = &includeScenarioHistory.
                                    , dateBasedFormat = &dateBasedFormat.
                                    , locationType = &locationType.
                                    , outScenarioSetDs = _tmp_scenario_set
                                    , outExportResponseDs = scenarios_export_info
                                    , outScenariosCasLib = &outScenariosLib.
                                    , outScenariosDs = _tmp_scenarios_&scenExportTag.
                                    , outScenariosFilePath = /Public
                                    , outScenariosFile = scenarios_&scenExportTag..csv
                                    , replaceOutScenarios = Y
                                    , casSessionName = &casSessionName.
                                    , debug = &debug.
                                    , outVarToken = &outVarToken.
                                    , outSuccess = httpSuccess
                                    , outResponseStatus = responseStatus
                                    );

      /* Stop macro execution if there were any errors */
      %if (&httpSuccess. = 0) %then %do;
         %put ERROR: Failed to export RSM scenario set with ID: &scenarioSetId.;
         %abort;
      %end;

      /* Verify the exported scenarios exist (in CAS or as a CSV in the files service).  If the scenarios were exported to the file services,
      import them into a SAS dataset and remove the CSV file */
      %if &outEngine. = SAS %then %do;
         %let scenFileRef = %rsk_get_unique_ref(prefix=scen, type=Fileref, engine=filesrvc, args=folderPath="/Public" filename="scenarios_&scenExportTag..csv");
         %if %sysfunc(fexist(&scenFileRef.))=0 %then %do;
            %put ERROR: Failed to export RSM scenario set with ID &scenarioSetId. to the file service - export path is: /Public/scenarios_&scenExportTag..csv;
            %abort;
         %end;

         /* Read the exported scenario CSV into a SAS dataset.  We must know the column names to set their lengths to avoid truncation (which happens with proc import). */
         data &outLibref.._tmp_scenarios_&scenExportTag.;
            attrib
               scenario_name           length = $200.    label = "Scenario Name"
               variable_name           length = $200.    label = "Variable Name"
               interval                length = $200.    label = "Interval"
               date                    length = 8.       label = "Date"             informat=yymmdd10.    format=yymmdd10.
               change_type             length = $200.    label = "Change Type"
               change_value            length = 8.       label = "Change Value"
               _priority_              length = 8.       label = "Priority"
            ;
            infile &scenFileRef. dlm = "," dsd firstobs = 2;

            input
               scenario_name     $
               variable_name     $
               interval          $
               date
               change_type       $
               change_value
               _priority_
            ;
         run;

         %if %sysfunc(fdelete(&scenFileRef.)) ne 0 %then
            %put WARNING: Failed to delete scenario file from the file service: /Public/scenarios_&scenExportTag..csv;

      %end;
      %else %do;
         %rsk_dsexist_cas(cas_lib=%superq(outScenariosLib),cas_table=_tmp_scenarios_&scenExportTag., cas_session_name=&casSessionName.);
         %if (not &cas_table_exists.) %then %do;
            %put ERROR: Failed to export RSM scenario set with ID &scenarioSetId. to CAS table &outScenariosLib.._tmp_scenarios_&scenExportTag.;
            %abort;
         %end;
      %end;

      /* Interval and asOfDate must be the same for all scenarios in the scenario set, so get them here */
      /* currScenario is 1 scenario name that is used later to ensure we get only 1 scenario's current horizon data,
      since all scenarios should have the same current horizon data and we must have only 1 row of data in the final scenarios */
      data &outLibref.._tmp_scenario_set (drop=forecastTime);
         length forecast_time 8;
         set _tmp_scenario_set;

         if vtype(forecastTime)="C" then
            forecast_time=input(forecastTime, 8.);
         else
            forecast_time=forecastTime;

         forecastTimeFlag=upcase(coalescec(forecastTimeFlag, "N"));

         /* Verify that all scenarios in a scenario set with forecastTimeFlag=Y have forecastTime values set */
         if forecastTimeFlag="Y" and forecast_time=. then do;
            put "ERROR: Scenario set '" name "' has forecastTimeFlag=Y but scenario '" scenarioName "' has no forecastTime value.";
            put "ERROR: All scenarios in a scenario set with forecastTimeFlag=Y must have a forecastTime custom attribute set to a non-negative integer.";
            abort;
         end;

         /* Verify this scenario set's asOfDate value matches the asOfDate for other scenario sets */
         %if "&asOfDate." ne "" %then %do;
            if input(asOfDate, YYMMDD10.) ne &asOfDate. then do;
               put "ERROR: asOfDate for scenario set &scenarioSetId. is " asOfDate ".";
               put "ERROR: asOfDate is &asOfDate. for 1 or more of these scenario sets: &scenarioSetIds..";
               put "ERROR: The asOfDate must the be same for all provided scenario sets.";
               abort;
            end;
         %end;

         /* append the scenarioVersion (if not missing) to the scenarioName to guarantee scenarioName is unique */
         if not missing(scenarioVersion) then do;
            scenarioName = catt(scenarioName, "/", scenarioVersion);
         end;

         call symputx("interval", periodType, "L");
         call symputx("asOfDate", input(asOfDate, YYMMDD10.), "L");
         call symputx("currScenario", scenarioName, "L");
      run;

      data &outLibref.._tmp_scenarios_&scenExportTag.;
         length scenario_name variable_name interval change_type &scenarioNameLength.;
         set &outLibref.._tmp_scenarios_&scenExportTag.;

         /* append the scenario_version (if it is not missing) to the scenario_name to guarantee scenarioName is unique */
         if not missing(scenario_version) then do;
            scenario_name = catt(scenario_name, "/", scenario_version);
         end;
      run;

      %let oldIds='';
      %let scenarioNames='';
      %if &i. ne 1 %then %do;
         proc sql noprint;
            select distinct catt("'",scenarioId,"'"), catt("'",scenarioName,"'") into :oldIds separated by ',', :scenarioNames separated by ','
            from &outLibref.._tmp_scenario_set_all
            ;
         quit;
      %end;

      /* append each scenario set's info to the final _tmp_scenario_set_all table */
      data &outLibref.._tmp_scenario_set_all;
         set
            %if &i. ne 1 %then %do;
               &outLibref.._tmp_scenario_set_all
            %end;
            %if &oldids ne '' %then %do;
               &outLibref.._tmp_scenario_set (where=(scenarioId not in (&oldIds)));
            %end;
            %else %do;
               &outLibref.._tmp_scenario_set;
            %end;
      run;

      %if &outEngine. = CAS %then %do;
         /* drop the &outScenarios. table if it exists */
         %if &i.=1 %then %do;
            %core_cas_drop_table(cas_session_name = &casSessionName.
                                 , cas_libref = &outScenariosLib.
                                 , cas_table = &outScenariosTable.);
         %end;
      %end;

      /* Produce the final scenarios table with a common format, regardless of if the scenarios in RSM are in
      date-based format or not */
      %if &dateBasedFormat. = true %then %do;

         data &outLibref.."&outScenariosTable."n
            %if &i.=&num_scenario_sets. %then %do;
               ( rename=(_date=date)
               %if %upcase("&promoteScenarios.") eq "Y" and &outEngine. = CAS %then %do;
                  promote=yes
               %end;
               )
            %end;
            ;

            format horizon 8. interval $10. _type_ $5. _date YYMMDD10.;

            set
               %if &i. ne 1 %then %do;
                  &outLibref.."&outScenariosTable."n (in=base)
               %end;
               %if &scenarioNames ne '' %then %do;
                  &outLibref.._tmp_scenarios_&scenExportTag. (in=new where=(scenario_name not in (&scenarioNames.)));
               %end;
               %else %do;
                  &outLibref.._tmp_scenarios_&scenExportTag. (in=new);
               %end;

            %if &i. ne 1 %then %do;
               if base then output;
            %end;
            if new then do;

               _date=intnx("&interval.", input(date, YYMMDD10.), 0, "SAME");
               scenario_name=scan(scenario_name, 1, "/");
               interval="&interval";
               _type_="VALUE"; /*date-based format is not valid with shocks, so only possible value is "VALUE" here*/
               horizon = intck("&interval.", &asOfDate., _date);

               /* always output future */
               if _date > &asOfDate. then output;
               else do;
                  %if &i.=1 %then %do;

                     /* only output current for first scenario set - current data must be the same for all scenario sets */
                     /* make sure to only get 1 scenario's current data, in case each scenario has duplicate current data */
                     if _date = &asOfDate. then do;
                        if scenario_name eq "&currScenario." or scenario_name eq "" then do;
                           scenario_name="";
                           output;
                        end;
                     end;

                     /* only output history for first scenario set - history data must be the same for all scenario sets */
                     else if _date < &asOfDate. then output;
                  %end;
               end;
            end;
         run;

      %end;
      %else %do;
         data &outLibref.."&outScenariosTable."n
            %if &i.=&num_scenario_sets. %then %do;
               %if %upcase("&promoteScenarios.") eq "Y" and &outEngine. = CAS %then %do;
                  (promote=yes)
               %end;
            %end;
            ;

            format horizon 8. date YYMMDD10.;

            set
               %if &i. ne 1 %then %do;
                  &outLibref.."&outScenariosTable."n (in=base)
               %end;
               %if &scenarioNames ne '' %then %do;
                  &outLibref.._tmp_scenarios_&scenExportTag. (in=new where=(scenario_name not in (&scenarioNames.)));
               %end;
               %else %do;
                  &outLibref.._tmp_scenarios_&scenExportTag. (in=new);
               %end;

            %if &i. ne 1 %then %do;
               if base then output;
            %end;
            if new then do;
               horizon = intck("&interval.", &asOfDate., date);

               /* always output future */
               if date > &asOfDate. then output;
               else do;
                  %if &i.=1 %then %do;

                     /* only output current for first scenario set - current data must be the same for all scenario sets */
                     /* make sure to only get 1 scenario's current data, in case each scenario has duplicate current data */
                     if date = &asOfDate. then do;
                        if scenario_name eq "&currScenario." or scenario_name eq "" then do;
                           scenario_name="";
                           output;
                        end;
                     end;

                     /* only output history for first scenario set - history data must be the same for all scenario sets*/
                     else if date < &asOfDate. then output;

                  %end;
               end;
            end;
         run;
      %end;

   %end; /* end loop over scenario set IDs */


   %if &outEngine. = CAS %then %do;
      /* Drop the temporary scenarios CAS table */
      %core_cas_drop_table(cas_session_name = &casSessionName.
                        , cas_libref = &outScenariosLib.
                        , cas_table = _tmp_scenarios_&scenExportTag.);

      /* Drop the scenario set CAS table, if it exists */
      %core_cas_drop_table(cas_session_name = &casSessionName.
                        , cas_libref = &outScenariosLib.
                        , cas_table = &outScenarioSetTable.);
   %end;

   /* create the output scenario set table */
   data &outLibref.."&outScenarioSetTable."n (rename=(scenario_name=scenarioName)
         %if %upcase("&promoteScenarioSet.") eq "Y" and &outEngine. = CAS %then %do;
            promote=yes
         %end;
         );
      length scenario_name &scenarioNameLength.;
      set &outLibref.._tmp_scenario_set_all;
      scenario_name=left(trim(scenarioName));
      drop scenarioName;
   run;

   %if &outEngine. = CAS %then %do;
      %core_cas_drop_table(cas_session_name = &casSessionName.
                        , cas_libref = &outScenariosLib.
                        , cas_table = _tmp_scenario_set_all);

      libname &outLibref. clear;
   %end;

%mend;