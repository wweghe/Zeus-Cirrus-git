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
                              , inSasScenarioFtMap =
                              , outScenarios =
                              , outScenarioSet = scenario_set
                              , promoteScenarios = Y
                              , promoteScenarioSet = Y
                              , outCasLib = Public
                              , casSessionName = casauto
                              , outVarToken = accessToken
                              , debug = false
                              );

   %local   outCasLibref httpSuccess responseStatus outScenariosCasLib outScenarioSetCasLib outScenariosCasTable outScenarioSetCasTable
            currScenario interval asOfDate scenarioSetId num_scenario_sets oldIds i;

   %if(%sysevalf(%superq(outScenarios) eq, boolean)) %then %do;
      %put ERROR: outScenarios is required;
      %abort;
   %end;

   %if (%sysevalf(%superq(scenarioSetIds) eq, boolean)) %then %do;
      %put ERROR: no eligible scenarios to prepare Credit Risk Analysis.;
      %abort;
   %end;

   %let outScenariosCasLib = &outCasLib.;
   %let outScenariosCasTable = &outScenarios.;

   %let outScenarioSetCasLib = &outCasLib.;
   %let outScenarioSetCasTable = &outScenarioSet.;

   %if(%sysevalf(%superq(dateBasedFormat) eq, boolean)) %then
      %let dateBasedFormat = false;

   /* If inSasScenarioFtMap, make sure it has the forecast_time/synthetic_scenario_name column (can happen if the user doesn't specify any
    values in the UI spreadsheet parameter that creates this table) */
   %if(%sysevalf(%superq(inSasScenarioFtMap) ne, boolean)) %then %do;
      data &inSasScenarioFtMap.;
         set &inSasScenarioFtMap.;
         %if (not %rsk_varexist(&inSasScenarioFtMap., forecast_time)) %then %do;
            length forecast_time 8.;
         %end;
         %if (not %rsk_varexist(&inSasScenarioFtMap., synthetic_scenario_name)) %then %do;
            length synthetic_scenario_name $64;
         %end;
      run;
   %end;

   /********************************************************************/
   /* Export all of the scenarios in the scenario sets to a CAS table. */
   /********************************************************************/
   %let outCasLibref = %rsk_get_unique_ref(type = lib, engine = cas, args = caslib="&outScenariosCasLib." sessref=&casSessionName.);

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
                                    , outScenarioSetDs = _tmp_scenario_set
                                    , outExportResponseDs = scenarios_export_info
                                    , outScenariosCasLib = &outScenariosCasLib.
                                    , outScenariosDs = _tmp_scenarios
                                    , replaceOutScenarios = Y
                                    , casSessionName = &casSessionName.
                                    , debug = &debug.
                                    , outVarToken = &outVarToken.
                                    , outSuccess = httpSuccess
                                    , outResponseStatus = responseStatus
                                    );

      /* Stop macro execution if there were any errors */
      %rsk_dsexist_cas(cas_lib=%superq(outScenariosCasLib),cas_table=_tmp_scenarios, cas_session_name=&casSessionName.);
      %if(&httpSuccess. = 0 or not &cas_table_exists.) %then %do;
         %put ERROR: Failed to export RSM scenario set with ID: &scenarioSetId.;
         %abort;
      %end;

      /* Interval and asOfDate must be the same for all scenarios in the scenario set, so get them here */
      /* currScenario is 1 scenario name that is used later to ensure we get only 1 scenario's current horizon data,
      since all scenarios should have the same current horizon data and we must have only 1 row of data in the final scenarios */
      data &outCasLibref.._tmp_scenario_set (
            drop=forecast_time
            rename=(forecast_time_num=forecast_time synthetic_scenario_name_new=synthetic_scenario_name)
            );

         length forecast_time_num 8 synthetic_scenario_name_new varchar(64);
         set _tmp_scenario_set;
         retain forecast_time_flag 0;

         /* If a scenario-to-forecast_time mapping is given, make the lookup here to add forecast_time to the scenario set. */
         %if(%sysevalf(%superq(inSasScenarioFtMap) ne, boolean)) %then %do;

            if _N_ = 0 then
               set &inSasScenarioFtMap. (keep=scenarioId forecast_time synthetic_scenario_name);

            if _N_ = 1 then do;
               declare hash hFT(dataset: "&inSasScenarioFtMap.");
               hFT.defineKey("scenarioId");
               hFT.defineData("forecast_time", "synthetic_scenario_name");
               hFT.defineDone();
            end;

            drop __rcFT__;
            call missing(forecast_time);
            __rcFT__ = hFT.find();

            synthetic_scenario_name_new=strip(synthetic_scenario_name);
            drop synthetic_scenario_name;

            if vtype(forecast_time)="C" then
               forecast_time_num=input(forecast_time, 8.);
            else
               forecast_time_num=forecast_time;
         %end;
         %else %do;
            forecast_time="";
            forecast_time_num=.;
         %end;

         /* If at least one forecast_time value is non-missing, then the analysis is using forecast times. */
         if forecast_time_num ne . then
            forecast_time_flag=1;

         /* Verify this scenario set's asOfDate value matches the asOfDate for other scenario sets */
         %if "&asOfDate." ne "" %then %do;
            if input(asOfDate, YYMMDD10.) ne &asOfDate. then do;
               put "ERROR: asOfDate for scenario set &scenarioSetId. is " asOfDate ".";
               put "ERROR: asOfDate is &asOfDate. for 1 or more of these scenario sets: &scenarioSetIds..";
               put "ERROR: The asOfDate must the be same for all provided scenario sets.";
               abort;
            end;
         %end;
         if not missing(scenarioVersion) then do;
            scenarioName = catt(scenarioName, "/", scenarioVersion);
         end;
         call symputx("interval", periodType, "L");
         call symputx("asOfDate", input(asOfDate, YYMMDD10.), "L");
         call symputx("currScenario", scenarioName, "L");
      run;

      data &outCasLibref.._tmp_scenarios;
         set &outCasLibref.._tmp_scenarios;
         if not missing(scenario_version) then do;
            scenario_name = catt(scenario_name, "/", scenario_version);
         end;
      run;

      %let oldIds='';
      %let scenarioNames='';
      %if &i. ne 1 %then %do;
         proc sql;
            select distinct catt("'",scenarioId,"'"), catt("'",scenarioName,"'") into :oldIds separated by ',', :scenarioNames separated by ','
            from &outCasLibref..scenario_set
            ;
         quit;
      %end;

      /* append each scenario set's info to the final scenario_set table */
      data &outCasLibref..scenario_set;
         set
            %if &i. ne 1 %then %do;
               &outCasLibref..scenario_set
            %end;
            %if &oldids ne '' %then %do;
               &outCasLibref.._tmp_scenario_set (where=(scenarioId not in (&oldIds)));
            %end;
            %else %do;
               &outCasLibref.._tmp_scenario_set;
            %end;
      run;

      /* drop the &outScenarios. table if it exists */
      %if &i.=1 %then %do;

         %core_cas_drop_table(cas_session_name = &casSessionName.
                              , cas_libref = &outScenariosCasLib.
                              , cas_table = &outScenariosCasTable.);

      %end;

      /* Produce the final CAS scenarios into a common format, regardless of if the scenarios in RSM are in
      date-based format or not */
      %if &dateBasedFormat. = true %then %do;

         data &outCasLibref.."&outScenariosCasTable."n
            %if &i.=&num_scenario_sets. %then %do;
               ( rename=(_date=date)
               %if %upcase("&promoteScenarios.") eq "Y" %then %do;
                  promote=yes
               %end;
               )
            %end;
            ;

            format horizon 8. interval $10. _type_ $5. _date YYMMDD10.;

            set
               %if &i. ne 1 %then %do;
                  &outCasLibref.."&outScenariosCasTable."n (in=base)
               %end;
               %if &scenarioNames ne '' %then %do;
                  &outCasLibref.._tmp_scenarios (in=new where=(scenario_name not in (&scenarioNames.)));
               %end;
               %else %do;
                  &outCasLibref.._tmp_scenarios (in=new);
               %end;

            %if &i. ne 1 %then %do;
               if base then output;
            %end;
            if new then do;

               _date=intnx("&interval.", input(date, YYMMDD10.), 0, "SAME");    /* Temporary workaround for RSM bug */
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
         data &outCasLibref.."&outScenariosCasTable."n
            %if &i.=&num_scenario_sets. %then %do;
               %if %upcase("&promoteScenarios.") eq "Y" %then %do;
                  (promote=yes)
               %end;
            %end;
            ;

            format horizon 8. date YYMMDD10.;

            set
               %if &i. ne 1 %then %do;
                  &outCasLibref.."&outScenariosCasTable."n (in=base)
               %end;
               %if &scenarioNames ne '' %then %do;
                  &outCasLibref.._tmp_scenarios (in=new where=(scenario_name not in (&scenarioNames.)));
               %end;
               %else %do;
                  &outCasLibref.._tmp_scenarios (in=new);
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

   /* Drop the temporary scenarios CAS table */
   %core_cas_drop_table(cas_session_name = &casSessionName.
                     , cas_libref = &outScenariosCasLib.
                     , cas_table = _tmp_scenarios);

   /* Drop the scenario set CAS table, if it exists */
   %core_cas_drop_table(cas_session_name = &casSessionName.
                     , cas_libref = &outScenarioSetCasLib.
                     , cas_table = &outScenarioSetCasTable.);

   /* promote the scenario_set table and convert scenario_name to a varchar (instead of char) */
   libname &outCasLibref. cas caslib="&outScenarioSetCasLib.";
   data &outCasLibref.."&outScenarioSetCasTable."n (rename=(scenario_name=scenarioName)
         %if %upcase("&promoteScenarioSet.") eq "Y" %then %do;
            promote=yes
         %end;
         );
      length scenario_name varchar(200);
      set &outCasLibref..scenario_set;
      scenario_name=left(trim(scenarioName));
      drop scenarioName;
   run;

   libname &outCasLibref. clear;

%mend;