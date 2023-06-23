%macro core_rest_export_rsm_scen_set(host =
                                    , server = riskScenarios
                                    , port =
                                    , logonHost =
                                    , logonPort =
                                    , username =
                                    , password =
                                    , authMethod = bearer
                                    , client_id =
                                    , client_secret =
                                    , scenarioSetId =
                                    , baselineId =
                                    , includeScenarioHistory = true
                                    , dateBasedFormat = false
                                    , outScenariosCasLib = casuser
                                    , outScenarioSetDs = scenario_set
                                    , outScenariosDs = scenarios
                                    , outExportResponseDs = scenarios_export_info
                                    , casSessionName =
                                    , replaceOutScenarios = Y
                                    , outVarToken = accessToken
                                    , outSuccess = httpSuccess
                                    , outResponseStatus = responseStatus
                                    , debug = false
                                    , logOptions =
                                    , restartLUA = Y
                                    , clearCache = Y
                                    );

   %local scenario_id_list;

   %if(%sysevalf(%superq(scenarioSetId) eq, boolean)) %then %do;
      %put ERROR: scenarioSetId must be provided;
      %abort;
   %end;

   /************************/
   /* Get the scenario set */
   /************************/
   %let &outSuccess. = 0;
   %let &outResponseStatus. = Not Set - Before Scenario Set Get Request;
   %core_rest_get_rsm_scenario_set(host =
                                 , server = &server.
                                 , port = &port.
                                 , logonHost = &logonHost.
                                 , logonPort = &logonPort.
                                 , username = &username.
                                 , password = &password.
                                 , authMethod = &authMethod.
                                 , client_id = &client_id.
                                 , client_secret = &client_secret.
                                 , id = &scenarioSetId.
                                 , outds = &outScenarioSetDs.
                                 , outVarToken = &outVarToken.
                                 , outSuccess = &outSuccess.
                                 , outResponseStatus = &outResponseStatus.
                                 , debug = &debug.
                                 , logOptions = &logOptions.
                                 , restartLUA = &restartLUA.
                                 , clearCache = &clearCache.
                                 );

    /* Stop macro execution if there were any errors */
    %if(&&&outSuccess.. = 0 or not %rsk_dsexist(&outScenarioSetDs.)) %then %do;
        %put ERROR: Failed to retrieve RSM scenario set with ID: &scenarioSetId.;
        %abort;
    %end;

   /* Get the IDs of the scenarios in the scenario set */
   %let scenario_id_list=;
   proc sql noprint;
      select catt('"', scenarioId, '"') into: scenario_id_list separated by ","
      from &outScenarioSetDs.
      ;
   quit;

   %if %bquote(&scenario_id_list.)= or %bquote(&scenario_id_list.)="" %then %do;
      %put ERROR: No scenarios were found in scenario set: &scenarioSetId.;
      %abort;
   %end;

   /**********************************/
   /* Get other needed scenario info */
   /**********************************/
   %core_rest_get_rsm_scen_summary(host =
                                 , server = &server.
                                 , port = &port.
                                 , logonHost = &logonHost.
                                 , logonPort = &logonPort.
                                 , username = &username.
                                 , password = &password.
                                 , authMethod = &authMethod.
                                 , client_id = &client_id.
                                 , client_secret = &client_secret.
                                 , filter = in(id,&scenario_id_list.)
                                 , outds = scenario_summary
                                 , debug = &debug.
                                 , outVarToken = &outVarToken.
                                 , outSuccess = &outSuccess.
                                 , outResponseStatus = &outResponseStatus.
                                 , logOptions = &logOptions.
                                 , restartLUA = &restartLUA.
                                 , clearCache = &clearCache.
                                 );

   /* Stop macro execution if there were any errors */
   %if(&&&outSuccess.. = 0 or not %rsk_dsexist(scenario_summary)) %then %do;
      %put ERROR: Failed to retrieve RSM scenario info for scenarios with IDs: &scenario_id_list.;
      %abort;
   %end;

   proc sort data=&outScenarioSetDs.; by scenarioId; run;
   proc sort data=scenario_summary; by id; run;
   data &outScenarioSetDs.;
      merge &outScenarioSetDs. scenario_summary (keep=id periodType rename=(id=scenarioId));
      by scenarioId;
   run;

   /*******************************/
   /* Export the scenarios to CAS */
   /*******************************/
   %let &outSuccess. = 0;
   %let &outResponseStatus. = Not Set - Before Scenarios Export Request;
   %core_rest_export_rsm_scenario(host =
                                 , server = &server.
                                 , port = &port.
                                 , logonHost = &logonHost.
                                 , logonPort = &logonPort.
                                 , username = &username.
                                 , password = &password.
                                 , authMethod = &authMethod.
                                 , client_id = &client_id.
                                 , client_secret = &client_secret.
                                 , scenarioIdsArray = %bquote([&scenario_id_list.])
                                 , baselineId = &baselineId.
                                 , includeScenarioHistory = &includeScenarioHistory.
                                 , dateBasedFormat = &dateBasedFormat.
                                 , casSessionName = &casSessionName.
                                 , replaceOutScenarios = &replaceOutScenarios.
                                 , outScenariosCasLib = &outScenariosCasLib.
                                 , outScenariosDs = &outScenariosDs.
                                 , outExportResponseDs = &outExportResponseDs.
                                 , outVarToken = &outVarToken.
                                 , outSuccess = &outSuccess.
                                 , outResponseStatus = &outResponseStatus.
                                 , debug = &debug.
                                 , logOptions = &logOptions.
                                 , restartLUA = &restartLUA.
                                 , clearCache = &clearCache.
                                 );

   /* Stop macro execution if there were any errors */
   %if(&&&outSuccess.. = 0) %then
      %abort;

%mend;