%macro corew_process_bep(solution =
                        , host =
                        , port =
                        , logonHost =
                        , logonPort =
                        , username =
                        , password =
                        , authMethod = bearer
                        , client_id =
                        , client_secret =
                        , cycleKey =
                        , analysisRunKey =
                        , base_dttm =
                        , allocationSchemeKey =
                        , inPortfolio =
                        , inScenarioSet =
                        , inScenarios =
                        , inBepScenMap =
                        , ds_out_bep_details = bep_details
                        , outGeneratedPositions = synthetic_positions
                        , outEliminatedPositions = eliminated_positions
                        , outPostRiskFref = postRisk
                        , outForecastTimeIntervalVar = forecast_time_interval
                        , asOfDate =
                        , portDataDefKey =
                        , configSetId =
                        , outCasLib =
                        , casSessionName = casauto
                        , outVarToken = accessToken
                        , debug = false
                         );

   %local   outCasLibref
            httpSuccess
            forecastTimeFlag
            bep_scen_map_flag scenario_group_list
            s
            gen_table_exists elim_table_exists
            allocSchemeParamName
            bepKey bep_name bepSegVars
            portSchemaName portSchemaVersion schemaNameHash found_configuration_flag;

   %if (%sysevalf(%superq(allocationSchemeKey) eq, boolean)) %then %do;
      %put NOTE: allocationSchemeKey was not provided - skipping BEP processing.;
      %return;
   %end;

   %if (%sysevalf(%superq(analysisRunKey) eq, boolean)) %then %do;
      %put ERROR: analysisRunKey is required.;
      %return;
   %end;

   %if (%sysevalf(%superq(portDataDefKey) eq, boolean)) %then %do;
      %put ERROR: portDataDefKey is required.;
      %return;
   %end;

   %if(%sysevalf(%superq(outForecastTimeIntervalVar) =, boolean)) %then
      %let outForecastTimeIntervalVar = forecast_time_interval;
   %if(not %symexist(&outForecastTimeIntervalVar.)) %then
      %global &outForecastTimeIntervalVar.;

   %let outCasLibref = %rsk_get_unique_ref(type = lib, engine = cas, args = caslib="&outCasLib." sessref=&casSessionName.);


   /************************************************/
   /* Get the allocation scheme and its linked BEP */
   /************************************************/
   /* Get the allocation scheme */
   %core_rest_get_alloc_scheme(solution = &solution.
                              , host = &host.
                              , port = &port.
                              , logonHost = &logonHost.
                              , logonPort = &logonPort.
                              , username = &username.
                              , password = &password.
                              , authMethod = &authMethod.
                              , client_id = &client_id.
                              , client_secret = &client_secret.
                              , key = &allocationSchemeKey.
                              , outds = alloc_scheme_summary
                              , outds_details = alloc_scheme_details
                              , debug = &debug.
                              , outVarToken = &outVarToken.
                              , outSuccess = httpSuccess
                              );

   /* Exit in case of errors */
   %if(not &httpSuccess.) %then %do;
      %put ERROR: Failed to get the allocation scheme with key &allocationSchemeKey.;
      %abort;
   %end;

   /* Exit if no errors but the allocation scheme was not found */
   %if %rsk_attrn(work.alloc_scheme_summary, NOBS) eq 0 %then %do;
      %put ERROR: Failed to find an allocation scheme with key &allocationSchemeKey.;
      %abort;
   %end;

   data _null_;
      set alloc_scheme_summary;
      call symputx("bepKey", bepDataKey, "L");
      call symputx("targetVar", targetVariable, "L");
   run;

   /* Exit if a linked BEP could not be found*/
   %if "&bepKey."="" %then %do;
      %put ERROR: Failed to find a Business Evolution Plan linked to allocation scheme with key &allocationSchemeKey.;
      %abort;
   %end;

   /* Get the BEP */
   %core_rest_get_bep(solution = &solution.
                     , host = &host.
                     , port = &port.
                     , logonHost = &logonHost.
                     , logonPort = &logonPort.
                     , username = &username.
                     , password = &password.
                     , authMethod = &authMethod.
                     , client_id = &client_id.
                     , client_secret = &client_secret.
                     , key = &bepKey.
                     , outds = bep_summary
                     , outds_details = _tmp_bep_details
                     , outSuccess = httpSuccess
                     , outVarToken = &outVarToken.
                     , debug = &debug.
                     );

   /* Exit in case of errors */
   %if(not &httpSuccess.) %then %do;
      %put ERROR: Failed to get the BEP with key &bepKey.;
      %abort;
   %end;

   data _null_;
      set bep_summary;
      call symputx("bep_name", name, "L");
      call symputx("&outForecastTimeIntervalVar.", intervalType);
   run;

   data _null_;
      set &outCasLibref.."&inScenarioSet."n;
      call symputx("forecastTimeFlag", coalescec(forecastTimeFlag, "N"), "L");
      stop;
   run;

   proc sql noprint;
      select distinct %sysfunc(ifc(&forecastTimeFlag.=Y,  catt('"', name, '"'), catt('"', scenarioName, '"')))
         into :scenario_group_list separated by ' '
      from &outCasLibref.."&inScenarioSet."n
      ;
   quit;

   %let bep_scen_map_flag=Y;
   %if(%sysevalf(%superq(inBepScenMap) eq, boolean)) %then %let bep_scen_map_flag=N;
   %else %if not %rsk_dsexist(&inBepScenMap.) %then %let bep_scen_map_flag=N;

   %if &bep_scen_map_flag.=N %then %do;
      %let inBepScenMap=bepScenarioMap;
      data &inBepScenMap.;
         length scenarioNames $200 bepScenarioName $200;
         stop;
      run;
   %end;

   /* Perform the scenario mappings on the BEP */
   data &ds_out_bep_details. (drop=scenarioNames);
      set _tmp_bep_details;
      if _n_=0 then
         set &inBepScenMap. (keep=scenarioNames);

      if _n_=1 then do;
         declare hash hBepScenMap(dataset: "&inBepScenMap. (rename=(bepScenarioName=scenario))");
         hBepScenMap.defineKey("scenario");
         hBepScenMap.defineData("scenarioNames");
         hBepScenMap.defineDone();
      end;

      /* If the scenario is missing in the BEP, those projections are for all scenarios.  Dupliate the row for each scenario in the run (only needed/done for the current target variable) */
      /* If the scenario has a value in the BEP, those projections are for a specific scenario - map on the appropriate scenario based on &inBepScenMap. for the run */
      if upcase(targetVar)=upcase("&targetVar.") and strip(scenario)="" then do;
         %do s=1 %to %sysfunc(countw(%bquote(&scenario_group_list.), " ", q));
            scenario=%scan(%bquote(&scenario_group_list.), &s., " ", q);
            output;
         %end;
      end;
      else do;
         if strip(scenario) ne "" then do;
            call missing(scenarioNames);
            __rc__ = hBepScenMap.find();
            if __rc__=0 then
               scenario=scenarioNames;
            drop __rc__;
         end;
         output;
      end;

   run;

   /* Get a space-separated list of that target variable's segmentation variables */
   data _null_;
      set _tmp_bep_details (where=(upcase(targetVar)=upcase("&targetVar.")));
      if strip(segVar1) ne "" then do;
         call symputx("bepSegVars", strip(catx(" ", segVar1, segVar2, segVar3, segVar4, segVar5)), "L");
         stop;
      end;
   run;


   /*******************************************************************/
   /* Get the allocation model code attachments from the analysis run */
   /*******************************************************************/
   /* Get the analysis run's parameters and their values */
   %core_rest_get_analysis_run(solution = &solution.
                              , host = &host.
                              , port = &port.
                              , logonHost = &logonHost.
                              , logonPort = &logonPort.
                              , username = &username.
                              , password = &password.
                              , authMethod = &authMethod.
                              , client_id = &client_id.
                              , client_secret = &client_secret.
                              , key = &analysisRunKey.
                              , outds = analysis_run
                              , outds_params = analysis_run_params
                              , outSuccess = httpSuccess
                              , outVarToken = &outVarToken.
                              , debug = &debug.
                              );

   /* Exit in case of errors */
   %if(not &httpSuccess.) %then %do;
      %put ERROR: Failed to get the parameters for analysis run &analysisRunKey.;
      %abort;
   %end;

   /* Get the name of the allocation scheme parameter (needed to identify the name of the allocation model code attachments) */
   data _null_;
      set analysis_run_params (where=(objectRestPath="allocationSchemes" and objectKey="&allocationSchemeKey."));
      call symputx("allocSchemeParamName", name, "L");
   run;

   /* Exit if the allocation scheme parameter was found for this analysis run */
   %if "&allocSchemeParamName." = "" %then %do;
      %put ERROR: Failed to find an allocation scheme parameter with key &allocationSchemeKey. in analysis run &analysisRunKey.;
      %abort;
   %end;

   /* Get the analysis run's model (execTypeCd=ALL) code attachments */
   %core_rest_get_file_attachment(solution = &solution.
                                 , host = &host.
                                 , port = &port.
                                 , logonHost = &logonHost.
                                 , logonPort = &logonPort.
                                 , username = &username.
                                 , password = &password.
                                 , authMethod = &authMethod.
                                 , client_id = &client_id.
                                 , client_secret = &client_secret.
                                 , objectKey = &analysisRunKey.
                                 , objectType = analysisRuns
                                 , attachmentNames = &allocSchemeParamName._genCode.sas|&allocSchemeParamName._postGenCode.sas|&allocSchemeParamName._postRiskCode.sas
                                 , outFileRefs = gen|postGen|&outPostRiskFref.
                                 , errorIfFileNotFound = N
                                 , getAttachmentContent = Y
                                 , outds = ar_alloc_model_attachments
                                 , outSuccess = httpSuccess
                                 , outVarToken = &outVarToken.
                                 , debug = &debug.
                                 );

   /* Exit in case of errors */
   %if(not &httpSuccess.) %then %do;
      %put ERROR: Failed to retrieve allocation model code attachments for analysis run &analysisRunKey.;
      %abort;
   %end;

   /* Exit if no generation code was found (required) */
   %if not %sysfunc(fexist(gen)) %then %do;
      %put ERROR: No generation code was found for the allocation model from allocation scheme with key &allocationSchemeKey.;
      %abort;
   %end;

   /* Delete the outGeneratedPositions/outEliminatedPositions tables if they already exists */
   %core_cas_drop_table(cas_session_name = &casSessionName.
                        , cas_libref = &outCasLib.
                        , cas_table = &outGeneratedPositions.);
   %core_cas_drop_table(cas_session_name = &casSessionName.
                        , cas_libref = &outCasLib.
                        , cas_table = &outEliminatedPositions.);


   /********************************************/
   /********* Run the allocation model *********/
   /********************************************/

   /* Run the allocation model's generation code */
   %include gen / source2 lrecl = 32000;
   filename gen clear;

   /* Check if the generation and elimination tables have been created.  If not, create empty tables */
   %rsk_dsexist_cas(cas_lib=&outCasLib.,cas_table=&outGeneratedPositions., cas_session_name=&casSessionName., out_var=gen_table_exists);
   %rsk_dsexist_cas(cas_lib=&outCasLib.,cas_table=&outEliminatedPositions., cas_session_name=&casSessionName., out_var=elim_table_exists);
   %if not &gen_table_exists. %then %do;
      %put WARNING: The allocation model (for allocation scheme with key &allocationSchemeKey.) did not produce required generation table &outCasLib..&outGeneratedPositions.;
      %put NOTE: An empty table will be created.;
      data &outCasLibref..&outGeneratedPositions.;
         length _inst_scenario_name_ varchar(200);
         set &outCasLibref..&inPortfolio.;
         stop;
      run;
   %end;
   %if not &elim_table_exists. %then %do;
      data &outCasLibref..&outEliminatedPositions.;
         length _inst_scenario_name_ varchar(200);
         set &outCasLibref..&inPortfolio.;
         stop;
      run;
   %end;

   /* If the generation code produced at least one of the &outGeneratedPositions. or &outEliminatedPositions. tables:
         1. Map the scenario short names to their actual scenario names for this run
         2. Set the _effectiveDate_ and _lastEffectiveDate_ RE variables
         3. Run the allocation model's postGen code (if any)
   */
   %if &gen_table_exists. or &elim_table_exists. %then %do;

      /* Create a map from the scenario set - this will map the synthetic instruments to an actual scenarioName in the scenario set like this:
            divergent (forecastTimeFlag=Y): scenarioSetName + forecast_time --> scenarioName (_inst_scenario_name_)
            common (forecastTimeFlag ne Y): scenarioName --> scenarioName (_inst_scenario_name_)
      */
      data &outCasLibref..scenario_set_map (keep=scenarioName _inst_scenario_name_ _inst_scenario_forecast_time_);
         length _inst_scenario_name_ varchar(200);
         set &outCasLibref.."&inScenarioSet."n;
         _inst_scenario_name_=strip(ifc("&forecastTimeFlag."="Y", name, scenarioName));
         rename forecast_time=_inst_scenario_forecast_time_;
      run;

      /* Perform the post-generation operations on the synthetic/eliminated instruments.  This includes:
            -setting _effectiveDate_ and _lastEffectiveDate_
            -setting the correct _inst_scenario_name_ for both the divergent and common use-cases (using the mapping table above)
            -carry eliminated/synthetic instruments over to subsequent forecast time scenarios (divergent)
         Note: The updated synthetic/eliminated positions and the scenario data are available for the postGen code */
      data  &outCasLibref..&outGeneratedPositions. (drop=__rc__ scenarioName _inst_scenario_name_orig_ _inst_scenario_ft_orig_ i _lastEffectiveDate_)
            &outCasLibref..&outEliminatedPositions. (drop=__rc__ scenarioName _inst_scenario_name_orig_ _inst_scenario_ft_orig_ i _effectiveDate_) ;
         length _inst_scenario_name_orig_ varchar(200) _inst_scenario_ft_orig_ 8. synthetic_instrument_flg varchar(3) forecast_interval varchar(32);
         format _effectiveDate_ _lastEffectiveDate_ yymmdd10.;
         set &outCasLibref..&outGeneratedPositions. (in=gen) &outCasLibref..&outEliminatedPositions. (in=elim);

         /* Add the scenarioName column from the lookup table */
         if _N_ = 0 then
            set &outCasLibref..scenario_set_map;

         /* Create the scenario lookup hash */
         if _N_ = 1 then do;
            declare hash hScenLookup(dataset: "&outCasLibref..scenario_set_map", multidata: "yes");
            hScenLookup.defineKey("_inst_scenario_name_"
               %if &forecastTimeFlag.=Y %then %do;
                  , "_inst_scenario_forecast_time_"
               %end;
               );
            hScenLookup.defineData("scenarioName");
            hScenLookup.defineDone();
         end;

         /* Perform the scenario lookup and overwrite _inst_scenario_name_ with the full scenario name value found */
         call missing(scenarioName);
         __rc__ = hScenLookup.find();

         _inst_scenario_name_orig_ = _inst_scenario_name_;
         _inst_scenario_ft_orig_ = _inst_scenario_forecast_time_;

         /* add interval type value for forecast period calculation in report*/
		   forecast_interval = "&&&outForecastTimeIntervalVar..";

         /* Output a row for every match at or after this forecast time in the scenario set (for this synthetic scenario) */
         i=0;
         do while(__rc__ eq 0);

            i=i+1;
            _inst_scenario_name_ = scenarioName;
            _inst_scenario_forecast_time_ = _inst_scenario_ft_orig_;

            if gen then do;
               synthetic_instrument_flg="Y";
               _effectiveDate_ = intnx("&&&outForecastTimeIntervalVar..", &asOfDate., _inst_scenario_forecast_time_, "S");
               output &outCasLibref..&outGeneratedPositions.;
            end;
            else do;
               synthetic_instrument_flg="E";
               /* Only set _lastEffectiveDate_ if forecast time is being used (divergent case).  If it is not being used (common case),
                  then we need to process the eliminated instrument at all horizons, even for its _inst_scenario_name_ scenario(s), in order to ensure
                  we can generate that instid's results for forecast times prior to the instid's _inst_scenario_forecast_time_ */
               %if &forecastTimeFlag.=Y %then %do;
                  _lastEffectiveDate_ = intnx("&&&outForecastTimeIntervalVar..", &asOfDate., _inst_scenario_forecast_time_-1, "S");
               %end;
               output &outCasLibref..&outEliminatedPositions.;
            end;

            %if &forecastTimeFlag.=Y %then %do;
               _inst_scenario_name_ = _inst_scenario_name_orig_;
               _inst_scenario_forecast_time_=_inst_scenario_ft_orig_+i;
               __rc__ = hScenLookup.find();
            %end;
            %else %do;
               __rc__ = -1;
            %end;

         end;

      run;

      /* Create a table with all eliminated instids at all scenarios with no _lastEffectiveDate_ set - used after the postGen code */
      data _null_;
         set &outCasLibref..&inScenarioSet.;
         call symputx(catt("scenario_name_", _N_), scenarioName, "L");
         call symputx("scenario_cnt", _N_, "L");
      run;

      data &outCasLibref..elim_insts_all_scenarios;
         set &outCasLibref..&outEliminatedPositions. (drop = _lastEffectiveDate_);
         by instid;
         if first.instid then do;
            %do s=1 %to &scenario_cnt.;
               _inst_scenario_name_="&&scenario_name_&s..";
               synthetic_instrument_flg="N";
               output;
            %end;
         end;
      run;

      /* Run the allocation model's post-generation code (if any) */
      %if %sysfunc(fexist(postGen)) %then %do;
         %include postGen / source2 lrecl = 32000;
         filename postGen clear;
      %end;

      /* Some final preparing of the eliminated instruments - if an instrument is eliminated for specific scenarios,
      we have to ensure that it also exists in the portfolio for the other scenarios with no _lastEffectiveDate_ set
      to ensure that RE will process it at all horizons for those scenarios.
         Ex: scenarios are Adverse, Basecase, Severe
             eliminated instrument is:
               instid="i1";   _inst_scenario_name_="Adverse";    _lastEffectiveDate_=31AUG2023;
            The above row says that instid "i1" will be processed in the Adverse scenario until the horizon (_date_) reaches 31AUG2023.
            However, we also need to add these additional rows to ensure instid "i1" is processed in the Basecase and Severe scenarios (at all horizons):
               instid="i1";   _inst_scenario_name_="Basecase";   _lastEffectiveDate_=.;
               instid="i1";   _inst_scenario_name_="Severe";     _lastEffectiveDate_=.;
      */
      data &outCasLibref..&outEliminatedPositions.;
         merge &outCasLibref..elim_insts_all_scenarios &outCasLibref..&outEliminatedPositions.;
         by instid _inst_scenario_name_;
      run;

   %end;


   /* Promote the outGenerationTable (if it is not already promoted in the postGen code) */
   %rsk_dsexist_cas(cas_lib=&outCasLib.,cas_table=&outGeneratedPositions., cas_session_name=&casSessionName., out_var=gen_table_exists);
   %rsk_dsexist_cas(cas_lib=&outCasLib.,cas_table=&outEliminatedPositions., cas_session_name=&casSessionName., out_var=elim_table_exists);
   %if &gen_table_exists.=1 %then %do;
      /* &outGeneratedPositions. is session-level scope.  Promote it for riskData to find it. */
      proc casutil;
         promote inCaslib="&outCasLib."  casData="&outGeneratedPositions."
                 outCaslib="&outCasLib." casOut="&outGeneratedPositions.";
         run;
      quit;
   %end;
   %if &elim_table_exists.=1 %then %do;
      /* &outEliminatedPositions. is session-level scope.  Promote it. */
      proc casutil;
         promote inCaslib="&outCasLib."  casData="&outEliminatedPositions."
               outCaslib="&outCasLib." casOut="&outEliminatedPositions.";
         run;
      quit;
   %end;


   /******************************************************************************/
   /********* Create Synthetic Instruments Data Definition/Analysis Data *********/
   /******************************************************************************/
   /* Get the datastore_config table structure */
   %core_rest_get_config_table(solution = &solution.
                           , host = &host.
                           , port = &port.
                           , logonHost = &logonHost.
                           , logonPort = &logonPort.
                           , username = &username.
                           , password = &password.
                           , authMethod = &authMethod.
                           , client_id = &client_id.
                           , client_secret = &client_secret.
                           , configSetId = &configSetId.
                           , configTableType = datastore_config
                           , outds_configTablesInfo = configTablesInfo
                           , outds_configTablesData = datastore_config
                           , outSuccess = httpSuccess
                           , outVarToken = &outVarToken.
                           , debug = &debug.
                           );

   /* Exit in case of errors */
   %if(not &httpSuccess.) %then %do;
      %put ERROR: Failed to get the datastore_config configuration table from config set ID &configSetId.;
      %abort;
   %end;

   /* Build the synthetic positions data definition to be similar to the datastore_config entry for the portDataDefKey data definition.
   If portDataDefKey's data definition's schema name/version is not found in datastore_config, create a generic synthetic positions data definition
   using its schemaName/schemaVersion. */
   %let found_configuration_flag=0;
   %core_rest_get_data_def(solution = &solution.
                           , host = &host.
                           , port = &port.
                           , logonHost = &logonHost.
                           , logonPort = &logonPort.
                           , username = &username.
                           , password = &password.
                           , authMethod = &authMethod.
                           , client_id = &client_id.
                           , client_secret = &client_secret.
                           , key = &portDataDefKey.
                           , outds = data_definitions
                           , outSuccess = httpSuccess
                           , outVarToken = &outVarToken.
                           , debug = &debug.
                           );

   /* Exit in case of errors */
   %if(not &httpSuccess.) %then %do;
      %put ERROR: Failed to get the portfolio data definition with key &portDataDefKey.;
      %abort;
   %end;

   data _null_;
      set data_definitions;
      call symputx("portSchemaName", schemaName, "L");
      call symputx("portSchemaVersion", schemaVersion, "L");
      call symputx("schemaNameHash", substr(hashing('md5', upcase(strip(schemaName))), 1, 8), "L");
   run;

   data datastore_config;
      set datastore_config (where=(schemaName="&portSchemaName." and schemaVersion="&portSchemaVersion."));
      call symputx("found_configuration_flag", 1, "L");
   run;

   /* Create a 1-row datastore config override table with the synthetic positions data definition/analysis data info */
   data work.datastore_config_override;

      %if &found_configuration_flag. %then %do;
         set datastore_config;
      %end;
      %else %do;
         if _N_=0 then set datastore_config;
      %end;

      schemaName="BEP_SYNTH_&schemaNameHash.";
      schemaVersion="&portSchemaVersion.";
      sourceLibref="&outCasLib.";
      targetTableName="&outGeneratedPositions.";
      dataDefinitionName="ST BEP Synthetic Instrument Definition (Portfolio schema: &portSchemaName._&portSchemaVersion.)";
      dataDefinitionDesc="ST BEP Synthetic Instrument Definition (Portfolio schema: &portSchemaName._&portSchemaVersion.)";
      analysisDataName="Synthetic Instruments (&bep_name.) <MONTH, 0, SAME, yymmddd10.>";
      analysisDataDesc="Synthetic Instruments (&bep_name.) for the base date <MONTH, 0, SAME, yymmddd10.>.\nCreated by user &SYS_COMPUTE_SESSION_OWNER.. on %sysfunc(datetime(), nldatmw200.)";
      schemaTypeCd=coalescec(schemaTypeCd, "FLAT");
      strategyType=coalescec(strategyType, "BYVARS");
      riskTypeCd="CREDIT";
      businessCategoryCd="ST";
      dataCategoryCd="SYNTHETIC_INSTRUMENT";
      dataStoreGroupId="";
      reportmartGroupId="";
      primaryKey=ifc(strip(primaryKey)="", "", catt(primaryKey, " _INST_SCENARIO_NAME_"));
      whereClauseFilterInput="";
      sourceLibname="";

   run;

   /* Create the synthetic instruments data definition and analysis data objects */
   %corew_store_analysisdata(solution = &solution.
                           , host = &host.
                           , port = &port.
                           , logonHost = &logonHost.
                           , logonPort = &logonPort.
                           , username = &username.
                           , password = &password.
                           , authMethod = &authMethod.
                           , client_id = &client_id.
                           , client_secret = &client_secret.
                           , sourceSystemCd = &solution.
                           , cycle_key = &cycleKey.
                           , analysis_run_key = &analysisRunKey.
                           , configSetId = &configSetId.
                           , casSessionName = &casSessionName.
                           , base_dttm = &base_dttm.
                           , ovrDatastoreConfig = work.datastore_config_override
                           , outSuccess = httpSuccess
                           , outVarToken = &outVarToken.
                           , debug = &debug.
                           );

   libname &outCasLibref. clear;

%mend;
