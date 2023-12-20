%macro corew_run_bep_overlay_model(solution =
                                 , host =
                                 , port =
                                 , logonHost =
                                 , logonPort =
                                 , username =
                                 , password =
                                 , authMethod = bearer
                                 , client_id =
                                 , client_secret =
                                 , analysisRunKey =
                                 , bepKey =
                                 , inScenariosDs =
                                 , inScenarioMap =
                                 , inScenarioName =
                                 , bepModelValue = absoluteValue
                                 , outBepStatusCd =
                                 , outBepDs = bep_details_new
                                 , outVarToken = accessToken
                                 , debug = false
                                  );

   %local   i bepModelFref bepModelWrapperFref bepTargetVars bepSegVarsValue bepSegVarsValueCsv bepHorizonCols targetVar
            inBepDs inBepDsScens
            modelKey modelParamName
            scen_map_flag bepScenarioMap bepScenarios
            bepNumHorizons bepInterval bepDataArray scenarioDataArray outBepOverlayDs outSasLib
            scenNum scenario_name_list scenName segNum
            analysisRunName bepCreateTime;

   /* Verify/set input parameters */
   %if (%sysevalf(%superq(analysisRunKey) eq, boolean)) %then %do;
      %put ERROR: analysisRunKey is required.;
      %abort;
   %end;

   %if (%sysevalf(%superq(bepKey) eq, boolean)) %then %do;
      %put ERROR: bepKey is required.;
      %abort;
   %end;

   %if (%sysevalf(%superq(inScenarioMap) eq, boolean)) and (%sysevalf(%superq(inScenarioName) eq, boolean))  %then %do;
      %put ERROR: inScenarioMap or inScenarioName is required.;
      %abort;
   %end;

   %let bepModelValue = %sysfunc(coalescec(&bepModelValue., absoluteValue));
   %if %upcase(&bepModelValue.) ne ABSOLUTEVALUE and %upcase(&bepModelValue.) ne RELATIVEVALUE  %then %do;
      %put ERROR: bepModelValue must be absoluteValue or relativeValue (current value is: &bepModelValue. );
      %abort;
   %end;

   %let outBepDs = %sysfunc(coalescec(&outBepDs., bep_details_new));

   %let outSasLib=WORK;
   %if "&debug." = "true" %then %do;
      libname arpvc "&__CORE_AR_DIR__.";
      %let outSasLib = arpvc;
   %end;

   /***********************************/
   /* Get the analysis run parameters */
   /***********************************/
   data object_param_output_tables;
      length objectType $64 outputDs $128;
      objectType="models"; outputDs="work.model_summary";   /* Get all model object parameters in the analysis run into table work.model_summary */
   run;

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
                              , outObjParamsConfig = work.object_param_output_tables
                              , outSuccess = httpSuccess
                              , outVarToken = &outVarToken.
                              , debug = &debug.
                              );

   /* Exit in case of errors */
   %if(not &httpSuccess.) %then %do;
      %put ERROR: Failed to get the parameters for analysis run &analysisRunKey.;
      %abort;
   %end;

   /* Exit if no model parameters were found for this analysis run */
   %if not %rsk_dsexist(work.model_summary) or %rsk_attrn(work.model_summary, nobs) = 0 %then %do;
      %put ERROR: Failed to find any model parameters for analysis run &analysisRunKey.;
      %abort;
   %end;

   /* Get the analysis run name */
   data _null_;
      set analysis_run;
      call symputx("analysisRunName", name, "L");
   run;

   /* Get the key of the BEP Overlay model (type=BEPOVERLAY) parameter */
   data _null_;
      set model_summary;
      if execTypeCd="BEPOVERLAY" then
         call symputx("modelKey", key, "L");
   run;

   /* Get the name of the BEP Overlay model parameter */
   data _null_;
      set analysis_run_params (where=(objectRestPath="models" and objectKey = "&modelKey."));
      call symputx("modelParamName", name, "L");
   run;

   %if "&modelParamName." = "" %then %do;
      %put ERROR: Failed to find a Business Evolution Plan Adjustment model (type=BEPOVERLAY) parameter on analysis run &analysisRunKey.;
      %abort;
   %end;

   /* Get the analysis run's model (execTypeCd=BEPOVERLAY) code attachments */
   %let bepModelFref = %rsk_get_unique_ref(prefix = bep, engine = temp);
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
                                 , attachmentNames = &modelParamName._modelCode.sas
                                 , outFileRefs = &bepModelFref.
                                 , errorIfFileNotFound = Y
                                 , getAttachmentContent = Y
                                 , outds = analysis_run_attachments
                                 , outSuccess = httpSuccess
                                 , outVarToken = &outVarToken.
                                 , debug = &debug.
                                 );

   /* Exit in case of errors */
   %if(not &httpSuccess.) %then %do;
      %put ERROR: Failed to retrieve BEP overlay code attachment for analysis run &analysisRunKey.;
      %abort;
   %end;

   /* Add a macro wrapper around the BEP model code */
   %let bepModelWrapperFref = %rsk_get_unique_ref(prefix = bep, engine = temp);
   data _null_;
      infile &bepModelFref end=eof lrecl = 32000;
      file &bepModelWrapperFref. lrecl = 32000;
      input;
      if _n_=1 then
         put '%macro corew_temp_run_bep_overlay_code;';
      put _infile_;
      if eof then do;
         put '%mend;';
         put '%corew_temp_run_bep_overlay_code;';
      end;
   run;


   /***************************/
   /* BEP/Scenario data setup */
   /***************************/
   /**************** BEP SETUP *****************/
   /* Get the BEP */
   %let inBepDs=bep_details_orig;
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
                     , outds_details = &inBepDs.
                     , outds_link_instances = bep_link_instances
                     , outds_target_vars = bep_target_vars
                     , outSuccess = httpSuccess
                     , outVarToken = &outVarToken.
                     , debug = &debug.
                     );

   /* Exit in case of errors */
   %if(not &httpSuccess.) %then %do;
      %put ERROR: Failed to get the BEP with key &bepKey.;
      %abort;
   %end;

   /* Get the interval for the BEP */
   data _null_;
      set bep_summary;
      call symputx("bepInterval", intervalType, "L");
      call symputx("bepNumHorizons", intervalCount, "L");
   run;

   /* Set BEP-related macrovariables */
   %let bepSegVarsValue=segVar1Value segVar2Value segVar3Value segVar4Value segVar5Value;
   %let bepSegVarsValueCsv=%sysfunc(prxchange(s/\s+/%str(, )/i, -1, &bepSegVarsValue.));
   %let bepHorizonCols=;
   %do i=0 %to &bepNumHorizons.;
      %let bepHorizonCols=&bepHorizonCols. "T_&i.";
   %end;

   /* add an order column to the original BEP to preserve its order when it is recreated with the adjustments */
   data &inBepDs.;
      set &inBepDs.;
      order=_n_;
   run;

  /* Get the scenarios for the BEP */
   proc sql noprint;
      select distinct scenario into :bepScenarios separated by ' '
      from &inBepDs.
      ;
   quit;

   %let inBepDsScens=&inBepDs.;
   %if "%trim(&bepScenarios.)" ne "" %then %do;
      %let inBepDsScens=in_bep_ds_scens;
      data &inBepDsScens.;
         set &inBepDs. (where=(scenario ne ""));
      run;
   %end;

   /* Prepare the BEP scenario map table */
   %let scen_map_flag=Y;
	%if (%sysevalf(%superq(inScenarioMap) eq, boolean)) %then
		%let scen_map_flag=N;
	%else %if not(%rsk_dsexist(&inScenarioMap.)) %then
		%let scen_map_flag=N;
	%else %if %rsk_getattr(&inScenarioMap., NOBS) eq 0 %then
		%let scen_map_flag=N;

   %let bepScenarioMap=&inScenarioMap.;
   %if "&scen_map_flag."="N" %then %do;
      %if "&inScenarioName." ne "" %then %do;
         data bepScenarioMap;
            length fromScenarioName toScenarioName $200;
            fromScenarioName=""; toScenarioName="&scenarioName.";
         run;
         %let bepScenarioMap=work.bepScenarioMap;
      %end;
      %else %do;
         %put ERROR: inScenarioMap (with at least 1 valid scenario mapping) or inScenarioName must be provided;
         %abort;
      %end;
   %end;

   data bep_details_mapped (keep=scenario key activationHorizon modelValue)
        bep_details_mapped_orig (drop=_rc_ key);

      length key $10000 modelValue 8;

      set &inBepDsScens. (where=(intervalName ne "" ));

      if _n_=0 then set &bepScenarioMap.;

      if _n_=1 then do;
         declare hash hScenMap(dataset: "&bepScenarioMap.");
         hScenMap.defineKey("fromScenarioName");
         hScenMap.defineData("toScenarioName");
         hScenMap.defineDone();
      end;

      key=strip(catx("|||", targetVar, &bepSegVarsValueCsv.));
      modelValue=ifn(activationHorizon=0,horizon0,&bepModelValue.);

      fromScenarioName=scenario;
      call missing(toScenarioName);
      _rc_=hScenMap.find();
      scenario=toScenarioName;

   run;

   proc sort data=bep_details_mapped; by scenario key activationHorizon; run;
   proc transpose data=bep_details_mapped out=bep_data_array_all (drop=_NAME_) prefix=T_;
      id activationHorizon;
      by scenario key;
   run;

   /**************** SCENARIO SETUP *****************/
   data _null_;
      set &bepScenarioMap.;
      if strip(toScenarioName)="" then do;
         put "ERROR: Scenario " fromScenarioName " does not have a mapped scenario.";
         abort;
      end;
   run;

   proc sql noprint;
      select distinct catt('"', toScenarioName, '"') into :scenario_name_list separated by ' '
      from &bepScenarioMap.
      ;
   quit;

   /*
      1. Filter the scenario data - only keep horizons at or greater than 0 and scenarios in the BEP-scenario map
      2. Expand the scenario data at h=0 - add an entry for each scenario.
   */
   data scenario_data;
      set &inScenariosDs. ( where=(horizon>=0 and scenario_name in ("" &scenario_name_list.)) );
      if horizon=0 then do;
         %do i=1 %to %sysfunc(countw(&scenario_name_list., %str( ), q));
            scenario_name=%scan(&scenario_name_list., &i., %str( ), q); output;
         %end;
      end;
      else output;
   run;

   /* Transpose the scenario data into an array form for IML: #variables * #horizons for each scenario */
   proc sort data=scenario_data; by scenario_name variable_name horizon; run;
   proc transpose data=scenario_data
         out=scenario_data_array_all (drop=_name_ _label_)
         prefix=T_;
      var change_value;
      id horizon;
      by scenario_name variable_name;
   run;

   proc datasets library = work nolist nodetails nowarn;
      delete scenario_data;
   quit;


   /*****************************/
   /* Run the BEP Overlay model */
   /*****************************/
   /* Loop over all scenarios - the BEP overlay model code is executed for 1 scenario at a time */
   %do scenNum=1 %to %sysfunc(countw(&scenario_name_list., %str( ), q));

      /* Filter the BEP and scenario data to just this scenario */
      %let scenName=%sysfunc(dequote(%scan(&scenario_name_list., &scenNum., %str( ), q)));

      /* Get the target variables from the BEP for this scenario */
      proc sql noprint;
         select distinct targetVar into :bepTargetVars separated by ' '
         from bep_details_mapped_orig
         where upcase(scenario)=upcase("&scenName.")
         ;
      quit;

      /* Get the BEP segmentation variables for each target variable for this scenario into &targetVar._segVars */
      %do i=1 %to %sysfunc(countw(&bepTargetVars., %str( )));

         %let targetVar=%scan(&bepTargetVars., &i., %str( ));
         data _null_;
            set bep_details_mapped_orig( where=(upcase(targetVar)="&targetVar." and upcase(scenario)=upcase("&scenName.")) );
            if _n_=1 then
               call symputx("&targetVar._segVars", coalescec(upcase(strip(catx(" ", segVar1, segVar2, segVar3, segVar4, segVar5))), "<none-specified>"), "L");
         run;

      %end;

      data &outSasLib..bep_data_array_&scenNum. (drop=scenario);
         set bep_data_array_all (where=(upcase(scenario)=upcase("&scenName.")));
      run;

      data &outSasLib..scenario_data_array_&scenNum. (drop=scenario_name);
         set scenario_data_array_all (where=(upcase(scenario_name)=upcase("&scenName.")));
      run;

      /* Set macrovariables that the BEP Overlay model can use */
      %let bepDataArray=&outSasLib..bep_data_array_&scenNum.;
      %let scenarioDataArray=&outSasLib..scenario_data_array_&scenNum.;
      %let outBepOverlayDs=&outSasLib..bep_overlay_&scenNum.;

      /* Run the BEP Overlay model code */
      %include &bepModelWrapperFref. / source2 lrecl = 32000;

      /* Verify the &outBepOverlayDs. dataset was created by the BEP Overlay model code */
      %if not %rsk_dsexist(&outBepOverlayDs.) %then %do;
         %put ERROR: The BEP Overlay model did not produce the required BEP overlay dataset &outBepOverlayDs. for scenario &scenName.;
         %abort;
      %end;

      /* Append each scenario's BEP Overlay results to a full BEP Overlay results dataset */
      data bep_final;
         set
            %if &scenNum. ne 1 %then %do;
               bep_final (in=a)
            %end;
            &outBepOverlayDs. (in=b);
         if _n_=0 then set &inBepDs.(keep=scenario);
         if b then
            scenario="&scenName.";
      run;

   %end; /* End scenario loop */

   filename &bepModelWrapperFref. clear;
   filename &bepModelFref. clear;

   /* Transpose the model's bep overlay results back to the BEP details form */
   proc transpose data=bep_final out=bep_final_trans;
      by scenario key;
   run;

   /* Map the bep overlay scenario's back to original BEP's scenario names and set the horizon0 value */
   data bep_details_overlay (drop=key _NAME_ _rc_ fromScenarioName toScenarioName);
      set bep_final_trans(rename=(col1=&bepModelValue.));
      retain horizon0;

      if _n_=0 then do;
         set &bepScenarioMap.;
         set &inBepDs. (keep=&bepSegVarsValue. targetVar horizon0 activationHorizon);
      end;

      if _n_=1 then do;
         declare hash hScenMap(dataset: "&bepScenarioMap.");
         hScenMap.defineKey("toScenarioName");
         hScenMap.defineData("fromScenarioName");
         hScenMap.defineDone();
      end;

      toScenarioName=scenario;
      call missing(fromScenarioName);
      _rc_=hScenMap.find();
      scenario=fromScenarioName;

      targetVar=scan(key, 1, "|||");
      %do i=1 %to %sysfunc(countw(&bepSegVarsValue.));
         %scan(&bepSegVarsValue., &i., %str( )) = scan(key, %eval(&i.+1), "|||");
      %end;

      activationHorizon=input(scan(_NAME_, -1, "_"), 8.);
      if activationHorizon=0 then do;
         horizon0=&bepModelValue.;
         &bepModelValue.=.;
      end;

   run;

   /* Overlay the new BEP projections onto the original BEP projections
         -re-calculate relativeValue and relativePct for the new BEP from the new absoluteValue values set by the bep overlay model.
         -do not overlay anything except for the projection values (absoluteValue/relativeValue/relativePct) - the segments/hierarchies/projection horizons/etc stay the same
   */
   proc sort data=&inBepDs.; by scenario targetVar &bepSegVarsValue. activationHorizon; run;
   proc sort data=bep_details_overlay; by scenario targetVar &bepSegVarsValue. activationHorizon; run;
   data _tmp_out_bep_ds_ (drop=priorAbsoluteValue);
      merge &inBepDs. (in=a) bep_details_overlay (in=b);
      by scenario targetVar &bepSegVarsValue. activationHorizon;

      retain priorAbsoluteValue 0;

      if a and b then do;
         if activationHorizon = 0 then
            priorAbsoluteValue = horizon0;
         else do;
            %if %upcase(&bepModelValue.)=ABSOLUTEVALUE %then %do;
               relativeValue = absoluteValue - priorAbsoluteValue;
            %end;
            %else %do;
               absoluteValue = priorAbsoluteValue + relativeValue;
            %end;
            relativePct = relativeValue/priorAbsoluteValue;
            priorAbsoluteValue = absoluteValue;
         end;
      end;

      if a or (a and b) then
         output;
   run;
   proc sort data=_tmp_out_bep_ds_ out=&outBepDs.(drop=order); by order; run;

   /* Create the new BEP object with the adjusted BEP projections */
   %let bepCreateTime=%sysfunc(datetime(),nldatm.);
   data bep_summary;
      set bep_summary;
      description=catt("This BEP was created from BEP '", objectId, "' by a BEP Overlay model at time &bepCreateTime. (analysis run: &analysisRunName.).");
   run;
   %core_rest_create_bep(solution = &solution.
                        , host = &host.
                        , port = &port.
                        , logonHost = &logonHost.
                        , logonPort = &logonPort.
                        , username = &username.
                        , password = &password.
                        , authMethod = &authMethod.
                        , client_id = &client_id.
                        , client_secret = &client_secret.
                        , inDsBepSummaryData = bep_summary
                        , inDsBepSpreadsheetData = &outBepDs.
                        , inDsBepLinkInstances = bep_link_instances
                        , inDsBepTargetAuxData = bep_target_vars
                        , newBepObjectId = bep_%substr(&analysisRunKey., 1, 7)_%sysfunc(tranwrd(&bepCreateTime.,%str(:),%str(-)))
                        , newBepName = Adjusted Business Evolution Plan (&bepCreateTime.)
                        , statusCd = &outBepStatusCd.
                        , outds = outBepInfo
                        , outSuccess = httpSuccess
                        , outVarToken = &outVarToken.
                        , debug = &debug.
                        );

   /* Exit in case of errors */
   %if(not &httpSuccess.) %then %do;
      %put ERROR: Failed to create the new BEP;
      %abort;
   %end;


%mend;