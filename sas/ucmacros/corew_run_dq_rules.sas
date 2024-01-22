/*
   Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/** \file
\anchor corew_run_dq_rules

   \brief   Run Data Quality Rules

   \param[in] ds_in_dq_config input table containing a record for each combination of AnalysisData key and RuleSet key
   \param [in] solution The solution short name from which this request is being made. This will get stored in the createdInTag and sharedWithTags attributes on the object (Default: 'blank').
   \param [in] casSessionName CAS Session name.
   \param [out] ds_out_summary Name of the output table data contains the summary rules to be applied (Default: _tmp_data_rule_set_sum_).
   \param [out] ds_out_details Name of the output table data contains the detail rules to be applied per each input row (Default: _tmp_data_rule_set_sum_).
   \param [out] outCasLib Name of CAS lib for the output table. If specified enables run rules in CAS.
   \param [out] outSpreLib Name of SPRE lib for the output 'Summary' and 'Details' table. If specified ensure that is not used any libref in 'ds_out_summary' or 'ds_out_details'.
   \param [in] debug True/False. If True, debugging informations are printed to the log (Default: false).
   \param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y).
   \param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y).
   \param [out] outVarToken Name of the output macro variable which will contain the access token (Default: accessToken).
   \param [out] outSuccess Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess).
   \param [out] outResponseStatus Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus).
   
   \details

   The structure of the input table ST_STG.DQ_CONFIG is as follows:

   | ANALYSIS_DATA_ID        | DQ_RULE_SET_ID   | dq_details_flg | dq_action_flg |
   |-------------------------|------------------|----------------|---------------|
   | <AnalysisDataId1>       | <RuleSetId1>     | <Y/N>          | <Y/N>         |
   | <AnalysisDataId2>       | <RuleSetId2>     | <Y/N>          | <Y/N>         |
   | ...                     | ...              |                |               |
   | <AnalysisDataIdN>       | <RuleSetIdN>     | <Y/N>          | <Y/N>         |

   dq_details_flg : Y - create (append) results to detail table
   dq_action_flg : Y - execute actions (other values - ignore Rule Set action records)

   \author  SAS Institute Inc.
   \date    2023
*/


%macro corew_run_dq_rules(ds_in_dq_config =
                          , ds_out_summary =
                          , ds_out_details =
                          , solution =
                          , outSpreLib = work
                          , outCasLib =
                          , authMethod = bearer
                          , casSessionName = casauto
                          , outVarToken = accessToken
                          , outSuccess = httpSuccess
                          , outResponseStatus = responseStatus
                          , debug = false
                          , restartLUA = Y
                          , clearCache = Y
                          );
   %local
      httpSuccess
      responseStatus
      TotRuns
      dq_run
      outLibref
      useCasLib
   ;
   %let useCasLib = N;
   %if (%sysevalf(%superq(ds_in_dq_config) eq, boolean)) %then %do;
      %put ERROR: Config table must be provided.;
      %abort;
   %end;

   %if (%sysevalf(%superq(ds_out_summary) eq, boolean)) %then %do;
      %put ERROR: Summary table must be provided.;
      %abort;
   %end;

   %if %scan(&ds_out_summary.,2,.) ne %then %do;
      %if (%sysevalf(%superq(ds_out_details) ne, boolean)) %then %do;
         %if (%scan(&ds_out_summary.,1,.) ne %scan(&ds_out_details.,1,.)) %then %do;
            %put ERROR: Summary and Details tables must use same libref.;
            %abort;
         %end;
         %let ds_out_details = %scan(&ds_out_details.,2,.);
      %end;
      %let outLibref = %scan(&ds_out_summary.,1,.);
      %let ds_out_summary = %scan(&ds_out_summary.,2,.);
      %put NOTE: Param 'ds_out_summary' is specified with lib.table therefore 'outCasLib' or 'outSpreLib' are not used.;
      %put NOTE: Information is processed according to the engine of libname specified.;
      %if %rsk_get_lib_engine(&outLibref.) eq CAS %then %do;
         %let useCasLib = Y;
         %let outLibref = %rsk_get_unique_ref(type = lib, engine = cas, args = caslib="&outLibref." sessref=&casSessionName.);
      %end;
      %else %do;
               %let useCasLib = N;
         %end;
   %end;
   %else %if(%sysevalf(%superq(outCasLib) ne, boolean)) %then %do;
            %let outLibref = %rsk_get_unique_ref(type = lib, engine = cas, args = caslib="&outCasLib." sessref=&casSessionName.);
            %let useCasLib = Y;
            %put NOTE: Param 'outCasLib' is set and param 'ds_out_summary' is not on format lib.table then information is processed in CAS.;
         %end;
         %else %do;
                  %let outLibref = &outSpreLib.;
                  %let useCasLib = N;
                  %put NOTE: Params 'outCasLib' and 'ds_out_summary' are not set, therefore information is processed in Spre lib: &outLibref..;
            %end;

   /* set macrovariables to empty if they are not set */
   %if not %symexist(analysis_run_key) %then %do;
      %let analysis_run_id = ;
   %end;
   %else %do;
            %let analysis_run_id = &analysis_run_key.;
   %end;
   %if not %symexist(analysis_run_name) %then %do;
      %let analysis_run_name = ;
   %end;
   %if not %symexist(cycle_key) %then %do;
      %let cycle_id = ;
   %end;
   %else %do;
            %let cycle_id = &cycle_key.;
   %end;
   %if not %symexist(cycle_name) %then %do;
      %let cycle_name = ;
   %end;
   %if not %symexist(base_dt) %then %do;
      %if %symexist(base_dttm) %then %do;
         %let base_dt = %sysfunc(datepart(&base_dttm));
      %end;
      %else %do;
         %let base_dt = .;
      %end;
   %end;
   /* set macrovariables to empty if they are not set */

   %let TotRuns = 0;
   /* Load all parameters into macro variable arrays */
   data _null_;
      /* Subset the records for the current partition */
      set &ds_in_dq_config. end = last;

      /* Set all macro variables */
      call symputx(cats("analysis_data_key_", put(_N_, 8.)), analysis_data_id, "L");
      call symputx(cats("dq_rule_set_key_", put(_N_, 8.)), dq_rule_set_id, "L");
      call symputx(cats("dq_details_flg_", put(_N_, 8.)), dq_details_flg, "L");
      call symputx(cats("dq_action_flg_", put(_N_, 8.)), dq_action_flg, "L");

      /* Total number of records processed */
      if last then
         call symputx("TotRuns", _N_, "L");
   run;

   /* Create output tables structure */
   data &outLibref..&ds_out_summary.;
      %if (&useCasLib. eq Y) %then %do;
            length
               cycle_id             VARCHAR(100)
               cycle_name           VARCHAR(256)
               analysis_run_id      VARCHAR(100)
               analysis_run_name    VARCHAR(256)
               base_dt              8.
               execution_dttm       8.
               source_table         VARCHAR(100)
               source_table_desc    VARCHAR(256)
               rule_id              VARCHAR(100)
               rule_name            VARCHAR(100)
               rule_desc            VARCHAR(256)
               rule_reporting_lev1  VARCHAR(1024)
               rule_reporting_lev2  VARCHAR(1024)
               rule_reporting_lev3  VARCHAR(1024)
               rule_condition_txt   VARCHAR(10000)
               rule_message_txt     VARCHAR(4096)
               rule_weight          8.
               rule_match_cnt       8.
               total_row_cnt        8.
            ;

            label
            cycle_id				   = "Cycle ID"
            cycle_name           = "Cycle Name"
            analysis_run_id      = "Analysis Run ID"
            analysis_run_name    = "Analysis Run Name"
            base_dt              = "Base Date"
            execution_dttm       = "Execution Datetime"
            source_table         = "Source Table"
            source_table_desc    = "Source Table Description"
            rule_id              = "Rule Id"
            rule_name            = "Rule Name"
            rule_desc            = "Rule Description"
            rule_reporting_lev1  = "Rule Reporting Level 1"
            rule_reporting_lev2  = "Rule Reporting Level 2"
            rule_reporting_lev3  = "Rule Reporting Level 3"
            rule_condition_txt   = "Rule Condition"
            rule_message_txt     = "Rule Message"
            rule_weight          = "Rule Weight"
            rule_match_cnt       = "Match Count"
            total_row_cnt        = "Total Row Count"
            ;

            format
            base_dt				 yymmddd10.
               execution_dttm        datetime21.
            ;
   %end;
   %else %do;
            attrib
               cycle_id             length = $100.     label = "Cycle ID"
               cycle_name           length = $256.     label = "Cycle Name"
               analysis_run_id      length = $100.     label = "Analysis Run ID"
               analysis_run_name    length = $256.     label = "Analysis Run Name"
               base_dt              length = 8.        label = "Base Date"                  format = yymmddd10.
               execution_dttm       length = 8.        label = "Execution Datetime"         format = datetime21.
               source_table         length = $100.     label = "Source Table"
               source_table_desc    length = $256.     label = "Source Table Description"
               rule_id              length = $100.     label = "Rule Id"
               rule_name            length = $100.     label = "Rule Name"
               rule_desc            length = $256.     label = "Rule Description"
               rule_reporting_lev1  length = $1024.    label = "Rule Reporting Level 1"
               rule_reporting_lev2  length = $1024.    label = "Rule Reporting Level 2"
               rule_reporting_lev3  length = $1024.    label = "Rule Reporting Level 3"
               rule_condition_txt   length = $10000.   label = "Rule Condition"
               rule_message_txt     length = $4096.    label = "Rule Message"
               rule_weight          length = 8.        label = "Rule Weight"
               rule_match_cnt       length = 8.        label = "Match Count"
               total_row_cnt        length = 8.        label = "Total Row Count"
            ;
      %end;
      stop;
   run;

   data &outLibref..&ds_out_details.;
      %if (&useCasLib. eq Y) %then %do;
            length
               cycle_id             VARCHAR(100)
               cycle_name           VARCHAR(256)
               analysis_run_id      VARCHAR(100)
               analysis_run_name    VARCHAR(256)
               base_dt              8.
               execution_dttm       8.
               source_table         VARCHAR(100)
               source_table_desc    VARCHAR(256)
               rule_id              VARCHAR(100)
               rule_name            VARCHAR(100)
               rule_desc            VARCHAR(256)
               rule_reporting_lev1  VARCHAR(1024)
               rule_reporting_lev2  VARCHAR(1024)
               rule_reporting_lev3  VARCHAR(1024)
               rule_primary_key     VARCHAR(4096)
               rule_condition_txt   VARCHAR(10000)
               rule_message_txt     VARCHAR(4096)
            ;

            label
               cycle_id             = "Cycle ID"
               cycle_name           = "Cycle Name"
               analysis_run_id      = "Analysis Run ID"
               analysis_run_name    = "Analysis Run Name"
               base_dt              = "Base Date"
               execution_dttm       = "Execution Datetime"
               source_table         = "Source Table"
               source_table_desc    = "Source Table Description"
               rule_id              = "Rule Id"
               rule_name            = "Rule Name"
               rule_desc            = "Rule Description"
               rule_reporting_lev1  = "Rule Reporting Level 1"
               rule_reporting_lev2  = "Rule Reporting Level 2"
               rule_reporting_lev3  = "Rule Reporting Level 3"
               rule_primary_key     = "Rule Primary Key"
               rule_condition_txt   = "Rule Condition"
               rule_message_txt     = "Rule Message"
            ;
    
            format
               base_dt              yymmddd10.
               execution_dttm       datetime21.
            ;
   %end;
   %else %do;
            attrib
               cycle_id             length = $100.     label = "Cycle ID"
               cycle_name           length = $256.     label = "Cycle Name"
               analysis_run_id      length = $100.     label = "Analysis Run ID"
               analysis_run_name    length = $256.     label = "Analysis Run Name"
               base_dt              length = 8.        label = "Base Date"                  format = yymmddd10.
               execution_dttm       length = 8.        label = "Execution Datetime"         format = datetime21.
               source_table         length = $100.     label = "Source Table"
               source_table_desc    length = $256.     label = "Source Table Description"
               rule_id              length = $100.     label = "Rule Id"
               rule_name            length = $100.     label = "Rule Name"
               rule_desc            length = $256.     label = "Rule Description"
               rule_reporting_lev1  length = $1024.    label = "Rule Reporting Level 1"
               rule_reporting_lev2  length = $1024.    label = "Rule Reporting Level 2"
               rule_reporting_lev3  length = $1024.    label = "Rule Reporting Level 3"
               rule_primary_key     length = $4096.    label = "Rule Primary Key"
               rule_condition_txt   length = $10000.   label = "Rule Condition"
               rule_message_txt     length = $4096.    label = "Rule Message"
               rule_weight          length = 8.        label = "Rule Weight"
               rule_match_cnt       length = 8.        label = "Match Count"
               total_row_cnt        length = 8.        label = "Total Row Count"
            ;
      %end;
      stop;
   run;

   %do dq_run = 1 %to &TotRuns.;

      /* *********************************************** */
      /* Retrieve AnalysisData views                     */
      /* *********************************************** */

      %if (&useCasLib. = Y) %then %do;
         %core_cas_drop_table(cas_session_name = &casSessionName.
                           , cas_libref = &outCasLib.
                           , cas_table = analysis_data_&dq_run.
                           , delete_table = Y
                           , delete_table_options = quiet=TRUE
                           , verify_table_deleted = Y
                           , delete_source = Y
                           );
         %corew_prepare_input_data(inTableList = &&analysis_data_key_&dq_run..
                                 , outTableList = analysis_data_&dq_run.
                                 , outCasLib = &outCasLib.
                                 , casSessionName = &casSessionName.
                                 , outCasTablesScope = session
                                 , debug = &debug.
                                 );
      %end;
      %else %do;
               %let httpSuccess = 0;
               %let responseStatus =; 
               %core_rest_get_analysis_data_view(key = &&analysis_data_key_&dq_run..
                                             , outview = &outLibref..analysis_data_&dq_run.
                                             , authMethod = &authMethod.
                                             , outVarToken =&outVarToken.
                                             , outSuccess = &outSuccess.
                                             , outResponseStatus = &outResponseStatus.
                                             , restartLUA = &restartLUA.
                                             , clearCache = &clearCache.
                                             );
               %if(not &httpSuccess. or not %rsk_dsexist(&outLibref..analysis_data_&dq_run.)) %then
                  %abort;
            %end;

      data _null_;
         call symputx("analysis_data_name_&dq_run.", "", "L");
         call symputx("schema_name_&dq_run.", "", "L");
      run;


      /* Continue with empty values if for some reason we cannot get objects data to set them */

      %let accessToken=;
      %core_rest_get_analysis_data(key = &&analysis_data_key_&dq_run..
                                      , outds = analysis_data_summary_&dq_run.
                                      , solution = &solution
                                      , outVarToken =&outVarToken.
                                      , outSuccess = &outSuccess.
                                      , outResponseStatus = &outResponseStatus.
                                      );

      %if(not &httpSuccess. or not %rsk_dsexist(analysis_data_summary_&dq_run.)) %then %do;
         %put WARNING: Cannot get analysis data properties for analysis data: &&analysis_data_key_&dq_run..;
      %end;
      %else %do;

         /* Get the Analysis Data name */
         data _null_;
            set analysis_data_summary_&dq_run.;
            call symputx("analysis_data_name_&dq_run.", name, "L");
         run; 

         /* Get the Schema Name */
         %let accessToken=;
         %core_rest_get_link_instances(
                                    objectType = analysisData
                                    , objectKey = &&analysis_data_key_&dq_run..
                                    , linkType = analysisData_dataDefinition
                                    , outds = link_instances_&dq_run.
                                    , solution = &solution
                                    , outVarToken = accessToken
                                    , outSuccess = httpSuccess
                                    , outResponseStatus = responseStatus
                                    );

         %if(not &httpSuccess. or not %rsk_dsexist(link_instances_&dq_run.)) %then %do;
            %put WARNING: Cannot get analysisData_dataDefinition link for analysis data: &&analysis_data_key_&dq_run..;
         %end;
         %else %do;
            data _null_;
               set link_instances_&dq_run.;
               call symputx("data_definition_key_&dq_run.", businessObject2, "L");
            run; 

            %if %sysevalf(%superq(data_definition_key_&dq_run.) ne, boolean) %then %do;
               %core_rest_get_data_def(
                                       key = &&data_definition_key_&dq_run..
                                       , outds = dataDef_summary_&dq_run.
                                       , solution = &solution
                                       , outVarToken = accessToken
                                       , outSuccess = httpSuccess
                                       , outResponseStatus = responseStatus
                                       );

                  /* Get the Schema Name */
                  data _null_;
                     set dataDef_summary_&dq_run.;
                     call symputx("schema_name_&dq_run.", schemaName, "L");
                  run; 

            %end;
            %else %do;
               %put WARNING: Cannot get data definition properties for analysis data: &&analysis_data_key_&dq_run..;
            %end;
         %end;
      %end;

      /* *********************************************** */
      /* Retrieve Primary RuleSet details                */
      /* *********************************************** */

      /* Get Rules Set table */
      %let accessToken=;
      %let httpSuccess=;
      %let responseStatus=;

      %core_rest_get_rule_set(
                                ruleSetId = &&dq_rule_set_key_&dq_run..
                                , outds_ruleSetInfo = ruleset_info_&dq_run.
                                , outds_ruleSetData = ruleset_data_&dq_run.
                                , solution = &solution
                                , outVarToken = accessToken
                                , outSuccess = httpSuccess
                                , outResponseStatus = responseStatus
                                );

      /* Exit in case of errors */
      %if(not &httpSuccess. or not %rsk_dsexist(ruleset_info_&dq_run.)
      or not %rsk_dsexist(ruleset_data_&dq_run.)) %then
         %return;

      /* Enrich rules info dataset */
      data ruleset_data_&dq_run.;
         length
            source_table $100.
            source_table_desc $256.
            target_table $100.
         ;
         /* Only process rule conditions (exclude rule actions): we are just checking for Data Quality, not fixing data at this stage */
         set ruleset_data_&dq_run.
            %if(&&dq_action_flg_&dq_run.. ne Y) %then %do;
               (where = (upcase(rule_component) = "CONDITION"))
            %end;
         ;
         /* source_table = "&&schema_name_&dq_run.."; */
         %if (&useCasLib. = N) %then %do;
            source_table = "&outLibref..analysis_data_&dq_run.";
         %end;
         %else %do;
            source_table = "&outCasLib..analysis_data_&dq_run.";
         %end;
         source_table_desc = "&&analysis_data_name_&dq_run.. (%left(&&analysis_data_key_&dq_run..))";
    
         /* Variables in key and data from rule are in the form '["v1", "v2"]', we need to change to 'v1 v2' for core_run_rules */ 
         lookup_key = compress(lookup_key, '"[]');
         lookup_key = tranwrd(lookup_key, ',', '');

         lookup_data = compress(lookup_data, '"[]');
         lookup_data = tranwrd(lookup_data, ',', '');
      run; 

      /* *********************************************** */
      /* Run Data Quality Rules                          */
      /* *********************************************** */

      /* Run DQ rules */
      %core_run_rules(ds_rule_def = ruleset_data_&dq_run.
                     , ds_out_summary = dq_summary_&dq_run.
                     %if(&&dq_details_flg_&dq_run.. = Y) %then %do;
                        , ds_out_details = dq_details_&dq_run.
                     %end;
                     %if (&useCasLib. eq Y) %then %do;
                        , outCasLib = &outCasLib.
                     %end;
                     );

      /* Exit in case of errors */
      %if(&syserr. > 4 or &syscc. > 4) %then
         %abort;

      data &outLibref..&ds_out_summary.
         %if (&useCasLib. = Y) %then %do;
            (append=yes)
         %end;
            ;
         set &outLibref..&ds_out_summary.
         
         %if (&useCasLib. = Y) %then %do;
         &outLibref..
         %end;
         dq_summary_&dq_run.;

         base_dt           = &base_dt.;
         cycle_id          = "&cycle_id.";
         cycle_name        = "&cycle_name.";
         analysis_run_id   = "&analysis_run_id.";
         analysis_run_name = "&analysis_run_name.";
         source_table      = "&&schema_name_&dq_run..";
      run;

      data &outLibref..&ds_out_details.
         %if (&useCasLib. = Y) %then %do;
            (append=yes)
         %end;
            ;
         set &outLibref..&ds_out_details. 
         %if (&useCasLib. = Y) %then %do;
            &outLibref..
         %end;
         dq_details_&dq_run.;

         base_dt           = &base_dt.;
         cycle_id          = "&cycle_id.";
         cycle_name        = "&cycle_name.";
         analysis_run_id   = "&analysis_run_id.";
         analysis_run_name = "&analysis_run_name.";
         source_table      = "&&schema_name_&dq_run..";
      run;

      %if (&useCasLib. = Y) %then %do;
         %core_cas_drop_table(cas_session_name = &casSessionName.
                  , cas_libref = &outCasLib.
                  , cas_table = analysis_data_&dq_run.
                  , delete_table = Y
                  , delete_table_options = quiet=TRUE
                  , verify_table_deleted = Y
                  , delete_source = Y
                  );
         %end;
         %else %do;
         proc datasets library = &outLibref. memtype = (data view) nolist nodetails;
               delete
               analysis_data_&dq_run.;
         quit;
      %end;

   %end;

%mend corew_run_dq_rules;
