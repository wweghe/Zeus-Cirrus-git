/*
   Copyright (C) 2018-2023 SAS Institute Inc. Cary, NC, USA
*/

/** \file 
\anchor corew_run_dq_rules
   
   \brief   Run Data Quality Rules
   
   \param[in] ds_in_dq_config input table containing a record for each combination of AnalysisData key and RuleSet key
   
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
                          , dr_libref = WORK
                          , ds_out_summary =
                          , ds_out_details =
                          , solution = 
                          );
   %local
      httpSuccess
      responseStatus
      TotRuns
      i
   ;

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
   data
      &ds_out_summary. (drop = rule_primary_key rule_primary_key)
      &ds_out_details. (drop = rule_weight rule_match_cnt total_row_cnt)
      ;
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
      stop;
   run;

   %do i = 1 %to &TotRuns.;

      /* Initialize temporary DQ tables */
      data &dr_libref..dq_summary_&i.;
         set &ds_out_summary.;
         stop;
      run;
      data &dr_libref..dq_details_&i.;
         set &ds_out_details.;
         stop;
      run;

      /* *********************************************** */
      /* Retrieve AnalysisData views                     */
      /* *********************************************** */

      %let accessToken=;
      %let httpSuccess=;
      %let responseStatus=;

      %core_rest_get_analysis_data_view(key = &&analysis_data_key_&i..
                                      , outview = &dr_libref..analysis_data_&i.
                                      , solution = &solution
                                      , outVarToken = accessToken
                                      , outSuccess = httpSuccess
                                      , outResponseStatus = responseStatus
                                      );

      /* Exit in case of errors */
      %if(not &httpSuccess. or not %rsk_dsexist(analysis_data_&i.)) %then
         %return;

      data _null_;
         call symputx("analysis_data_name_&i.", "", "L");
         call symputx("schema_name_&i.", "", "L");
      run; 

      /* Continue with empty values if for some reason we cannot get objects data to set them */

      %let accessToken=;
      %core_rest_get_analysis_data(key = &&analysis_data_key_&i..
                                      , outds = &dr_libref..analysis_data_summary_&i.
                                      , solution = &solution
                                      , outVarToken =accessToken
                                      , outSuccess = httpSuccess
                                      , outResponseStatus = responseStatus
                                      );

      %if(not &httpSuccess. or not %rsk_dsexist(&dr_libref..analysis_data_summary_&i.)) %then %do;
         %put WARNING: Cannot get analysis data properties for analysis data: &&analysis_data_key_&i..;
      %end;
      %else %do;

         /* Get the Analysis Data name */
         data _null_;
            set &dr_libref..analysis_data_summary_&i.;
            call symputx("analysis_data_name_&i.", name, "L");
         run; 

         /* Get the Schema Name */
         %let accessToken=;
         %core_rest_get_link_instances(
                                    objectType = analysisData
                                    , objectKey = &&analysis_data_key_&i..
                                    , linkType = analysisData_dataDefinition
                                    , outds = &dr_libref..link_instances_&i.
                                    , solution = &solution
                                    , outVarToken = accessToken
                                    , outSuccess = httpSuccess
                                    , outResponseStatus = responseStatus
                                    );

         %if(not &httpSuccess. or not %rsk_dsexist(&dr_libref..link_instances_&i.)) %then %do;
            %put WARNING: Cannot get analysisData_dataDefinition link for analysis data: &&analysis_data_key_&i..;
         %end;
         %else %do;
            data _null_;
               set &dr_libref..link_instances_&i.;
               call symputx("data_definition_key_&i.", businessObject2, "L");
            run; 
            
            %if %sysevalf(%superq(data_definition_key_&i) ne, boolean) %then %do;
               %core_rest_get_data_def(
                                       key = &&data_definition_key_&i..
                                       , outds = &dr_libref..dataDef_summary_&i.
                                       , solution = &solution
                                       , outVarToken = accessToken
                                       , outSuccess = httpSuccess
                                       , outResponseStatus = responseStatus
                                       );

                  /* Get the Schema Name */
                  data _null_;
                     set &dr_libref..dataDef_summary_&i.;
                     call symputx("schema_name_&i.", schemaName, "L");
                  run; 

            %end;
            %else %do;
               %put WARNING: Cannot get data definition properties for analysis data: &&analysis_data_key_&i..;
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
                                ruleSetId = &&dq_rule_set_key_&i..
                                , outds_ruleSetInfo = &dr_libref..ruleset_info_&i.
                                , outds_ruleSetData = &dr_libref..ruleset_data_&i.
                                , solution = &solution
                                , outVarToken = accessToken
                                , outSuccess = httpSuccess
                                , outResponseStatus = responseStatus
                                );

      /* Exit in case of errors */
      %if(not &httpSuccess. or not %rsk_dsexist(&dr_libref..ruleset_info_&i.) 
      or not %rsk_dsexist(&dr_libref..ruleset_data_&i.)) %then
         %return;

      /* Enrich rules info dataset */
      data &dr_libref..ruleset_data_&i;
         length
            source_table $100.
            source_table_desc $256.
            target_table $100.
         ;
         /* Only process rule conditions (exclude rule actions): we are just checking for Data Quality, not fixing data at this stage */
         set &dr_libref..ruleset_data_&i
            %if(&&dq_action_flg_&i.. ne Y) %then %do;
               (where = (upcase(rule_component) = "CONDITION"))
            %end;
         ;
/*          source_table = "&&schema_name_&i.."; */
         source_table = "&dr_libref..analysis_data_&i.";
         source_table_desc = "&&analysis_data_name_&i.. (%left(&&analysis_data_key_&i..))";
    
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
      %core_run_rules(ds_rule_def = &dr_libref..ruleset_data_&i.
                     , ds_out_summary = &dr_libref..dq_summary_&i.
                     %if(&&dq_details_flg_&i.. = Y) %then %do;
                        , ds_out_details = &dr_libref..dq_details_&i.
                     %end;
                     );

      /* Set additional columns in the output */ 
      data dq_summary_view_&i. / view = dq_summary_view_&i.;
         attrib
            cycle_id             length = $100.     label = "Cycle ID"
            cycle_name           length = $256.     label = "Cycle Name"
            analysis_run_id      length = $100.     label = "Analysis Run ID"
            analysis_run_name    length = $256.     label = "Analysis Run Name"
      ;
   format base_dt yymmddd10.; 
         set &dr_libref..dq_summary_&i.;
         base_dt = &base_dt.;
         cycle_id = "&cycle_id.";
         cycle_name = "&cycle_name.";
         analysis_run_id = "&analysis_run_id.";
         analysis_run_name = "&analysis_run_name."; 
         source_table = "&&schema_name_&i..";
      run;

      /* Set additional columns in the output */
      data dq_details_view_&i. / view = dq_details_view_&i.;
         attrib
            cycle_id             length = $100.     label = "Cycle ID"
            cycle_name           length = $256.     label = "Cycle Name"
            analysis_run_id      length = $100.     label = "Analysis Run ID"
            analysis_run_name    length = $256.     label = "Analysis Run Name"
      ;
         format base_dt yymmddd10.; 
         set &dr_libref..dq_details_&i.;
         base_dt = &base_dt.;
         cycle_id = "&cycle_id.";
         cycle_name = "&cycle_name.";
         analysis_run_id = "&analysis_run_id.";
         analysis_run_name = "&analysis_run_name.";
         source_table = "&&schema_name_&i..";
      run;

      /* Append info about the created analysis data object to the output table */
      proc append data = dq_summary_view_&i.
                  base = &ds_out_summary.
                  force;
      run;

      /* Append info about the created analysis data object to the output table */
      proc append data = dq_details_view_&i.
                  base = &ds_out_details.
                  force;
      run;

   %end;
%mend;
