/*
   Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/** \file
\anchor corew_data_adjustments

    \brief   Run Manual Adjustments Rules

    \param [in] ds_in_dq_config input table containing a record for each combination of AnalysisData key and RuleSet key.
    \param [in] base_dttm Base Datetime (SAS datetime value).
    \param [in] target_table_nm Prefix name of output Summary and Details tables. The finale table name will be added the date (ie. 20181231) and eTag (ie. 0BB9A68).
        Two tables will be created such as this example: '<target_table_nm>_&asOfDateFmtAd._&casTablesTag._sum' and '<target_table_nm>&asOfDateFmtAd._&casTablesTag._detail'
    \param [in] keep_sum_detail_data_flg .
    \param [in] solution The solution short name from which this request is being made. This will get stored in the createdInTag and sharedWithTags attributes on the object (Default: 'blank').
    \param [in] casSessionName CAS Session name.
    \param [in] debug True/False. If True, debugging informations are printed to the log (Default: false).
    \param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y).
    \param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y).
    \param [out] rset_out_summary Name of the output table data contains the summary rules to be applied (Default: _tmp_data_rule_set_sum_).
    \param [out] rset_out_details Name of the output table data contains the detail rules to be applied per each input row (Default: _tmp_data_rule_set_sum_).
    \param [out] rset_out_audit Name of the output table data contains the audit data to be applied per each input row (Default: _tmp_data_rule_set_sum_).
    \param [out] outSpreLib Name of SPRE lib for the output 'Summary' and 'Details' table. If specified ensure that is not used any libref in 'rset_out_summary' or 'rset_out_details'.
    \param [out] outCasLib Name of CAS lib for the output table. If specified enables run rules in CAS.
    \param [out] outVarToken Name of the output macro variable which will contain the access token (Default: accessToken).
    \param [out] outSuccess Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess).
    \param [out] outResponseStatus Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus).

    \details

    The structure of the input table ST_STG.DATA_ADJ_CONFIG (given by parameter : ds_in_dq_config) is as follows:

    | CYCLE_KEY        | CYCLE_NAME     | ANALYSIS_RUN_KEY     | ANALYSIS_RUN_NAME    | ANALYSIS_DATA_KEY     | ANALYSIS_DATA_NAME     | DATA_ADJ_RULE_SET_KEY     | RULESET_TYPE    |
    |------------------|----------------|----------------------|----------------------|-----------------------|------------------------|---------------------------|-----------------|
    | <CYCLE_KEY1>     | <CYCLE_NAME1>  | <ANALYSIS_RUN_KEY1>  | <ANALYSIS_RUN_NAME1> | <ANALYSIS_RUN_NAME1>  | <ANALYSIS_DATA_NAME1>  | <DATA_ADJ_RULE_SET_KEY1>  | <RULESET_TYPE1> |
    | <CYCLE_KEY2>     | <CYCLE_NAME2>  | <ANALYSIS_RUN_KEY2>  | <ANALYSIS_RUN_NAME2> | <ANALYSIS_RUN_NAME2>  | <ANALYSIS_DATA_NAME2>  | <DATA_ADJ_RULE_SET_KEY1>  | <RULESET_TYPE2> |
    | ...              | ...            | ...                  | ...                  | ...                   | ...                    | ...                       | ...             |
    | <CYCLE_KEYN>     | <CYCLE_NAMEN>  | <ANALYSIS_RUN_KEYN>  | <ANALYSIS_RUN_NAMEN> | <ANALYSIS_RUN_NAMEN>  | <ANALYSIS_DATA_NAMEN>  | <DATA_ADJ_RULE_SET_KEYN>  | <RULESET_TYPEN> |
  
    CYCLE_NAME : is optional. If field is empty the macro will get from the object instance
    ANALYSIS_RUN_NAME : is optional. If field is empty the macro will get from object instance
    ANALYSIS_DATA_NAME : is optional. If field is empty the macro will get from objects instance

    1) Set up the environment (set SASAUTOS and required LUA libraries).  Assumes the spre folder is under /riskcirruscore/core/code_libraries/release-core-{cadence-version}
    \code
        %let cadence_version=2022.11;
        %let core_root_path=/riskcirruscore/core/code_libraries/release-core-&cadence_version.;
        option insert = (
            SASAUTOS = (
            "&core_root_path./spre/sas/ucmacros"
            )
            );
        filename LUAPATH ("&core_root_path./spre/lua");
    \endcode

    2) Prepares the rule set data table to run against a ruleSet definition. Generates two outputs with summary and details with rules applied.
    \code
        %let accessToken =;
        %corew_data_adjustments(sourceSystemCd =
                                , host =
                                , port =
                                , logonHost =
                                , logonPort =
                                , username =
                                , password =
                                , authMethod = bearer
                                , client_id =
                                , client_secret =
                                , ds_in_data_adj_config = data_adj_config
                                , base_dttm =
                                , rset_out_summary = _tmp_data_rule_set_sum_
                                , rset_out_details = _tmp_data_rule_set_details_
                                , target_table_nm = ds_out
                                , keep_sum_detail_data_flg = Y
                                , solution = ECL
                                , outSpreLib =
                                , outCasLib = CASUSER
                                , casSessionName = CASAUTO
                                , outVarToken = accessToken
                                , outSuccess = httpSuccess
                                , outResponseStatus = responseStatus
                                , debug = false
                                , restartLUA = Y
                                , clearCache = Y
                                );

    \endcode

    \author  SAS Institute Inc.
    \date    2023
*/

%macro corew_data_adjustments(sourceSystemCd =
                            , host =
                            , port =
                            , logonHost =
                            , logonPort =
                            , username =
                            , password =
                            , authMethod = bearer
                            , client_id =
                            , client_secret =
                            , ds_in_data_adj_config =
                            , base_dttm =
                            , rset_out_summary = _tmp_data_rule_set_sum_
                            , rset_out_details = _tmp_data_rule_set_details_
                            , rset_out_audit = _tmp_data_rule_set_audit_
                            , target_table_nm = ds_out
                            , keep_sum_detail_data_flg = Y
                            , solution =
                            , outSpreLib = work
                            , outCasLib =
                            , casSessionName = casauto
                            , outVarToken = accessToken
                            , outSuccess = httpSuccess
                            , outResponseStatus = responseStatus
                            , debug = false
                            , restartLUA = Y
                            , clearCache = Y
                            );

    %local
        dadj
        outLibref
    ;

    %if (%sysevalf(%superq(ds_in_data_adj_config) eq, boolean)) %then %do;
        %put ERROR: Parameter for Table configuration is required.;
        %abort;
    %end;
    %if(not %rsk_dsexist(&ds_in_data_adj_config.)) %then %do;
        %put ERROR: Table configuration is required.;
        %abort;
    %end;

    %if (%sysevalf(%superq(rset_out_summary) eq, boolean)) %then %do;
        %put ERROR: Summary table must be provided.;
        %abort;
    %end;

    /* Reset syscc variable */
    %let syscc = 0;

    %if (%sysevalf(%superq(base_dttm) ne, boolean)) %then %do;
        %let base_dt = %sysfunc(datepart(&base_dttm.));
    %end;
    %else %do;
            %let currentDTTM = %sysfunc(dhms(%sysfunc(today()), 0, 0, 0));
            %let base_dt = %sysfunc(datepart(&currentDTTM.));
        %end;

    %if %scan(&rset_out_summary.,2,.) ne %then %do;
        %if (%sysevalf(%superq(rset_out_details) ne, boolean)) %then %do;
            %if (%scan(&rset_out_summary.,1,.) ne %scan(&rset_out_details.,1,.)) %then %do;
                %put ERROR: Summary and Details tables must use same libref.;
                %abort;
            %end;
            %let rset_out_details = %scan(&rset_out_details.,2,.);
        %end;
        %let outLibref = %scan(&rset_out_summary.,1,.);
        %let rset_out_summary = %scan(&rset_out_summary.,2,.);
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

    %let TotRuns = 0;
    /* Load all parameters into macro variable arrays */
    data _null_;
        /* Subset the records for the current partition */
        set &ds_in_data_adj_config. end = last;

        /* Set all macro variables */
        call symputx("_cycle_key_", cycle_key, "L");
        call symputx(cats("cycle_key_", put(_N_, 8.)), cycle_key, "L");
        call symputx(cats("cycle_name_", put(_N_, 8.)), cycle_name, "L");
        call symputx(cats("analysis_run_key_", put(_N_, 8.)), analysis_run_key, "L");
        call symputx(cats("analysis_run_name_", put(_N_, 8.)), analysis_run_name, "L");
        call symputx(cats("analysis_data_key_", put(_N_, 8.)), analysis_data_key, "L");
        call symputx(cats("analysis_data_name_", put(_N_, 8.)), analysis_data_name, "L");
        call symputx(cats("data_adj_rule_set_key", put(_N_, 8.)), data_adj_rule_set_key, "L");
        call symputx(cats("ruleSet_type_", put(_N_, 8.)), ruleSet_type, "L");

        /* Total number of records processed */
        if last then do;
            call symputx("TotRuns", _N_, "L");
            call symputx("casTablesTag",substr(analysis_run_key, 1, 7));
            call symputx("asOfDateFmtAd",put(&asOfDate, yymmddn8.));
        end;
    run;

    %do dadj = 1 %to &TotRuns.;

        /******************************************************************/
        /* Retrieve analysis data in view format */
        /******************************************************************/

        %if (&useCasLib. = Y) %then %do;
            /* delete CAS table if exists */
            %core_cas_drop_table(cas_session_name = &casSessionName.
                            , cas_libref = &outCasLib.
                            , cas_table = ado_&dadj._&asOfDateFmtAd._&casTablesTag.
                            , delete_table = Y
                            , delete_table_options = quiet=TRUE
                            , verify_table_deleted = Y
                            , delete_source = Y
                            );
            %corew_prepare_input_data(inTableList = &&analysis_data_key_&dadj.
                                    , outTableList = ado_&dadj._&asOfDateFmtAd._&casTablesTag.
                                    , outCasLib = &outCasLib.
                                    , casSessionName = &casSessionName.
                                    , outCasTablesScope = session
                                    , debug = &debug.
                                    );
        %end;
        %else %do;
                %let httpSuccess = 0;
                %let responseStatus =; 
                %core_rest_get_analysis_data_view(key = &&analysis_data_key_&dadj.
                                                , outview = &outLibref..ado_view_&dadj._&asOfDateFmtAd._&casTablesTag.
                                                , authMethod = &authMethod.
                                                , outVarToken =&outVarToken.
                                                , outSuccess = &outSuccess.
                                                , outResponseStatus = &outResponseStatus.
                                                , restartLUA = &restartLUA.
                                                , clearCache = &clearCache.
                                                );
                %if(not &httpSuccess. or not %rsk_dsexist(&outLibref..ado_view_&dadj._&asOfDateFmtAd._&casTablesTag.)) %then
                    %abort;
                /* create physical table to allow data update when run run rules */
                data &outLibref..ado_&dadj._&asOfDateFmtAd._&casTablesTag.;
                    set &outLibref..ado_view_&dadj._&asOfDateFmtAd._&casTablesTag.;
                run;
        %end;

        %if ( %sysevalf(%superq(_cycle_key_) ne, boolean) ) %then %do;
            %if ( %sysevalf(%superq(cycle_name) eq, boolean) ) %then %do;
                %core_rest_get_cycle(key = &_cycle_key_.
                                    , solution = &solution.
                                    , outds = cycle_&dadj.
                                    , outVarToken = &outVarToken.
                                    , outSuccess = &outSuccess.
                                    , outResponseStatus = &outResponseStatus.
                                    );
                /* Exit in case of errors */
                %if(not &httpSuccess. or not %rsk_dsexist(cycle_&dadj.) or %rsk_attrn(cycle_&dadj., nlobs) eq 0 ) %then %do;
                    %put ERROR: Can not get the cycle information.;
                    %return;
                %end;

                data _null_;
                    set cycle_&dadj.;
                    call symputx('cycle_name_', name, 'L');
                run;
            %end;
        %end;
        %else %do;
                %put ERROR: No cycle key defined for this run.;
                %return;
            %end;

        /* Continue with empty values if for some reason we cannot get objects data to set them */
        %if ( "&&analysis_data_key_&dadj." ne "" ) %then %do;
            %if ( "&&analysis_data_name_&dadj." eq "" ) %then %do;
                %core_rest_get_analysis_data(key = &&analysis_data_key_&dadj.
                                                , outds = analysis_data_summary_&dadj.
                                                , solution = &solution.
                                                , outVarToken =&outVarToken.
                                                , outSuccess = &outSuccess.
                                                , outResponseStatus = &outResponseStatus.
                                                );

                %if(not &httpSuccess. or not %rsk_dsexist(analysis_data_summary_&dadj.) or %rsk_attrn(analysis_data_summary_&dadj., nlobs) eq 0 ) %then %do;
                    %put ERROR: Cannot get analysis data properties for analysis data: &&analysis_data_key_&dadj..;
                    %return;
                %end;
                %else %do;
                        /* Get the Analysis Data name */
                        data _null_;
                        set work.analysis_data_summary_&dadj.;
                        call symputx("analysis_data_name_&dadj.", name, "L");
                        run;
                    %end;
            %end;
        %end;
        %else %do;
                %put ERROR: No analysis data key defined for this run.;
                %return;
            %end;

        /* Get the Schema Name */
        %core_rest_get_link_instances(objectType = analysisData
                                    , objectKey = &&analysis_data_key_&dadj.
                                    , linkType = analysisData_dataDefinition
                                    , outds = link_instances_&dadj.
                                    , solution = &solution.
                                    , outVarToken = &outVarToken.
                                    , outSuccess = &outSuccess.
                                    , outResponseStatus = &outResponseStatus.
                                    );

        %if(not &httpSuccess. or not %rsk_dsexist(link_instances_&dadj.) or %rsk_attrn(link_instances_&dadj., nlobs) eq 0 ) %then %do;
            %put ERROR: Cannot get analysisData_dataDefinition link for analysis data: &&analysis_data_key_&dadj..;
            return;
        %end;
        %else %do;
                data _null_;
                    set work.link_instances_&dadj.;
                    call symputx("data_definition_key_&dadj.", businessObject2, "L");
                run;

                %if %sysevalf(%superq(data_definition_key_&dadj.) ne, boolean) %then %do;
                    %core_rest_get_data_def(key = &&data_definition_key_&dadj.
                                            , outds = dataDef_summary_&dadj.
                                            , solution = &solution.
                                            , outVarToken = &outVarToken.
                                            , outSuccess = &outSuccess.
                                            , outResponseStatus = &outResponseStatus.
                                            );
                    /* Exit in case of errors */
                    %if(not &&&outSuccess.. or not %rsk_dsexist(dataDef_summary_&dadj.) or %rsk_attrn(dataDef_summary_&dadj., nlobs) eq 0 ) %then %do;
                        %put ERROR: Could not find any Data Definition &&dataDefinitionKey_&i..;
                        %return;
                    %end;
                    %else %do;
                            /* Get the Schema Name */
                            data _null_;
                                set dataDef_summary_&dadj.;
                                call symputx("schema_name_&dadj.", schemaName, "L");
                            run;
                        %end;
                %end;
                %else %do;
                        %put ERROR: Cannot get data definition properties for analysis data: &&analysis_data_key_&dadj..;
                        %return;
                    %end;
            %end;

        /**********************************************/
        /* Get Rules from RuleSet */
        /**********************************************/

        /* Retrieve the Rule set for data adjustments */
        %core_rest_get_rule_set(key = &&data_adj_rule_set_key&dadj.
                                , outds_ruleSetInfo = ruleset_info_&dadj.
                                , outds_ruleSetData = ruleset_data_&dadj.
                                , solution = &solution.
                                , outVarToken = &outVarToken.
                                , outSuccess = &outSuccess.
                                , outResponseStatus = &outResponseStatus.
                                );

        /* Exit in case of errors */
        %if( not &httpSuccess. or not %rsk_dsexist(ruleset_info_&dadj.) or not %rsk_dsexist(ruleset_data_&dadj.) or %rsk_attrn(ruleset_data_&dadj., nlobs) eq 0 ) %then %do;
            %put ERROR: Cannot get ruleset data for the ruleSetId = &ruleset_key. Skipping execution.;
            %return;
        %end;

        /* add SOURCE_TABLE|TARGET_TABLE to rule set data processing */
        data ruleset_data_&dadj.;
            set ruleset_data_&dadj.;
            /* set information of analysis data view */
            source_table = "&outLibref..ado_&dadj._&asOfDateFmtAd._&casTablesTag";
            source_table_desc = "&&analysis_data_name_&dadj.. (%left(&&analysis_data_key_&dadj..))";
            target_table = "&outLibref..ado_&dadj._&asOfDateFmtAd._&casTablesTag.";
        run;


        /* *********************************************** */
        /* Run Data Adjustments Rules - main process       */
        /* *********************************************** */

        /**************************************************/
        /* Business Rules */
        /**************************************************/
        %if ("&&ruleSet_type_&dadj." eq "BUSINESS_RULES") %then %do;
            %if &dadj. eq 1 %then %do;                
                /* Create output tables structure */
                data &outLibref..&rset_out_summary.;
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
                        cycle_id            = "Cycle ID"
                        cycle_name          = "Cycle Name"
                        analysis_run_id     = "Analysis Run ID"
                        analysis_run_name   = "Analysis Run Name"
                        base_dt             = "Base Date"
                        execution_dttm      = "Execution Datetime"
                        source_table        = "Source Table"
                        source_table_desc   = "Source Table Description"
                        rule_id             = "Rule Id"
                        rule_name           = "Rule Name"
                        rule_desc           = "Rule Description"
                        rule_reporting_lev1 = "Rule Reporting Level 1"
                        rule_reporting_lev2 = "Rule Reporting Level 2"
                        rule_reporting_lev3 = "Rule Reporting Level 3"
                        rule_condition_txt  = "Rule Condition"
                        rule_message_txt    = "Rule Message"
                        rule_weight         = "Rule Weight"
                        rule_match_cnt      = "Match Count"
                        total_row_cnt       = "Total Row Count"
                        ;
                
                        format
                        base_dt             yymmddd10.
                        execution_dttm      datetime21.
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

                data &outLibref..&rset_out_details.;
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
            %end;

            %core_run_rules(ds_rule_def = ruleset_data_&dadj.
                            , ds_out_summary = data_adj_summary_&dadj.
                            %if(&keep_sum_detail_data_flg. = Y) %then
                            , ds_out_details = data_adj_details_&dadj.
                            %if (&useCasLib. eq Y) %then %do;
                                , outCasLib = &outCasLib.
                                , casSessionName = &casSessionName.
                            %end;
                            );

            /* Exit in case of errors */
            %if(&syserr. > 4 or &syscc. > 4) %then
                %abort;

                /* Append info about the created analysis data object to the output table */
                data &outLibref..&rset_out_summary.
                %if (&useCasLib. = Y) %then %do;
                    (append=yes)
                %end;
                    ;
                    set &outLibref..&rset_out_summary. 
                    
                    %if (&useCasLib. = Y) %then %do;
                        &outLibref..
                    %end;                    
                    data_adj_summary_&dadj.;

                    cycle_id = "&&cycle_key_&dadj.";
                    cycle_name = "&&cycle_name_&dadj.";
                    analysis_run_id = "&&analysis_run_key_&dadj.";
                    analysis_run_name = "&&analysis_run_name_&dadj.";
                    base_dt = &base_dt.;
                    source_table = "&&schema_name_&dadj..";
                run;

                %if(&keep_sum_detail_data_flg. = Y) %then %do;

                    /* Append info about the created analysis data object to the output table */
                    data &outLibref..&rset_out_details.
                    %if (&useCasLib. = Y) %then %do;
                        (append=yes)
                    %end;
                        ;

                        set &outLibref..&rset_out_details.
                        %if (&useCasLib. = Y) %then %do;
                            &outLibref..
                        %end;                    
                        data_adj_details_&dadj.;

                        base_dt = &base_dt.;
                        cycle_id = "&&cycle_key_&dadj.";
                        cycle_name = "&&cycle_name_&dadj.";
                        analysis_run_id = "&&analysis_run_key_&dadj.";
                        analysis_run_name = "&&analysis_run_name_&dadj.";
                        source_table = "&&schema_name_&dadj..";
                    run;

                %end; /* %if(&keep_sum_detail_data_flg. = Y) */
        %end; /* %if ("&&ruleSet_type_&dadj." eq "BUSINESS_RULES") */


        /**************************************************/
        /* Allocation | QFactor | MgtAction Rules */
        /**************************************************/
        %if("&&ruleSet_type_&dadj." eq "ALLOCATION_RULES" or "&&ruleSet_type_&dadj." eq "QFACTOR_RULES" or "&&ruleSet_type_&dadj." eq "MGTACTION_RULES") %then %do;
            %if &dadj. eq 1 %then %do;
                /* TODO: To update the columns according to output tables needs. check in macro core_apply_allocation_rules */
                /* TODO: adapt the code to support tables for CAS and SPRE according to dataTypes */
                data 
                    &outLibref..&rset_out_summary.  (drop = rule_primary_key)
                    &outLibref..&rset_out_details. (drop = affected_row_cnt total_row_cnt)
                    &outLibref..&rset_out_audit. (drop = affected_row_cnt total_row_cnt)
                    ;
                    attrib
                        source_table               length = $100.     label = "Source Table"
                        source_table_desc          length = $256.     label = "Source Table Description"
                        execution_dttm             length = 8.        label = "Execution Datetime"         format = datetime21.
                        ruleSetKey                 length = 8.        label = "Ruleset Key"
                        rule_condition             length = $10000.   label = "Rule Condition"
                        adjustment_value           length = $150.     label = "Adjustment Value"
                        measure_var_nm             length = $150.     label = "Measure Variable Name"
                        adjustment_type            length = $150.     label = "Adjustment Type"
                        allocation_method          length = $150.     label = "Allocation Method"
                        aggregation_method         length = $32.      label = "Aggregation Method"
                        weight_var_nm              length = $150.     label = "Weight Variable Name"
                        weighted_aggregation_flg   length = $3.       label = "Weighted Aggregation Flag"
                        affected_row_cnt           length = 8.        label = "Affected Row Count"
                        total_row_cnt              length = 8.        label = "Total Row Count"
                    ;
                    stop;
                run;
            %end;

            /* Workaround for the SQLContraintTransformer which requires a RGF hotfix */
            data ruleset_info_&dadj.;
                set ruleset_info_&dadj.;
                filter_exp = prxchange('s/LOWER\(\s*(\w+)\s*\) like LOWER\(\s*"%([^%]+)%"\s*\)/prxmatch("\/\Q$2\E\/i", $1)/i', -1, filter_exp);
                /* Convert != to ne */
                filter_exp = prxchange("s/(!=)/ne/i", -1, filter_exp);
                /* If rule set type is Management Action, update the filter expression and drop record_id */
                if ruleSetType = "MGTACTION_RULES" then do;
                    filter_exp = catt(filter_exp, " and (FORECAST_TIME ne 0)");
                    drop record_id;
                end;
            run;

            /* Run Allocation Rules */
            %core_apply_allocation_rules(ds_in = ado_&dadj._&asOfDateFmtAd._&casTablesTag.
                                        , rule_def_ds = ruleset_data_&dadj.
                                        , exclude_filter_vars = ruleSetKey
                                        , custom_filter_var = filter_exp
                                        , ds_out_rule_summary = data_adj_summary_&dadj.
                                        , ds_out = data_adj_allocated_&dadj.
                                        , ds_out_audit = data_adj_audit_&dadj.
                                        %if (&useCasLib. = Y) %then %do;
                                            , outCasLib = &outCasLib.
                                            , casSessionName = &casSessionName.
                                        %end;
                                        );

            /* Exit in case of errors */
            %if(&syserr. > 4 or &syscc. > 4) %then
                %abort;

            /*  */
            data &outLibref..&rset_out_summary.
            %if (&useCasLib. = Y) %then %do;
                (append=yes)
            %end;
                ;
                set &outLibref..&rset_out_summary. 
                %if (&useCasLib. = Y) %then %do;
                    &outLibref..
                %end; 
                data_adj_summary_&dadj.;
            run;

            %if(&keep_sum_detail_data_flg. = Y) %then %do;
                data &outLibref..&rset_out_details.
                %if (&useCasLib. = Y) %then %do;
                    (append=yes)
                %end;
                ;
                    set &outLibref..&rset_out_details. 
                    %if (&useCasLib. = Y) %then %do;
                        &outLibref..
                    %end; 
                    data_adj_allocated_&dadj.;
                run;

                data &outLibref..&rset_out_audit.
                %if (&useCasLib. = Y) %then %do;
                    (append=yes)
                %end;
                ;
                    set &outLibref..&rset_out_audit. 
                    %if (&useCasLib. = Y) %then %do;
                        &outLibref..
                    %end;
                    data_adj_audit_&dadj.;
                run;
            %end;

            /* Set the parameters needed to store the analysis data TODO: verify if these variables are necessary when this step is enabled. */
            /*%let rule_summary_schema_name = %lowcase(&ds_out_summary.);
            %let filterable_vars = measure_var_nm adjustment_type allocation_method aggregation_method weight_var_nm weighted_aggregation_flg;
            %let dataDefinition_name = Allocation Rules Summary Schema;
            %let dataDefinition_desc = Allocation Rules Summary schema definition;*/

        %end; /* %if("&&ruleSet_type_&dadj." eq "ALLOCATION_RULES" or "&&ruleSet_type_&dadj." eq "QFACTOR_RULES" or "&&ruleSet_type_&dadj." eq "MGTACTION_RULES") */


        /**************************************************/
        /* Classification Rules */
        /**************************************************/
        %if("&&ruleSet_type_&dadj." eq "CLASSIFICATION_RULES" ) %then %do;
            %if &dadj. eq 1 %then %do;
                /* TODO: To update the columns according to output tables needs. check in macro core_apply_allocation_rules */
                /* TODO: adapt the code to support tables for CAS and SPRE according to dataTypes */
                data 
                    &outLibref..&rset_out_summary. (drop = rule_primary_key)
                    &outLibref..&rset_out_details. (drop = total_row_cnt)
                    &outLibref..&rset_out_audit.
                    ;
                    attrib
                        table_id                   length = $40.      label = "Table ID"
                        cycle_id                   length = $40.      label = "Cycle ID"
                        cycle_name                 length = $256.     label = "Cycle Name"
                        analysis_run_id            length = $40.      label = "Analysis Run ID"
                        analysis_run_name          length = $256.     label = "Analysis Run Name"
                        base_dt                    length = 8.        label = "Base Date"                  format = yymmddd10.
                        execution_dttm             length = 8.        label = "Execution Datetime"         format = datetime21.
                        workgroup                  length = $32.      label = "Risk Workgroup"
                        reporting_dt               length = 8.        label = "Base Date"                  format = yymmddd10.
                        source_table               length = $100.     label = "Source Table"
                        source_table_desc          length = $256.     label = "Source Table Description"
                        ruleSetKey                 length = 8.        label = "Ruleset Key"
                        rule_condition             length = $10000.   label = "Rule Condition"
                        adjustment_value           length = $150.     label = "Adjustment Value"
                        measure_name               length = $32.      label = "Classification Field"
                        current_txt_value          length = $150.     label = "Current Value"
                        previous_txt_value         length = $150.     label = "Previous Value"
                        processed_dttm             length = 8.        label = "Processed Datetime"         format = datetime21.
                        measure_var_nm             length = $150.     label = "Measure Variable Name"
                        adjustment_type            length = $150.     label = "Adjustment Type"
                        allocation_method          length = $150.     label = "Allocation Method"
                        aggregation_method         length = $32.      label = "Aggregation Method"
                        weight_var_nm              length = $150.     label = "Weight Variable Name"
                        weighted_aggregation_flg   length = $3.       label = "Weighted Aggregation Flag"
                        affected_row_cnt           length = 8.        label = "Affected Row Count"
                        total_row_cnt              length = 8.        label = "Total Row Count"
                    ;
                    stop;
                run;
            %end;

            /* Workaround for the SQLContraintTransformer which requires a RGF hotfix */
            data ruleset_info_&dadj.;
                set ruleset_info_&dadj. (rename = (classification_value = adjustment_value
                                            classification_field = measure_var_nm));
                filter_exp = prxchange('s/LOWER\(\s*(\w+)\s*\) like LOWER\(\s*"%([^%]+)%"\s*\)/prxmatch("\/\Q$2\E\/i", $1)/i', -1, filter_exp);
                /* Convert != to ne */
                filter_exp = prxchange("s/(!=)/ne/i", -1, filter_exp);
                /* If rule set type is Management Action, update the filter expression and drop record_id */
                if ruleSetType = "CLASSIFICATION_RULES" then do;
                    allocation_method = "INDIVIDUAL";
                    adjustment_type = "ABSOLUTE";
                end;
            run;

            /* Run Allocation Rules */
            %core_apply_allocation_rules(ds_in = ado_&dadj._&asOfDateFmtAd._&casTablesTag.
                                        , rule_def_ds = ruleset_data_&dadj.
                                        , exclude_filter_vars = ruleSetKey
                                        , custom_filter_var = filter_exp
                                        , ds_out_rule_summary = data_adj_summary_&dadj.
                                        , ds_out = data_adj_classification_&dadj.
                                        , ds_out_audit = data_adj_audit_&dadj.
                                        %if (&useCasLib. = Y) %then %do;
                                            , outCasLib = &outCasLib.
                                            , casSessionName = &casSessionName.
                                        %end;
                                        );

            /* Exit in case of errors */
            %if(&syserr. > 4 or &syscc. > 4) %then
                %abort;

            /*%let rule_summary_schema_name = %lowcase(&ds_out_summary.);
            %let filterable_vars = measure_var_nm adjustment_type allocation_method aggregation_method weight_var_nm weighted_aggregation_flg;
            %let dataDefinition_name = Allocation Rules Summary Schema;
            %let dataDefinition_desc = Allocation Rules Summary schema definition;*/

            data &outLibref..&rset_out_summary.
            %if (&useCasLib. = Y) %then %do;
                (append=yes)
            %end;
            ;
                set &outLibref..&rset_out_summary. 
                %if (&useCasLib. = Y) %then %do;
                    &outLibref..
                %end;
                data_adj_summary_&dadj.;

                base_dt = &base_dt.;
                table_id = "&&analysis_data_key_&dadj.";
                cycle_id = "&&cycle_key_&dadj.";
                cycle_name = "&&cycle_name_&dadj.";
                analysis_run_id = "&&analysis_run_key_&dadj.";
                analysis_run_name = "&&analysis_run_name_&dadj."; 
                source_table = "&&schema_name_&dadj.";
            run;

            %if(&keep_sum_detail_data_flg. = Y) %then %do;
                data &outLibref..&rset_out_details.
                %if (&useCasLib. = Y) %then %do;
                    (append=yes)
                %end;
                ;
                    set &outLibref..&rset_out_details. 
                    %if (&useCasLib. = Y) %then %do;
                        &outLibref..
                    %end;
                    data_adj_classification_&dadj.;

                    base_dt = &base_dt.;
                    table_id = "&&analysis_data_key_&dadj.";
                    cycle_id = "&&cycle_key_&dadj.";
                    cycle_name = "&&cycle_name_&dadj.";
                    analysis_run_id = "&&analysis_run_key_&dadj.";
                    analysis_run_name = "&&analysis_run_name_&dadj."; 
                    source_table = "&&schema_name_&dadj.";
                run;

                data &outLibref..&rset_out_audit.
                %if (&useCasLib. = Y) %then %do;
                    (append=yes)
                %end;
                ;
                    set &outLibref..&rset_out_audit. 
                    %if (&useCasLib. = Y) %then %do;
                        &outLibref..
                    %end;
                    data_adj_audit_&dadj.;

                    base_dt = &base_dt.;
                    table_id = "&&analysis_data_key_&dadj.";
                    cycle_id = "&&cycle_key_&dadj.";
                    cycle_name = "&&cycle_name_&dadj.";
                    analysis_run_id = "&&analysis_run_key_&dadj.";
                    analysis_run_name = "&&analysis_run_name_&dadj."; 
                    source_table = "&&schema_name_&dadj.";
                run;
            %end;
            /* Set the parameters needed to store the analysis data TODO: verify if these variables are necessary when this step is enabled. */
            /*%let rule_audit_dtl_schema_name = %lowcase(&solutionId._alloc_rule_audit);
            %let filterable_vars_dtl = classification_field;
            %let dataDef_name_audit_dtl = Rules Audit Definition;
            %let dataDef_desc_audit_dtl = Rules Audit schema definition;*/

        %end; /* %if("&&ruleSet_type_&dadj." eq ClassificationRuleSet) */

        /* Promote the results target analysis data - CAS table */
        %if (&useCasLib. = Y) %then %do;
            proc casutil SESSREF=&casSessionName.;
                promote inCaslib="&outCasLib."   casData="ado_&dadj._&asOfDateFmtAd._&casTablesTag."
                        outCaslib="&outCasLib." casOut="ado_&dadj._&asOfDateFmtAd._&casTablesTag.";
                run;
            quit;
            %core_cas_upload_table(cas_session_name = &casSessionName.
                                    , ds_in = &outLibref..&rset_out_summary.
                                    , cas_library_name = &outCasLib.
                                    , target_table_nm = &target_table_nm._&asOfDateFmtAd._&casTablesTag._sum
                                    , mode = replace
                                    , error_if_ds_in_not_exist = YES
                                    );
            %if(&keep_sum_detail_data_flg. = Y) %then %do;
                %core_cas_upload_table(cas_session_name = &casSessionName.
                                        , ds_in = &outLibref..&rset_out_details.
                                        , cas_library_name = &outCasLib.
                                        , target_table_nm = &target_table_nm._&asOfDateFmtAd._&casTablesTag._detail
                                        , mode = replace
                                        , error_if_ds_in_not_exist = YES
                                        );
                %core_cas_upload_table(cas_session_name = &casSessionName.
                                        , ds_in = &outLibref..&rset_out_audit.
                                        , cas_library_name = &outCasLib.
                                        , target_table_nm = &target_table_nm._&asOfDateFmtAd._&casTablesTag._audit
                                        , mode = replace
                                        , error_if_ds_in_not_exist = NO
                                        );
            %end;
        %end;
        
        /* drop intermediate tables in each do loop interaction - non CAS are being created in WORK */
        %if (&useCasLib. = Y) %then %do;
            %core_cas_drop_table(cas_session_name = &casSessionName., cas_libref = &outCasLib., cas_table = data_adj_summary_&dadj.);
            %core_cas_drop_table(cas_session_name = &casSessionName., cas_libref = &outCasLib., cas_table = data_adj_details_&dadj.);
            %core_cas_drop_table(cas_session_name = &casSessionName., cas_libref = &outCasLib., cas_table = data_adj_details_&dadj.);
            %core_cas_drop_table(cas_session_name = &casSessionName., cas_libref = &outCasLib., cas_table = data_adj_audit_&dadj.);
            %core_cas_drop_table(cas_session_name = &casSessionName., cas_libref = &outCasLib., cas_table = data_adj_classification_&dadj.);
            %core_cas_drop_table(cas_session_name = &casSessionName., cas_libref = &outCasLib., cas_table = data_adj_allocated_&dadj.);
        %end; /* %if (&useCasLib. = Y) */
    %end;

    /* drop tables at the end of do loop */
    %if (&useCasLib. = Y) %then %do;
        %core_cas_drop_table(cas_session_name = &casSessionName., cas_libref = &outCasLib., cas_table = &rset_out_summary.);
        %core_cas_drop_table(cas_session_name = &casSessionName., cas_libref = &outCasLib., cas_table = &rset_out_details.);
        %core_cas_drop_table(cas_session_name = &casSessionName., cas_libref = &outCasLib., cas_table = &rset_out_audit.);
    %end;
    %if (&useCasLib. = N) %then %do;
        %rsk_delete_ds(&outLibref..&rset_out_summary.);
        %rsk_delete_ds(&outLibref..&rset_out_details.);
        %rsk_delete_ds(&outLibref..&rset_out_audit.);
    %end;

%mend corew_data_adjustments;