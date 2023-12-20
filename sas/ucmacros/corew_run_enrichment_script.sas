/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
    \file
\anchor corew_run_enrichment_script

    \brief   Macro to execute scripts for data enrichment

    \param [in] host (Optional) Host url, including the protocol.
    \param [in] port (Optional) Server port.
    \param [in] server Name that provides the REST service (Default: riskData).
    \param [in] logonHost (Optional) Host/IP of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app host/ip is the same as the host/ip in the url parameter.
    \param [in] logonPort (Optional) Port of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app port is the same as the port in the url parameter.
    \param [in] username (Optional) Username credentials
    \param [in] password (Optional) Password credentials: it can be plain text or SAS-Encoded (it will be masked during execution).
    \param [in] authMethod: Authentication method (accepted values: BEARER). (Default: BEARER).
    \param [in] client_id The client id registered with the Viya authentication server. If blank, the internal SAS client id is used (only if GRANT_TYPE = password).
    \param [in] client_secret The secret associated with the client id.
    \param [in] sourceSystemCd The source system code to assign to the object when registering it in Cirrus Objects (Default: 'blank').
    \param [in] solution The solution short name from which this request is being made. This will get stored in the createdInTag and sharedWithTags attributes on the object (Default: 'blank').
    \param [in] src_param_tables Set of tables with 'enrichment_config' and 'run_option' with params for macro execution i.e.: work.run_option with param and value 'configSetId="ConfigSet-2023.09"'.
    \param [in] configSetId Object Id filter to apply on the GET request when a value for key is specified.
    \param [in] ds_libref libref where the intermediate tables are stored and can be shared across execution. i.e.: ARPVC.ds_config_data | ARPVC.nested_script_params
    \param [in] script_filter Filter to send to /Scripts endpoint rest request call.
    \param [in] task_run_type The run type to send to the macro %core_task_runner() to run the script(s). Tasks are: script | report | sascode.
    \param [in] analysisRunKey (Optional) The key of the analysis run this macro is being called in. If none is provided, no links will be created.
    \param [in] debug True/False. If True, debugging informations are printed to the log (Default: false).
    \param [in] logOptions Logging options (i.e. mprint mlogic symbolgen ...).
    \param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y).
    \param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y).
    \param [out] outds_configTablesInfo Name of the output table that contains the schema info of 'datastore_config' (Default: config_tables_info).
    \param [out] outds_configTablesData Name of the output table data contains the schema of the analysis data structure (Default: config_tables_data).
    \param [out] outds_dataStore_eligibleTables Name of the output table data contains the eligible tables after filters (if any exist) applied (Default: work.ds_config_data_eligible).
    \param [out] outVarAnalysisDataKey Name of the ouput macro variable that contains the new created analysis data key (Default: new_analysis_data_key)
    \param [out] outVarToken Name of the output macro variable which will contain the access token (Default: accessToken).
    \param [out] outSuccess Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess).
    \param [out] outResponseStatus Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus).

    \details
    This macro sends a POST request to <b><i>\<host\>:\<port\>/riskData/objects?locationType=&locationType.&location=&location.&fileName=&filename/</i></b> and collects the results in the output table. \n
    See \link core_rest_request.sas \endlink for details about how to send GET/POST requests and parse the response.

    <b>Example:</b>

    1) Set up the environment (set SASAUTOS and required LUA libraries).  Assumes the spre folder is under /riskcirruscore/core/code_libraries/release-core-{cadence-version}
    \code
        %let cadence_version=2023.07;
        %let core_root_path=/riskcirruscore/core/code_libraries/release-core-&cadence_version.;
        option insert = (
            SASAUTOS = (
                "&core_root_path./spre/sas/ucmacros"
                )
            );
        filename LUAPATH ("&core_root_path./spre/lua");
    \endcode

    2) Send a Http GET request and parse the JSON response into the outputs tables work.configuration_tables_info and work.configuration_tables_data
    \code

        %let accessToken =;
        %corew_run_enrichment_script(solution =
                                    , server = riskCirrusObjects
                                    , sourceSystemCd =
                                    , host =
                                    , port =
                                    , logonHost =
                                    , logonPort =
                                    , username =
                                    , password =
                                    , authMethod =
                                    , client_id =
                                    , client_secret =
                                    , src_param_tables =
                                    , configSetId =
                                    , ds_libref = work
                                    , script_filter =
                                    , task_run_type =
                                    , clearCache = Y
                                    , outVarToken = accessToken
                                    , outSuccess = httpSuccess
                                    , outResponseStatus = responseStatus
                                    , debug = false
                                    , logOptions =
                                    , restartLUA = Y
                                    , clearCache = Y);
        %put &=httpSuccess;
        %put &=responseStatus;
    \endcode

    \ingroup coreRestUtils

    \author  SAS Institute Inc.
    \date    2023
*/
%macro corew_run_enrichment_script(solution =
                                , server = riskCirrusObjects
                                , sourceSystemCd =
                                , host =
                                , port =
                                , logonHost =
                                , logonPort =
                                , username =
                                , password =
                                , authMethod =
                                , client_id =
                                , client_secret =
                                , src_param_tables =
                                , configSetId =
                                , ds_libref = work
                                , script_filter =                       /* i.e.: eq(subtypeCd,"NESTED") */
                                , task_run_type =
                                , analysisRunKey =
                                , outVarToken = accessToken
                                , outSuccess = httpSuccess
                                , outResponseStatus = responseStatus
                                , debug = false
                                , logOptions =
                                , restartLUA = Y
                                , clearCache = Y
                                );
    %local
        rscp
    ;

    %if(%length(&logOptions.)) %then options &logOptions.;;
    %let oldLogOptions = %sysfunc(getoption(mlogic)) %sysfunc(getoption(symbolgen));

    /* Validates if 'src_param_tables' is empty or does not have the intended table to run enrichment. If TRUE get them from configuratio tables */
    %if( (%sysfunc(prxmatch(m/run_option/oi,"%superq(src_param_tables)")) < 1) or (%sysevalf(%superq(src_param_tables) eq, boolean)) ) %then %do;
        /* Get information for run_option and enrichment from configuration tables */
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
                                 , server = &server.
                                 , configSetId = &configSetId.
                                 , configTableType = run_option
                                 , logSeverity = &log_level.
                                 , outds_configTablesInfo = &ds_libref..run_option_info
                                 , outds_configTablesData = &ds_libref..run_option
                                 , outVarToken = &outVarToken.
                                 , outSuccess = &outSuccess.
                                 , outResponseStatus = &outResponseStatus.
                                 , debug = &debug.
                                 , logOptions = &oldLogOptions.
                                 , restartLUA = &restartLUA.
                                 , clearCache = &clearCache.
                                );
    %end;
    %if( (%sysfunc(prxmatch(m/enrichment_config/oi,"%superq(src_param_tables)")) < 1) or (%sysevalf(%superq(src_param_tables) eq, boolean)) ) %then %do;
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
                                 , server = &server.
                                 , configSetId = &configSetId.
                                 , configTableType = enrichment_config
                                 , logSeverity = &log_level.
                                 , outds_configTablesInfo = &ds_libref..enrichment_config_info
                                 , outds_configTablesData = &ds_libref..enrichment_config
                                 , outVarToken = &outVarToken.
                                 , outSuccess = &outSuccess.
                                 , outResponseStatus = &outResponseStatus.
                                 , debug = &debug.
                                 , logOptions = &oldLogOptions.
                                 , restartLUA = &restartLUA.
                                 , clearCache = &clearCache.
                                );
    %end;

    data _null_;
        set &ds_libref..run_option;
        call symputx(config_name,config_value);
    run;

    data _null_;
        set &ds_libref..enrichment_config;
        call symputx(config_name,config_value);
    run;
    /* Mvar comes from enrichment_config */
    %let execution_group = %str(%upcase(&execution_group.));

    /*****************************************************/
    /*             Get configuration tables              */
    /*****************************************************/
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
                            , server = &server.
                            , configSetId = &configSetId.
                            , configTableType = execution_config
                            , logSeverity = &log_level.
                            , outds_configTablesInfo = work.execution_config_info
                            , outds_configTablesData = work.execution_config_code
                            , outVarToken = &outVarToken.
                            , outSuccess = &outSuccess.
                            , outResponseStatus = &outResponseStatus.
                            , debug = &debug.
                            , logOptions = &oldLogOptions.
                            , restartLUA = &restartLUA.
                            , clearCache = &clearCache.
                            );

    %if( not(%rsk_dsexist(work.execution_config_info)) or %rsk_attrn(work.execution_config_info, nobs) = 0 ) %then %do;
        %put WARNING: Scripts execution config is unavailable.;
        %return;
    %end;

    %let execution_stage_num = 0;
    proc sql noprint;
        select max(execution_stage) into:execution_stage_num
        from work.execution_config_code;
    ;quit;

    %if &execution_stage_num. eq 0 %then %do;
        %GOTO exit_no_stages;
    %end;

    %do _scstg_ = 1 %to &execution_stage_num.;

        /* generates a stage table for each set of execution_config code */
        /* scripts will run according to the stage tables ascending order */
        data &ds_libref..enrichment_stage&_scstg_.;
            set work.execution_config_code;
            where (execution_stage = &_scstg_.) and (upcase(execution_group) in (&execution_group.));
        run;

        data _NULL_;
            if 0 then set &ds_libref..enrichment_stage&_scstg_. nobs=n;
            call symputx("tot_runScripts",n,'L');
            stop;
        run;
        %if ( &tot_runScripts. < 1 ) %then %do;
            %GOTO next_stage;
        %end;

        %do rscp=1 %to &tot_runScripts.;
            /* get script name from EXECUTION_CONFIG object without sas extension if it has */
            data _null_;
                set &ds_libref..enrichment_stage&_scstg_.(firstobs=&rscp. obs=&rscp.);
                call symputx("scriptName",prxchange("s/.sas\b//i", 1, source_code));
            run;

            %core_rest_get_script(solution = &solution.
                                 , host = &host.
                                 , port = &port.
                                 , logonHost = &logonHost.
                                 , logonPort = &logonPort.
                                 , username = &username.
                                 , password = &password.
                                 , authMethod = &authMethod.
                                 , client_id = &client_id.
                                 , client_secret = &client_secret.
                                 , server = &server.
                                 , sourceSystemCd = &sourceSystemCd.
                                 %if(%sysevalf(%superq(script_filter) ne, boolean)) %then %do;
                                    , filter = and(eq(objectId,"&scriptName."),&script_filter.)
                                 %end;
                                 %else %do;
                                    , filter = eq(objectId,"&scriptName.")
                                 %end;
                                 , outds_scriptInfo = work.outds_scriptSrcInfo
                                 , outds_scriptCustomFields = work.outds_scriptSrcCode
                                 , outVarToken = &outVarToken.
                                 , outSuccess = &outSuccess.
                                 , outResponseStatus = &outResponseStatus.
                                 , debug = &debug.
                                 , logOptions = &oldLogOptions.
                                 , restartLUA = &restartLUA.
                                 , clearCache = &clearCache.
                                );

            %if( not(%rsk_dsexist(work.outds_scriptSrcInfo)) ) %then %do;
                    %put WARNING: Script does not exist.;
                    %GOTO next_script;
            %end;
            %if( not(%rsk_dsexist(work.outds_scriptSrcCode)) ) %then %do;
                %put WARNING: Nested script code is missing; therefore, there is no code to execute.;
                %GOTO next_script;
            %end;
            %else %do;

               /* get code from script:code object */
               %let fref_nested = %rsk_get_unique_ref(prefix = scp, engine = temp);

               /********** test the usage of TEMP */
               filename &fref_nested. temp;
               data _null_;
                  file &fref_nested.;
                  length script_code $ 32000;
                  set work.outds_scriptSrcCode;
                  /* this step with a macro declaration is a workaround to enable instructions like %return %abort %GOTO run in %include with open code */
                  script_code = cats('%macro run_script_',"&fref_nested.;");
                  put script_code;
                  script_code = code; /* full script code */
                  put script_code;
                  script_code = cats('%mend run_script_',"&fref_nested.;");
                  put script_code;
                  script_code = cats('%run_script_',"&fref_nested.;");
                  put script_code;
               run;

               /* Define task table config to run in 'task runner' */
               data work.task_execution_config;
                  length
                  source_code $8.;
                  source_code = "&fref_nested.";
               run;

               /* Run 'task runner' to get nested script outcome */
               %corew_task_runner(solution = &solution.
                                 , host = &host.
                                 , port = &port.
                                 , logonHost = &logonHost.
                                 , logonPort = &logonPort.
                                 , username = &username.
                                 , password = &password.
                                 , authMethod = &authMethod.
                                 , client_id = &client_id.
                                 , client_secret = &client_secret.
                                 , server = &server.
                                 , sourceSystemCd = &solution.
                                 , ds_in_execution_config = work.task_execution_config
                                 , task_run_type = &task_run_type.
                                 , ds_out = work.execution_summary
                                 , debug = &debug.
                                 );
                
                /* Create link from analysis run to nested script */
                %if %sysevalf(%superq(analysisRunKey)^=,boolean) %then %do;
                    /* Get the key for the script */
                    %let scriptKey=;
                    data _null_;
                        set work.outds_scriptSrcInfo;
                        call symputx("scriptKey", key, "L");
                    run;

                    /* Get the keys required to build a link */
                    %let keyPrefixAnalysisRun = %substr(&analysisRunKey., 1, 7);
                    %let keyPrefixScript = %substr(&scriptKey., 1, 7);

                    %let &outSuccess. = 0;
                    %core_rest_create_link_inst(host = &host.
                                            , port = &port.
                                            , logonHost = &logonHost.
                                            , logonPort = &logonPort.
                                            , username = &username.
                                            , password = &password.
                                            , authMethod = &authMethod.
                                            , client_id = &client_id.
                                            , client_secret = &client_secret.    
                                            , link_instance_id = analysisRun_nested_script_&keyPrefixAnalysisRun._&keyPrefixScript.
                                            , linkSourceSystemCd = &sourceSystemCd.
                                            , link_type = analysisRun_nested_script
                                            , solution = &sourceSystemCd.
                                            , business_object1 = &analysisRunKey.
                                            , business_object2 = &scriptkey.
                                            , collectionObjectKey = &analysisRunKey.
                                            , collectionName = analysisRuns
                                            , outds = link_instance
                                            , outVarToken = &outVarToken.
                                            , outSuccess = &outSuccess.
                                            , outResponseStatus = &outResponseStatus.
                                            );
                    /* On failure */
                    %if(not &&&outSuccess..) %then %do;
                        %put ERROR: Unable to create link between analysis run: &analysisRunKey. to nested script: &scriptKey.;
                        %abort;
                    %end;
                %end;

            %end; /* %if %else ( not(%rsk_dsexist(work.outds_scriptSrcCode)) or ... */

            %next_script: /* %if not(%rsk_dsexist(work.outds_scriptSrcInfo)) | %if not(%rsk_dsexist(work.outds_scriptSrcCode)) ... */

        %end; /* &tot_runScripts. */

        %next_stage: /* %if ( &tot_runScripts. < 1 ) */
        %rsk_delete_ds(&ds_libref..enrichment_stage&_scstg_.);
    %end; /* %do _scstg_ = 1 %to &execution_stage_num. */

    %exit_no_stages:

%mend corew_run_enrichment_script;