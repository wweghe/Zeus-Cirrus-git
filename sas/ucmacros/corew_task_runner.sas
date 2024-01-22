/*
   Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/** \file
\anchor corew_task_runner

   \brief   Run Tasks through include, rest call executeScript and filename/include
   \param [in] host (optional) Host url, including the protocol
   \param [in] port (optional) Server port (Default: 443)
   \param [in] server Name that provides the REST service (Default: riskCirrusObjects)
   \param [in] logonHost (Optional) Host/IP of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app host/ip is the same as the host/ip in the url parameter
   \param [in] logonPort (Optional) Port of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app port is the same as the port in the url parameter
   \param [in] username (optional) Username credentials
   \param [in] password (optional) Password credentials: it can be plain text or SAS-Encoded (it will be masked during execution).
   \param [in] authMethod: Authentication method (accepted values: BEARER). (Default: BEARER).
   \param [in] client_id The client id registered with the Viya authentication server. If blank, the internal SAS client id is used (only if GRANT_TYPE = password).
   \param [in] client_secret The secret associated with the client id.
   \param [in] ds_in_execution_config input table containing a record for each execution process
   \param [in] ds_in_execution_parameters input table containing parameters needed for execution. No structure predefined. (i.e.: report_id = 'HTM_Collateral_Analysis' )
   \param [in] cycle_key Cycle key to use for the analysis data instance
   \param [in] analysis_run_key Analysis Run key to use for the analysis data instance
      Needed when task_run_type='report'. Needed when task_run_type='script' and to associate a solution AR. Not needed for single script|sascode execution.
   \param [in] task_run_type Defines the type of task to run. Tasks are: script | report | sascode. (Default: script)
   \param [in] start Specify the starting point of the records to get. Start indicate the starting index of the subset. Start SHOULD be a zero-based index. The default start SHOULD be 0. Applicable only when a filter is used.
   \param [in] limit Limit controls the maximum number of items to get from the start position (Default = 1000). Applicable only when a filter is used.
   \param [out] outds Name of the output table that contains the outcome result form rest call request
   \param [in] debug True/False. If True, debugging informations are printed to the log (Default: false)
   \param [in] logOptions Logging options (i.e. mprint mlogic symbolgen ...)
   \param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y)
   \param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y)
   \param [in] sampleDataRun Flag (Y/N). Controls whether /sample/ is added to the sol_root_folder path. (Default: N)
   \param [out] outVarToken Name of the output macro variable which will contain the access token (Default: accessToken)
   \param [out] outSuccess Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess)
   \param [out] outResponseStatus Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus)
   \details

   The structure of the input table 'task_execution_config' is as follows:

   | source_code | prg_str   | code_folder | report_id | template_name | src_data | prev_scr_data |
   |-------------|-----------|-------------|-----------|---------------|----------|---------------|

   The 'task_run_type' macro parameter defines .
   script:  source_code (script_key)
            src_data (optional)
            prev_scr_data (optional)
   report:  source_code
            report_id
            report_desc
            template_name
            src_data (optional)
            prev_scr_data (optional)
   sascode: fileref

   Notes: tmp_lib - could be used as a temporary libname or a persistent libname in order to keep datasets available across other called programs.
         src_data - source table data. Use the format <lib>.<table> (at the moment it only work for one single table)
         prev_scr_data - prev source table data. Use the format <lib>.<table> (at the moment it only work for one single table)

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

2) Send a Http GET request and parse the JSON response into the output table work.configuration_sets
\code
   %let accessToken =;
   %corew_task_runner(solution = ECL
                        , sourceSystemCd =
                        , ds_in_execution_config =
                        , filter =
                        , start = 0
                        , limit = 100
                        , logSeverity = WARNING
                        , outds_scriptInfo = work._tmp_script_info_
                        , outds_scriptCustomFields = work._tmp_script_customfields_
                        , outVarToken = accessToken
                        , outSuccess = httpSuccess
                        , outResponseStatus = responseStatus
                        , debug = false
                        , logOptions =
                        , restartLUA = Y
                        , clearCache = Y
                        );

   %put &=accessToken;
   %put &=httpSuccess;
   %put &=responseStatus;
\endcode

\author  SAS Institute Inc.
\date    2023
*/

%macro corew_task_runner(host =
                        , port =
                        , server = riskCirrusCore
                        , solution =
                        , sourceSystemCd =
                        , logonHost =
                        , logonPort =
                        , username =
                        , password =
                        , authMethod = bearer
                        , client_id =
                        , client_secret =
                        , ds_in_execution_config =
                        , ds_in_execution_parameters =
                        , cycle_key =
                        , analysis_run_key =
                        , solutionRootFolder =
                        , task_run_type = script                  /* script | report | sascode */
                        , ds_out = work.execution_summary
                        , outVarToken = accessToken
                        , outSuccess = httpSuccess
                        , outResponseStatus = responseStatus
                        , debug = false
                        , logOptions =
                        , restartLUA = Y
                        , clearCache = Y
                        , sampleDataRun = N
                        );

   %global
      FR_HTM_DATA
      PREV_FR_HTM_DATA
   ;
   %local
      TotRuns
      ds_out_result_list
      program_name
      report_id
      report_desc
      report_template_name
      report_file
      attachmentName
      attachmentDesc
      i_run
      rc
   ;

   %let TotRuns = 0;
   /* Load all parameters into macro variable arrays */
   data _null_;
      length
         report_desc $256.
         work_lib $8.
         src_data prev_scr_data $50.
      ;
      set &ds_in_execution_config. end = last;
      call symputx(cats("source_code_", put(_N_, 8.)), source_code, "L");
      prg_str=scan(source_code,1,.)|| ';';
      call symputx(cats("prg_str_", put(_N_, 8.)), prg_str, "L");
      if ( "&task_run_type." eq "report" ) then do;
         call symputx(cats("report_id_", put(_N_, 8.)), report_id, "L");
         call symputx(cats("template_name_", put(_N_, 8.)), template_name, "L");
         call symputx(cats("report_desc_", put(_N_, 8.)), coalescec(report_desc, report_id), "L");
      end;
      call symputx(cats("src_data_", put(_N_, 8.)), coalescec(src_data, src_data), "L");
      call symputx(cats("prev_scr_data_", put(_N_, 8.)), coalescec(prev_scr_data, prev_scr_data), "L");
      /* Total number of records processed */
      if last then
         call symputx("TotRuns", _N_, "L");
   run;

   %do i_run = 1 %to &TotRuns.;

      /* Reset syscc variable */
      %let SYSCC = 0;

      /* Set macro variables for collecting the results */
      %let ds_out_result_list = result_list_&i_run.;
      %let program_name = &&source_code_&i_run..;
      %let prg_str = &&PRG_STR_&i_run..;

      %if ( "&task_run_type." eq "report" ) %then %do;

         /* Set current report */
         %let report_id = &&report_id_&i_run..;
         %let report_template_name = &&template_name_&i_run..;
         %let report_desc = &&report_desc_&i_run..;

         /* Define Report parameters if available */
         %if ( %rsk_dsexist(&ds_in_execution_parameters.) ) %then %do;
            data _null_;
               set &ds_in_execution_parameters.(where = (report_id = "&report_id."));
               call symputx(parameter_name, parameter_value, "L");
            run;
         %end;

         %let report_file = &report_template_name.;
         /****************************************************************************************************/
         /* Copy report template from the template library to the persistence area (rptlib should be active) */
         /****************************************************************************************************/

         %let sol_root_folder=&solutionRootFolder./%sysget(SAS_RISK_CIRRUS_CADENCE);

         %if  %upcase(&sampleDataRun) = Y %then %do;
            %let sol_root_folder=&sol_root_folder./sample;
         %end;

         %let targ_folder=%sysfunc(pathname(rptlib));
         /*Remove any quotes or parenthesys */
         %let targ_folder = %sysfunc(translate(%superq(targ_folder), %str( ), %str(""()'')));

         filename source filesrvc
            folderpath = "&sol_root_folder/reports/disclosures/"
            filename   = "&report_template_name."
         ;

         filename dest "&targ_folder./&report_file.";

         /* Use fcopy to copy from source to dest */
         %let rc = %sysfunc(fcopy(source, dest));

         libname rptloc xlsx "&targ_folder./&report_file.";

         /*******************************************************************************/
         /* Set the input source data. This value is used in all reporting macro files. */
         /*******************************************************************************/

         %let PREV_FR_HTM_DATA = &&prev_scr_data_&i_run..;
         %let FR_HTM_DATA = &&src_data_&i_run..;

         /* Run for reports */
         /************************************/
         /* Execute the report specific code */
         /************************************/

         %&PRG_STR.;
         %let PREV_FR_HTM_DATA =;
         %let FR_HTM_DATA =;

      %end; /* %if ( "&task_run_type." eq "report" ) */

      /*****************************/
      /* Execute the specific code */
      /*****************************/

      /* Run for scripts */
      %if ( "&task_run_type." eq "script" ) %then %do;
         %core_set_base_url(host=&host, server=&server., port=&port.);

         %if (%sysevalf(%superq(analysis_run_key) ne, boolean)) %then %do;
            %let requestUrl = &baseUrl./&server./executeScript?computeContextName=%str(&)objectRestPath=analysisRuns%str(&)objectKey=&analysis_run_key.;
         %end;
         %else %do;
            %let requestUrl = &baseUrl./&server./executeScript/runCode?scriptKey=%str(&program_name.)%str(&)preserveComputeSessionFlg=false%str(&)preserveComputeSessionOnTimeOut=false;
         %end;
         %core_rest_request(url = &requestUrl.
                           , method = POST
                           , logonHost = &logonHost.
                           , logonPort = &logonPort.
                           , username = &username.
                           , password = &password.
                           , authMethod = &authMethod.
                           , headerIn = Accept: application/vnd.sas.risk.core.job+json
                           , contentType = application/vnd.sas.risk.core.job+json
                           , fout = _resp
                           , outds = rest_request_post_response
                           , outVarToken = &outVarToken.
                           , outSuccess = &outSuccess.
                           , outResponseStatus = &outResponseStatus.
                           , debug = &debug.
                           , restartLUA = &restartLUA.
                           , clearCache = &clearCache.
                           );
      %end;

      /* Run for sascode */
      %if ( "&task_run_type." eq "sascode" ) %then %do;
         %include &prg_str.;
      %end;
      /*****************************/

      %if ( "&task_run_type." eq "report" ) %then %do;
         /* Deassign report library */
         libname rptloc;

         /*******************************************/
         /* Set the Attachment name and Description */
         /*******************************************/

         %let attachmentName = &report_template_name.;
         %let attachmentDesc = &report_desc.;

         %if (&syscc. > 4) %then %do;
            %let attachmentName = ERROR - &attachmentName.;
            %let attachmentDesc = ERROR - &attachmentDesc.;
         %end;

         /*********************************/
         /* Attach Report to the Analysis */
         /*********************************/
         %core_rest_create_file_attachment(solution = &solution.
                                          , host = &host.
                                          , port = &port.
                                          , logonHost = &logonHost.
                                          , logonPort = &logonPort.
                                          , username = &username.
                                          , password = &password.
                                          , authMethod = &authMethod.
                                          , client_id = &client_id.
                                          , client_secret = &client_secret.
                                          , objectKey = &analysis_run_key.
                                          , objectType = analysisRuns
                                          , file = &targ_folder./&report_file.
                                          , attachmentSourceSystemCd = &sourceSystemCd.
                                          , attachmentName = &attachmentName.
                                          , attachmentDisplayName = &attachmentName.
                                          , attachmentDesc = &attachmentDesc.
                                          , attachmentGrouping = report_attachments
                                          , replace = Y
                                          , outds = object_file_attachments
                                          , outVarToken = accessToken
                                          , outSuccess = httpSuccess
                                          , outResponseStatus = responseStatus
                                          );

         /******************************/
         /* Attach Report to the Cycle */
         /******************************/
         %core_rest_create_file_attachment(solution = &solution.
                                          , host = &host.
                                          , port = &port.
                                          , logonHost = &logonHost.
                                          , logonPort = &logonPort.
                                          , username = &username.
                                          , password = &password.
                                          , authMethod = &authMethod.
                                          , client_id = &client_id.
                                          , client_secret = &client_secret.
                                          , objectKey = &cycle_key.
                                          , objectType = cycles
                                          , file = &targ_folder./&report_file.
                                          , attachmentSourceSystemCd = &sourceSystemCd.
                                          , attachmentName = &attachmentName.
                                          , attachmentDisplayName = &attachmentName.
                                          , attachmentDesc = &attachmentDesc.
                                          , attachmentGrouping = report_attachments
                                          , replace = Y
                                          , outds = object_file_attachments
                                          , outVarToken = accessToken
                                          , outSuccess = httpSuccess
                                          , outResponseStatus = responseStatus
                                          );

         /********************/
         /* Check for errors */
         /********************/
         %if not &httpSuccess. %then
            %put ERROR: Could not create report attachment for file &report_template_name..;

      %end; /* %if ( "&task_run_type." eq "report" ) */

      /*******************************/
      /* Create output summary table */
      /*******************************/
      data &ds_out_result_list.;
         length
            source_code $256.
            %if ( "&task_run_type." eq "report" ) %then %do;
               report_id $100.
               report_name $100.
               report_desc $200.
               template_name $32.
            %end;
            data_type $20.
            status $20.
         ;
         source_code = "&program_name.";
         %if ( "&task_run_type." eq "report" ) %then %do;
            report_id = "&report_id.";
            report_name = "&report_template_name.";
            report_desc = "&report_desc.";
            template_name = "&report_template_name.";
         %end;
         data_type = "&task_run_type.";
         %if(&syscc. <= 4) %then
            status = "Created";
         %else
            status = "Failed";
         ;
      run;

      /*******************************************/
      /* Append results to output status dataset */
      /*******************************************/
      %if (%rsk_dsexist(&ds_out_result_list.)) %then %do;
         proc append data = &ds_out_result_list.
                     base = &ds_out. force;
         run;
      %end;

   %end; /* TotRuns */

%mend corew_task_runner;