/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor core_load_to_reportmart

   \brief   Load data to Reportmart

   \param [in] host (Optional) Host url, including the protocol.
   \param [in] port (Optional) Server port.
   \param [in] server Name that provides the REST service (Default: riskCirrusObjects).
   \param [in] logonHost (Optional) Host/IP of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app host/ip is the same as the host/ip in the url parameter.
   \param [in] logonPort (Optional) Port of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app port is the same as the port in the url parameter.
   \param [in] username (Optional) Username credentials
   \param [in] password (Optional) Password credentials: it can be plain text or SAS-Encoded (it will be masked during execution).
   \param [in] authMethod: Authentication method (accepted values: BEARER). (Default: BEARER).
   \param [in] client_id The client id registered with the Viya authentication server. If blank, the internal SAS client id is used (only if GRANT_TYPE = password).
   \param [in] client_secret The secret associated with the client id.
   \param [in] solution The solution short name from which this request is being made. This will get stored in the createdInTag and sharedWithTags attributes on the object (Default: 'blank').
   \param [in] ds_in_reportmart_config Input dataset containing a record for each analysis data instance to process.
   \param [in] mode Specifies the action to perform on each analysis data instance defined as input. Values: APPEND/REPLACE/REMOVE/REFRESH.
   \param [in] dataDefKeyForRefresh Specifies a data definition key that is specifically used if the user requests the mode REFRESH.
   \param [in] debug True/False. If True, debugging informations are printed to the log (Default: false).
   \param [in] logOptions Logging options (i.e. mprint mlogic symbolgen ...).
   \param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y).
   \param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y).
   \param [out] ds_out_load_result Name of the output table data contains the results (Default: work.load_results).
   \param [out] outVarToken Name of the output macro variable which will contain the access token (Default: accessToken).
   \param [out] outSuccess Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess).
   \param [out] outResponseStatus Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus).

   \details
     This macro performs the following operations for each analysis data instance defined as input:
        1. Retrieves the linked data definition.
        2. Gets the data definition details.
        3. Sends a GET request to /riskData/reportMart/<dataDefinitionKey> to get the Reportmart details.
        4. Sends a PATCH request to /riskData/reportMart/<dataDefinitionKey> to perform the specified action on the Reportmart.
        5. Collects the results in the output table.

   <b>Example:</b>

   1) Set up the environment (set SASAUTOS and required LUA libraries).  Assumes the spre folder is under /riskcirruscore/core/code_libraries/release-core-{cadence-version}
   \code
      %let cadence_version=2023.05;
      %let core_root_path=/riskcirruscore/core/code_libraries/release-core-&cadence_version.;
      option insert = (
         SASAUTOS = (
            "&core_root_path./spre/sas/ucmacros"
            )
         );
      filename LUAPATH ("&core_root_path./spre/lua");
   \endcode

   2) Send a Http GET request and parse the JSON response into the output table work.load_results
   \code

      %let accessToken =;
      %core_load_to_reportmart(ds_in_reportmart_config = work.analysis_data_key_list
                              , mode = APPEND
                              , ds_out_load_result = work.load_results);
      %put &=httpSuccess;
      %put &=responseStatus;
   \endcode

   \ingroup rgfRestUtils

   \author  SAS Institute Inc.
   \date    2023
*/

%macro core_load_to_reportmart(host =
                              , port =
                              , server = riskCirrusObjects
                              , solution =
                              , logonHost =
                              , logonPort =
                              , username =
                              , password =
                              , authMethod = bearer
                              , client_id =
                              , client_secret =
                              , ds_in_reportmart_config =
                              , mode =
                              , dataDefKeyForRefresh = 
                              , exitMode = abort
                              , ds_out_load_result = work.load_results
                              , outVarToken = accessToken
                              , outSuccess = httpSuccess
                              , outResponseStatus = responseStatus
                              , debug = false
                              , logOptions =
                              , restartLUA = Y
                              , clearCache = Y
                              ) / minoperator;

   %local
      oldLogOptions
      i
      TotRuns
   ;

   /* Set the required log options */
   %if(%length(&logOptions.)) %then
      options &logOptions.;
   ;

   /* Get the current value of mlogic and symbolgen options */
   %let oldLogOptions = %sysfunc(getoption(mlogic)) %sysfunc(getoption(symbolgen));

   /* Create output table structure */
   data &ds_out_load_result.;
      format
         analysis_data_key $36.
         mart_library_name $256.
         mart_table_name $256.
         load_mode $32.
         message $1024.
         processed_dttm datetime21.
      ;
      stop;
   run;

   /* Check the mode parameter */
   %let mode = %upcase(&mode.);
   %if not ("&mode." in ("APPEND" "REPLACE" "REMOVE" "SKIP" "REFRESH")) %then %do;
      %put ERROR: Unsupported mode: &mode..;
      %abort;
   %end;

   /* Temporary macro that refreshes the reportmart */
   %macro rest_refresh_reportmart(server = riskData, data_definition_key=);
      %core_set_base_url(host=&host, server=&server., port=&port.);

      %let refreshUrl = &baseUrl./&server./reportMart/&data_definition_key./export;
      %core_rest_request(url = &refreshUrl.
                       , method = GET
                       , logonHost = &logonHost.
                       , logonPort = &logonPort.
                       , username = &username.
                       , password = &password.
                       , authMethod = &authMethod.
                       , client_id = &client_id.
                       , client_secret = &client_secret.
                       , parser =
                       , outVarToken = &outVarToken.
                       , outSuccess = &outSuccess.
                       , outResponseStatus = &outResponseStatus.
                       , debug = &debug.
                       , logOptions = &LogOptions.
                       , restartLUA = &restartLUA.
                       , clearCache = &clearCache.
                       );
                       
      /* Exit in case of errors */
      %if(not &&&outSuccess..) %then %do;
         %put ERROR: Failed to get the Report Mart &data_definition_key.;
         %abort;
      %end;
   %mend rest_refresh_reportmart;

   /* Simply refresh the reportmart for the specified data definition */
   %if (&mode. eq REFRESH) %then %do;
      %rest_refresh_reportmart(data_definition_key=&dataDefKeyForRefresh.);
      %goto continueAfterRefresh;
   %end;

   %let TotRuns = 0;
   /* Load all parameters into macro variable arrays */
   data _null_;

      length
         analysis_data_key $36.
      ;

      set &ds_in_reportmart_config. end = last;

      /* Set all macro variables */
      call symputx(cats("analysis_data_key_", put(_N_, 8.)), analysis_data_key, "L");

      /* Total number of records processed */
      if last then
         call symputx("TotRuns", _N_, "L");
   run;

   %do i=1 %to &TotRuns.;

      %local
         append_flg_&i.
         message_&i.
         skip_message_&i.
         TotItems_&i.
         fref_etag_&i.
         fref_body_&i.
      ;

      %if %sysevalf(%superq(analysis_data_key_&i) ne, boolean) %then %do;

         /* *********************************************** */
         /* Get linked Data Definition                      */
         /* *********************************************** */

         /* Find out the data definition associated to the current analysis data key */
         %core_rest_get_link_instances(host = &host.
                                       , port = &port.
                                       , solution = &solution.
                                       , logonHost = &logonHost.
                                       , logonPort = &logonPort.
                                       , username = &username.
                                       , password = &password.
                                       , authMethod = &authMethod.
                                       , client_id = &client_id.
                                       , client_secret = &client_secret.
                                       , objectKey = &&analysis_data_key_&i..
                                       , objectType = analysisData
                                       , linkType = analysisData_dataDefinition
                                       , linkInstanceFilter =
                                       , outds = dataDefinition_&i.
                                       , outVarToken = &outVarToken.
                                       , outSuccess = &outSuccess.
                                       , outResponseStatus = &outResponseStatus.
                                       , debug = &debug.
                                       , logOptions = &LogOptions.
                                       , restartLUA = &restartLUA.
                                       , clearCache = &clearCache.
                                       );

         /* Exit in case of errors */
         %if(not &&&outSuccess.. or not %rsk_dsexist(dataDefinition_&i.) or %rsk_attrn(dataDefinition_&i., nlobs) eq 0) %then %do;
            %put ERROR: Failed to get the Data Definition linked to Analysis Data &&analysis_data_key_&i..;
            %abort;
         %end;

         data _null_;
            set dataDefinition_&i.;
            call symputx("dataDefinitionKey_&i.", businessObject2, "L");
         run;

         /* Debug logging */
         %if("%upcase(&debug.)" = "TRUE") %then %do;
            %put NOTE: Data Definition %superq(dataDefinitionKey_&i.) is linked to Analysis Data %superq(analysis_data_key_&i.);
         %end;

         /* *********************************************** */
         /* Get Data Definition details                     */
         /* *********************************************** */

         %core_rest_get_data_def(host = &host.
                                 , port = &port.
                                 , server = &server.
                                 , logonHost = &logonHost.
                                 , logonPort = &logonPort.
                                 , username = &username.
                                 , password = &password.
                                 , authMethod = &authMethod.
                                 , client_id = &client_id.
                                 , client_secret = &client_secret.
                                 , solution = &solution.
                                 , key = &&dataDefinitionKey_&i..
                                 , outds = data_definition_&i.
                                 , outds_columns = data_definitions_columns_&i.
                                 , outds_aggregation_config = aggregation_config_&i.
                                 , outVarToken = &outVarToken.
                                 , outSuccess = &outSuccess.
                                 , outResponseStatus = &outResponseStatus.
                                 , debug = &debug.
                                 , logOptions = &LogOptions.
                                 , restartLUA = &restartLUA.
                                 , clearCache = &clearCache.
                                 );

         /* Exit in case of errors */
         %if(not &&&outSuccess.. or not %rsk_dsexist(data_definition_&i.) or %rsk_attrn(data_definition_&i., nlobs) eq 0) %then %do;
            %put ERROR: Could not find any Data Definition &&dataDefinitionKey_&i..;
            %abort;
         %end;

         /* Retrieve Data Definition details */
         data _null_;
            set data_definition_&i.;
            call symputx("schema_name_&i.", schemaName, "L");
            call symputx("schema_version_&i.", schemaVersion, "L");
            call symputx("mart_library_name_&i.", martLibraryNm, "L");
            call symputx("mart_table_name_&i.", martTableNm, "L");
         run;

         /* *********************************************** */
         /* Get Report Mart details                         */
         /* *********************************************** */

         %macro rest_get_reportmart(server = riskData
                                    , data_definition_key =
                                    , outfref_header =
                                    , outds_analysis_data =
                                    );

            %local
               fref_hout
               fref_fout
               requestUrl
               etag
               libref
            ;

            %let etag =;

            /* Set the Request URL */
            %core_set_base_url(host=&host, server=&server., port=&port.);
            %let requestUrl = &baseUrl./&server./reportMart/&data_definition_key.;

            %let fref_hout = %rsk_get_unique_ref(prefix = hout, engine = temp);
            %let fref_fout = %rsk_get_unique_ref(prefix = fout, engine = temp);

            /* Temporary disable mlogic and symbolgen options to avoid printing of userid/pwd to the log */
            *option nomlogic nosymbolgen;
            /* Send the REST request */
            %core_rest_request(url = &requestUrl.
                              , method = GET
                              , logonHost = &logonHost.
                              , logonPort = &logonPort.
                              , username = &username.
                              , password = &password.
                              , authMethod = &authMethod.
                              , client_id = &client_id.
                              , client_secret = &client_secret.
                              , headerOut = &fref_hout.
                              , fout = &fref_fout.
                              , parser =
                              , outVarToken = &outVarToken.
                              , outSuccess = &outSuccess.
                              , outResponseStatus = &outResponseStatus.
                              , debug = &debug.
                              , logOptions = &LogOptions.
                              , restartLUA = &restartLUA.
                              , clearCache = &clearCache.
                              );

            /* Exit in case of errors */
            %if(not &&&outSuccess..) %then %do;
               %put ERROR: Failed to get the Report Mart &data_definition_key.;
               %abort;
            %end;

            /* Assign libref to parse the JSON response */
            %let libref = %rsk_get_unique_ref(type = lib, engine = JSON, args = fileref = &fref_fout.);

            %if %sysevalf(%superq(outds_analysis_data) ne, boolean) %then %do;

               /* Delete output table if it exists */
               %if (%rsk_dsexist(&outds_analysis_data.)) %then %do;
                  proc sql;
                     drop table &outds_analysis_data.;
                  quit;
               %end;

               /* Create output table */
               %if (%rsk_dsexist(&libref..analysisdata)) %then %do;
                  data &outds_analysis_data.;
                     set &libref..analysisdata;
                  run;
               %end;
            %end;

            /* Get the object instance's eTag from the response header - needed for PUT/PATCH requests to riskCirrusObjects */
            data _null_;
                length Header $ 50 Value $ 200;
                infile &fref_hout. dlm=':';
                input Header $ Value $;
                if lowcase(Header) = 'etag';
                call symputx("etag", Value);
            run;

            filename &fref_hout. clear;
            filename &fref_fout. clear;
            libname &libref. clear;

            /* Build header for the PATCH request*/
            data _null_;
                file &outfref_header.;
                put "Accept: application/json";
                put 'If-Match: '"%superq(etag)";
            run;

         %mend rest_get_reportmart;

         %let fref_etag_&i. = %rsk_get_unique_ref(prefix = etag, engine = temp);

         %rest_get_reportmart(data_definition_key = &&dataDefinitionKey_&i..
                              , outfref_header = &&fref_etag_&i..
                              , outds_analysis_data = work.analysisDataItems_&i.
                              );

         /* *********************************************** */
         /* Patch Report Mart                               */
         /* *********************************************** */

         %let fref_body_&i. = %rsk_get_unique_ref(prefix = body, engine = temp);

         /* Build body for the PATCH request */
         %if "&mode." eq "APPEND" %then %do;

            /* Prevent server error response: the Analysis Data object is ALREADY a part of the Report Mart */
            %let skip_message_&i. = The request was skipped because the specified Analysis Data is already a part of the Report Mart;
            data _null_;
               %if (%rsk_dsexist(work.analysisDataItems_&i.)) %then %do;
                  set work.analysisDataItems_&i.(where=(key eq "&&analysis_data_key_&i.."));
                  if _N_ gt 0 then call symputx("message_&i.", "&&skip_message_&i..", "L");
               %end;
            run;
            /* Skip processing the current analysis data key */
            %if "&&message_&i.." ne "" %then %do;
               %goto continue;
            %end;

            /* Add the current analysis data key */
            data _null_;
               file &&fref_body_&i..;
               if _n_=1 then do;
                  put "[";
                  put "{""op"": ""add"", ""path"": ""/analysisData/"", ""value"":{""key"":""&&analysis_data_key_&i..""} }";
                  put "]";
               end;
            run;

         %end;
         %else %if "&mode." eq "REPLACE" %then %do;

            %let append_flg_&i. = Y;
            %let TotItems_&i. = 0;

            %if (%rsk_dsexist(work.analysisDataItems_&i.)) %then %do;
               /* Count rows */
               %let TotItems_&i. = %rsk_attrn(work.analysisDataItems_&i., nlobs);

               /* Check if the current Analysis Data object is ALREADY a part of the Report Mart --> Do not append it later if it is already there */
               data _null_;
                  set work.analysisDataItems_&i.(where=(key eq "&&analysis_data_key_&i.."));
                  if _N_ gt 0 then call symputx("append_flg_&i.", "N", "L");
               run;
            %end;

            /* Remove all items if any, except the current Analysis Data object */
            data _null_;
               file &&fref_body_&i..;
               length
                  line $256.
               ;
               %if (%rsk_dsexist(work.analysisDataItems_&i.)) %then %do;
                  set work.analysisDataItems_&i.(where=(key ne "&&analysis_data_key_&i..")) end = last;
                  if _n_=1 then do;
                     put "[";
                  end;
                  line = cats('{"op": "remove", "path": "/analysisData/',key,'"}');
                  if &&TotItems_&i.. gt 1 and not last then
                     line = cats(line,',');
                  put line;
               %end;
               if last then do;
                  put "]";
               end;
            run;

         %end;
         %else %if "&mode." eq "REMOVE" %then %do;

            %let TotItems_&i. = 0;

            %if (%rsk_dsexist(work.analysisDataItems_&i.)) %then %do;
               data _null_;
                  set work.analysisDataItems_&i.(where=(key eq "&&analysis_data_key_&i..")) end = last;
                  if last then call symputx("TotItems_&i.", _N_, "L");
               run;
            %end;

            /* Prevent server error response: the Analysis Data object IS NOT part of the Report Mart */
            %let skip_message_&i. = The request was skipped because the specified Analysis Data is currently not part of the Report Mart;
            %if &&TotItems_&i.. eq 0 %then %do;
               %let message_&i. = &&skip_message_&i..;
            %end;
            /* Skip processing the current analysis data key */
            %if "&&message_&i.." ne "" %then %do;
               %goto continue;
            %end;

            /* Remove the current analysis data key */
            data _null_;
               file &&fref_body_&i..;
               if _n_=1 then do;
                  put "[";
                  put "{""op"": ""remove"", ""path"": ""/analysisData/&&analysis_data_key_&i.."" }";
                  put "]";
               end;
            run;

         %end;
         %else %if "&mode." eq "SKIP" %then %do;
            %goto continue;
         %end;

         %macro rest_patch_reportmart(server = riskData
                                    , data_definition_key =
                                    , infref_header =
                                    , infref_body =
                                    );

            %local
               fref_fout
               requestUrl
               requestBodyIsNotEmpty
               fid
               rc
               rc_fread
               str
            ;

            /* Check if the body of the patch request is not empty */
            %let requestBodyIsNotEmpty = false;
            /* Open the file */
            %let fid = %sysfunc(fopen(&infref_body.));
            %if(&fid. > 0) %then %do;
               /* Set the file separator to be CR('0D'x) or LF('0A'x), forcing fread to read the entire line */
               %let rc = %sysfunc(fsep(&fid.,0D0A,x));
               %let rc_fread = 0;
               /* Loop through all records */
               %do %while(&rc_fread. = 0);
                  /* Read a record to the file data buffer */
                  %let rc_fread = %sysfunc(fread(&fid.));
                  %if(&rc_fread. = 0) %then %do;
                     %let str =;
                     /* Copy the content of the file data buffer to the STR variable */
                     %let rc = %sysfunc(fget(&fid., str));
                     /* Check STR variable length */
                     %if %length(str) gt 0 %then %let requestBodyIsNotEmpty = true;
                  %end; /* %if(&rc_fread. = 0) */
               %end; /* Loop through all records */
               /* Close the file */
               %let rc = %sysfunc(fclose(&fid.));
            %end; /* %if(&fid. > 0) */

            %if "&requestBodyIsNotEmpty." eq "true" %then %do;
               /* Set the Request URL */
               %core_set_base_url(host=&host, server=&server., port=&port.);
               %let requestUrl = &baseUrl./&server./reportMart/&data_definition_key.;

               %let fref_fout = %rsk_get_unique_ref(prefix = fout, engine = temp);

               /* Temporary disable mlogic and symbolgen options to avoid printing of userid/pwd to the log */
               *option nomlogic nosymbolgen;
               /* Send the REST request */
               %core_rest_request(url = &requestUrl.
                                 , method = PATCH
                                 , logonHost = &logonHost.
                                 , logonPort = &logonPort.
                                 , username = &username.
                                 , password = &password.
                                 , authMethod = &authMethod.
                                 , client_id = &client_id.
                                 , client_secret = &client_secret.
                                 , headerIn = &infref_header.
                                 , body = &infref_body.
                                 , contentType = application/json
                                 , parser =
                                 , outds =
                                 , fout = &fref_fout.
                                 , outVarToken = &outVarToken.
                                 , outSuccess = &outSuccess.
                                 , outResponseStatus = &outResponseStatus.
                                 , debug = &debug.
                                 , logOptions = &LogOptions.
                                 , restartLUA = &restartLUA.
                                 , clearCache = &clearCache.
                                 );

               /* Debug logging */
               %if("%upcase(&debug.)" = "TRUE") %then %do;
                  /* Print the body of the PATCH request */
                  %rsk_print_file(file = %sysfunc(pathname(&infref_body.))
                                 , title = Body of the PATCH request sent to the Server:
                                 , logSeverity = WARNING
                                 );
               %end;

               /* Exit in case of errors */
               %if(not &&&outSuccess..) %then %do;
                  %put ERROR: Failed to patch the Report Mart &data_definition_key.;
                  %if &exitMode. = return %then %return; 
                  %abort;
               %end;

               filename &fref_fout. clear;
            %end;

         %mend rest_patch_reportmart;

         %rest_patch_reportmart(data_definition_key = &&dataDefinitionKey_&i..
                              , infref_header = &&fref_etag_&i..
                              , infref_body = &&fref_body_&i..
                              );

         /* Exit in case of errors */
         %if(not &&&outSuccess..) %then %do;
            %if &exitMode. = return %then %return; 
         %end;

         filename &&fref_etag_&i.. clear;
         filename &&fref_body_&i.. clear;

         %if "&mode." eq "REPLACE" %then %do;
            %if "&&append_flg_&i.." eq "Y" %then %do;
               %let fref_etag_&i. = %rsk_get_unique_ref(prefix = etag, engine = temp);

               /* Get the ETag */
               %rest_get_reportmart(data_definition_key = &&dataDefinitionKey_&i..
                                    , outfref_header = &&fref_etag_&i..
                                    );

               %let fref_body_&i. = %rsk_get_unique_ref(prefix = body, engine = temp);

               /* Add the current analysis data key */
               data _null_;
                  file &&fref_body_&i..;
                  if _n_=1 then do;
                     put "[";
                     put "{""op"": ""add"", ""path"": ""/analysisData/"", ""value"":{""key"":""&&analysis_data_key_&i..""} }";
                     put "]";
                  end;
               run;

               %rest_patch_reportmart(data_definition_key = &&dataDefinitionKey_&i..
                                    , infref_header = &&fref_etag_&i..
                                    , infref_body = &&fref_body_&i..
                                    );

               /* Exit in case of errors */
               %if(not &&&outSuccess..) %then %do;
                  %if &exitMode. = return %then %return; 
               %end;

               filename &&fref_etag_&i.. clear;
               filename &&fref_body_&i.. clear;
            %end;
         %end;

         %continue:

         /* Create output table */
         data load_result_&i.;
            format
               analysis_data_key $36.
               mart_library_name $256.
               mart_table_name $256.
               load_mode $32.
               message $1024.
               processed_dttm datetime21.
            ;
            analysis_data_key = "&&analysis_data_key_&i..";
            mart_library_name = "&&mart_library_name_&i..";
            mart_table_name = "&&mart_table_name_&i..";
            load_mode = propcase("%superq(mode)");
            message = coalescec(ifc("%superq(message_&i.)" ne "", "%superq(message_&i.)", ""), "Completed");
            processed_dttm = "%sysfunc(datetime(), datetime21.)"dt;
         run;

         proc append data = load_result_&i.
                     base = &ds_out_load_result. force;
         run;

      %end; /* %if %sysevalf(%superq(analysis_data_key_&i) ne, boolean) */

   %end; /* %do i=1 %to &TotRuns. */

   %continueAfterRefresh:

%mend;
