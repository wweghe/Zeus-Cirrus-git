/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor core_rest_delete_re_pipeline

   \brief   Deletes a SAS Risk Engine pipeline and input data

   \param [in] host (optional) Host url, including the protocol
   \param [in] server Name of the REST service (Default: riskPipeline)
   \param [in] port (optional) Server port
   \param [in] logonHost (Optional) Host/IP of the sas-logon-app service or ingress.
   \param [in] logonPort (Optional) Port of the sas-logon-app service or ingress.
   \param [in] username (optional) Username credentials
   \param [in] password (optional) Password credentials: it can be plain text or SAS-Encoded (it will be masked during execution).
   \param [in] authMethod: Authentication method (accepted values: BEARER). (Default: BEARER).
   \param [in] client_id The client id registered with the Viya authentication server. If blank, the internal SAS client id is used (only if GRANT_TYPE = password).
   \param [in] client_secret The secret associated with the client id.
   \param [in] del_inputs Flag (Y/N). Controls whether the process deletes all the CAS tables used in the RE pipeline. (Default: Y)
   \param [in] del_pipeline Flag (Y/N). Controls whether the process deletes the RE pipeline. (Default: Y)
   \param [in] del_results Flag (Y/N). Controls whether the process deletes the RE pipeline results. (Default: Y)
   \param [in] pipeline_key Identifier of the existing pipeline run.
   \param [in] debug True/False. If True, debugging informations are printed to the log (Default: false)
   \param [in] logOptions Logging options (i.e. mprint mlogic symbolgen ...)
   \param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y)
   \param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y)
   \param [out] outds Name of the output table that contains the Risk Pipelines project(s) details (Default: re_deleted_pipeline)
   \param [out] outVarToken Name of the output macro variable which will contain the access token (Default: access_token)
   \param [out] outSuccess Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess)
   \param [out] outResponseStatus Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus)

   \details
   This macro sends a GET request to <b><i>\<host\>/riskPipeline/projects</i></b> and collects the results in the output table. Deletes all input tables and then the pipeline \n
   See \link core_rest_request.sas \endlink for details about how to send GET requests and parse the response.

   <b>Example:</b>

   1) Set up the environment (set SASAUTOS and required LUA libraries).  Assumes the spre folder is under /riskcirruscore/core/code_libraries/release-core-2022.11
   \code
      %let core_root_path=/riskcirruscore/core/code_libraries/release-core-2022.11;
      option insert = (
         SASAUTOS = (
            "&core_root_path./spre/sas/ucmacros"
            )
         );
      filename LUAPATH ("&core_root_path./spre/lua");
   \endcode

   2) Send a Http GET request and parse the JSON response into the output table WORK.vre_model
   \code
      %let access_token =;
      %core_rest_delete_re_pipeline(pipeline_key = ad914942-d6a6-4922-bd66-7e5428696793
                            , outds = vre_model
                            , outVarToken = access_token
                            , outSuccess = httpSuccess
                            , outResponseStatus = responseStatus
                            );
      %put &=access_token;
      %put &=httpSuccess;
      %put &=responseStatus;
   \endcode

   \ingroup rgfRestUtils

   \author  SAS Institute Inc.
   \date    2021
*/
%macro core_rest_delete_re_pipeline(host =
                                 , server = riskPipeline
                                 , port =
                                 , logonHost =
                                 , logonPort =
                                 , username =
                                 , password =
                                 , authMethod = bearer
                                 , client_id =
                                 , client_secret =
                                 , casSessionName =
                                 , del_inputs = Y
                                 , del_pipeline = Y
                                 , del_results = Y
                                 , pipeline_key =
                                 , outds = re_deleted_pipeline
                                 , outVarToken = access_token
                                 , outSuccess = httpSuccess
                                 , outResponseStatus = responseStatus
                                 , debug = false
                                 , logOptions =
                                 , restartLUA = Y
                                 , clearCache = Y
                                 );

   %local
      requestUrl
      oldLogOptions
      i
      j
      datasets_referencing_cas_table
      dataset_referencing_cas_table
      vre_fout
      status_pipeline
      status_inputs
      status_results
   ;

   /* Set the required log options */
   %if(%length(&logOptions.)) %then
      options &logOptions.;
   ;

   /* Initialize the output table */
   data &outds.;
      attrib
         project_name      length = $150.
         project_key       length = $100.
         project_type      length = $40.
         pipeline_name     length = $150.
         pipeline_key      length = $100.
         status_pipeline   length = $20.
         status_inputs     length = $20.
         status_results    length = $20.
      ;
   run;

   /* Get the current value of mlogic and symbolgen options */
   %let oldLogOptions = %sysfunc(getoption(mlogic)) %sysfunc(getoption(symbolgen));

   /* Initialize the status macros */
   %let status_pipeline = NOT_DELETED;
   %let status_inputs = NOT_DELETED;
   %let status_results = NOT_DELETED;

   /* Set the request URL for the riskPipeline requests */
   %core_set_base_url(host=&host, server=&server., port=&port.);
   %let requestUrl = &baseUrl./&server./riskPipelines;

   %if(%sysevalf(%superq(pipeline_key) ne, boolean)) %then %do;
      /* Request the specified resource by the key */
      %let requestUrl = &requestUrl./&pipeline_key.;
   %end;
   %else %do;
      %put ERROR: Pipeline key was not provided.;
      %abort;
   %end;

   /* Get a Unique fileref and assign a temp file to it */
   %let vre_fout = %rsk_get_unique_ref(prefix = RE, engine = temp);

   /* Temporary disable mlogic and symbolgen options to avoid printing of userid/pwd to the log */
   option nomlogic nosymbolgen;
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
                     , parser =
                     , outds =
                     , fout = &vre_fout.
                     , outVarToken = &outVarToken.
                     , outSuccess = &outSuccess.
                     , outResponseStatus = &outResponseStatus.
                     , debug = &debug.
                     , logOptions = &oldLogOptions.
                     , restartLUA = &restartLUA.
                     , clearCache = &clearCache.
                     );

   /* Exit in case of errors */
   %if(not &&&outSuccess..) %then %do;
      %put ERROR: Request to get the pipeline information was not successful.;
      %abort;
   %end;

   libname vre_mdl json fileref=&vre_fout.;
   filename &vre_fout. clear;

   /* Get the project information */
   data _null_;
      set vre_mdl.project;
      call symputx( 'project_name', name, 'L');
      call symputx( 'project_key', id, 'L');
      call symputx( 'project_type', type, 'L');
   run;

   /* Get the pipeline information */
   data _null_;
      set vre_mdl.root;
      call symputx( 'pipeline_name', name, 'L');
      call symputx( 'pipeline_key', id, 'L');
   run;

   /* Get the output CAS library */
   data _null_;
      set vre_mdl.configuration;
      call symputx('cas_lib', outputCaslib, 'L');
   run;

   %if(%upcase(&del_inputs.) = Y) %then %do;
      /* Find the JSON tables that contain the column tableName; tableName is the field in the JSON that references a CAS table */
      proc sql noprint;
         select memname into : datasets_referencing_cas_table separated by ' '
         from dictionary.columns
         where lowcase(libname) = 'vre_mdl' and lowcase(name) = 'tablename';
      quit;

      /* Check if there are any CAS tables to be removed */
      %if(%sysevalf(%superq(datasets_referencing_cas_table) ne, boolean)) %then %do;

         /* Iterate over the tables containg the tableName column to find exactly which CAS tables should be deleted */
         %do i = 1 %to %sysfunc(countw(&datasets_referencing_cas_table.));
            %let dataset_referencing_cas_table = %scan(&datasets_referencing_cas_table., &i.);
            /* In the dataset, find the CAS library and table name of each table that needs to be deleted */
            data _null_;
               set vre_mdl.&dataset_referencing_cas_table.;
               call symputx(catx('_', 'table_to_delete_lib', _N_), casLibName, 'L');
               call symputx(catx('_', 'table_to_delete', _N_), tableName, 'L');
               call symputx("num_tables_to_delete_&i.", _N_, 'L');
            run;

            /* Iterate over the to-be-deleted tables, and run a CAS action to delete them */
            %do j = 1 %to &&&num_tables_to_delete_&i.;
               /* Delete pipeline input CAS tables and source if exist */
               %core_cas_drop_table(cas_session_name = &casSessionName.
                                    , cas_libref = &&&table_to_delete_lib_&j.
                                    , cas_table = &&&table_to_delete_&j.
                                    , delete_source = N);
            %end;
         %end;

         %let status_inputs = DELETED;
      %end;
      %else %do;
         %put NOTE: Pipeline does not have any CAS tables to be deleted.;
         %let status_inputs = NOTHING_TO_DELETE;
      %end;
   %end;

   %if(%upcase(&del_results.) = Y) %then %do;

      /* Temporary disable mlogic and symbolgen options to avoid printing of userid/pwd to the log */
      option nomlogic nosymbolgen;
      /* Send the REST request to delete the pipeline's results*/
      %core_rest_request(url = &requestUrl./results
                        , method = DELETE
                        , logonHost = &logonHost.
                        , logonPort = &logonPort.
                        , username = &username.
                        , password = &password.
                        , authMethod = &authMethod.
                        , client_id = &client_id.
                        , client_secret = &client_secret.
                        , parser =
                        , outds =
                        , fout =
                        , outVarToken = &outVarToken.
                        , outSuccess = &outSuccess.
                        , outResponseStatus = &outResponseStatus.
                        , debug = &debug.
                        , logOptions = &oldLogOptions.
                        , restartLUA = &restartLUA.
                        , clearCache = &clearCache.
                        );

      /* Exit in case of errors */
      %if(not &&&outSuccess..) %then %do;
         %put ERROR: Request to delete the pipeline was not successful;
         %goto EXIT;
      %end;

      %let status_results = DELETED;
   %end;

   %if(%upcase(&del_pipeline.) = Y) %then %do;
      /* Temporary disable mlogic and symbolgen options to avoid printing of userid/pwd to the log */
      option nomlogic nosymbolgen;
      /* Send the REST request to delete the pipeline*/
      %core_rest_request(url = &requestUrl.
                        , method = DELETE
                        , logonHost = &logonHost.
                        , logonPort = &logonPort.
                        , username = &username.
                        , password = &password.
                        , authMethod = &authMethod.
                        , client_id = &client_id.
                        , client_secret = &client_secret.
                        , parser =
                        , outds =
                        , fout =
                        , outVarToken = &outVarToken.
                        , outSuccess = &outSuccess.
                        , outResponseStatus = &outResponseStatus.
                        , debug = &debug.
                        , logOptions = &oldLogOptions.
                        , restartLUA = &restartLUA.
                        , clearCache = &clearCache.
                        );

      /* Exit in case of errors */
      %if(not &&&outSuccess..) %then %do;
         %put ERROR: Request to delete the pipeline was not successful;
         %goto EXIT;
      %end;
      %else %let status_pipeline = DELETED;
   %end;

   %EXIT:

   /* Create output table metadata */
   data &outds.;
      set &outds.;
      project_name  = "&project_name.";
      project_key   = "&project_key.";
      project_type  = "&project_type.";
      pipeline_name = "&pipeline_name.";
      pipeline_key  = "&pipeline_key.";
      status_pipeline  = "&status_pipeline.";
      status_inputs  = "&status_inputs.";
      status_results = "&status_results.";
   run;

   libname vre_mdl clear;

%mend;
