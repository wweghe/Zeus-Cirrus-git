/*
 Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor corew_process_calc_config

   \brief   Calculate a provided calculation config and update an Analysis Data with new values.

   \param [in] host (Optional) Host url, including the protocol.
   \param [in] port (Optional) Server port.
   \param [in] server Name of the Web Application Server that provides the REST service (Default: riskCirrusObjects)
   \param [in] logonHost (Optional) Host/IP of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app host/ip is the same as the host/ip in the url parameter
   \param [in] logonPort (Optional) Port of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app port is the same as the port in the url parameter
   \param [in] username Username credentials
   \param [in] password Password credentials: it can be plain text or SAS-Encoded (it will be masked during execution).
   \param [in] authMethod: Authentication method (accepted values: BEARER). (Default: BEARER).
   \param [in] client_id The client id registered with the Viya authentication server. If blank, the internal SAS client id is used (only if GRANT_TYPE = password).
   \param [in] client_secret The secret associated with the client id.
   \param [in] solution Solution identifier (Source system code) for Cirrus Core content packages (Default: currently blank)
   \param [in] ds_in_curr_period CAS Input data or ADO object key for the current period. (Pass CAS table name or ADO cirrus Object key)
   \param [in] ds_in_prev_period_key ADO object key for the previous period. (ADO cirrus Object key)
   \param [in] ds_in_calculation_cfg Input dataset with calculation expressions for model output data according to map type.
   \param [in] ds_in_aggregation_cfg Input dataset with all var aggregations statistics.
   \param [in] map_type
   \param [in] analysis_type
   \param [in] scenario_selection
   \param [in] debug True/False. If True, debugging informations are printed to the log (Default: false)
   \param [in] logOptions Logging options (i.e. mprint mlogic symbolgen ...)
   \param [in] casSessionName The name of a CAS session to use for local CAS actions.  If one doesn't exist, a new session with this name will be created.
   \param [out] ds_out Name of the output table after calculation mapping .
   \param [out] outCaslib libname for model data.
   \param [out] ds_out_map Name of the output with mapping configuration table stored in WORK library.
   \param [out] outVarToken Name of the output macro variable which will contain the access token (Default: accessToken)

\details
   <b>Example:</b>

   1) Set up the environment (set SASAUTOS and required LUA libraries).  Assumes the spre folder is under /riskcirruscore/core/code_libraries/release-core-{cadence-version}
   \code
      %let cadence_version=2024.02;
      %let core_root_path=/riskcirruscore/core/code_libraries/release-core-&cadence_version.;
      option insert = (
         SASAUTOS = (
            "&core_root_path./spre/sas/ucmacros"
            )
         );
      filename LUAPATH ("&core_root_path./spre/lua");
   \endcode

   \code
      %let accessToken =;
      %corew_process_calc_config(server = riskData
                              , authMethod = bearer
                              , solution = ACL
                              , ds_in_curr_period =
                              , ds_in_prev_period_key =
                              , ds_in_calculation_cfg =
                              , ds_in_aggregation_cfg =
                              , map_type = CREDIT_RISK_DETAIL
                              , analysis_type = *
                              , scenario_selection = Weighted
                              , ds_out =
                              , ds_out_map =
                              , outCaslib =
                              , casSessionName =
                              , outVarToken =
                              , debug =
                              );

   \endcode

   2) Input and Output data
   Input table <i>DS_IN</> is expected to have the following structure:

   Output table <i>DS_OUT</> has the following structure:

\author  SAS Institute Inc.
\date    2024
*/

%macro corew_process_calc_config(host =
                              , port =
                              , server = riskData
                              , logonHost =
                              , logonPort =
                              , username =
                              , password =
                              , authMethod = bearer
                              , client_id =
                              , client_secret =
                              , solution =
                              , ds_in_curr_period =
                              , ds_in_prev_period_key =
                              , ds_in_calculation_cfg =
                              , ds_in_aggregation_cfg =
                              , map_type = CREDIT_RISK_DETAIL
                              , analysis_type = *
                              , scenario_selection =
                              , ds_out =
                              , ds_out_map =
                              , outCaslib = Public
                              , casSessionName = casauto
                              , outVarToken = accessToken
                              , debug = false
                              );

   %local
      schema_name
      schema_version
      primary_key
      prev_vars
      prev_vars_rename
      rename_str
      comma_primary_key
      quoted_primary_key
      comma_prev_vars
      quoted_prev_vars
      comma_prev_vars_r
      quoted_prev_vars_r
      httpSuccess
      responseStatus
      icalc
      calc_aggr_vars
      tot_calc_aggr_vars
      calc_movement_id
      calc_curr_var
      calc_curr_var_rename
      calc_comma_aggr_vars
      calc_quoted_aggr_vars
      non_aggr_prev_vars
   ;

   %let outLibref = %rsk_get_unique_ref(type = lib, engine = cas, args = caslib="&outCaslib." sessref=&casSessionName.);

   %let casTablesTag = %substr(%sysfunc(UUIDGEN()), 1, 7);


   /* *********************************************** */
   /*       Pull the current period data set         */
   /* *********************************************** */
   %if %sysevalf(%superq(ds_in_curr_period) ne, boolean) %then %do;
      %if %sysfunc(index("&ds_in_curr_period.", -)) > 0 %then %do;
         /* Retrieve the current period data */
         %let httpSuccess = 0;
         %let responseStatus =;
         %corew_prepare_input_data(host = &host.
                                    , port = &port.
                                    , logonHost = &logonHost.
                                    , logonPort = &logonPort.
                                    , username = &username.
                                    , password = &password.
                                    , authMethod = &authMethod.
                                    , client_id =  &client_id.
                                    , client_secret = &client_secret.
                                    , inTableList = &ds_in_curr_period.
                                    , outTableList = _curr_data_&casTablesTag.
                                    , outCasLib = &outCaslib.
                                    , casSessionName = &casSessionName.
                                    , outCasTablesScope = session
                                    , outVarToken = &outVarToken.
                                    , debug = &debug.
                                    );
         %let _curr_data_ = _curr_data_&casTablesTag.;
      %end; /* %if %sysfunc(index("&ds_in_curr_period.", -)) > 0 */
      %else %do;
               %let _curr_data_ = &ds_in_curr_period.;
         %end;
   %end; /* %if %sysevalf(%superq(ds_in_curr_period) ne, boolean) */


   /* *********************************************** */
   /*       Pull the previous period data set         */
   /* *********************************************** */

   %if %sysevalf(%superq(ds_in_prev_period_key) ne, boolean) %then %do;
      /* Retrieve the previous period data */
      %let httpSuccess = 0;
      %let responseStatus =;
      %corew_prepare_input_data(host = &host.
                                 , port = &port.
                                 , logonHost = &logonHost.
                                 , logonPort = &logonPort.
                                 , username = &username.
                                 , password = &password.
                                 , authMethod = &authMethod.
                                 , client_id =  &client_id.
                                 , client_secret = &client_secret.
                                 , inTableList = &ds_in_prev_period_key.
                                 , outTableList = _prev_data_&casTablesTag.
                                 , outCasLib = &outCaslib.
                                 , casSessionName = &casSessionName.
                                 , outCasTablesScope = session
                                 , outVarToken = &outVarToken.
                                 , debug = &debug.
                                 );
      %let _prev_data_ = _prev_data_&casTablesTag.;

      /* Get the primary key from the previous period data */
      %let primary_key=;
      %let schema_name=;
      %let schema_version=;
      %let accessToken=;
      %core_rest_get_link_instances(
                                 objectType = analysisData
                                 , server = riskCirrusObjects
                                 , objectKey = &ds_in_prev_period_key.
                                 , linkType = analysisData_dataDefinition
                                 , outds = _data_def_link_instances_
                                 , solution = &solution.
                                 , outVarToken = accessToken
                                 , outSuccess = httpSuccess
                                 , outResponseStatus = responseStatus
                                 );

      %if(not &httpSuccess. or not %rsk_dsexist(_data_def_link_instances_)) %then %do;
         %put ERROR: Cannot get analysisData_dataDefinition link for analysis data: &analysis_data_key..;
         %abort;
      %end;

      data _null_;
         set _data_def_link_instances_;
         call symputx("data_definition_key", businessObject2, "L");
      run;

      %if %sysevalf(%superq(data_definition_key) eq, boolean) %then %do;
         %put ERROR: Cannot get data definition key for analysis data: &analysis_data_key..;
         %abort;
      %end;
      %core_rest_get_data_def(
                              key = &data_definition_key.
                              , outds = _data_def_summary_
                              , outds_columns = _data_def_columns_
                              , outVarToken = accessToken
                              , outSuccess = httpSuccess
                              , outResponseStatus = responseStatus
                              , debug = true
                              );
      %if(not &httpSuccess. or not %rsk_dsexist(_data_def_columns_)) %then %do;
         %put ERROR: Cannot get data definition columns data set for: &data_definition_key..;
         %abort;
      %end;

      proc sql noprint;
         select name into 
               :primary_key separated by ' '
         from _data_def_columns_
         where primaryKeyFlag="true";

         select distinct schemaName, schemaVersion into 
               :schema_name,
               :schema_version
         from _data_def_summary_
      ;
      quit;

      %if %sysevalf(%superq(primary_key) eq, boolean) %then %do;
         %put ERROR: No primary key columns defined for the data definition: &data_definition_key..;
         %abort;
      %end;
   %end; /* %if %sysevalf(%superq(ds_in_prev_period_key) ne, boolean) */

   /* *********************************************** */
   /*       Pull any variable information needed      */
   /* *********************************************** */

   %if (not %rsk_dsexist(&ds_in_calculation_cfg.)) %then %do;
      %put ERROR: Calculation config table work.&ds_in_calculation_cfg. is not available.;
      %abort;
   %end;
   /* Subset ds_in_calculation_cfg based on map_type */
   data work.ds_in_calculation_cfg;
      set &ds_in_calculation_cfg.;
      if (upcase(map_type) = upcase("&map_type.") or map_type = "*")
         and (upcase(analysis_type) = upcase("&analysis_type.") or analysis_type = "*")
      ;
   run;

   /* Get list of previous period variables */
   proc sql noprint;
      select prev_period_var into :prev_vars separated by ' '
      from work.ds_in_calculation_cfg
         where not missing(prev_period_var);

      select coalescec(prev_period_var_rename, catt("PREV_",prev_period_var)) into :prev_vars_rename separated by ' '
      from work.ds_in_calculation_cfg
         where not missing(prev_period_var);
   quit;

   /* Remove movement_id/scenario_name/reporting_dt/horizon from primary_key */
   %let primary_key = %sysfunc(prxchange(s/(horizon)|(movement_id)|(scenario_name)|(reporting_dt)//i, -1, &primary_key.));

   /* Convert primary key to quoted comma-separated list */
   %let comma_primary_key = %sysfunc(prxchange(s/\s+/%str(, )/i, -1, &primary_key.));
   %let quoted_primary_key = %sysfunc(prxchange(s/(\w+)/"$1"/i, -1, %bquote(&comma_primary_key.)));

   /* Convert previous period vars to quoted comma-separated list */
   %let comma_prev_vars = %sysfunc(prxchange(s/\s+/%str(, )/i, -1, &prev_vars.));
   %let quoted_prev_vars = %sysfunc(prxchange(s/(\w+)/"$1"/i, -1, %bquote(&comma_prev_vars.)));

   /* Convert previous period renamed vars to quoted comma-separated list */
   %let comma_prev_vars_r = %sysfunc(prxchange(s/\s+/%str(, )/i, -1, &prev_vars_rename.));
   %let quoted_prev_vars_r = %sysfunc(prxchange(s/(\w+)/"$1"/i, -1, %bquote(&comma_prev_vars_r.)));


   /* *********************************************** */
   /*         Get Movement Information                */
   /* *********************************************** */

   %if %sysevalf(%superq(ds_in_prev_period_key) ne, boolean) %then %do;
      /* Generate rename statement for previous period variables */
      %let rename_str=;
      %do icalc = 1 %to %sysfunc(countw(&prev_vars., %str( )));
         %let calc_curr_var = %scan(&prev_vars., &icalc., %str( ));
         %let calc_curr_var_rename = %scan(&prev_vars_rename., &icalc., %str( ));
         %if(%rsk_varexist(&outLibref..&_prev_data_., &calc_curr_var.)) %then
            %let rename_str = &rename_str. &calc_curr_var.=&calc_curr_var_rename.;
      %end;

      /* Pull the previous period variables from Weighted scenario */
      data &outLibref..ds_in_prev_weighted_&casTablesTag.(drop=scenario_name);
         set &outLibref..&_prev_data_.(keep = &primary_key.
                                 &prev_vars.
                                 scenario_name
                              where = (scenario_name = "&scenario_selection.")
                           );
      run;

      /* Get the current movement version */
      %let calc_movement_id = .;
      proc fedsql sessref=&casSessionName.;
         create table "&outCaslib.".calc_max_movement_&casTablesTag. {options replace=true} as
         select max(movement_id) as movement_id
         from "&outCaslib.".&_prev_data_.
         ;
      quit;
      proc sql noprint;
         select
            movement_id into :calc_movement_id
         from &outLibref..calc_max_movement_&casTablesTag.;
      ;
      quit;

      %if %sysevalf(%superq(ds_in_aggregation_cfg) ne, boolean) %then %do;
         /* Retrieve mart aggregation rules */
         %if (%rsk_dsexist(&ds_in_aggregation_cfg.)) %then %do;
            proc sql noprint;
               select variable_name
                  into :calc_aggr_vars separated by ' '
               from &ds_in_aggregation_cfg.
               where upcase(schema_name) = upcase("&schema_name.")
                  and upcase(resolve(schema_version)) = upcase("&schema_version.")
                  and variable_name in (&quoted_prev_vars.)
               ;
            quit;

            %let tot_calc_aggr_vars = %sysfunc(countw(&calc_aggr_vars., %str( )));

            %let calc_comma_aggr_vars = %sysfunc(prxchange(s/\s+/%str(, )/i, -1, &calc_aggr_vars.));
            %let calc_quoted_aggr_vars = %sysfunc(prxchange(s/(\w+)/"$1"/i, -1, %bquote(&calc_comma_aggr_vars.)));

            /* Retrieve list of non-aggregated fields to pull from previous period */
            proc sql noprint;
               select prev_period_var
                  into :non_aggr_prev_vars separated by ' '
               from work.ds_in_calculation_cfg
               where prev_period_var not in (&calc_quoted_aggr_vars.);
            quit;

            /* If this is not the first adjustment ever made to this mart table and there are info available about how to aggregate the mart variables */
            %if(&calc_movement_id. > 1 and &tot_calc_aggr_vars. > 0) %then %do;
               /* Aggregate all data by the primary key */
               /* The CAS aggregation.aggregate action will be used to perform the initial summarization. */
               proc summary data = &outLibref..ds_in_prev_weighted_&casTablesTag. missing nway;
                  class &primary_key. &non_aggr_prev_vars.;
                  var &calc_aggr_vars.;
                  output
                     out = &outLibref..ds_in_prev_weighted_r_&casTablesTag. (drop = _type_ _freq_
                                                      /* Rename the previous period variables */
                                                      rename = (&rename_str.)
                                                      )
                     sum =
                  ;
               run;

               /* Move data to CAS to convert CHAR into VARCHAR variables */
               %core_cas_upload_and_convert(inLib = &outCaslib.
                                          , inTable = ds_in_prev_weighted_r_&casTablesTag.
                                          , encoding_adjustment = Y
                                          , outLib = &outCaslib.
                                          , outTable = ds_in_prev_weighted_r_&casTablesTag.
                                          , casSessionName = &casSessionName.
                                          );
            %end;
         %end; /* %if (%rsk_dsexist(&ds_in_aggregation_cfg.)) */
         %else %put NOTE: Aggregation config table work.&ds_in_aggregation_cfg. is not available. No aggregation will be applied.;

      %end; /* %if %sysevalf(%superq(ds_in_aggregation_cfg) ne, boolean) */

      %if (not %rsk_dsexist(&outLibref..ds_in_prev_weighted_r_&casTablesTag.)) %then %do;
         data work.ds_in_prev_weighted_r_&casTablesTag. /view=work.ds_in_prev_weighted_r_&casTablesTag.;
            set &outLibref..ds_in_prev_weighted_&casTablesTag.(rename = (&rename_str.));
         run;
         %core_cas_upload_and_convert(inLib = work
                                    , inTable = ds_in_prev_weighted_r_&casTablesTag.
                                    , encoding_adjustment = Y
                                    , outLib = &outCaslib.
                                    , outTable = ds_in_prev_weighted_r_&casTablesTag.
                                    , casSessionName = &casSessionName.
                                    );
      %end;

      data work.&ds_out.;
         /* Get the attributes of the previous period variables - 0 rows to create only the structure */
         attrib
            %rsk_get_attrib_def(ds_in = &outLibref..ds_in_prev_weighted_r_&casTablesTag., keep_vars = &prev_vars_rename.);
         ;
         stop;
      run;
   
      /* Move data to CAS to convert CHAR into VARCHAR variables */
      %core_cas_upload_and_convert(inLib = work
                        , inTable = &ds_out.
                        , encoding_adjustment = Y
                        , outLib = &outCasLib.
                        , outTable = _&ds_out._
                        , casSessionName = &casSessionName.
                        );

   %end; /* %if %sysevalf(%superq(ds_in_prev_period_key) ne, boolean) */

   /* *********************************************** */
   /*   Create the output data set and data map       */
   /* *********************************************** */

   data &outLibref..&ds_out. /*(promote=yes)*/;
      merge &outLibref..&_curr_data_.

      %if %sysevalf(%superq(ds_in_prev_period_key) ne, boolean) %then %do;
         &outLibref.._&ds_out._;

         /* Initialize any previous period variables to missing */
         %if %sysevalf(%superq(comma_prev_vars_r) ne, boolean) %then %do;
            call missing(&comma_prev_vars_r.);
         %end;

         /* Set lookup for retrieving the previous period fields */
         if _N_ = 1 then do;
            declare hash hPrev(dataset: "&outLibref..ds_in_prev_weighted_r_&casTablesTag.", multidata: "yes");
            hPrev.defineKey(&quoted_primary_key.);
            hPrev.defineData(&quoted_prev_vars_r.);
            hPrev.defineDone();
         end;
         _rc_ = hPrev.find();
         drop _rc_;
      %end;
      ;;
   run;

   /* Prepare the mapping configuration table */
   data work.&ds_out_map.;
      set work.ds_in_calculation_cfg (drop= analysis_type
                                       prev_period_var
                                       prev_period_var_rename
                                 );
   run;

%mend corew_process_calc_config;