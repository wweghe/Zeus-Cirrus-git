/*
 Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor core_map_data

   \brief   Calculate a provided calculation config and update an Analysis Data with new values.

   \param [in] host (Optional) Host url, including the protocol.
   \param [in] port (Optional) Server port.
   \param [in] server Name of the Web Application Server that provides the REST service (Default: riskCirrusObjects).
   \param [in] logonHost (Optional) Host/IP of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app host/ip is the same as the host/ip in the url parameter.
   \param [in] logonPort (Optional) Port of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app port is the same as the port in the url parameter.
   \param [in] username Username credentials
   \param [in] password Password credentials: it can be plain text or SAS-Encoded (it will be masked during execution).
   \param [in] authMethod: Authentication method (accepted values: BEARER). (Default: BEARER).
   \param [in] client_id The client id registered with the Viya authentication server. If blank, the internal SAS client id is used (only if GRANT_TYPE = password).
   \param [in] client_secret The secret associated with the client id.
   \param [in] solution Solution identifier (Source system code) for Cirrus Core content packages (Default: currently blank)
   \param [in] ds_in
   \param [in] dataMap_key
   \param [in] map_type
   \param [in] ds_in_map_config
   \param [in] ds_out
   \param [in] ds_out_map
   \param [in] fout
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
      %core_map_data(ds_in =
                     , ds_in_map_config =
                     , map_type = CREDIT_RISK_DETAIL
                     , fout = fmap2
                     , ds_out =
                     , ds_out_map =
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

%macro core_map_data(host =
                     , port =
                     , server = riskCirrusObjects
                     , logonHost =
                     , logonPort =
                     , username =
                     , password =
                     , authMethod = bearer
                     , client_id =
                     , client_secret =
                     , solution =
                     , ds_in =
                     , dataMap_key =
                     , map_type =
                     , ds_in_map_config =
                     , ds_out =
                     , ds_out_map =
                     , fout =
                     , outVarToken = accessToken
                     , outSuccess = httpSuccess
                     , outResponseStatus = responseStatus
                     );

   %local map_config;

   %let map_config = &ds_in_map_config.;

   /*************************************************************************************************************************** */
   /* DISCLAIMER : Uncomment this block (for testing) for rest request call of 'dataMap' objects when fully available in Cirrus */
   /*************************************************************************************************************************** */

   /* Retrieve the specified dataMap */
   /*
   %if %sysevalf(%superq(dataMap_key) ne, boolean) %then %do;
      %let &outSuccess. = 0;
      %let &outResponseStatus. =;
      %core_rest_get_data_map(host = &host.
                           , port = &port.
                           , logonHost = &logonHost.
                           , logonPort = &logonPort.
                           , username = &username.
                           , password = &password.
                           , authMethod = &authMethod.
                           , client_id =  &client_id.
                           , client_secret = &client_secret.
                           , solution = &solution.
                           , key = &dataMap_key.
                           , outds = dataMap_summary
                           , outds_details = dataMap_details
                           , outVarToken = &outVarToken.
                           , outSuccess = &outSuccess.
                           , outResponseStatus = &outResponseStatus.
                           , restartLUA = Y
                           , clearCache = Y
                           );

      %if(not &&&outSuccess.. or not %rsk_dsexist(dataMap_details)) %then
         %return;

      data map_config;
         length target_var_length $32.;
         set dataMap_details(rename = (target_var_length = target_var_length_num)) end = last;
         drop target_var_length_num;

         if (not missing(target_var_type)) then do;
            if (target_var_type = "Char") then
               target_var_length = cats("$", put(coalesce(target_var_length_num, 32), 8.), ".");
            else
               target_var_length = "8.";
         end;
         expression_txt = coalescec(expression_txt, source_var_name);
         if last then
            call symputx("map_type", map_type, "L");
      run;

      %let map_config = map_config;

   %end;
   */
   /**************************** */
   /* end of commented block     */
   /**************************** */

   /* Apply mapping */
   %core_map_variables(ds_in = &ds_in.
                      , map_ds = &map_config.
                      , map_type = &map_type.
                      , include_wildcard_flg = N
                      , ds_out = &ds_out.
                      , ds_out_map = &ds_out_map.
                      , fout = &fout.
                      );

%mend core_map_data;