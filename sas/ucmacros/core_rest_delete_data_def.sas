/*
 Copyright C 2020-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file 
\anchor core_rest_delete_data_def

   \brief   Delete an data definition instance in SAS Risk Cirrus objects

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
   \param [in] key Instance key of the Analysis data object to delete.
   \param [in] outds Output table with rest call response.
   \param [in] debug True/False. If True, debugging informations are printed to the log (Default: false)
   \param [in] logOptions Logging options (i.e. mprint mlogic symbolgen ...)
   \param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y)
   \param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y)
   \param [out] outVarToken Name of the output macro variable which will contain the access token (Default: accessToken).
   \param [out] outSuccess Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess).
   \param [out] outResponseStatus Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus).

   \details
   This macro sends a DELETE request to <b><i>\<host\>:\<port\>/riskCirrusObjects/objects/datadefinition/</i></b> and collects the results in the output table. \n
   See \link core_rest_request.sas \endlink for details about how to send DELETE requests and parse the response.

   <b>Example:</b>

   1) Set up the environment (set SASAUTOS and required LUA libraries).  Assumes the spre folder is under /riskcirruscore/core/code_libraries/release-core-{cadence-version}
   \code
      %let cadence_version=2023.03;
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
      %core_rest_delete_data_def(host = <host>
                                      , port = <port>
                                      , server = riskCirrusObjects
                                      , username =
                                      , password =
                                      , key = <key>
                                      , outVarToken = accessToken
                                      , outSuccess = httpSuccess
                                      , outResponseStatus = responseStatus
                                      );
      %put &=accessToken;
      %put &=httpSuccess;
      %put &=responseStatus;
   \endcode


   \ingroup coreRestUtils

   \author  SAS Institute Inc.
   \date    2023
*/
%macro core_rest_delete_data_def(host =
                                      , port =
                                      , server = riskCirrusObjects
                                      , logonHost =
                                      , logonPort =
                                      , username =
                                      , password =
                                      , authMethod = bearer
                                      , key =
                                      , outds = _tmp_del_dd
                                      , outVarToken = accessToken
                                      , outSuccess = httpSuccess
                                      , outResponseStatus = responseStatus
                                      , debug = true
                                      , logOptions =
                                      , restartLUA = Y
                                      , clearCache = Y
                                      );

   %local requestUrl;

   /* Set the required log options */
   %if(%length(&logOptions.)) %then
      options &logOptions.;
   ;

   /* Get the current value of mlogic and symbolgen options */
   %local oldLogOptions;
   %let oldLogOptions = %sysfunc(getoption(mlogic)) %sysfunc(getoption(symbolgen));

   %if(%sysevalf(%superq(key) eq, boolean)) %then %do;
      %put ERROR: Required input parameter KEY is missing. Skipping execution..;
      %return;
   %end;

   /* Set the base Request URL */
   %core_set_base_url(host=&host, server=&server., port=&port.);
   %let requestUrl = &baseUrl./&server./objects/dataDefinitions/&key.;

   /* Temporary disable mlogic and symbolgen options to avoid printing of userid/pwd to the log */
   options nomlogic nosymbolgen;
   /* Send the REST request */
   %core_rest_request(url = &requestUrl.
                     , method = DELETE
                     , logonHost = &logonHost.
                     , logonPort = &logonPort.
                     , username = &username.
                     , password = &password.
                     , authMethod = &authMethod.
                     , parser =
                     , outds = &outds.
                     , fout =
                     , outVarToken = &outVarToken.
                     , outSuccess = &outSuccess.
                     , outResponseStatus = &outResponseStatus.
                     , debug = &debug.
                     , logOptions = &oldLogOptions.
                     , restartLUA = &restartLUA.
                     , clearCache = &clearCache.
                     );

%mend core_rest_delete_data_def;