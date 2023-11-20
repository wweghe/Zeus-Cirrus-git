/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor core_rest_delete_flow_definition

   \brief   Delete a single flow definition registered in SAS Process Orchestration

   \param [in] host (optional) Host url, including the protocol
   \param [in] server Name of the Web Application Server that provides the REST service (Default: riskCirrusObjects)
   \param [in] solution Solution identifier (Source system code) for Cirrus Core content packages (Default: currently blank)
   \param [in] port (optional) Server port
   \param [in] logonHost (Optional) Host/IP of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app host/ip is the same as the host/ip in the url parameter
   \param [in] logonPort (Optional) Port of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app port is the same as the port in the url parameter
   \param [in] username (optional) Username credentials
   \param [in] password (optional) Password credentials: it can be plain text or SAS-Encoded (it will be masked during execution).
   \param [in] authMethod: Authentication method (accepted values: BEARER). (Default: BEARER).
   \param [in] client_id The client id registered with the Viya authentication server. If blank, the internal SAS client id is used (only if GRANT_TYPE = password).
   \param [in] client_secret The secret associated with the client id.
   \param [in] flow_id Id of the flow definition that is to be deleted
   \param [in] start Specify the starting point of the records to get. Start indicate the starting index of the subset. Start SHOULD be a zero-based index. The default start SHOULD be 0. Applicable only when a filter is used.
   \param [in] limit Limit controls the maximum number of items to get from the start position (Default = 1000). Applicable only when a filter is used.
   \param [in] debug True/False. If True, debugging informations are printed to the log (Default: false)
   \param [in] logOptions Logging options (i.e. mprint mlogic symbolgen ...)
   \param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y)
   \param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y)
   \param [out] outVarToken Name of the output macro variable which will contain the access token (Default: accessToken)
   \param [out] outSuccess Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess)
   \param [out] outResponseStatus Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus)

   \details
   This macro sends a DELETE request to <b><i>\<host\>:\<port\>/processOrchestration/flows</i></b> and to remove a flow definition.


   <b>Example:</b>

   1) Set up the environment (set SASAUTOS and required LUA libraries).  Assumes the spre folder is under /riskcirruscore/core/code_libraries/release-core-{cadence-version}
   \code
      %let cadence_version=2023.10;
      %let core_root_path=/riskcirruscore/core/code_libraries/release-core-&cadence_version.;
      option insert = (
         SASAUTOS = (
            "&core_root_path./spre/sas/ucmacros"
            )
         );
      filename LUAPATH ("&core_root_path./spre/lua");
   \endcode

   2) Send a Http GET request and parse the JSON response into the output table WORK.flow_definitions
   \code
      %let accessToken=;
      %core_rest_delete_flow_definition(  flow_id = 2102ccd8-6f32-11ee-b962-0242ac120002
                                        , outVarToken =accessToken
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




%macro core_rest_delete_flow_definition(host =
                                 , server = processOrchestration
                                 , solution =
                                 , port =
                                 , logonHost =
                                 , logonPort =
                                 , username =
                                 , password =
                                 , authMethod = bearer
                                 , client_id =
                                 , client_secret =
                                 , flow_id =
                                 , wait_flg = Y
                                 , pollInterval = 1
                                 , maxWait = 3600
                                 , outVarToken =accessToken
                                 , outSuccess = httpSuccess
                                 , outResponseStatus = responseStatus
                                 , debug = false
                                 , logOptions =
                                 , restartLUA = Y
                                 , clearCache = Y
                                 )  / minoperator;

   %local
      oldLogOptions
      requestUrl
      tmplib
      tmp_flow_def
      fbody
      keep_option
      fresp
      libref
      root
      resp_message
      execution_status
      current_dttm
      start_dttm
      rc
   ;

   /* Set the required log options */
   %if(%length(&logOptions.)) %then
      options &logOptions.;
   ;

   /* Get the current value of mlogic and symbolgen options */
   %let oldLogOptions = %sysfunc(getoption(mlogic)) %sysfunc(getoption(symbolgen));

   /* Set the base request URL */
   %core_set_base_url(host=&host, server=&server., port=&port.);
   %let requestUrl = &baseUrl./&server./flows;
   
   %let tmplib = work;

   /* Validate input parameter flow_id */
   %if(%sysevalf(%superq(flow_id) ne, boolean)) %then %do;
   
      %let tmp_flow_def = %rsk_get_unique_dsname(&tmplib.);
      
      /* Send the REST request to check if provided flow definition exists */
      %core_rest_get_flow_definitions(host = &host.
                                    , server = processOrchestration
                                    , solution = &solution.
                                    , port = &port.
                                    , logonHost = &logonHost.
                                    , logonPort = &logonPort.
                                    , username = &username.
                                    , password = &password.
                                    , authMethod = bearer
                                    , client_id = &client_id.
                                    , client_secret = &client_secret.
                                    , flow_id = &flow_id.
                                    , outds = &tmp_flow_def.
                                    , debug = &debug.
                                    , logOptions = &oldLogOptions.
                                    , restartLUA = &restartLUA.
                                    , clearCache = &clearCache.
                                    );
                                    
      %if (not %rsk_dsexist(&tmp_flow_def.) or %rsk_attrn(&tmp_flow_def., nlobs) eq 0) %then %do;
         %put ERROR: Could not find any flow definition with id "&flow_id.".;
         %abort;
      %end;
      
      /* Delete temporary table if it exists */
      %if (%rsk_dsexist(&tmp_flow_def.)) %then %do;
         proc sql;
            drop table &tmp_flow_def.;
         quit;
      %end;
    
      /* Set the request URL */
      %let requestUrl = &requestUrl./&flow_id.;
      
   %end;
   %else %do;
      %put ERROR: Input parameter flow_id is required. You must specify the flow definition to execute.;
      %abort;
   %end;
   
   /* Get a Unique fileref and assign a temp file to it */
   %let fresp = %rsk_get_unique_ref(prefix = resp, engine = temp);
   
   /* Temporary disable mlogic and symbolgen options to avoid printing of userid/pwd to the log */
   option nomlogic nosymbolgen;
   /* Send the REST request to trigger the flow run */
   %core_rest_request(url = &requestUrl.
                     , method = DELETE
                     , logonHost = &logonHost.
                     , logonPort = &logonPort.
                     , username = &username.
                     , password = &password.
                     , authMethod = &authMethod.
                     , headerIn = Accept:application/json
                     , body = 
                     , contentType = application/json
                     , parser =
                     , outds =
                     , fout = &fresp.
                     , printResponse = N
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
      %put ERROR: The request to delete the flow definition "&flow_id." was not successful.;
      %abort;
   %end;
   
   /* Clear out the temporary files */
   filename &fresp. clear;
   libname &libref. clear;
   
%mend;