/*
 Copyright (C) 2022 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor core_rest_get_attachment_history
   \brief   Retrieve the attachment (and deleted attachment) history for an object instance registered in SAS Risk Cirrus Objects

   \param [in] host (Optional) Host url, including the protocol.
   \param [in] port (Optional) Server port.
   \param [in] server Name of the Web Application Server that provides the REST service (Default: riskCirrusObjects)
   \param [in] solution The solution short name from which this request is being made. This will get stored in the createdInTag and sharedWithTags attributes on the object (Default: 'blank').
   \param [in] username Username credentials
   \param [in] password Password credentials: it can be plain text or SAS-Encoded (it will be masked during execution)
   \param [in] authMethod: Authentication method (accepted values: BEARER). (Default: BEARER)
   \param [in] client_id The client id registered with the Viya authentication server. If blank, the internal SAS client id is used (only if GRANT_TYPE = password).
   \param [in] client_secret The secret associated with the client id.
   \param [in] objectType The type of object for which to retrieve the attachment history
   \param [in] objectKey Instance key of the Cirrus object of objectType.  Attachment history for this object instance will be retrieved.
   \param [in] logOptions Logging options (i.e. mprint mlogic symbolgen ...).
   \param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y).
   \param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y).
   \param [out] outds_history Name of the output table that contains the attachment history (Default: work.attachment_history).
   \param [out] outds_deleted Name of the output table that contains the deleted attachment history (Default: work.deleted_attachment_history).
   \param [out] outVarToken Name of the output macro variable which will contain the access token (Default: accessToken).
   \param [out] outSuccess Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess).
   \param [out] outResponseStatus Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus).

   \details
   This macro sends a GET request to
      attachment history (outds_history):  <b><i>\<host\>:\<port\>/riskCirrusObjects/objects/<objectType>/<objectKey>/attachments/history</i></b>
      attachments deleted (outds_deleted): <b><i>\<host\>:\<port\>/riskCirrusObjects/objects/<objectType>/<objectKey>/attachments/deleted</i></b>
   and collects the results in the output tables. \n
   See \link core_rest_request.sas \endlink for details about how to send GET requests and parse the response.

   <b>Example:</b>

   1) Set up the environment (set SASAUTOS and required LUA libraries).  Assumes the spre folder is under /riskcirruscore/core/code_libraries/release-core-{cadence-version}
   \code
      %let core_root_path=/riskcirruscore/core/code_libraries/release-core-%sysget(SAS_RISK_CIRRUS_CADENCE);
      option insert = (
         SASAUTOS = (
            "&core_root_path./spre/sas/ucmacros"
            )
         );
      filename LUAPATH ("&core_root_path./spre/lua");
   \endcode

   2) Send a Http GET request and parse the JSON response into the output table work.wf_process_history_summary and work.wf_process_history_tasks
   \code
      %let accessToken =;
      %core_rest_get_attachment_history(solution = ACL
                                       , objectType = cycles
                                       , objectKey = 30b7809f-cf98-4490-b6ce-dcb88d8e445c
                                       , outds_history = attachment_history
                                       , outds_deleted = deleted_attachment_history
                                       , debug = true
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
%macro core_rest_get_attachment_history(host =
                                       , logonHost =
                                       , LogonPort =
                                       , server = riskCirrusObjects
                                       , solution =
                                       , port =
                                       , username =
                                       , password =
                                       , authMethod = bearer
                                       , client_id =
                                       , client_secret =
                                       , objectType =
                                       , objectKey =
                                       , outds_history = attachment_history
                                       , outds_deleted = deleted_attachment_history
                                       , outVarToken = accessToken
                                       , outSuccess = httpSuccess
                                       , outResponseStatus = responseStatus
                                       , debug = false
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

   %if %sysevalf(%superq(objectType) eq, boolean) %then %do;
      %put ERROR: The objectType parameter must be specified;
      %abort;
   %end;

   %if %sysevalf(%superq(objectKey) eq, boolean) %then %do;
      %put ERROR: The objectKey parameter must be specified;
      %abort;
   %end;

   %if "&outds_history." ne "" %then %do;

      /* Set the base Request URL */
      %core_set_base_url(host=&host, server=&server., port=&port.);
      %let requestUrl=&baseUrl./&server./objects/&objectType./&objectKey./attachments/history;

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
                        , parser = sas.risk.cirrus.core_rest_parser.coreRestFileAttachmentsHistory
                        , outds = &outds_history.
                        , outVarToken = &outVarToken.
                        , outSuccess = &outSuccess.
                        , outResponseStatus = &outResponseStatus.
                        , debug = &debug.
                        , logOptions = &oldLogOptions.
                        , restartLUA = &restartLUA.
                        , clearCache = &clearCache.
                        );

      /* Exit in case of errors */
      %if(not &&&outSuccess.. or not %rsk_dsexist(&outds_history.)) %then %do;
         %put ERROR: Unable to get the attachments history (objectType=&objectType., objectKey=&objectKey.);
         %abort;
      %end;

   %end;

   %if "&outds_deleted." ne "" %then %do;

      /* Set the base Request URL */
      %core_set_base_url(host=&host, server=&server., port=&port.);
      %let requestUrl=&baseUrl./&server./objects/&objectType./&objectKey./attachments/deleted;

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
                        , parser = sas.risk.cirrus.core_rest_parser.coreRestFileAttachmentsHistory
                        , outds = &outds_deleted.
                        , outVarToken = &outVarToken.
                        , outSuccess = &outSuccess.
                        , outResponseStatus = &outResponseStatus.
                        , debug = &debug.
                        , logOptions = &oldLogOptions.
                        , restartLUA = &restartLUA.
                        , clearCache = &clearCache.
                        );

      /* Exit in case of errors */
      %if(not &&&outSuccess.. or not %rsk_dsexist(&outds_deleted.)) %then %do;
         %put ERROR: Unable to get the deleted attachments history (objectType=&objectType., objectKey=&objectKey.);
         %abort;
      %end;

   %end;

%mend;