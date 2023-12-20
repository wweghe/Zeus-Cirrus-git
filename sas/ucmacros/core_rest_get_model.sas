/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor core_rest_get_model

   \brief   Retrieve Model instance(s) registered in SAS Risk Cirrus Objects

   \param [in] host (optional) Host url, including the protocol
   \param [in] port (Optional) Server port.
   \param [in] server Name of the Web Application Server that provides the REST service (Default: riskCirrusObjects)
   \param [in] solution Solution identifier (Source system code) for Cirrus Core content packages (Default: currently blank)
   \param [in] logonHost (Optional) Host/IP of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app host/ip is the same as the host/ip in the url parameter
   \param [in] logonPort (Optional) Port of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app port is the same as the port in the url parameter
   \param [in] username (optional) Username credentials
   \param [in] password (optional) Password credentials: it can be plain text or SAS-Encoded (it will be masked during execution).
   \param [in] authMethod: Authentication method (accepted values: BEARER). (Default: BEARER).
   \param [in] client_id The client id registered with the Viya authentication server. If blank, the internal SAS client id is used (only if GRANT_TYPE = password).
   \param [in] client_secret The secret associated with the client id.
   \param [in] key Instance key of the Cirrus object that is fetched with this REST request. If no Key is specified, the records are fetched using filter parameters.
   \param [in] filter Filters to apply on the GET request when no value for key is specified. Example: request GET /models?name=Data1|Data2&statusCd=Draft
   \param [in] start Specify the starting point of the records to get. Start indicate the starting index of the subset. Start SHOULD be a zero-based index. The default start SHOULD be 0. Applicable only when a filter is used.
   \param [in] limit Limit controls the maximum number of items to get from the start position (Default = 1000). Applicable only when a filter is used.
   \param [in] inDsFileRefs input SAS dataset containing 2 columns:
      fref (character): a fileref (can be an existing fileref or a new one that will get created)
      codeEditorField (character): the name of a code editor custom field on the model.
      For each row in this dataset, the LUA parser will use/create a fileref pointing to a file containing the code of the corresponding codeEditorField.
      Note: this dataset is only used if the request is made to a single model (using the key= parameter)
   \param [in] debug True/False. If True, debugging informations are printed to the log (Default: false)
   \param [in] logOptions Logging options (i.e. mprint mlogic symbolgen ...)
   \param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y)
   \param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y)
   \param [out] outds Name of the output table that contains the model instances (Default: model_summary)
   \param [out] outVarToken Name of the output macro variable which will contain the access token (Default: accessToken)
   \param [out] outSuccess Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess)
   \param [out] outResponseStatus Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus)

   \details
   This macro sends a GET request to <b><i>\<host\>:\<port\>/riskCirrusObjects/objects/models</i></b> and collects the results in the output tables. \n
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

   2) Send a Http GET request and parse the JSON response into the output table work.model_summary.  Also,
   write the code from the code editor fields and point filerefs to those files as specified in work.codeFileRefs.

   \code

      data work.codeFileRefs;
         length fref $8 codeEditorField $128;
         fref="preMod";    codeEditorField="preModelCode";        output;
         fref="preRe";     codeEditorField="preReActionsCode";    output;
         fref="postRe";    codeEditorField="postReActionsCode";   output;
         fref="postMod";   codeEditorField="postModelCode";       output;
      run;

      %let accessToken =;
      %core_rest_get_model(key = f5cbf66a-1b06-4df7-914e-f108ae9c80a8
                           , inDsFileRefs = work.codeFileRefs
                           , outds = work.model_summary
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
%macro core_rest_get_model(host =
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
                        , key =
                        , filter =
                        , start =
                        , limit = 1000
                        , inDsFileRefs =
                        , outds = model_summary
                        , outVarToken = accessToken
                        , outSuccess = httpSuccess
                        , outResponseStatus = responseStatus
                        , debug = false
                        , logOptions =
                        , restartLUA = Y
                        , clearCache = Y
                        );

   %local requestUrl link_types_ds;

   /* Set the required log options */
   %if(%length(&logOptions.)) %then
      options &logOptions.;
   ;

   /* Get the current value of mlogic and symbolgen options */
   %local oldLogOptions;
   %let oldLogOptions = %sysfunc(getoption(mlogic)) %sysfunc(getoption(symbolgen));

   /* Set the base request URL */
   %core_set_base_url(host=&host, server=&server., port=&port.);
   %let requestUrl = &baseUrl./&server./objects/models;

   /* Add filters to the request URL */
   %core_set_rest_filter(key=&key., solution=&solution., filter=%superq(filter), start=&start., limit=&limit.);

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
                     , parser = sas.risk.cirrus.core_rest_parser.coreRestModel
                     , outds = &outds.
                     , arg1 = &inDsFileRefs.
                     , outVarToken = &outVarToken.
                     , outSuccess = &outSuccess.
                     , outResponseStatus = &outResponseStatus.
                     , debug = &debug.
                     , logOptions = &oldLogOptions.
                     , restartLUA = &restartLUA.
                     , clearCache = &clearCache.
                     );

      /* Exit in case of errors */
   %if(not &&&outSuccess.. or not %rsk_dsexist(&outds.)) %then %do;
      %put ERROR: Unable to get the model;
      %abort;
   %end;

%mend;