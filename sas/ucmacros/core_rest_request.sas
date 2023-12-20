/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor core_rest_request

   \brief   Send HTTP request to the specified REST API endpoint and parse the result

   \param [in] url Full url to the REST resource
   \param [in] logonHost (Optional) Host/IP of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app host/ip is the same as the host/ip in the url parameter
   \param [in] logonPort (Optional) Port of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app port is the same as the port in the url parameter
   \param [in] method The HTTP request method: GET/PUT/POST/DELETE. (Default: GET)
   \param [in] username Username credentials
   \param [in] password Password credentials: it can be plain text or SAS-Encoded (it will be masked during execution).
   \param [in] authMethod: Authentication method (accepted values: BEARER). (Default: BEARER).
   \param [in] client_id The client id registered with the Viya authentication server. If blank, the internal SAS client id is used (only if GRANT_TYPE = password).
   \param [in] client_secret The secret associated with the client id.
   \param [in] headerIn Optional. Request header: this can be either a fileref or a string.
   \param [in] headerOut Optional. Response header fileref.
   \param [in] body Optional. Request body: this can be either a fileref or a string.
   \param [in] fout Optional. Fileref for the reponse body. A temporary fileref is created is missing
   \param [in] contentType Optional. Request content type. (Default: application/json)
   \param [in] parser Name of the LUA parser function responsible for converting the JSON response into the output SAS table (Default: coreRestPlain -> plain one-level JSON structure). See \link core_rest_parser.lua \endlink for details).
   \param [in] debug True/False. If True, debugging informations are printed to the log (Default: false)
   \param [in] logOptions Logging options (i.e. mprint mlogic symbolgen ...)
   \param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y)
   \param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y)
   \param [in] printResponse Flag (Y/N). Controls whether the Reponse body is printed to the log when no log parser is provided (Default: Y)
   \param [in] arg1 Additional parameter passed to the LUA parser function
   \param [in] arg2 Additional parameter passed to the LUA parser function
   \param [in] arg3 Additional parameter passed to the LUA parser function
   \param [in] arg4 Additional parameter passed to the LUA parser function
   \param [in] arg5 Additional parameter passed to the LUA parser function
   \param [in] arg6 Additional parameter passed to the LUA parser function
   \param [in] arg7 Additional parameter passed to the LUA parser function
   \param [in] arg8 Additional parameter passed to the LUA parser function
   \param [out] outds Name of the output table
   \param [out] outVarToken Name of the output macro variable which will contain the Access Token (Default: accessToken)
   \param [out] outSuccess Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess)
   \param [out] outResponseStatus Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus)

   \details
   This macro performs the following operations:
      1. Get an Access Token for the requested URL (see \link core_rest_get_ticket.sas \endlink for details)
         1. a token is created only if the macro variable referenced by outVarToken is blank (i.e. -> <i> if &&&outVarToken = %str() </i>)
      2. Send Http request (GET/POST/PUT/...) to the specified URL
      3. Parse the JSON response using the specified parser function (only if the parser is provided)
      4. Return the parsed response into a SAS table.


   <b>Example:</b>

   1) Set up the environment (set SASAUTOS and required LUA libraries).  Assumes the spre folder is under /riskcirruscore/core/code_libraries/release-core-{cadence-version}
   \code
      %let cadence_version=2022.10;
      %let core_root_path=/riskcirruscore/core/code_libraries/release-core-&cadence_version.;
      option insert = (
         SASAUTOS = (
            "&core_root_path./spre/sas/ucmacros"
            )
         );
      filename LUAPATH ("&core_root_path./spre/lua");
   \endcode

   2) Send a Http GET request and parse the JSON response into the output table WORK.result using the default parser coreRestPlain
   \code
      %let accessToken =;
      %core_rest_request(url = <host>:<port>/riskCirrusObjects/<resourceType>/<resource>
                        , method = GET
                        , logonHost = &logonHost.
                        , logonPort = &logonPort.
                        , username = <userid>
                        , password = <pwd>
                        , headerIn = Accept:application/json
                        , body = {"param1":"value1", "param2":"value2", ...}
                        , parser = coreRestPlain
                        , outds = WORK.result
                        , outVarToken = accessToken
                        , outSuccess = httpSuccess
                        , outResponseStatus = responseStatus
                        );
      %put &=accessToken;
      %put &=httpSuccess;
      %put &=responseStatus;
   \endcode

   \note The structure of the output table depends on the JSON content and the parser function


   \ingroup irmRestUtils

   \author  SAS Institute Inc.
   \date    2022
*/

%macro core_rest_request(url =
                           , method = GET
                           , logonHost =
                           , logonPort =
                           , username =
                           , password =
                           , authMethod = bearer
                           , client_id =
                           , client_secret =
                           , refresh_token =
                           , user_token =
                           , authorization_code =
                           , grant_type =
                           , headerIn =
                           , headerOut =
                           , body =
                           , fout =
                           , contentType = application/json
                           , parser = coreRestPlain
                           , outds =
                           , outVarToken = accessToken
                           , outVarRefreshToken =
                           , outSuccess = httpSuccess
                           , outResponseStatus = responseStatus
                           , debug = false
                           , logOptions =
                           , restartLUA = Y
                           , clearCache = Y
                           , printResponse = Y
                           , logSeverity = ERROR
                           , arg1 =
                           , arg2 =
                           , arg3 =
                           , arg4 =
                           , arg5 =
                           , arg6 =
                           , arg7 =
                           , arg8 =
                           );

   %local
      request_url
      auth_options
      without_credentials_flg
      out_headerOut_flg
   ;

   /* Set the required log options */
   %if(%length(&logOptions.)) %then
      options &logOptions.;
   ;

   /* Get the current value of mlogic and symbolgen options */
   %local oldLogOptions;
   %let oldLogOptions = %sysfunc(getoption(mlogic)) %sysfunc(getoption(symbolgen));

   /* Delete output table if it exists */
   %if %sysevalf(%superq(outds)^=,boolean) %then %do;
       %if (%rsk_dsexist(&outds.)) %then %do;
          proc sql;
             drop table &outds.;
          quit;
       %end;
   %end;

   /* ************************************************ */
   /* Process the outSuccess parameter                 */
   /* ************************************************ */

   /* OutSuccess cannot be missing. Set a default value */
   %if(%sysevalf(%superq(outSuccess) =, boolean)) %then
      %let outSuccess = httpSuccess;

   /* Declare the output variable as global if it does not exist */
   %if(not %symexist(&outSuccess.)) %then
      %global &outSuccess.;


   /* ************************************************ */
   /* Process the OutResponseStatus parameter          */
   /* ************************************************ */

   /* OutResponseStatus cannot be missing. Set a default value */
   %if(%sysevalf(%superq(outResponseStatus) =, boolean)) %then
      %let outResponseStatus = responseStatus;

   /* Declare the output variable as global if it does not exist */
   %if(not %symexist(&outResponseStatus.)) %then
      %global &outResponseStatus.;


   /* ************************************************ */
   /* Process the OutVarToken parameter               */
   /* ************************************************ */

   /* OutVarToken cannot be missing. Set a default value */
   %if(%sysevalf(%superq(outVarToken) =, boolean)) %then
      %let outVarToken = accessToken;

   /* Declare the output variable as global if it does not exist */
   %if(not %symexist(&outVarToken.)) %then
      %global &outVarToken.;

   /* ************************************************ */
   /* Process the OutVarRefreshToken parameter               */
   /* ************************************************ */

   /* if outVarRefreshToken is provided, make sure it's not set to refresh_token and that it's a global macrovariable*/
   %if(%sysevalf(%superq(outVarRefreshToken) ne, boolean)) %then %do;
      %if &outVarRefreshToken.=refresh_token %then %do;
         %put ERROR: outVarRefreshToken cannot be set to refresh_token because refresh_token is a local macro parameter;
         %abort;
      %end;
      %if(not %symexist(&outVarRefreshToken.)) %then
         %global &outVarRefreshToken.;
   %end;


   /* Make sure the authMethod parameter is set */
   %if %sysevalf(%superq(authMethod) =, boolean) %then %do;
      %let authMethod = bearer;
      %put WARNING: Input parameter authMethod is blank. Using bearer authentication.;
   %end;
   %else
      %let authMethod = %lowcase(&authMethod.);


   /* ******************* */
   /* Get an Access Token */
   /* ******************* */

   /* Initialize authentication flags */
   %let without_credentials_flg = N;

   /* Check if we have an Access Token (Viya) */
   %if %sysevalf(%superq(&outVarToken.) =, boolean) %then %do;

      /* Check if we need to retrieve the credentials from the AUTHINFO file. */
      %if(/* No credentials ahs been provided */
          %sysevalf(%superq(username)=, boolean) and "%superq(password)" = ""
          and ( /* We are using Bearer authentication and the grant_type is either <blank> or "password" */
                &authMethod. = bearer and %sysfunc(prxmatch(/^(password)?$/i, %superq(grant_type)))
              )
          ) %then %do;
         /* Retrieve the credentials from the AUTHINFO file */
         %core_get_authinfo_credentials(url = %superq(url), outVarUser = username, outVarPwd = password, debug = &debug.);
         /* Check if we were able to retrieve the credentials */
         %if(%sysevalf(%superq(username)=, boolean) and "%superq(password)" = "") %then
            %let without_credentials_flg = Y;
      %end;


      /* Check if we need to generate a SAS Viya Access Token */
      %if(&authMethod. = bearer
                and (
                     /* If credentials have been provided  */
                     &without_credentials_flg. = N
                     /* OR No credentals AND grant_type is neither <blank> or "password" */
                     or not %sysfunc(prxmatch(/^(password)?$/i, %superq(grant_type)))
                     )
                ) %then %do;

         /* Temporary disable mlogic and symbolgen options to avoid printing of userid/pwd to the log */
         option nomlogic nosymbolgen;
         /* Get the Access Token (Don't throw Warnings/Errors in the log if we can't get the token, we can still try to use OAUTH_BEARER = SAS_SERVICES below) */
         %core_rest_get_token(url = &url.
                                , logonPort = &logonPort.
                                , logonHost = &logonHost.
                                , username = &username.
                                , password = &password.
                                , client_id = &client_id.
                                , client_secret = &client_secret.
                                , refresh_token = &refresh_token.
                                , user_token = &user_token.
                                , authorization_code = &authorization_code.
                                , grant_type = &grant_type.
                                , logSeverity = &logSeverity.
                                , outVarToken = &outVarToken.
                                , outVarRefreshToken = &outVarRefreshToken.
                                , debug = &debug.
                                , logOptions = &oldLogOptions.
                                );
      %end;
   %end; /* %if %sysevalf(%superq(&outVarToken.) =, boolean) */


   /* ************************************************ */
   /* Process the headerIn parameter                   */
   /* ************************************************ */
   %local hin_fref_flg;
   %let hin_fref_flg = N;
   %if(%sysevalf(%superq(headerIn) ne, boolean)) %then %do;
      /* Check if the headerIn is a fileref */
      %if(%length(%superq(headerIn)) <= 8) %then %do;
         %if(%sysfunc(fileref(&headerIn.)) ne 0) %then
            /* It is not a fileref. Will need to create a temporary file */
            %let hin_fref_flg = Y;
      %end;
      %else
         %let hin_fref_flg = Y;
   %end;

   /* Create a temporary fileref and write the header content in it */
   %if (&hin_fref_flg. = Y) %then %do;
      filename __hin__ temp;
      data _null_;
         file __hin__;
         put "%sysfunc(prxchange(s/[""]/""/i, -1, %superq(headerIn)))";
      run;

      %let headerIn = __hin__;
   %end;


   /* ************************************************ */
   /* Process the body parameter                       */
   /* ************************************************ */
   %if(%sysevalf(%superq(body) ne, boolean)) %then %do;
 	  /* Check if the body is a MULTI or FORM expression. If so, leave it alone */
	  %if(not %sysfunc(prxmatch(/[\s].*(?i)[multi|form] .*/, %superq(body)))) %then %do;
      	  /* Check if the body is a fileref */
	      %if(%length(%superq(body)) <= 8) %then %do;
	         %if(%sysfunc(fileref(&body.)) ne 0) %then %do;
	            /* It is not a fileref. Although short, it must be a string. Convert " to "" and enclose the entire string within double quotes.*/
	            %let body = "%sysfunc(prxchange(s/[""]/""/i, -1, %superq(body)))";
	         %end;
	      %end;
	      %else
	         /* Convert " to "" and enclose the entire string within double quotes. */
	         %let body = "%sysfunc(prxchange(s/[""]/""/i, -1, %superq(body)))";
	  %end;
   %end;


   /* ************************************************ */
   /* Process the headerOut parameter                  */
   /* ************************************************ */
   %let out_headerOut_flg = N;
   %if(%sysevalf(%superq(headerOut) =, boolean)) %then %do;
      %let headerOut = __hout_;
      %let out_headerOut_flg = Y;
   %end;

   /* Assign the HeaderOut fileref if it does not exist */
   %if(%sysfunc(fileref(&headerOut.)) ne 0) %then %do;
      filename &headerOut. temp;
   %end;

   /* Make sure the fout parameter is set */
   %let out_fref_flg = N;
   %if(%sysevalf(%superq(fout) =, boolean)) %then %do;
      %let fout = __out__;
      %let out_fref_flg = Y;
   %end;

   /* Check the fout parameter */
   %if(%sysevalf(%superq(fout) ne, boolean)) %then %do;
      /* Assign the fout fileref if it does not exist */
      %if(%sysfunc(fileref(&fout.)) gt 0) %then %do;
         filename &fout. temp;
      %end;
   %end;

   /* Check if the url contains any question mark */
   %local question_mark;
   %if(%index(&url.,?) = 0) %then
      %let question_mark = ?;

   /* Set the Request URL */
   %let request_url = &url.;

   /* Print the request URL to the log */
   %if (%upcase(&debug.) eq TRUE) %then %do;
      %put ------------------------------------------------;
      %put Request URL: %str(&request_url.);
      %put ------------------------------------------------;
   %end;

   /* Check if we are using Bearer authentication (Viya) */
   %if %sysevalf(%superq(&outVarToken.) ne, boolean) %then
      /* Set the Access Token in the request header */
      %let auth_options = oauth_bearer = "&&&outVarToken..";
   %else
      /* Rely on the cached credentials (only if this code is running on an authenticated Viya compute server session) */
      %let auth_options = oauth_bearer = sas_services;


   /* ************************************************ */
   /* Send HTTP request                                */
   /* ************************************************ */
   proc http
      url = "%str(&request_url.)"
      method = "%upcase(&method.)"
      /* Request header */
      %if(%sysevalf(%superq(headerIn) ne, boolean)) %then %do;
         headerin = &headerIn.
      %end;
      /* Request body */
      %if(%sysevalf(%superq(body) ne, boolean)) %then %do;
         in = &body.
      %end;
      /* Response header */
      headerout = &headerOut.
      /* Response body */
      %if(%sysevalf(%superq(fout) ne, boolean)) %then %do;
         out = &fout.
      %end;
      %if(%sysevalf(%superq(contentType) ne, boolean)) %then %do;
         ct = "&contentType."
      %end;
      /* Set the Bearer token or Http_TokenAuth authentication options */
      &auth_options.
      %if(%upcase(&clearCache.) = Y) %then %do;
         /* Clear connection cache */
         clear_cache
      %end;
      ;
      /* Add Debug level */
      %if(%upcase(&debug.) = TRUE) %then %do;
         debug level = 1;
      %end;
   run;


   /* Check the response header for errors */
   %let &outSuccess. = 0;
   %let &outResponseStatus. = Not Available;
   data _null_;
      infile &headerOut.;
      /* Read the response header */
      input;
      if _N_ = 1;
      /* Extract the response code */
      response_code = input(scan(_infile_, 2, " "), 8.);
      /* Check whether the HTTP return code is in the 200's (200 OK, 201 CREATED, etc) */
      success = response_code >= 200 and response_code < 300;
      call symputx("&outSuccess.", success, "F");
      call symputx("&outResponseStatus.", _infile_, "F");
      stop;
   run;

   /* Stop macro execution if there were any errors */
   %if(&&&outSuccess.. = 0) %then %do;
      %put &logSeverity.: The server response returned &&&outResponseStatus...;
      /* Avoid parsing the response. We will just return the response body in the output table as-is. */
      %let parser =;
   %end;

   /* Parse the request if a JSON parser is provided */
   %if(%sysevalf(%superq(parser) ne, boolean)) %then %do;
      /* Declare the PRODUCT macro variable, required in sas.risk.rmx.rsk_init @ line # 24 */
      %local
         product
         entrypoint_module
         entrypoint_function
      ;
      %let product = IRM;
      %let entrypoint_module = %sysfunc(prxchange(s/(^|(((\.?\w+)+)\.))(\w+)$/$3/i, -1, &parser.));
      %let entrypoint_function = %sysfunc(prxchange(s/(^|(((\.?\w+)+)\.))(\w+)$/$5/i, -1, &parser.));
      /* Assign default parser module sas.risk.cirrus.core_rest_parser if not specifically provided */
      %if(%sysevalf(%superq(entrypoint_module) =, boolean)) %then
         %let entrypoint_module = sas.risk.cirrus.core_rest_parser;

      %put NOTE: Parsing response using Lua module &entrypoint_module..&entrypoint_function.;

      /* Parse the JSON file and write the result in the output table */
      %rsk_call_riskexec( entrypoint_module         =   &entrypoint_module.
                            , entrypoint_function     =   &entrypoint_function.
                            , restartLUA              =   &restartLUA.
                            , arg1                    =   &fout.
                            , arg2                    =   &outds.
                            , arg3                    =   &arg1.
                            , arg4                    =   &arg2.
                            , arg5                    =   &arg3.
                            , arg6                    =   &arg4.
                            , arg7                    =   &arg5.
                            , arg8                    =   &arg6.
                            , arg9                    =   &arg7.
                            , arg10                   =   &arg8.
                            );
   %end;
   %else %do;
      /* Check if we got any response */
      %if(%sysfunc(fileref(&fout.)) = 0) %then %do;
         /* Read the server response AS-IS and return it into a table */
         data
            %if (%sysevalf(%superq(outds)^=, boolean)) %then
               &outds.;
            %else
               _null_;
            ;
            length response $32000.;
            infile &fout. lrecl = 32000;
            input;
            response = strip(_infile_);
            %if(&&&outSuccess.. = 0 or (%sysevalf(%superq(outds)=, boolean) and &printResponse. = Y)) %then %do;
               if _N_ = 1 then
                  put "Response from the Server:";
               put response;
            %end;
         run;
      %end;
   %end;

   /* Cleanup */
   %if(%upcase(&debug.) ne TRUE) %then %do;
      %if (&hin_fref_flg. = Y) %then %do;
         filename __hin__ clear;
      %end;
      %if(&out_fref_flg. = Y) %then %do;
         filename __out__ clear;
      %end;
      %if(&out_headerOut_flg. = Y) %then %do;
         filename __hout_ clear;
      %end;
   %end;

%mend;
