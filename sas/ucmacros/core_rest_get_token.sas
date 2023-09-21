/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/




/**
   \file
\anchor core_rest_get_token

   \brief   Authenticate and retrieve a Viya access token from the SAS Logon Manager


   \param [in] url Base URL to the Viya server
   \param [in] logonHost (Optional) Host/IP of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app host/ip is the same as the host/ip in the url parameter
   \param [in] logonPort (Optional) Port of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app port is the same as the port in the url parameter
   \param [in] username Username credentials.
   \param [in] password Password credentials: it can be plain text or SAS-Encoded (it will be masked during execution). Used when GRANT_TYPE = password. If either username or password are left blank, an attempt is made to retrieve the credentials from an AUTHINFO file.
   \param [in] client_id The client id registered with the Viya authentication server.  If grant_type=password, this parameter is optional, and if it is not provided, the internal SAS client id is used.  For all other grant types, this parameter is required.
   \param [in] client_secret The secret associated with the client id.  Required unless the internal SAS client id is used or grant_type = user_token
   \param [in] refresh_token A refresh token used to retrieve a new access token. Used when GRANT_TYPE = refresh_token.
   \param [in] user_token A user token (access token) used to retrieve a new access token using whatever client is specified for client_id. Used when GRANT_TYPE = user_token.
   \param [in] authorization_code An authorization code used to retrieve the access token. Used when GRANT_TYPE = authorization_code.
   \param [in] grant_type Controls the mechanism through which the access token is retrieved. Valid options: password|refresh_token|authorization_code|user_token.
   \param [in] debug Enable debugging: True/False. If True, debugging infos are printed to the log, including credentials/tokens, etc. (Default: false)
   \param [in] logOptions = Logging options (i.e. mprint mlogic symbolgen ...)
   \param [out] outVarToken Name of the output macro variable which will contain the Access Token (Default: accessToken)
   \param [out] ds_out (Optional) Name of the output dataset containing the Access Token as well as additional information.

   \details

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

   2) Get the access token
   \code
      %let accessToken =;
      %let url = <protocol>://<host>:<port>;
      %core_rest_get_token(url = &url.
                           , username = <userid>
                           , password = <pwd>
                           , outVarToken = accessToken
                           );
      %put accessToken = &accessToken.;
   \endcode

   \ingroup macroUtils

   \author  SAS Institute Inc.
   \date    2021
*/

%macro core_rest_get_token(url =
                          , logonHost =
                          , logonPort =
                          , username =
                          , password =
                          , client_id =
                          , client_secret =
                          , refresh_token =
                          , user_token =
                          , authorization_code =
                          , grant_type =
                          , logSeverity = Error
                          , outVarToken = accessToken
                          , outVarRefreshToken =
                          , ds_out =
                          , debug = false
                          , logOptions =
                          );

   %local
      use_internal_client_id_flg
      baseUrl
      oldLogOptions
      requestBody
      status_code
      status_message
      i
   ;

   /* Set the required log options */
   %if(%length(&logOptions.)) %then %do;
      options &logOptions.;
   %end;

   /* Get the current value of notest, mprint, mlogic and symbolgen options */
   %let oldLogOptions =
      %sysfunc(getoption(notes))
      %sysfunc(getoption(mprint))
      %sysfunc(getoption(mlogic))
      %sysfunc(getoption(symbolgen))
   ;

   /* Initialize the status code. Assume Error unless we succesfully retrieve a token */
   %let status_code = Error;

   /* Make sure output variable outVarToken is set */
   %if %sysevalf(%superq(outVarToken) =, boolean) %then
      %let outVarToken = accessToken;

   /* Declare the output variable as global if it does not exist */
   %if not %symexist(&outVarToken.) %then
      %global &outVarToken.;

   /* Initialize output varable */
   %let &outVarToken. =;

   /* if outVarRefreshToken is provided, make sure it's not set to refresh_token and that it's global*/
   %if(%sysevalf(%superq(outVarRefreshToken) ne, boolean)) %then %do;
      %if &outVarRefreshToken.=refresh_token %then %do;
         %put ERROR: outVarRefreshToken cannot be set to refresh_token because refresh_token is a local macro parameter;
         %abort;
      %end;
      %if not %symexist(&outVarRefreshToken.) %then
         %global &outVarRefreshToken.;
   %end;

   /* Set a default value for the debug parameter (if missing) */
   %if %sysevalf(%superq(debug) =, boolean) %then
      %let debug = false;
   %else
      %let debug = %lowcase(&debug.);

   /* Extract the base url */
   %let baseUrl = %sysfunc(prxchange(s/((http[s]?):\/)?([^:\/]+)((:\d+)|\/|$)(.*)/$1$3$5/i, -1, %superq(url)));
   %let ingressUrl = ;
   %if %sysfunc(sysexist(SAS_SERVICES_URL)) %then
      %let ingressUrl= %sysget(SAS_SERVICES_URL);

   /* We base the SASLogon URL off of the input url if:
         a. $SAS_SERVICES_URL is not defined (we're not in the cluster), or
         b. The input url = $SAS_SERVICES_URL (the user has explicitly said to use the ingress URL)
      Otherwise, we set SASLogon to http://sas-logon-app:80 or https://sas-logon-app:443
         (and overwrite with logonHost and logonPort if provided)
   */
   %if "&ingressUrl."="" or "&baseUrl."="&ingressUrl." %then %do;
      %if %sysevalf(%superq(baseUrl) =, boolean) %then %do;
         %if %sysevalf(%superq(url) =, boolean) %then
            %let status_message = Input parameter url is required.;
         %else
            %let status_message = Input parameter url: &url. is not a valid url;
         %goto ERROR;
      %end;

      /* Update the host and port in the URL to be logonHost and logonPort respectively, if provided */
      %if %sysevalf(%superq(logonHost) ne, boolean) %then %do;
         %let logonHost=%sysfunc(prxchange(s/((https?):\/\/)?(.*)/$3/i, -1, %superq(host)));
         %let baseUrl = %sysfunc(prxchange(s/((http[s]?):\/)?([^:\/]+)((:\d+)|\/|$)/${1}&logonHost.$4/i, -1, %superq(baseUrl)));
      %end;

      %if %sysevalf(%superq(logonPort) ne, boolean) %then
         %let baseUrl = %sysfunc(prxchange(s/((http[s]?):\/)?([^:\/]+)((:\d+)|\/|$)/$1$3:&logonPort./i, -1, %superq(baseUrl)));
   %end;
   %else %do;
      %core_set_base_url(host=&logonHost, server=SASLogon, port=&logonPort.);
   %end;

   /* Validate the Grant_Type parameter */
   %if %sysevalf(%superq(grant_type) =, boolean) %then %do;
      /* Check if we have a user token */
      %if %sysevalf(%superq(user_token) ne, boolean) %then
         %let grant_type = user_token;
      /* Check if we have a refresh token */
      %else %if %sysevalf(%superq(refresh_token) ne, boolean) %then
         %let grant_type = refresh_token;
      /* Check if we have an authorization code */
      %else %if %sysevalf(%superq(authorization_code) ne, boolean) %then
         %let grant_type = authorization_code;
      %else
         /* Default to Password grant type */
         %let grant_type = password;
   %end;

   /* Make sure we have a valid grant type option */
   %let grant_type = %lowcase(&grant_type.);
   %if(not %sysfunc(prxmatch(/user_token|refresh_token|authorization_code|password/i, &grant_type.))) %then %do;
      %let status_message = Input parameter grant_type = &grant_type. is invalid. Valid options (case insensitive) are: user_token, refresh_token, authorization_code, password;
      %goto ERROR;
   %end;

   /* Initialize the flag */
   %let use_internal_client_id_flg = N;
   /* initialize the request body */
   %let requestBody = grant_type=&grant_type.;

   %if(&grant_type. = password) %then %do;
      /* Check if username and password have been provided */
      %if %sysevalf(%superq(username) =, boolean) or %sysevalf(%superq(password) =, boolean) %then %do;
         /* Check if we can retrieve the credentials from the authinfo file */
         %core_get_authinfo_credentials(url = %superq(baseUrl), outVarUser = username, outVarPwd = password, debug = &debug.);
         /* Check if we were able to retrieve the credentials */
         %if %sysevalf(%superq(username) =, boolean) or %sysevalf(%superq(password) =, boolean) %then %do;
            %let status_message = No username or password were specified. Either specify these parameters or configure the credentials in your AUTHINFO file.;
            %goto ERROR;
         %end;
      %end;
      /* Use internal SAS client id (no secret) if the client_id has not been specified */
      %if %sysevalf(%superq(client_id) =, boolean) %then %do;
         %let use_internal_client_id_flg = Y;
         %let client_id = sas.ec:;
         %let client_secret =;
      %end;

      /* Add username and password to the request body */
      %let requestBody = &requestBody.%nrstr(&)username=%bquote(&username.)%nrstr(&)password=%sysfunc(urlencode(%bquote(&password.)));
   %end;
   %else %do; /* grant_type = refresh_token|authorization_code|user_token */

      /* Check for the case of a refresh token */
      %if(&grant_type. = refresh_token) %then %do;
         /* Make sure we have a refresh token */
         %if %sysevalf(%superq(refresh_token) =, boolean) %then %do;
            %let status_message = Input parameter refresh_token is missing. A valid refresh token is required when grant_type = &grant_type.;
            %goto ERROR;
         %end;

         /* Add the refresh_token to the request body */
         %let requestBody = &requestBody.%nrstr(&)refresh_token=&refresh_token.;
      %end;

      /* Check for the case of an authorization code */
      %if(&grant_type. = authorization_code) %then %do;
         /* Make sure we have an authorization code */
         %if %sysevalf(%superq(authorization_code) =, boolean) %then %do;
            %let status_message = Input parameter authorization_code is missing. A valid authorization code is required when grant_type = &grant_type.;
            %goto ERROR;
         %end;

         /* Add the authorization_code to the request body */
         %let requestBody = &requestBody.%nrstr(&)code=&authorization_code.;
      %end;

      /* Check for the case of a user token */
      %if(&grant_type. = user_token) %then %do;

         /* Make sure we have a user token */
         %if %sysevalf(%superq(user_token) =, boolean) %then %do;
            %let status_message = Input parameter user_token is missing. A valid user token (access token) is required when grant_type = &grant_type.;
            %goto ERROR;
         %end;

         /* Add the client_id to the request body */
         %let requestBody = &requestBody.%nrstr(&)client_id=&client_id.;

      %end;

   %end;

   /* Unless we are using the internal SAS client id (sas.ec:) both client_id and secret are required */
   %if(&use_internal_client_id_flg. = N) %then %do;
      /* Make sure the client_id is not missing */
      %if %sysevalf(%superq(client_id) =, boolean) %then %do;
         %let status_message = Input parameter client_id is missing. A valid client_id is required when grant_type = &grant_type.;
         %goto ERROR;
      %end;

      /* Make sure the client_secret is not missing */
      %if %sysevalf(%superq(client_secret) =, boolean) %then %do;
         %let status_message = Input parameter client_secret is missing. A valid client_secret is required when grant_type = &grant_type.;
         %goto ERROR;
      %end;
   %end;

   /* Assign a temporary filename to hold the server response */
   %let fref = %rsk_get_unique_ref(prefix = tmp, engine = temp, debug = &debug.);
   /* Set the authentication url */
   %let url = &baseUrl./SASLogon/oauth/token;

   %if(&debug. ne true) %then %do;
      /* Avoid printing credentials to the log */
      option nomprint nomlogic nosymbolgen;
   %end;

   /* Send the request */
   proc http
      url    = "&url."
      out    = &fref.
      method = 'post'
      ct     = 'application/x-www-form-urlencoded'
      in     = "&requestBody."
      /* Add the client id and secret in case of authorization-code and refresh-token grant types */
      %if(&use_internal_client_id_flg. = N) and (&grant_type. ne user_token) %then %do;
         webUsername = "&client_id."
         webPassword = "&client_secret."
         auth_basic
      %end;
      clear_cache
      ;

      headers
         "accept" = "application/json"
         %if(&use_internal_client_id_flg. = Y) %then %do;
            "Authorization" = "Basic %sysfunc(putc(&client_id., $base64x16.))"
         %end;
         %if(&grant_type. = user_token) %then %do;
            "Authorization" = "bearer &user_token." /*note that keyword "bearer" here must be lowercase due to a UAA bug*/
         %end;
      ;
   run;

   %if(&debug. ne true) %then %do;
      /* Restore log options */
      option &oldLogOptions.;
   %end;

   /* Check if we got a response from the server */
   %if not %symexist(sys_prochttp_status_code) %then %do;
      %let status_message = No response was received from the Server at the following url: &url.;
      /* Deassign the fileref and exit */
      filename &fref.;
      %goto ERROR;
   %end;

   /* Check the response code from the server */
   %if(&sys_prochttp_status_code. ne 200) %then %do;
      %let status_message = Expected response code 200 from the server. Received instead: &sys_prochttp_status_code. &sys_prochttp_status_phrase..;
      /* Print the server response */
      %rsk_print_file(file = %sysfunc(pathname(&fref.))
                      , title = Response from the Server:
                      , logSeverity = WARNING
                      );
      /* Deassign the fileref and exit */
      filename &fref.;
      %goto ERROR;
   %end;

   /* Assign libref to parse the JSON response */
   %let libref = %rsk_get_unique_ref(type = lib, engine = JSON, args = fileref = &fref.);

   /* Ensure the libref variable was set */
   %if %sysevalf(%superq(libref) ne, boolean) %then %do;
      /* Read the access token and refresh token (if returned in the response) */
      data
         %if %sysevalf(%superq(ds_out) ne, boolean) %then
            &ds_out.;
         %else
            _null_;
         ;
         length
            token_type $32.
            access_token $4098.
            refresh_token $4098.
            scope $4098.
            status_code $32.
            status_message $2048.
         ;
         set &libref..root;
         keep
            token_type
            access_token
            refresh_token
            scope
            status_code
            status_message
         ;
         status_code = "Successful";
         call symputx("&outVarToken.", access_token, "F");

         %if(%rsk_varexist(&libref..root, refresh_token)) %then %do;
            call symputx("&outVarRefreshToken.", refresh_token, "F");
         %end;
      run;

      /* Deassign the JSON library and the response body */
      libname &libref.;
      filename &fref.;
   %end;

   /* Make sure we got the access token */
   %if %sysevalf(%superq(&outVarToken.) eq, boolean) %then %do;
      %let status_message = Could not get an Access Token from the server.;
      %goto ERROR;
   %end;

   %return;

/* Error handling */
%ERROR:

   /* Create output table (if required) */
   %if %sysevalf(%superq(ds_out) ne, boolean) %then %do;
      data &ds_out.;
         length
            token_type $32.
            accessToken $4098.
            refresh_token $4098.
            scope $4098.
            status_code $32.
            status_message $2048.
         ;
         status_code = "&status_code.";
         status_message = "&status_message.";
      run;
   %end;

   /* Print the error message */
   %put %upcase(&logSeverity.): &status_message.;
%mend;
