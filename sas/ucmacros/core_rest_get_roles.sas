/*
 Copyright (C) 2022 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor core_rest_get_roles

   \brief   Gets classification namedTrees keys and names for registrations.

   \param [in] host (optional):       Host url, including the protocol
   \param [in] server:                Name of the Web Application Server that provides the REST service
                                        (Default: riskCirrusObjects)
   \param [in] port: (optional):      Server port
   \param [in] solution (optional):   Solution identifier (Source system code) for Cirrus Core content packages
                                        (Default: currently blank)
   \param [in] logonHost (Optional):  Host/IP of the sas-logon-app service or ingress.  If blank, it is assumed that
                                         the sas-logon-app host/ip is the same as the host/ip in the url parameter.
   \param [in] logonPort (Optional):  Port of the sas-logon-app service or ingress.  If blank, it is assumed that
                                         the sas-logon-app port is the same as the port in the url parameter
   \param [in] username (optional):   Username credentials
   \param [in] password (optional):   Password credentials: it can be plain text or SAS-Encoded
                                         (it will be masked during execution).
   \param [in] authMethod:            Authentication method (accepted values: BEARER). (Default: BEARER).
   \param [in] client_id:             The client id registered with the Viya authentication server. If blank, the internal
                                         SAS client id is used (only if GRANT_TYPE = password).
   \param [in] client_secret:         The secret associated with the client id.
   \param [in] filter:                Request filtering expression.
   \param [in] get_permissions:       Whether to include permissions information in the returned data. If true returns
                                         a single row for each permission of each role. Accepts Y/N. (Default: Y)
   \param [in] itemLimit:             Maximal number of items (registrations) there are in a server response. This macro
                                         assumes that all the neccessary data can be obtained in a single response,
                                         so the value should be no less than number of registrations filtered by <filter>.
                                         Default: 1000.
   \param [out] outds:                Name of the output table.
   \param [in] debug:                 If True, debugging informations are printed to the log (Default: false).
                                         Accepts true/false.
   \param [in] logOptions:            Logging options (i.e. mprint mlogic symbolgen ...)
   \param [in] clearCache:            Controls whether the connection cache is cleared across multiple proc http calls.
                                         Accepts Y/N. (Default: Y)
   \param [out] outVarToken:          Name of the output macro variable which will contain the access token
                                         (Default: accessToken)
   \param [out] outSuccess:           Name of the output macro variable that indicates if the request was successful
                                         (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess)
   \param [out] outResponseStatus:    Name of the output macro variable containing the HTTP response header status:
                                         i.e. HTTP/1.1 200 OK. (Default: responseStatus)
   \param [in] errflg:                The name of a macro variable name set to 1 in case of error.

   \details
      This macro generates a set of data about roles (key, objectId, sourceSystemCd, name, description) one row per role.
   Returned roles are subject to filtering by <filter>.
   Optionally, data about constituent permissions can be attached. The returned set contains one row per permission and role
   in that case.

   <b>Example:</b>

   1) Set up the environment (set SASAUTOS and required LUA libraries).
      Assumes the spre folder is under /riskcirruscore/core/code_libraries/release-core-{cadence-version}
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

   2) Run the macro to get output table WORK.__roles
   \code
      %core_rest_get_roles(host = <host>
                           , username = <username>
                           , password = <password>
                           , outds = WORK.__roles
                           );
   \endcode


   \ingroup coreRestUtils

   \author  SAS Institute Inc.
   \date    2022
*/


%macro core_rest_get_roles( host =
                              , server = riskCirrusObjects
                              , port =
                              , solution =
                              , logonHost =
                              , logonPort =
                              , username =
                              , password =
                              , authMethod = bearer
                              , client_id =
                              , client_secret =
                              , filter =
                              , get_permissions = N
                              , itemLimit = 1000
                              , outds =
                              , outVarToken = accessToken
                              , outSuccess = httpSuccess
                              , outResponseStatus = responseStatus
                              , debug = true
                              , logOptions =
                              , clearCache = Y
                              , errflg = );

   %local tmplib filter fields requestUrl oldLogOptions sscFilter;

   %if(%length(&logOptions.)) %then options &logOptions.;;
   %let oldLogOptions = %sysfunc(getoption(mlogic)) %sysfunc(getoption(symbolgen));

   %if %sysevalf(%superq(errflg) eq, boolean) %then %do;
      %local __err;
      %let errflg = __err;
   %end;
   %else %if not %symexist(%superq(errflg)) %then %do;
      %put WARNING: Specified ERRFLG macro variable does not exist;
      %local __err;
      %let errflg = __err;
   %end;
   %if %sysevalf(%superq(get_permissions) ne N, boolean) and %sysevalf(%superq(get_permissions) ne Y, boolean) %then %do;
      %put ERROR: Wrong GET_PERMISSIONS parameter;
      %let &errflg = 1;
      %return;
   %end;

   %core_set_base_url(host=&host, server=&server., port=&port.);
   %let fields = objectId,sourceSystemCd,name,description,key;
   %if &get_permissions. = Y %then %let fields = permissions,&fields.;
   %let solution = %upcase(%sysfunc(coalescec(&solution., RCC)));
   %let sscFilter = in(sourceSystemCd,%27&solution.%27,%27RCC%27);
   %let requestUrl = &baseUrl./&server./roles?fields=%superq(fields);
   %core_set_rest_filter(filter=%superq(filter),
                         customFilter=&sscFilter.,
                         start=0,
                         limit=%superq(itemLimit),
                         outUrlVar=requestUrl);

   %let tmplib = WORK;

   %if %sysevalf(%superq(outds) eq, boolean) %then %do;
      %let outds=&tmplib..__roles;
   %end;

   /* Temporary disable mlogic and symbolgen options to avoid printing of userid/pwd to the log */
   option nomlogic nosymbolgen;
   %core_rest_request(  url = &requestUrl
                        , method = GET
                        , logonHost = &logonHost.
                        , logonPort = &logonPort.
                        , username = &username.
                        , password = &password.
                        , authMethod = &authMethod.
                        , client_id = &client_id.
                        , client_secret = &client_secret.
                        , headerIn=Accept:application/json
                        , parser = sas.risk.cirrus.core_rest_parser.coreRestRoles
                        , arg1 = &get_permissions.
                        , outds = &outds.
                        , outVarToken=&outVarToken.
                        , outSuccess=&outSuccess.
                        , outResponseStatus=&outResponseStatus.
                        , debug = &debug.
                        , logOptions = &oldLogOptions.
                        , clearCache = &clearCache.
                        );

   %if not %superq(&outSuccess.) %then %do;
      %put ERROR: Request failure.;
      %let &errflg = 1;
      %goto exit;
   %end;

   %if %upcase(&debug.) = TRUE %then %do;
      %rsk_check_ds_sanity( &outds.,
                              key objectId sourceSystemCd,
                              check=KEY NEMPTY NEMPTY, card=0+, errflg = &errflg.);
      %if %sysevalf(%superq(&errflg) eq 1, boolean) %then %goto exit;
   %end;

   %exit:
      %return;
%mend;


