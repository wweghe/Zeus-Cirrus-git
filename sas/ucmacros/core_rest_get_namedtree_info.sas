/*
 Copyright (C) 2022 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor core_rest_get_namedtree_info

   \brief   Gets attributes of named trees by keys.

   \param [in] host (optional):       Host url, including the protocol
   \param [in] server:                Name of the Web Application Server that provides the REST service
                                        (Default: riskCirrusObjects)
   \param [in] port: (optional):      Server port
   \param [in] logonHost (Optional):  Host/IP of the sas-logon-app service or ingress.  If blank, it is assumed that
                                         the sas-logon-app host/ip is the same as the host/ip in the url parameter.
   \param [in] logonPort (Optional):  Port of the sas-logon-app service or ingress.  If blank, it is assumed that
                                         the sas-logon-app port is the same as the port in the url parameter.
   \param [in] username (optional):   Username credentials
   \param [in] password (optional):   Password credentials: it can be plain text or SAS-Encoded
                                         (it will be masked during execution).
   \param [in] authMethod:            Authentication method (accepted values: BEARER). (Default: BEARER).
   \param [in] client_id:             The client id registered with the Viya authentication server. If blank, the internal
                                         SAS client id is used (only if GRANT_TYPE = password).
   \param [in] client_secret:         The secret associated with the client id.
   \param [in] filter:                Request filtering expression.
   \param [out] outds:                Name of the output table
   \param [in] debug True/False:      If True, debugging informations are printed to the log (Default: false)
   \param [in] logOptions:            Logging options (i.e. mprint mlogic symbolgen ...)
   \param [in] clearCache:            Controls whether the connection cache is cleared across multiple proc http calls.
                                         Accepts Y/N. (Default: Y)
   \param [out] outVarToken:          Name of the output macro variable which will contain the access token
                                         (Default: accessToken)
   \param [out] outSuccess:           Name of the output macro variable that indicates if the request was successful
                                         (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess)
   \param [out] outResponseStatus:    Name of the output macro variable containing the HTTP response header status:
                                         i.e. HTTP/1.1 200 OK. (Default: responseStatus)
   \param [in] itemLimit:             Maximal number of items (registrations) there are in a server response. This macro
                                         assumes that all the neccessary data can be obtained in a single response,
                                         so the value should be no less than number of registrations filtered by <filter>.
                                         Default: 1000.
   \param [in] logOptions:            Logging options (i.e. mprint mlogic symbolgen ...)
   \param [in] clearCache:            Controls whether the connection cache is cleared across multiple proc http calls.
                                         Accepts Y/N. (Default: Y).
   \param [in] errflg:                The name of a macro variable name set to 1 in case of error.


   \details
      This macro returns attributes of named tree for named tree keys inlcuded in <keys>:
      - key
      - sourceSystemCd
      - objectId
      - name
      - classTypeKey

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

   2) Run the macro to get output table WORK.__nt_info.
   \code
      %core_rest_get_namedtree_info(host = <host>
                                    , username = <username>
                                    , password = <password>
                                    , keys = <key1> <key2> ...
                                    , outds = __nt_info
                                    );
   \endcode


   \ingroup coreRestUtils

   \author  SAS Institute Inc.
   \date    2022
*/


%macro core_rest_get_namedtree_info(host =
                                       , server = riskCirrusObjects
                                       , port =
                                       , logonHost =
                                       , logonPort =
                                       , username =
                                       , password =
                                       , authMethod = bearer
                                       , client_id =
                                       , client_secret =
                                       , outVarToken = accessToken
                                       , outSuccess = httpSuccess
                                       , outResponseStatus = responseStatus
                                       , itemLimit = 1000
                                       , filter =
                                       , outds =
                                       , debug = false
                                       , logOptions =
                                       , clearCache = Y
                                       , errflg =);


   %local respFile mapFile tmplib jsonlib i oldLogOptions requestUrl;

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

   %rsk_validation_funcs

   %if not %isValidBool(%superq(debug)) %then %do;
      %put ERROR: Wrong DEBUG parameter;
      %let &errflg. = 1;
      %return;
   %end;

   %let tmplib = WORK;

   %if %sysevalf(%superq(outds) eq, boolean) %then %do;
      %let outds=WORK.__nt_info;
   %end;

   %core_set_base_url(host=&host, server=&server., port=&port.);
   %let fields = name,classTypeKey,sourceSystemCd,objectId,key;
   %let requestUrl = &baseUrl./&server./classifications/namedTrees?fields=%superq(fields);
   %core_set_rest_filter(solution = ,
                         filter = %superq(filter),
                         start=0,
                         limit=&itemLimit.,
                         outUrlVar=requestUrl);

   /* Temporary disable mlogic and symbolgen options to avoid printing of userid/pwd to the log */
   option nomlogic nosymbolgen;

   %core_rest_request(url = &requestUrl
                      , method = GET
                      , logonHost = &logonHost.
                      , logonPort = &logonPort.
                      , username = &username.
                      , password = &password.
                      , authMethod = &authMethod.
                      , parser = sas.risk.cirrus.core_rest_parser.coreRestNamedTreeInfo
                      , outds = &outds.
                      , outVarToken = &outVarToken.
                      , outSuccess = &outSuccess.
                      , outResponseStatus = &outResponseStatus.
                      , debug = &debug.
                      , logOptions = &oldLogOptions.
                      , clearCache = &clearCache.
                      );

   %if not %superq(&outSuccess.) %then %do;
      %put ERROR: Request failure.;
      %let &errflg = 1;
      %goto exit;
   %end;

   %if %upcase(&debug.) = TRUE %then %rsk_check_ds_sanity(&outds.,
                                                            key sourceSystemCd objectId name classTypeKey,
                                                            check=KEY NEMPTY NEMPTY NEMPTY KEY, card=1+,
                                                            errflg=&errflg);

   %exit:
      %if not (&tmplib. eq WORK) %then %do; libname &tmplib; %end;
%mend;