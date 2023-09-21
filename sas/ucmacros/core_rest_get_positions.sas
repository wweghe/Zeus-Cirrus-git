/*
 Copyright (C) 2022 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor core_rest_get_positions

   \brief   Gets list of positions.

   \param [in] host (optional):          Host url, including the protocol
   \param [in] server:                   Name of the Web Application Server that provides the REST service
                                            (Default: riskCirrusObjects)
   \param [in] port: (optional):         Server port
   \param [in] solution:                 Solution identifier (Source system code) for Cirrus Core content packages
                                            (Default: currently blank)
   \param [in] logonHost (optional):     Host/IP of the sas-logon-app service or ingress.  If blank, it is assumed that
                                            the sas-logon-app host/ip is the same as the host/ip in the url parameter.
   \param [in] logonPort (optional):     Port of the sas-logon-app service or ingress.  If blank, it is assumed that
                                            the sas-logon-app port is the same as the port in the url parameter
   \param [in] username (optional):      Username credentials
   \param [in] password (optional):      Password credentials: it can be plain text or SAS-Encoded
                                            (it will be masked during execution).
   \param [in] authMethod:               Authentication method (accepted values: BEARER). (Default: BEARER).
   \param [in] client_id:                The client id registered with the Viya authentication server. If blank, the internal
                                            SAS client id is used (only if GRANT_TYPE = password).
   \param [in] client_secret:            The secret associated with the client id.
   \param [in] filter:                   Filters to apply on the GET request.
   \param [in] itemLimit:                Maximal number of items (objects) there are in a server response.
                                            Default: 1000.
   \param [in] maxIter:                  Maximal number of object requests sent.
   \param [out] outds:                   Name of the output table.
   \param [out] outVarToken:             Name of the output macro variable which will contain the access token
                                            (Default: accessToken)
   \param [out] outSuccess:              Name of the output macro variable that indicates if the request was successful
                                            (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess)
   \param [out] outResponseStatus:       Name of the output macro variable containing the HTTP response header status:
                                            i.e. HTTP/1.1 200 OK. (Default: responseStatus)
   \param [in] debug True/False:         If True, debugging informations are printed to the log (Default: false)
   \param [in] logOptions:               Logging options (i.e. mprint mlogic symbolgen ...)
   \param [in] clearCache:               Controls whether the connection cache is cleared across multiple proc http calls.
                                            Accepts Y/N. (Default: Y).
   \param [in] errflg (optional):        The name of a macro variable name set to 1 in case of error.

   \details
      This macro returns list of positions. The list of reported objects can be narrowed down by using <filter>.
      This macro sends multiple requests getting <itemLimit> of positions at a time. The maximal number of requests sent is
      <maxIter>.

      Data returned:
         - ident: identity id (e.g. userId)
         - point: key of the point where the position is set
         - role: key of role that's granted

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

   2) Run the macro to get output table WORK.positions
   \code
      %core_rest_get_positions(  filter = eq(role,00000000-0000-0000-0000-000000000000)
                                 , outds=work.positions
                                 , outVarToken = accessToken
                                 , outSuccess = httpSuccess
                                 , outResponseStatus = responseStatus);

   \endcode

   \ingroup coreRestUtils

   \author  SAS Institute Inc.
   \date    2022

*/

%macro core_rest_get_positions(host =
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
                                 , itemLimit = 1000
                                 , maxIter = 1000
                                 , outds =
                                 , outVarToken = accessToken
                                 , outSuccess = httpSuccess
                                 , outResponseStatus = responseStatus
                                 , debug = true
                                 , logOptions =
                                 , clearCache = Y
                                 , errflg = );

   %local tmplib fields sscFilter;
   %local __chcnt start limit leave;

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

   %let tmplib = WORK;

   %if %sysevalf(%superq(outds) eq, boolean) %then %do;
      %let outds=&tmplib..__positions;
   %end;

   %rsk_delete_ds(&outds.);
   
   %local __ident_x_point_tmp __root;   
   %let __ident_x_point_tmp = %rsk_get_unique_dsname(&tmplib.);
   %let __root = %rsk_get_unique_dsname(&tmplib., exclude = &__ident_x_point_tmp.);

   %core_set_base_url(host=&host., server=&server., port=&port.);
   %local requestUrl;
   %let fields = group,user,points,role,sourceSystemCd,key;

   %let __chcnt = 1;
   %let start = 0;
   %let leave = 0;

   %do %while (&__chcnt. < &maxiter. and not &leave.);

      %let requestUrl = &baseUrl/&server./positions?fields=%superq(fields);
      %let solution = %upcase(%sysfunc(coalescec(&solution., RCC)));
      %let sscFilter = in(sourceSystemCd,%27&solution.%27,%27RCC%27);
      %core_set_rest_filter(  filter=%superq(filter),
                              customFilter=&sscFilter.,
                              start=&start.,
                              limit=&itemLimit.,
                              outUrlVar=requestUrl);

      %rsk_delete_ds(&tmplib..&__ident_x_point_tmp.);
      %rsk_delete_ds(&tmplib..&__root.);

      /* Temporary disable mlogic and symbolgen options to avoid printing of userid/pwd to the log */
      option nomlogic nosymbolgen;
      %core_rest_request(url = &requestUrl.
                           , method = GET
                           , logonHost = &logonHost.
                           , logonPort = &logonPort.
                           , username = &username.
                           , password = &password.
                           , authMethod = &authMethod.
                           , client_id = &client_id.
                           , client_secret = &client_secret.
                           , parser = sas.risk.cirrus.core_rest_parser.coreRestPositions
                           , outds = &tmplib..&__ident_x_point_tmp.
                           , arg1 = &tmplib..&__root.
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

      %rsk_check_ds_sanity(&tmplib..&__root., start count limit, check=ORD ORD ORD, card=1, errflg = &errflg.);
      %if %sysevalf(%superq(&errflg) eq 1, boolean) %then %goto exit;

      %if %upcase(&debug.) = TRUE %then %rsk_check_ds_sanity(&tmplib..&__ident_x_point_tmp.,
                                                               ident point role,
                                                               check=NEMPTY EKEY EKEY, card=0+, errflg = &errflg.);
      %if %sysevalf(%superq(&errflg) eq 1, boolean) %then %goto exit;

      %if (&__chcnt. eq 1) %then %do;
         data &outds.;
            set &tmplib..&__ident_x_point_tmp.;
         run;
      %end;
      %else %do;
         proc append base=&outds. data=&tmplib..&__ident_x_point_tmp.;
         quit;
      %end;
      data _null_;
         set &tmplib..&__root.;
         if start+limit le start then call symputx("&errflg", 1, "F");
         call symputx('start', put(start+limit, best8.), 'L');
         call symputx('count', put(count, best8.), 'L');
      run;
      %if %sysevalf(%superq(&errflg) eq 1, boolean) %then %do;
         %put ERROR: Object request iteration failure.;
         %goto exit;
      %end;

      %if &start. ge &count. %then %let leave = 1;
      %let __chcnt = %eval(&__chcnt + 1);
   %end;

   proc sort data = &outds. nodupkey;
      by ident point;
   run;

   %exit:
      %if not (&tmplib. eq WORK) %then %do; libname &tmplib; %end;
      %else %do;
         %rsk_delete_ds(&tmplib..&__ident_x_point_tmp.);
         %rsk_delete_ds(&tmplib..&__root.);
      %end;
      %return;

%mend;

