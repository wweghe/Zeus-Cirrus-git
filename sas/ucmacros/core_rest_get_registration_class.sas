/*
 Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor core_rest_get_registration_class

   \brief   Gets classification namedTrees keys and names for registrations.

   \param [in] host (optional):     Host url, including the protocol
   \param [in] server:              Name of the Web Application Server that provides the REST service
                                       (Default: riskCirrusObjects)
   \param [in] port: (optional):    Server port
   \param [in] solution (optional): Solution identifier (Source system code) for Cirrus Core content packages
                                      (Default: currently blank)
   \param [in] logonHost (Optional):Host/IP of the sas-logon-app service or ingress.  If blank, it is assumed that
                                       the sas-logon-app host/ip is the same as the host/ip in the url parameter.
   \param [in] logonPort (Optional):Port of the sas-logon-app service or ingress.  If blank, it is assumed that
                                       the sas-logon-app port is the same as the port in the url parameter
   \param [in] username (optional): Username credentials
   \param [in] password (optional): Password credentials: it can be plain text or SAS-Encoded
                                       (it will be masked during execution).
   \param [in] authMethod:          Authentication method (accepted values: BEARER). (Default: BEARER).
   \param [in] client_id:           The client id registered with the Viya authentication server. If blank, the internal
                                    SAS client id is used (only if GRANT_TYPE = password).
   \param [in] client_secret:       The secret associated with the client id.
   \param [in] filter:              Request filtering expression.
   \param [in] context:             Classification context used. Default: default.
   \param [in] get_ntk_names:       Whether to include namedTrees names in the returned data.
   \param [out] outds:              Name of the output table.
   \param [in] itemLimit:           Maximal number of items (registrations) there are in a server response. This macro
                                       assumes that all the neccessary data can be obtained in a single response,
                                       so the value should be no less than number of registrations filtered by <filter>.
                                       Default: 1000.
   \param [in] debug True/False:    If True, debugging informations are printed to the log (Default: false)
   \param [in] logOptions:          Logging options (i.e. mprint mlogic symbolgen ...)
   \param [in] clearCache:          Controls whether the connection cache is cleared across multiple proc http calls.
                                       Accepts Y/N. (Default: Y)
   \param [out] outVarToken:        Name of the output macro variable which will contain the access token
                                       (Default: accessToken)
   \param [out] outSuccess:         Name of the output macro variable that indicates if the request was successful
                                       (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess)
   \param [out] outResponseStatus:  Name of the output macro variable containing the HTTP response header status:
                                       i.e. HTTP/1.1 200 OK. (Default: responseStatus)
   \param [in] errflg:              The name of a macro variable name set to 1 in case of error.

   \details
      This macro returns classification namedTrees for registrations in the given <contex>. Optionally, names of the namedTrees
   can be obtained.

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

   2) Run the macro to get output table WORK.__reg_x_cls
   \code
      %core_rest_get_registration_class(  host = <host>
                                          , username = <username>
                                          , password = <password>
                                          );
   \endcode


   \ingroup coreRestUtils

   \author  SAS Institute Inc.
   \date    2022
*/

%macro core_rest_get_registration_class(host =
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
                                          , context = default
                                          , get_ntk_names = N
                                          , itemLimit = 1000
                                          , outds =
                                          , outVarToken = accessToken
                                          , outSuccess = httpSuccess
                                          , outResponseStatus = responseStatus
                                          , debug = true
                                          , logOptions =
                                          , clearCache = Y
                                          , errflg = );

   %local tmplib requestUrl oldLogOptions fields sscFilter;

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

   %if not %isValidId(%superq(context)) %then %do;
      %put ERROR: Wrong CONTEXT parameter;
      %let &errflg. = 1;
      %return;
   %end;

   %if not %isValidOrdN(%superq(itemLimit)) %then %do;
      %put ERROR: Wrong ITEMLIMIT parameter;
      %let &errflg. = 1;
      %return;
   %end;

   %if %sysevalf(%superq(get_ntk_names) ne N, boolean) and %sysevalf(%superq(get_ntk_names) ne Y, boolean) %then %do;
      %put ERROR: Wrong GET_NTK_NAMES parameter;
      %let &errflg. = 1;
      %return;
   %end;

   %let tmplib = WORK;

   %if %sysevalf(%superq(outds) eq, boolean) %then %do;
      %let outds=WORK.__reg_x_cls;
   %end;

   %core_set_base_url(host=&host, server=&server., port=&port.);
   %let fields = name,classification,sourceSystemCd,objectId,key;
   %let requestUrl = &baseUrl./&server./objectRegistrations?fields=%superq(fields);
   %let solution = %upcase(%sysfunc(coalescec(&solution., RCC)));
   %let sscFilter = in(sourceSystemCd,%27&solution.%27,%27RCC%27);
   %core_set_rest_filter(solution = ,
                           filter = %superq(filter),
                           customFilter=&sscFilter.,
                           start=0,
                           limit=&itemLimit.,
                           outUrlVar=requestUrl);

   %local __reg_x_cls_tmp;
   %let __reg_x_cls_tmp = %rsk_get_unique_dsname(&tmplib.);

   /* Temporary disable mlogic and symbolgen options to avoid printing of userid/pwd to the log */
   option nomlogic nosymbolgen;
   %core_rest_request(url = &requestUrl
                        , method = GET
                        , logonHost = &logonHost.
                        , logonPort = &logonPort.
                        , username = &username.
                        , password = &password.
                        , authMethod = &authMethod.
                        , client_id = &client_id.
                        , client_secret = &client_secret.
                        , headerIn=Accept:application/json
                        , parser = sas.risk.cirrus.core_rest_parser.coreRestRegistrationClass
                        , outds = &tmplib..&__reg_x_cls_tmp.
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

   %if %upcase(&debug.) = TRUE %then %do;
      %rsk_check_ds_sanity( &tmplib..&__reg_x_cls_tmp.,
                              objectId sourceSystemCd name key namedTreeKey,
                              check=NEMPTY NEMPTY NEMPTY KEY EKEY, errflg = &errflg.);
      %if %sysevalf(%superq(&errflg) eq 1, boolean) %then %goto exit;
   %end;

   %if &get_ntk_names. = Y %then %do;
      %local __urclass;
      %let __urclass = %rsk_get_unique_dsname(&tmplib.);
      proc sql;
         create view &tmplib..&__urclass. as
         select distinct namedTreeKey
         from &tmplib..&__reg_x_cls_tmp.;
      quit;

      %local key ccFile filter;

      %let __tmp_src = %rsk_get_unique_catname(work.temp).source;
      %let ccFile = %rsk_get_unique_ref(prefix=_, type=FILE, engine=TEMP);

      filename &ccFile catalog "&__tmp_src.";
      %rsk_col_to_macvars(&tmplib..&__urclass., namedtreekey, __ntk, qchar=%nrstr(%27), sep=%str(,), fileref=&ccFile, errflg = &errflg.);
      %include &ccFile.;

      %let filter = in(key,&__ntk.);

      %rsk_delete_ds(&__urclass.);

      %local __ntk_x_ntnm;
      %let __ntk_x_ntnm = %rsk_get_unique_dsname(&tmplib.);

       /* Temporary disable mlogic and symbolgen options to avoid printing of userid/pwd to the log */
      option nomlogic nosymbolgen;
      %core_rest_get_namedtree_info(host = &host.
                                       , server = &server.
                                       , port = &port.
                                       , logonHost = &logonHost.
                                       , logonPort = &logonPort.
                                       , username = &username.
                                       , password = &password.
                                       , authMethod = &authMethod.
                                       , client_id = &client_id.
                                       , client_secret = &client_secret.
                                       , outVarToken = &outVarToken.
                                       , outSuccess = &outSuccess.
                                       , outResponseStatus = &outResponseStatus.
                                       , filter = %superq(filter)
                                       , outds = &tmplib..&__ntk_x_ntnm.
                                       , debug = &debug.
                                       , logOptions = &logOptions.
                                       , clearCache = &clearCache.
                                       , errflg = &errflg.);

      %if not %superq(&outSuccess.) %then %do;
         %put ERROR: Request failure.;
         %let &errflg = 1;
         %goto exit;
      %end;
      %if %sysevalf(%superq(&errflg) eq 1, boolean) %then %goto exit;

      %rsk_delete_ds(&outds.);
      proc sql;
         create table &outds. as
         select   x.*,
                  y.name as namedTreeName

         from &tmplib..&__reg_x_cls_tmp. x
         left join &tmplib..&__ntk_x_ntnm. y
         on x.namedTreeKey = y.key
         where context = "&context.";
      quit;
      %rsk_delete_ds(&__ntk_x_ntnm.);
   %end;
   %else %do;
      %rsk_delete_ds(&outds.);
      data &outds.;
         set &tmplib..&__reg_x_cls_tmp. (where = (context = "&context."));
      run;
   %end;

   %exit:
      %if not (&tmplib. eq WORK) %then %do; libname &tmplib; %end;
      %else %do;
         %rsk_delete_ds(&__reg_x_cls_tmp.);
      %end;
      %return;
%mend;
