/*
 Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor core_rest_point_project

   \brief   Project points on a set of named trees.

   \param [in] host (optional):       Host url, including the protocol
   \param [in] server:                Name of the Web Application Server that provides the REST service
                                         (Default: riskCirrusObjects)
   \param [in] port (optional):       Server port
   \param [in] logonHost (Optional):  Host/IP of the sas-logon-app service or ingress.  If blank, it is assumed that
                                         the sas-logon-app host/ip is the same as the host/ip in the url parameter
   \param [in] logonPort (Optional):  Port of the sas-logon-app service or ingress.  If blank, it is assumed that the
                                         sas-logon-app port is the same as the port in the url parameter
   \param [in] username (optional):   Username credentials
   \param [in] password (optional):   Password credentials: it can be plain text or SAS-Encoded
                                         (it will be masked during execution).
   \param [in] authMethod:            Authentication method (accepted values: BEARER). (Default: BEARER).
   \param [in] client_id:             The client id registered with the Viya authentication server. If blank,
                                         the internal SAS client id is used (only if GRANT_TYPE = password).
   \param [in] client_secret:         The secret associated with the client id.
   \param [in] point_set:             Data set of point keys to project.
   \param [in] point_var:             Column name of <point_set> containing the keys of points to project.
   \param [in] nmtk_set:              Data set of namedTreeKeys on which we project.
   \param [in] nmtk_var:              Column name of <nmtk_set> containing the keys of namedTrees to on which we project.
   \param [out] outds:                Name of the output table
   \param [in] itemLimit:             Maximal number of items (points) there are in a server response. This macro
                                         assumes that all the neccessary data can be obtained in a single response,
                                         so the value should be no less than number of unique <point_var> values.
                                         Default: 1000.
   \param [in] debug:                 True/False. If True, debugging informations are printed to the log (Default: false)
   \param [in] logOptions:            Logging options (i.e. mprint mlogic symbolgen ...)
   \param [in] clearCache:            Flag (Y/N). Controls whether the connection cache is cleared across multiple
                                         proc http calls. (Default: Y)
   \param [out] outVarToken:          Name of the output macro variable which will contain the access token
                                        (Default: accessToken)
   \param [out] outSuccess:           Name of the output macro variable that indicates if the request was successful
                                         (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess)
   \param [out] outResponseStatus:    Name of the output macro variable containing the HTTP response header status:
                                         i.e. HTTP/1.1 200 OK. (Default: responseStatus)

   \details
   This macro performs projection of points defined in <point_set> onto named trees defined in <nmtk_set>.
   Uses /classifications/points/projection endpoint.

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

   2) Run the macro to get output table WORK.projection
   \code
      %core_rest_point_project(host = <host>
                              , point_set = ptset
                              , point_var = ptvar
                              , nmtk_set = nmtkset
                              , nmtk_var = nmtkvar
                              );
   \endcode


   \ingroup coreRestUtils

   \author  SAS Institute Inc.
   \date    2022
*/

%macro core_rest_point_project(host =
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
                                 , point_set =
                                 , point_var =
                                 , nmtk_set =
                                 , nmtk_var =
                                 , outds =
                                 , itemLimit = 1000
                                 , debug = false
                                 , logOptions =
                                 , clearCache = Y
                                 , errflg =);


   %local oldLogOptions requestUrl;

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
      %put ERROR: Invalid DEBUG parameter;
      %let &errflg. = 1;
      %return;
   %end;

   %if not %isValidOrdN(%superq(itemLimit)) %then %do;
      %put ERROR: Invalid ITEMLIMIT parameter;
      %let &errflg. = 1;
      %return;
   %end;

   %rsk_check_ds_sanity(&point_set., &point_var., errflg = &errflg.);
   %rsk_check_ds_sanity(&nmtk_set., &nmtk_var., errflg = &errflg.);
   %if %sysevalf(%superq(&errflg) eq 1, boolean) %then %goto exit;

   %let tmplib = WORK;
   %let bodyFile = %rsk_get_unique_ref(prefix=_, type=FILE, engine=TEMP);

   %if %sysevalf(%superq(outds) eq, boolean) %then %do;
      %let outds=&tmplib..__proj_keys;
   %end;

	%local point_set_tmp nmtk_set_tmp;

   %let point_set_tmp = %rsk_get_unique_dsname(&tmplib.);
   proc sort data = &point_set. (keep=&point_var.) out = &tmplib..&point_set_tmp. nodupkey;
      by &point_var.;
   run;
   %let nmtk_set_tmp = %rsk_get_unique_dsname(&tmplib.);
   proc sort data = &nmtk_set. (keep=&nmtk_var.) out = &tmplib..&nmtk_set_tmp. nodupkey;
      by &nmtk_var.;
   run;

   filename &bodyFile. TEMP;
   %let _tmpvar = %rsk_get_unique_varname(&point_set.);
   data _null_;
      if _n_ = 1 and last then call symputx("&errflg.", 1, "F");
      file &bodyFile.;
      length &_tmpvar. $ 38;
      set &tmplib..&point_set_tmp.;

      if _n_ = 1 then put '{"pointKeys": [';
      %if %upcase(&debug.) = TRUE %then %do;
         if %isValidKeyInline(&point_var.) then do;
              &_tmpvar. = quote(strip(&point_var.));
            if _n_ gt 1 then put ',';
            put  &_tmpvar.;
         end;
         else do;
            call symputx("&errflg.", 1, 'F');
            abort;
         end;
      %end;
      %else %do;
         &_tmpvar. = quote(strip(&point_var.));
         if _n_ gt 1 then put ',';
         put  &_tmpvar.;
      %end;
   run;
   %if %sysevalf(%superq(&errflg) eq 1, boolean) %then %goto exit;

   %let _tmpvar = %rsk_get_unique_varname(&nmtk_set.);
   data _null_;
      if _n_ = 1 and last then call symputx("&errflg.", 1, "F");
      file &bodyFile. MOD;
      length &_tmpvar. $ 38;
      set &tmplib..&nmtk_set_tmp. end=last;

      if _n_ = 1 then put '],"targetNamedTreeKeys": [';
      %if %upcase(&debug.) = TRUE %then %do;
         if %isValidKeyInline(&nmtk_var.) then do;
            &_tmpvar. = quote(strip(&nmtk_var.));
            if _n_ gt 1 then put ',';
            put &_tmpvar.;
         end;
         else do;
            call symputx("&errflg.", 1, 'F');
            abort;
         end;
      %end;
      %else %do;
         &_tmpvar. = quote(strip(&nmtk_var.));
         if _n_ gt 1 then put ',';
         put  &_tmpvar.;
      %end;
      if last then put ']}';
   run;
   %if %sysevalf(%superq(&errflg) eq 1, boolean) %then %goto exit;

   %core_set_base_url(host=&host, server=&server., port=&port.);
   %let requestUrl = &baseUrl./&server./classifications/points/projection;
   %core_set_rest_filter(solution=, filter=, start=, limit=%superq(itemLimit), outUrlVar=requestUrl);

   /* Temporary disable mlogic and symbolgen options to avoid printing of userid/pwd to the log */
   option nomlogic nosymbolgen;

   %core_rest_request(url = &requestUrl.
                        , method = POST
                        , logonHost = &logonHost.
                        , logonPort = &logonPort.
                        , username = &username.
                        , password = &password.
                        , authMethod = &authMethod.
                        , contentType = application/vnd.sas.business.objects.point.projection.request+json
                        , body = &bodyFile.
                        , parser = sas.risk.cirrus.core_rest_parser.coreRestProject
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

   %if %upcase(&debug.) = TRUE %then %do;
      %rsk_check_ds_sanity(&outds., key, check=KEY, card=1+, errflg = &errflg.);
   %end;
   %else %do;
      %rsk_check_ds_sanity(&outds., key, errflg = &errflg.);
   %end;
   %if %sysevalf(%superq(&errflg) eq 1, boolean) %then %goto exit;

   %exit:
      %if not (&tmplib. eq WORK) %then %do; libname &tmplib; %end;
      %else %do;
	      %rsk_delete_ds(&tmplib..&point_set_tmp.);
	      %rsk_delete_ds(&tmplib..&nmtk_set_tmp.);
      %end;
      %return;
%mend;
