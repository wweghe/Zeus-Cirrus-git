/*
 Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor core_rest_get_objects_with_users

   \brief   Gets objects of a type together with linked users.

   \param [in] host (optional):          Host url, including the protocol
   \param [in] server:                   Name of the Web Application Server that provides the REST service
                                            (Default: riskCirrusObjects)
   \param [in] port: (optional):         Server port
   \param [in] solution:                 The solution short name from which this request is being made.
                                           createdInTag and sharedWithTags attributes are tested against this value
                                           (Default: 'blank').
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
                                            Example: isUnder('00000000-0000-0000-0000-000000000000')
   \param [in] objectType:               Type of the queried object as used by endpoints, e.g models
   \param [in] userLinkTypes (optional): List of user link keys to use. If empty all linked users links are reported.
   \param [in] itemLimit:                Maximal number of items (objects) there are in a server response.
                                            Default: 1000.
   \param [in] maxIter:                  Maximal number of object requests sent.
   \param [in] getLinkId:                Whether to get ids of types of links [Y/N]
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
   \param [in] restartLUA:               Resets the state of Lua code submission for a SAS session if set to Y [Y/N]
                                            (Default: Y)
   \param [in] errflg (optional):        The name of a macro variable name set to 1 in case of error.

   \details
      This macro returns objects of a type given by <objectType> together with ids of users linked to each of the objects.
      The list of reported objects can be narrowed down by using <filter>.
      The list of reported link types can be narrowed down by <userLinkTypes>, which specifies keys of types of links that
      are reported. Optionally, for each type of link its id can be reported if <getLinkId>=Y.
      This macro sends multiple requests getting <itemLimit> of objects at a time. The maximal number of requests sent is
      <maxIter>.

      Data returned:
         - objectKey: object key
         - objectId: object id
         - sourceSystemCd: object's sourceSystemCd
         - name: object name
         - description: object description
         - linkType: key of the type of the link
         - userId: linked user id
         - linkId (optiional): id of the type of the link

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

   2) Run the macro to get output table WORK.__model_x_user.
   \code
      %core_rest_get_objects_with_users(  filter = %nrstr(isUnder%(%27)00000000-0000-0000-0000-000000000000%nrstr(%27%))
                                          , solution = MRM
                                          , objectType = models
                                          , outds=work.__model_x_user
                                          , outVarToken = accessToken
                                          , outSuccess = httpSuccess
                                          , outResponseStatus = responseStatus);

   \endcode

   \ingroup coreRestUtils

   \author  SAS Institute Inc.
   \date    2022

*/


%macro core_rest_get_objects_with_users(  host =
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
                                             , objectType =
                                             , userLinkTypes =
                                             , itemLimit = 1000
                                             , maxIter = 1000
                                             , getLinkId = N
                                             , outds =
                                             , outVarToken = accessToken
                                             , outSuccess = httpSuccess
                                             , outResponseStatus = responseStatus
                                             , debug = true
                                             , logOptions =
                                             , clearCache = Y
                                             , restartLUA = Y
                                             , errflg = );

   %local filter i oldLogOptions;

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

   %if not %isValidId(%superq(objectType)) %then %do;
      %put ERROR: Wrong OBJECTTYPE parameter;
      %let &errflg. = 1;
      %return;
   %end;

   %if %sysevalf(%superq(userLinkTypes) ne, boolean) %then %do;
      %let i = 1;
      %do %while (%qscan(%superq(userLinkTypes), &i.,, sr) ne );
         %if not %isValidKey(%qscan(%superq(userLinkTypes), &i.,, sr)) %then %do;
            %put ERROR: Wrong USERLINKTYPES parameter;
            %let &errflg. = 1;
            %return;
         %end;
         %let i = %eval(&i + 1);
      %end;
      %let userLinkTypes = %rsk_quote_list(list=%superq(userLinkTypes), quote=%str(%"), dlm = %str( ), case=);
   %end;

   %if %sysevalf(%superq(getLinkId) ne N, boolean) and %sysevalf(%superq(getLinkId) ne Y, boolean) %then %do;
      %put ERROR: Wrong GETLINKID parameter;
      %let &errflg. = 1;
      %return;
   %end;

   %local fields __chcnt start limit leave requestUrl;
   %let tmplib = WORK;

   %if %sysevalf(%superq(outds) eq, boolean) %then %do;
      %let outds=&tmplib..__object_x_user;
   %end;

   %rsk_delete_ds(&outds.);

   %let __chcnt = 1;
   %let start = 0;
   %let leave = 0;

   %let __object_x_user_tmp = %rsk_get_unique_dsname(&tmplib.);
   %let __root = %rsk_get_unique_dsname(&tmplib., exclude=&__object_x_user_tmp.);

   %core_set_base_url(host=&host., server=&server., port=&port.);
   %local requestUrl;
   %let fields = objectLinks,objectId,sourceSystemCd,name,description,createdInTag,key;

   %do %while (&__chcnt. < &maxiter. and not &leave.);

      %let requestUrl = &baseUrl./&server./objects/%superq(objectType)?fields=%superq(fields);
      %core_set_rest_filter(  solution = %superq(solution),
                              filter = %superq(filter),
                              start=&start.,
                              limit=&itemLimit.,
                              outUrlVar=requestUrl);

      /* Temporary disable mlogic and symbolgen options to avoid printing of userid/pwd to the log */
      option nomlogic nosymbolgen;
      %core_rest_request(  url = &requestUrl.
                           , method = GET
                           , logonHost = &logonHost.
                           , logonPort = &logonPort.
                           , username = &username.
                           , password = &password.
                           , authMethod = &authMethod.
                           , client_id = &client_id.
                           , client_secret = &client_secret.
                           , parser = sas.risk.cirrus.core_rest_parser.coreRestObjectWithUsers
                           , outds = &tmplib..&__object_x_user_tmp.
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

      %rsk_check_ds_sanity(&tmplib..&__root, start count limit, check=ORD ORD ORD, card=1, errflg=&errflg.);
      %if %sysevalf(%superq(&errflg) eq 1, boolean) %then %goto exit;

      data &tmplib..&__object_x_user_tmp (rename = (key = objectKey));
         set &tmplib..&__object_x_user_tmp (where = (linkType in (&userLinkTypes.)));
      run;

      %if %upcase(&debug.) = TRUE %then %rsk_check_ds_sanity(  &tmplib..&__object_x_user_tmp.,
                                                               objectKey objectId sourceSystemCd,
                                                               check=KEY NEMPTY NEMPTY, errflg=&errflg.);
      %if %sysevalf(%superq(&errflg) eq 1, boolean) %then %goto exit;

      %if (&__chcnt. eq 1) %then %do;
         data &outds.;
            set &tmplib..&__object_x_user_tmp.;
         run;
      %end;
      %else %do;
         proc append base=&outds. data=&tmplib..&__object_x_user_tmp.;
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

   %if &getLinkId. = Y %then %do;

      %let __linkTypeKeys = %rsk_get_unique_dsname(&tmplib.);
      proc sql;
         create view &tmplib..&__linkTypeKeys. as
         select distinct linkType
         from &outds.;
      quit;

      %local __lt __lt_no;
      %rsk_col_to_macvars(&tmplib..&__linkTypeKeys., linkType, __lt, qchar=%nrstr(%27), sep=%str(,),
                          cut=2048, scope = F, errflg = &errflg.);
      %rsk_delete_ds(&tmplib..&__linkTypeKeys.);
      %if %sysevalf(%superq(&errflg) eq 1, boolean) %then %goto exit;

      %if &__lt_no gt 1 %then %do;
         %put ERROR: Too many linkedTypes;
         %let &errflg. = 1;
         %goto exit;
      %end;
      %else %if &__lt_no lt 1 %then %goto exit;

      %let __linkTypeInfo = %rsk_get_unique_dsname(&tmplib.);
      /* Temporary disable mlogic and symbolgen options to avoid printing of userid/pwd to the log */
       option nomlogic nosymbolgen;
      %core_rest_get_link_types( host = &host.
                                 , port = &port.
                                 , server = &server.
                                 , solution = RCC
                                 , logonHost = &logonHost.
                                 , logonPort = &logonPort.
                                 , username = &username.
                                 , password = &password.
                                 , authMethod = &authMethod.
                                 , client_id = &client_id.
                                 , client_secret = &client_secret.
                                 , filter = in(key,%superq(__lt))
                                 , limit = &itemLimit.
                                 , outds = &tmplib..&__linkTypeInfo.
                                 , outVarToken = &outVarToken.
                                 , outSuccess = &outSuccess.
                                 , outResponseStatus = &outResponseStatus.
                                 , debug = &debug.
                                 , logOptions = &logOptions.
                                 , restartLUA = &restartLUA.
                                 , clearCache = &clearCache.
                                 );

      %if not %superq(&outSuccess.) %then %do;
         %put ERROR: Request failure.;
         %let &errflg = 1;
         %goto exit;
      %end;

      %let __linkTypeFmt = %rsk_get_unique_dsname(&tmplib.);
      data &tmplib..&__linkTypeFmt. / view= &tmplib..&__linkTypeFmt.;
         set &tmplib..&__linkTypeInfo. end=last;
         length fmtname $ 9 start $ 36 end $ 36 label $ 50 type $ 1 hlo $ 1;

         fmtname = "linkTypes"; start = key; end = key; label = objectId; type = 'C'; hlo = ' '; output;
         if last then do;
            fmtname = "linkTypes"; label = ''; type = 'C'; hlo = 'O'; output;
         end;
         keep fmtname start end label type hlo;
      run;

      %local __catnm oldFmtSearch;
      %let __catnm = %rsk_get_unique_catname(&tmplib.);
      %let oldFmtSearch = %sysfunc(getoption(fmtsearch));
      options fmtsearch=(&__catnm.);
      proc format cntlin=&tmplib..&__linkTypeFmt. library=&__catnm.;
      run;

      data &outds.;
         set &outds.;
         length linkId $ 50;
         if not missing(linkType) then do;
            linkId = put(linkType, $linkTypes.);
            if missing(linkId) then call symputx("&errflg", 1, 'F');
         end;
      run;
      options fmtsearch=(&oldFmtSearch.);

      %if %sysevalf(%superq(&errflg) eq 1, boolean) %then %do;
         %put ERROR: Unknown LINKTYPE;
         %goto exit;
      %end;
   %end;

   %exit:
      %if not (&tmplib. eq WORK) %then %do; libname &tmplib; %end;

      %rsk_delete_ds(&tmplib..&__object_x_user_tmp.);
      %rsk_delete_ds(&tmplib..&__root.);
      %if %symexist(__linkTypeKeys) %then %rsk_delete_ds(&tmplib..&__linkTypeKeys.);
      %if %symexist(__linkTypeInfo) %then %rsk_delete_ds(&tmplib..&__linkTypeInfo.);
      %if %symexist(__linkTypeFmt) %then %rsk_delete_ds(&tmplib..&__linkTypeFmt.);

      %return;

%mend;

