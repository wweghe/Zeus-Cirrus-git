/*
 Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor core_rest_get_link_instances
   \brief   Retrieve the Link Instance(s) registered in SAS Risk Cirrus Objects

   \param [in] host (optional) Host url, including the protocol
   \param [in] server Name of the Web Application Server that provides the REST service (Default: riskCirrusObjects)
   \param [in] solution Solution identifier (Source system code) for Cirrus Core content packages (Default: currently blank)
   \param [in] port (optional) Server port
   \param [in] logonHost (Optional) Host/IP of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app host/ip is the same as the host/ip in the url parameter
   \param [in] logonPort (Optional) Port of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app port is the same as the port in the url parameter
   \param [in] username (optional) Username credentials
   \param [in] password (optional) Password credentials: it can be plain text or SAS-Encoded (it will be masked during execution).
   \param [in] authMethod: Authentication method (accepted values: BEARER). (Default: BEARER).
   \param [in] client_id The client id registered with the Viya authentication server. If blank, the internal SAS client id is used (only if GRANT_TYPE = password).
   \param [in] client_secret The secret associated with the client id.
   \param [in] objectType The type of object for which to retrieve link instances.
   \param [in] objectKey Instance key of the Cirrus object of objectType.  Link instances for this object key will be retrieved.
   \param [in] objectFilter Filter for objectType.  Link instances for the object key meeting this filter will be retrieved.  Must result in only 1 instance.
   \param [in] linkType The link type.  If linkTypeKey is not provided, this is used to get linkTypeKey.
   \param [in] linkTypeKey The key of the link type.
   \param [in] linkInstanceFilter Link instances filter.  This filters the link instances retrieved for the object.
   \param [in] start Specify the starting point of the link instances to get. Start indicate the starting index of the subset. Start SHOULD be a zero-based index. The default start SHOULD be 0. Applicable only when filter is used.
   \param [in] limit Limit controls the maximum number of link instances to get from the start position (Default = 1000). Applicable only when filter is used.
   \param [in] get_unique_link_instances (Y/N). If Y, then for each link instance ID (objectId) returned in the outds, only output 1 row.  This could happen if
      the same link instance ID exists for both sourceSystemCd=<solution> and sourceSystemCd=RCC.  Priority is given to sourceSystemCd=<solution>. (Default: Y)
   \param [in] debug True/False. If True, debugging informations are printed to the log (Default: false)
   \param [in] logOptions Logging options (i.e. mprint mlogic symbolgen ...)
   \param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y)
   \param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y)
   \param [out] outds Name of the output table that contains the link instances (default is link_instances)
   \param [out] outVarToken Name of the output macro variable which will contain the access token (Default: accessToken)
   \param [out] outSuccess Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess)
   \param [out] outResponseStatus Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus)

   \details
   This macro sends a GET request to <b><i>\<host\>:\<port\>/riskCirrusObjects/objects/&solution./linkInstances</i></b> and collects the results in the output table. \n
   See \link core_rest_request.sas \endlink for details about how to send GET requests and parse the response.

   <b>Example:</b>

   1) Set up the environment (set SASAUTOS and required LUA libraries).  Assumes the spre folder is under /riskcirruscore/core/code_libraries/release-core-2022.1.4
   \code
      %let core_root_path=/riskcirruscore/core/code_libraries/release-core-2022.1.4;
      option insert = (
         SASAUTOS = (
            "&core_root_path./spre/sas/ucmacros"
            )
         );
      filename LUAPATH ("&core_root_path./spre/lua");
   \endcode

   2) Send a Http GET request and parse the JSON response into the output table WORK.link_instances
   \code
      %let accessToken =;
      %core_rest_get_link_instances(outds = link_instances
                                       , outVarToken = accessToken
                                       , outSuccess = httpSuccess
                                       , outResponseStatus = responseStatus
                                       );
      %put &=accessToken;
      %put &=httpSuccess;
      %put &=responseStatus;
   \endcode

   <b>Sample output:</b>

   | sourceSystemCd | linkInstanceId    | businessObject1 | isDisabled | businesscollectionNameNm1 | businessObject2 | lastModifiedDttm         | businesscollectionNameNm2 | itemsCount | linkType | creator | modifiedDttm             | key   |
   |----------------|-------------------|-----------------|------------|-----------------------|-----------------|--------------------------|-----------------------|------------|----------|---------|--------------------------|-------|
   | RMC            | 10105_10000_10000 | 10000           | FALSE      | customObject225       | 10000           | 2019-11-13T17:41:43.957Z | customObject214       | 8          | 10105    | 10000   | 2019-11-13T17:41:43.957Z | 10000 |
   | RMC            | 10105_10000_10001 | 10000           | FALSE      | customObject225       | 10001           | 2019-11-13T17:41:43.965Z | customObject214       | 8          | 10105    | 10000   | 2019-11-13T17:41:43.965Z | 10001 |
   | RMC            | 10021_10005_10042 | 10005           | FALSE      | customObject220       | 10042           | 2019-11-14T20:03:21.177Z | customObject209       | 8          | 10021    | 10000   | 2019-11-14T20:03:21.177Z | 10016 |

   \ingroup rgfRestUtils

   \author  SAS Institute Inc.
   \date    2018
*/
%macro core_rest_get_link_instances(host =
                                    , server = riskCirrusObjects
                                    , solution =
                                    , port =
                                    , logonHost =
                                    , logonPort =
                                    , username =
                                    , password =
                                    , authMethod = bearer
                                    , client_id =
                                    , client_secret =
                                    , objectType =
                                    , objectKey =
                                    , objectFilter =
                                    , linkType =
                                    , linkTypeKey =
                                    , linkInstanceFilter =
                                    , get_unique_link_instances = Y
                                    , logSeverity = WARNING
                                    , start =
                                    , limit = 1000
                                    , outds = link_instances
                                    , outVarToken = accessToken
                                    , outSuccess = httpSuccess
                                    , outResponseStatus = responseStatus
                                    , debug = false
                                    , logOptions =
                                    , restartLUA = Y
                                    , clearCache = Y
                                    );

   %local
      requestUrl
      object_key
      link_type_key
      oldLogOptions
      num_keys
      link_type_key
      sscFilter
      ;

   /* Set the required log options */
   %if(%length(&logOptions.)) %then
      options &logOptions.;
   ;

   /* Get the current value of mlogic and symbolgen options */
   %let oldLogOptions = %sysfunc(getoption(mlogic)) %sysfunc(getoption(symbolgen));

   %if %sysevalf(%superq(objectType) eq, boolean) %then %do;
      %put ERROR: The objectType parameter must be specified;
      %abort;
   %end;

   /* Set the base URL */
   %core_set_base_url(host=&host, server=&server., port=&port.);

   /* Retrieve the object key if the objectFilter parameter has been specified */
   %if %sysevalf(%superq(objectFilter) ne, boolean) %then %do;

      %let requestUrl = &baseUrl./&server./objects/&objectType.;

      /* Add filters to the request URL */
      %core_set_rest_filter(solution=&solution., filter=%superq(objectFilter));

      filename resp temp;

      /* Temporary disable mlogic and symbolgen options to avoid printing of userid/pwd to the log */
      option nomlogic nosymbolgen;
      /* Send the REST request */
      %core_rest_request(url = &requestUrl.
                        , method = GET
                        , logonHost = &logonHost.
                        , logonPort = &logonPort.
                        , username = &username.
                        , password = &password.
                        , authMethod = &authMethod
                        , client_id = &client_id.
                        , client_secret = &client_secret.
                        , fout = resp
                        , parser =
                        , outVarToken = &outVarToken.
                        , outSuccess = &outSuccess.
                        , outResponseStatus = &outResponseStatus.
                        , debug = &debug.
                        , logOptions = &oldLogOptions.
                        , restartLUA = &restartLUA.
                        , clearCache = &clearCache.
                        );

      /* Exit in case of errors */
      %if(not &&&outSuccess..) %then
         %abort;

      libname resp_lib json fileref=resp noalldata nrm;

      %let num_keys=0;
      data _null_;
         set resp_lib.items end=last;
         call symputx("object_key", key, "L");
         if last then call symputx("num_keys", _N_, "L");
      run;

      filename resp clear;
      libname resp_lib;

      /*Exit if no object instances met the objectFilter*/
      %if "&object_key"="." %then %do;
         %put ERROR: No instances were found for object type "&objectType" with filter "&objectFilter.";
         %abort;
      %end;

      /*Exit if 2 or more object instances met the objectFilter*/
      %if &num_keys. ne 1 %then %do;
         %put ERROR: More than 1 instance was found for object type "&objectType" with filter "&objectFilter.";
         %abort;
      %end;

   %end; /* %if %sysevalf(%superq(objectFilter) ne, boolean) */
   %else %if %sysevalf(%superq(objectKey) ne, boolean) %then %do;
      %let object_key=&objectKey.;
   %end;
   %else %do;
      %put ERROR: Either the objectKey or objectFilter parameter must be provided;
      %abort;
   %end;


   /* Retrieve the linkType key if the linkType parameter has been specified */
   %if %sysevalf(%superq(linkType) ne, boolean) %then %do;

      %if %sysevalf(%superq(logSeverity) =, boolean) %then
         %let logSeverity = WARNING;
      %else
         %let logSeverity = %upcase(&logSeverity.);

      /* Temporary disable mlogic and symbolgen options to avoid printing of userid/pwd to the log */
      option nomlogic nosymbolgen;
      /* Send the REST request */
      %core_rest_get_link_types(host = &host
                              , solution = &solution.
                              , port = &port.
                              , logonHost = &logonHost.
                              , logonPort = &logonPort.
                              , username = &username.
                              , password = &password.
                              , authMethod = &authMethod.
                              , client_id = &client_id.
                              , client_secret = &client_secret.
                              , filter = eq(objectId, %27&linkType.%27)
                              , outds = _tmp_link_type_
                              , outVarToken = &outVarToken.
                              , outSuccess = &outSuccess.
                              , outResponseStatus = &outResponseStatus.
                              , debug = &debug.
                              , logOptions = &oldLogOptions.
                              , restartLUA = &restartLUA.
                              , clearCache = &clearCache.
                              );

      /* Exit in case of errors */
      %if(not &&&outSuccess.. or not %rsk_dsexist(_tmp_link_type_)) %then
         %abort;

      /* Check if we found the link types */
      %if(%rsk_attrn(_tmp_link_type_, nobs) = 0) %then %do;
         %put ERROR: Could not find LinkType objects of type &linkType..;
         %abort;
      %end;
      %else %do;
         /* Get the linkType key */
         data _null_;
            set _tmp_link_type_;
            call symputx("link_type_key", key, "L");
         run;
      %end;

   %end; /* %if %sysevalf(%superq(linkType) ne, boolean) */
   %else %if %sysevalf(%superq(linkTypeKey) ne, boolean) %then %do;
      %let link_type_key=&linkTypeKey.;
   %end;
   %else %do;
      %put ERROR: Either the linkTypeKey or linkType parameter must be provided;
      %abort;
   %end;

   /* Set the base request URL */
   %let requestUrl = &baseUrl./&server./objects/&objectType./&object_key./linkInstances/&link_type_key.;

   /* Add filters to the request URL */
   %let solution = %upcase(%sysfunc(coalescec(&solution., RCC)));
   %let sscFilter = in(sourceSystemCd,%27&solution.%27,%27RCC%27);
   %core_set_rest_filter(filter=%superq(linkInstanceFilter), customFilter=&sscFilter., start=&start., limit=&limit.);

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
                     , parser = sas.risk.cirrus.core_rest_parser.coreRestLinkInstances
                     , outds = &outds.
                     , outVarToken = &outVarToken.
                     , outSuccess = &outSuccess.
                     , outResponseStatus = &outResponseStatus.
                     , debug = &debug.
                     , logOptions = &oldLogOptions.
                     , restartLUA = &restartLUA.
                     , clearCache = &clearCache.
                     );

   /* If requested, ensure that each returned link instance is unique.  From the request above, we could have
   a link instance with the same objectId returned for both sourceSystemCd=<solution> and sourceSystemCd=RCC.  So here, for each
   link instance id (objectId), output only 1 row, with priority given to sourceSystemCd=<solution>. */
   %if "&get_unique_link_instances." = "Y" %then %do;

      proc sort data=&outds.; by objectId; run;

      data &outds. (drop=solution_link_instance_found);
         set &outds.;
         by objectId;
         retain solution_link_instance_found 0;

         if first.objectId then solution_link_instance_found=0;

         if upcase(sourceSystemCd) = upcase("&solution.") then do;
            output;
            solution_link_instance_found=1;
         end;
         else if last.objectId and not solution_link_instance_found then output;
      run;

   %end;

%mend;