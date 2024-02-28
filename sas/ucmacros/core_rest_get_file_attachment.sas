/*
 Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor core_rest_get_file_attachment

  \brief   Retrieve the attachment(s) registered in SAS Risk Cirrus Objects for a specific object instance

   \param [in] host (optional) Host url, including the protocol
   \param [in] server Name of the Web Application Server that provides the REST service (Default: riskCirrusObjects)
   \param [in] solution Solution identifier (Source system code) for Cirrus Core content packages (Default: currently blank)
   \param [in] port (optional) Server port (Default: 443)
   \param [in] logonHost (Optional) Host/IP of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app host/ip is the same as the host/ip in the url parameter
   \param [in] logonPort (Optional) Port of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app port is the same as the port in the url parameter
   \param [in] username (optional) Username credentials
   \param [in] password (optional) Password credentials: it can be plain text or SAS-Encoded (it will be masked during execution).
   \param [in] authMethod: Authentication method (accepted values: BEARER). (Default: BEARER).
   \param [in] objectKey The uuid of the object that the file is to be attached to.
   \param [in] objectType Type of object that the file is to be attached to. E.g cycle
   \param [in] client_id The client id registered with the Viya authentication server. If blank, the internal SAS client id is used (only if GRANT_TYPE = password).
   \param [in] client_secret The secret associated with the client id.
   \param [in] attachmentNames (Optional) Pipe-separated names of specific attachments to retrieve from the object.  If not specified, all attachments are retrieved.
   \param [in] outFileRefs (Optional) Pipe-separated names of file refs that point to the retrieved file contents for each file in attachmentNames, respectively.  If not provided, random filerefs are used and added to &outds.
   \param [in] errorIfFileNotFound (Y/N) If Y, and an attachment in attachmentNames is not found, then an error is thrown and processing stops.
   \param [in] getAttachmentContent (Optional) (Y/N) Controls whether the file attachment content is retrieved (Default: N)
   \param [in] debug True/False. If True, debugging informations are printed to the log (Default: false)
   \param [in] logOptions Logging options (i.e. mprint mlogic symbolgen ...)
   \param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y)
   \param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y)
   \param [out] outds Name of the output table that contains the attachment info (Default: object_file_attachments)
   \param [out] outVarToken Name of the output macro variable which will contain the Access Token (Default: accessToken)
   \param [out] outSuccess Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess)
   \param [out] outResponseStatus Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus)

   \details
   This macro sends a GET request to <b><i>\<host\>:\<port\>/riskCirrusObjects/objects/<objectType>/<objectKey> </i></b> and collects the results in the output tables. \n
   See \link core_rest_request.sas \endlink for details about how to send GET requests and parse the response.


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

   2) Send a Http GET request and parse the JSON response into the output table WORK.object_file_attachments
   \code
      %let accessToken =;
      %core_rest_get_file_attachment(objectKey = 5493a173-74f2-49fa-a7ac-823d9e7a1f07
                              , objectType = analysisRuns
                              , attachmentNames = myCode.sas
                              , outFileRefs = fRef
                              , getAttachmentContent = Y
                              , outds = object_file_attachments
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
   \date    2022
*/
%macro core_rest_get_file_attachment(host =
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
                                    , objectKey =
                                    , objectType =
                                    , attachmentNames =
                                    , outFileRefs =
                                    , errorIfFileNotFound = Y
                                    , getAttachmentContent = N
                                    , outds = object_file_attachments
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
      quotedCsvAttchmentNameList
      oldLogOptions
      num_attachments
      num_attachments_found
      attachmentName
      fileRef
      i
   ;

   /* Set the required log options */
   %if(%length(&logOptions.)) %then
      options &logOptions.;
   ;

   /* Get the current value of mlogic and symbolgen options */
   %let oldLogOptions = %sysfunc(getoption(mlogic)) %sysfunc(getoption(symbolgen));

   %if %sysevalf(%superq(errorIfFileNotFound) eq, boolean) %then
      %let errorIfFileNotFound = Y;

   /* Set the base request URL */
   %core_set_base_url(host=&host, server=&server., port=&port.);
   %let requestUrl = &baseUrl./&server./objects/&objectType.;

   /* Add filters to the request URL */
   %core_set_rest_filter(key=&objectKey., solution=&solution.);
   %let requestUrl = &requestUrl.%str(&)fields=fileAttachments;

   /* Temporary disable mlogic and symbolgen options to avoid printing of userid/pwd to the log */
   option nomlogic nosymbolgen;
   /* Send the REST request */
   %core_rest_request(url = &requestUrl.
                     , method = GET
                     , logonHost = &logonHost.
                     , logonPort = &logonPort.
                     , username = %superq(username)
                     , password = %superq(password)
                     , authMethod = &authMethod.
                     , client_id = &client_id.
                     , client_secret = &client_secret.
                     , parser = sas.risk.cirrus.core_rest_parser.coreRestFileAttachments
                     , outds = object_info
                     , arg1 = _tmp_object_file_attachments_
                     , outVarToken = &outVarToken.
                     , outSuccess = &outSuccess.
                     , outResponseStatus = &outResponseStatus.
                     , debug = &debug.
                     , logOptions = &oldLogOptions.
                     , restartLUA = &restartLUA.
                     , clearCache = &clearCache.
                     );


   /* Exit in case of errors */
   %if(not &&&outSuccess..) %then %do;
      %put ERROR: Failed to retrieve file attachments for object &objectKey. (type=&objectType.);
      %abort;
   %end;

   /* Exit in case of errors */
   %if(%rsk_attrn(object_info, nobs) = 0) %then %do;
      %put ERROR: Failed to find an object of type &objectType. with key &objectKey. (solution=&solution.);
      %abort;
   %end;

   %if %sysevalf(%superq(attachmentNames) eq, boolean) %then %do;
      proc sql noprint;
         select name into :attachmentNames separated by "|"
         from _tmp_object_file_attachments_
         ;
      quit;
   %end;

   %let num_attachments = %sysfunc(countw("&attachmentNames.", "|"));

   data &outds. (drop=i);
      format fileref $8.;
      set _tmp_object_file_attachments_ end=last;
      retain i 0;

      %do i=1 %to &num_attachments.;
         %let attachmentName=%sysfunc(scan("&attachmentNames.", &i., "|"));
         %let attachFound&i.=N;

         %let fileRef=%sysfunc(scan("&outFileRefs.", &i., "|"));
         %if "&fileRef." = "" %then %let fileRef=_fRef&i.;   /* if no fileref given, use _fRef<attachmentNum> */

         %if %sysfunc(find(&attachmentName., %str(.))) %then %do;  /* compare by <name>.<extension>, if we have an extension */
            if upcase(name)=upcase("&attachmentName.") then do;
         %end;
         %else %do;
            if upcase(scan(name, 1, "."))=upcase("&attachmentName.") then do;  /* compare by <name> only, if the &attachName doesn't have an extension */
         %end;
            i=i+1;
            fileref="&fileRef.";
            call symputx(catt("attachRef", i), fileref, "L");
            call symputx(catt("attachKey", i), key, "L");
            call symputx(catt("attachName", i), name, "L");
            call symputx("attachFound&i.", "Y");
            output;
         end;

      %end;

      if last then
         call symputx("num_attachments_found", i, "L");

   run;

   %if &errorIfFileNotFound. = Y %then %do;
      %if "&num_attachments." ne "&num_attachments_found." %then %do;
         %put ERROR: 1 or more attachments were not found on object &objectKey. (type=&objectType.);
         %do i=1 %to &num_attachments.;
            %if &&&attachFound&i.. = N %then
               %put ERROR: Could not find attachment %sysfunc(scan("&attachmentNames.", &i., "|"));
         %end;
         %let &outSuccess.=0;
         %abort;
      %end;
   %end;

   /* If requested, download the attachment's content into a fileref */
   %if %upcase("&getAttachmentContent.") eq "Y" %then %do;

      %do i=1 %to &num_attachments_found.;

         /* Assign the fileref if it does not exist */
         %if("%sysfunc(pathname(&&&attachRef&i..))" eq "") %then %do;
            filename &&&attachRef&i.. "%sysfunc(pathname(WORK))/&&&attachName&i..";
         %end;

         %let requestUrl = &baseUrl./&server./objects/&objectType./&objectKey./attachments/&&&attachKey&i../content;

         /* Make the request to get the attachment's contents in the fileref */
         option nomlogic nosymbolgen;
         %core_rest_request(url = &requestUrl.
                           , method = GET
                           , logonHost = &logonHost.
                           , logonPort = &logonPort.
                           , username = %superq(username)
                           , password = %superq(password)
                           , authMethod = &authMethod.
                           , client_id = &client_id.
                           , client_secret = &client_secret.
                           , parser =
                           , fout = &&&attachRef&i..
                           , outVarToken = &outVarToken.
                           , outSuccess = &outSuccess.
                           , outResponseStatus = &outResponseStatus.
                           , debug = &debug.
                           , logOptions = &oldLogOptions.
                           , restartLUA = &restartLUA.
                           , clearCache = &clearCache.
                           );

      %end;

   %end;
   %else %do;
      %put NOTE: Attachment file content not requested.  Not retrieving.;
   %end;

%mend;
