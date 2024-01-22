/*
 Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor core_rest_create_file_attachment

   \brief   Attach a file to an object in SAS Risk Cirrus Objects

   \param [in] host (optional) Host url, including the protocol
   \param [in] server Name of the Web Application Server that provides the REST service (Default: riskCirrusObjects)
   \param [in] port (optional) Server port (Default: 443)
   \param [in] logonHost (Optional) Host/IP of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app host/ip is the same as the host/ip in the url parameter
   \param [in] logonPort (Optional) Port of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app port is the same as the port in the url parameter
   \param [in] solution Solution identifier (Source system code) (Default:rcc)
   \param [in] username (optional) Username credentials
   \param [in] password (optional) Password credentials: it can be plain text or SAS-Encoded (it will be masked during execution).
   \param [in] authMethod: Authentication method (accepted values: BEARER). (Default: BEARER).
   \param [in] objectKey The uuid of the object that the file is to be attached to.
   \param [in] objectType Type of object that the file is to be attached to. E.g cycle
   \param [in] file Full file path to the file that is being attached
   \param [in] attachmentSourceSystemCd The source system code of the file attachment.  (Default: <solution>)
   \param [in] attachmentName Name of the attachment (does not have to be the file name)
   \param [in] attachmentDisplayName Display name of the attachment
   \param [in] attachmentDesc Description of the attachment
   \param [in] replace If Y, replace an object's existing attachment with the new attachment if it has the same name (attachmentName)
   \param [in] debug True/False. If True, debugging informations are printed to the log (Default: false)
   \param [in] logOptions Logging options (i.e. mprint mlogic symbolgen ...)
   \param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y)
   \param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y)
   \param [out] outds Name of the output table that contains the attachment_status information (Default: attachment_status)
   \param [out] outVarToken Name of the output macro variable which will contain the access token (Default: accessToken)
   \param [out] outSuccess Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess)
   \param [out] outResponseStatus Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus)

   \details
     This macro
        1. Sends a POST request to /riskCirrusObjects/files to upload file as a temporary risk-cirrus-objects attachment.
        2. Sends a PATCH request to /riskCirrusObjects/objects/<objectType>/<objectKey> to move the temporary file attachment to a file attachment on an object.

     Note: risk-cirrus-objects requires that the attachment's name and displayName be unique (individually).
        If &replace=N, risk-cirrus-objects will error out if &attachmentName or &attachmentDisplayName are not unique.
        If &replace=Y, if &attachmentName is the same as an existing attachment's name, we replace that attachment with the new one.  If another attachment's displayName is the same as the new &attachmentDisplayName, risk-cirrus-objects will error out.

   b>Example:</b>

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

   2) Send a Http GET request and parse the JSON response into the output table WORK.data_map
   \code
      %let accessToken =;
      %core_rest_create_file_attachment(objectKey = adf99d11-075a-40a4-88b1-18c6103cd529
							 , objectType = cycles
							 , file = /tmp/init.sas
							 , attachmentName = init.sas
						    , attachmentDisplayName = Initialize Script
						    , attachmentDesc = Initialize Script
                      , attachmentGrouping = Initialization
							 , replace = Y
						    , debug = true
							 );
      %put &=accessToken;
      %put &=httpSuccess;
      %put &=responseStatus;
   \endcode

   <b>Sample output:</b>

   \ingroup coreRestUtils

   \author  SAS Institute Inc.
   \date    2019
*/
%macro core_rest_create_file_attachment(host =
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
                                      , file =
                                      , attachmentSourceSystemCd =
                                      , attachmentName =
                                      , attachmentDisplayName =
                                      , attachmentDesc =
                                      , attachmentGrouping =
                                      , replace = N
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
      fileName
      fileDesc
   ;

   /* Set the required log options */
   %if(%length(&logOptions.)) %then
      options &logOptions.;
   ;

   /* Get the current value of mlogic and symbolgen options */
   %local oldLogOptions;
   %let oldLogOptions = %sysfunc(getoption(mlogic)) %sysfunc(getoption(symbolgen));

   %if (%sysevalf(%superq(solution) eq, boolean)) %then %do;
      %put ERROR: solution is required.;
      %abort;
   %end;

   %if (%sysevalf(%superq(attachmentSourceSystemCd) eq, boolean)) %then %do;
      %let attachmentSourceSystemCd = &solution.;
   %end;
   %let attachmentSourceSystemCd = %upcase(&attachmentSourceSystemCd.);

   %if(not %rsk_fileexist(&file.)) %then %do;
      %put ERROR: File &file. could not be found;
      %abort;
   %end;

   %let fileName = &attachmentName.;
   %let fileDesc = &attachmentDesc.;
   /* Extract the file name from the full path if not externally provided */
   %if %sysevalf(%superq(fileName) =, boolean) %then
      %let fileName = %scan(&file., -1, %str(\/));

   %if %sysevalf(%superq(attachmentDisplayName) =, boolean) %then
      %let attachmentDisplayName = &fileName.;

   /*****************************************************/
   /* 1. POST the file to risk cirrus objects temp files*/
   /*****************************************************/

   /* Base Request URL for objects temporary files */
   %core_set_base_url(host=&host, server=&server., port=&port.);
   %let requestUrl = &baseUrl/&server./files;

   /*Create the body of the multipart request */
   filename _tmpin temp termstr = crlf;
   data mpContent;
      length contentDisposition contentFormat contentType content $256;

      contentDisposition='name="attachment"';
      contentFormat='string';
      contentType='text/plain';
      content="{""name"":""&fileName."",""displayName"":""&attachmentDisplayName."",""description"":""&fileDesc."",""changeReason"":""batch upload by core_rest_create_attachment""}";
      output;

      contentDisposition="name=""file""; filename=""&fileName.""";
      contentFormat='filepath';
      contentType='application/octet-stream';
      content="&file.";
      output;
   run;
   %core_rest_create_multipart(body=_tmpin
                             ,boundary=----multpartbound271828182845
                             ,content=mpContent
                             );

   %let &outSuccess. = 0;

   /* Temporary disable mlogic and symbolgen options to avoid printing of userid/pwd to the log */
   option nomlogic nosymbolgen;

   /* Send the REST request */
   %core_rest_request(url = &requestUrl.
                     , method = POST
                     , logonHost = &logonHost.
                     , logonPort = &logonPort.
                     , username = &username.
                     , password = &password.
                     , authMethod = &authMethod.
                     , client_id = &client_id.
                     , client_secret = &client_secret.
                     , headerIn = Accept: application/json
                     , body = _tmpin
                     , contentType = multipart/form-data; boundary=----multpartbound271828182845
                     , parser = coreRestPlain
                     , printResponse = N
                     , logSeverity = ERROR
                     , outds = _tmp_attachment_
                     , outVarToken = &outVarToken.
                     , outSuccess = &outSuccess.
                     , outResponseStatus = &outResponseStatus.
                     , debug = &debug.
                     , logOptions = &oldLogOptions.
                     , restartLUA = &restartLUA.
                     , clearCache = &clearCache.
                     );

    /* Clear references if we're not debugging */
   %if %upcase(&debug) ne TRUE %then %do;
      filename _tmpin;
   %end;

   /* Exit in case of errors */
   %if(not &&&outSuccess.. or not %rsk_dsexist(_tmp_attachment_)) %then
      %abort;

   data _null_;
      set _tmp_attachment_;
      call symputx("objectId", objectId, "L");
      call symputx("tempUploadKey", tempUploadKey, "L");
      call symputx("name", name, "L");
      call symputx("displayName", displayName, "L");
      call symputx("fileSize", fileSize, "L");
      call symputx("fileExtension", fileExtension, "L");
      call symputx("fileMimeType", fileMimeType, "L");
   run;

   /*****************************************************/
   /* 2. PATCH the object with the temp file attachment */
   /*****************************************************/

   /* a. Verify that the user has access to the object */

   /* Set the request URL */
   %let requestUrl = &baseUrl./&server./objects/&objectType.;
   filename _hout_ temp;

   /* Add filters to the request URL */
   %core_set_rest_filter(key=&objectKey., solution=&solution.);

   /* Temporary disable mlogic and symbolgen options to avoid printing of userid/pwd to the log */
   option nomlogic nosymbolgen;

   /* Send the GET REST request to get the object */
   %core_rest_request(url = &requestUrl.
                     , method = GET
                     , logonHost = &logonHost.
                     , logonPort = &logonPort.
                     , username = %superq(username)
                     , password = %superq(password)
                     , authMethod = &authMethod.
                     , client_id = &client_id.
                     , client_secret = &client_secret.
                     , headerOut = _hout_
                     , outds = object_info
                     , parser = sas.risk.cirrus.core_rest_parser.coreRestPlain
                     , outVarToken = &outVarToken.
                     , outSuccess = &outSuccess.
                     , outResponseStatus = &outResponseStatus.
                     , debug = &debug.
                     , logOptions = &logOptions.
                     , restartLUA = &restartLUA.
                     , clearCache = &clearCache.
                     );

   /* Exit in case of errors */
   %if(not &&&outSuccess.. or not %rsk_dsexist(object_info)) %then
      %abort;

   data _null_;
      set object_info;
      if count=0 then do;
         put "ERROR: Failed to find an object of type &objectType. with key &objectKey. (solution=&solution.)";
         abort;
      end;
   run;

   /* b. Get the object's file attachments - request made directly on the object to get its etag as well */

   /* Set the request URL */
   %let requestUrl = &baseUrl./&server./objects/&objectType./&objectKey.?fields=fileAttachments;
   filename _hout_ temp;

   /* Temporary disable mlogic and symbolgen options to avoid printing of userid/pwd to the log */
   option nomlogic nosymbolgen;

   /* Send the GET REST request to get the object */
   %core_rest_request(url = &requestUrl.
                     , method = GET
                     , logonHost = &logonHost.
                     , logonPort = &logonPort.
                     , username = %superq(username)
                     , password = %superq(password)
                     , authMethod = &authMethod.
                     , client_id = &client_id.
                     , client_secret = &client_secret.
                     , headerOut = _hout_
                     , outds = _tmp_object_info_
                     , arg1 = _tmp_object_attachments_
                     , parser = sas.risk.cirrus.core_rest_parser.coreRestFileAttachments
                     , outVarToken = &outVarToken.
                     , outSuccess = &outSuccess.
                     , outResponseStatus = &outResponseStatus.
                     , debug = &debug.
                     , logOptions = &logOptions.
                     , restartLUA = &restartLUA.
                     , clearCache = &clearCache.
                     );

   /* Exit in case of errors */
   %if(not &&&outSuccess.. or not %rsk_dsexist(_tmp_object_attachments_)) %then
      %abort;

   /* Get the object instance's eTag from the response header - needed for PUT/PATCH requests to riskCirrusObjects */
   %let etag =;
   data _null_;
      length Header $ 50 Value $ 200;
      infile _hout_ dlm=':';
      input Header $ Value $;
      if Header = 'ETag';
      call symputx("etag", Value);
   run;

   /* Build header for the PATCH request*/
   filename _hin_ temp;
   data _null_;
      file _hin_;
      put 'Accept: application/json';
      put 'If-Match: "' &etag. '"';
   run;

   /* add an empty row (in case the object had no attachments) */
   data _tmp_object_attachments_;
      output;
      set _tmp_object_attachments_;
   run;

   /* Build request body for the PATCH request */
   filename _body_ temp;
   data _null_;
      length curr_var_name $ 32;
      set _tmp_object_attachments_ end=last;
      array char_vars {*} _CHARACTER_;
      array num_vars {*} _NUMERIC_;

      file _body_;

      if _n_=1 then do;
         put "{";
         put "   ""changeReason"": ""batch upload by core_rest_create_attachment"",";
         put "   ""fileAttachments"": [";
      end;

      /* Add existing attachment - if &replace=Y, don't add the attachment if it has the same displayName as the new one*/
      if name not in ('', "&fileName") or "&replace" ne "Y" then do;

         put "      {";

         /* add the character variables to the attachment item */
         do i=1 to dim(char_vars);
            curr_var_name = vname(char_vars(i));
            if char_vars(i) ne "" and curr_var_name ne "curr_var_name" then do;

               if lowcase(curr_var_name) in("links", "customfields") then
                  put '         "' curr_var_name +(-1)'": ' char_vars(i) +(-1) @;
               else do;
                  /* to avoid JSON request body 400 errors, convert double-quotes to single quotes */
                  char_vars(i)=prxchange('s/"/''/', -1, char_vars(i));
                  put '         "' curr_var_name +(-1)'": "' char_vars(i) +(-1)'"' @;
               end;
               if i=dim(char_vars) and dim(num_vars)=0 then put;
               else put ',';

            end;
         end;

         /* add the numeric variables to the attachment item */
         do i=1 to dim(num_vars);
            curr_var_name = vname(num_vars(i));
            if num_vars(i) ne . then do;
               put '         "' curr_var_name +(-1)'": ' num_vars(i) +(-1) @;
               if i=dim(num_vars) then put;
               else put ',';
            end;
         end;

         put "      },";
      end;

      /* Add the new file attachment */
      if last then do;
         put "      {";
         put "         ""sourceSystemCd"": ""&attachmentSourceSystemCd."",";
         put "         ""objectId"": ""&objectId."",";
         put "         ""tempUploadKey"": ""&tempUploadKey."",";
         put "         ""name"": ""&name."",";
         %if %sysevalf(%superq(attachmentGrouping) ne, boolean) %then %do;
            put "         ""grouping"": ""&attachmentGrouping."",";
         %end;
         put "         ""displayName"": ""&displayName."",";
         put "         ""fileSize"": &fileSize.,";
         put "         ""fileExtension"": ""&fileExtension."",";
         put "         ""fileMimeType"": ""&fileMimeType.""";
         put "      }";
         put "   ]";
         put "}";
      end;

   run;

    /* Clear references if we're not debugging */
   %if %upcase(&debug) ne TRUE %then %do;
      filename _hout_;
   %end;

   %let requestUrl = &baseUrl./&server./objects/&objectType./&objectKey.;

    /* Temporary disable mlogic and symbolgen options to avoid printing of userid/pwd to the log */
   option nomlogic nosymbolgen;
   /* Send the REST request */
   %core_rest_request(url = &requestUrl.
                     , method = PATCH
                     , logonHost = &logonHost.
                     , logonPort = &logonPort.
                     , username = &username.
                     , password = &password.
                     , authMethod = &authMethod.
                     , client_id = &client_id.
                     , client_secret = &client_secret.
                     , headerIn = _hin_
                     , body = _body_
                     , contentType = application/json
                     , parser = sas.risk.cirrus.core_rest_parser.coreRestFileAttachments
                     , outds = &outds.
                     , outVarToken = &outVarToken.
                     , outSuccess = &outSuccess.
                     , outResponseStatus = &outResponseStatus.
                     , debug = &debug.
                     , logOptions = &oldLogOptions.
                     , restartLUA = &restartLUA.
                     , clearCache = &clearCache.
                     );


%mend;
