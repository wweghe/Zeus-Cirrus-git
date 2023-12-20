/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor core_rest_get_analysis_data_view

   \brief   Retrieve a view of an Analysis Data instance from SAS Risk Data

   \param [in] host (optional) Host url, including the protocol
   \param [in] server Name of the Web Application Server that provides the REST service (Default: riskData)
   \param [in] solution Solution identifier (Source system code) for Cirrus Core content packages (Default: currently blank)
   \param [in] port (optional) Server port
   \param [in] logonHost (Optional) Host/IP of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app host/ip is the same as the host/ip in the url parameter
   \param [in] logonPort (Optional) Port of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app port is the same as the port in the url parameter
   \param [in] username (optional) Username credentials
   \param [in] password (optional) Password credentials: it can be plain text or SAS-Encoded (it will be masked during execution).
   \param [in] authMethod: Authentication method (accepted values: BEARER). (Default: BEARER).
   \param [in] client_id The client id registered with the Viya authentication server. If blank, the internal SAS client id is used (only if GRANT_TYPE = password).
   \param [in] client_secret The secret associated with the client id.
   \param [in] key Instance key of the analysis data object to export to a view
   \param [in] objectFilter Filters to get the analysis data object folowing the SAS REST APIs filtering logic Example: eq(name,'AUD')
   \param [in] dataFilter Filters to apply on the analysis data data folowing the SAS REST APIs filtering logic Example: eq(currency,'AUD')
   \param [in] fields List of analaysis data fields to keep separated by commas Example: %str(txn_id,sec_id,currency)
   \param [in] dataMapKey The unique identifier for the data map 
   \param [in] debug True/False. If True, debugging informations are printed to the log (Default: false)
   \param [in] logOptions Logging options (i.e. mprint mlogic symbolgen ...)
   \param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y)
   \param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y)
   \param [in] inAnalysisDataKey Table with the information of the analysis data to be retrieved. If provided only the locationType and location parameters are valid.
   \param [in] locationType The type of server location from which data will be imported. If it is LIBNAME it will collect the path and pass it as DIRECTORY.  
   \param [in] location The server location from which data will be imported. Interpretation of this parameter varies based on the value of locationType. When DIRECTORY, the filesystem path on the server where the import data is located. When LIBNAME, the name of the library in which the import data may be found.
   \param [out] outview Name of the output view that contains the analysis data instance (Default: analysis_data_view)
   \param [out] outVarToken Name of the output macro variable which will contain the access token (Default: accessToken)
   \param [out] outSuccess Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess)
   \param [out] outResponseStatus Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus)

   \details
   This macro sends a GET request to <b><i>\<host\>:\<port\>/riskData/objects/<anaylsisId>/view</i></b> and collects the results in the output view. \n
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

   2) Send a Http GET request and parse the JSON response into the output table WORK.analysis_data
   \code
      %let accessToken=;
      %core_rest_get_analysis_data_view(objectFilter = eq(name,"Portfolio (PORTFOLIO) 2023-09-21")
                                      , dataFilter = eq(currency,'AUD')
								              , fields = %str(txn_id,sec_id,currency)
                                      , outview = analysis_data_view
                                      , outVarToken =accessToken
                                      , outSuccess = httpSuccess
                                      , outResponseStatus = responseStatus
                                      );
      %put &=accessToken;
      %put &=httpSuccess;
      %put &=responseStatus;

      Multiple views: 

      data test;
         key = "56982b7e-aef8-453e-a83c-b64cb16d30fc";
         outputName = "Counterparty_test";
         fields = "rating_grade";
         filter = "eq(rating_grade,'AAA')";
         dataMapKey = "";
         output;
         key = "b6c96b44-3ae6-444d-a4c7-eb11d2e758bc";
         outputName = "Portfolio_test";
         fields = "currency";
         filter =  "eq(currency,'USD')";
         dataMapKey = "";
         output;
      run;
      
      %core_rest_get_analysis_data_view(inAnalysisDataKey = test
                                  , locationType = DIRECTORY
								          , location = /riskcirruscore/
                                  , outVarToken =accessToken
                                  , outSuccess = httpSuccess
                                  , outResponseStatus = responseStatus
                                  );

%put &=accessToken;
%put &=httpSuccess;
%put &=responseStatus;


   \endcode


   \ingroup coreRestUtils

   \author  SAS Institute Inc.
   \date    2018
*/
%macro core_rest_get_analysis_data_view(host =
                                      , server = riskData
                                      , solution =
                                      , port =
                                      , logonHost =
                                      , logonPort =
                                      , username =
                                      , password =
                                      , authMethod = bearer
                                      , client_id =
                                      , client_secret =
                                      , key =
                                      , objectFilter = 
                                      , dataFilter =
                                      , fields =
                                      , dataMapKey =
                                      , inAnalysisDataKey =
                                      , locationType =
                                      , location = 
                                      , outview = analysis_data_view
                                      , outVarToken = accessToken
                                      , outSuccess = httpSuccess
                                      , outResponseStatus = responseStatus
                                      , debug = false
                                      , logOptions =
                                      , restartLUA = Y
                                      , clearCache = Y
                                      );

   %local requestUrl libref view oldLogOptions;
   
   /* Get the current value of mlogic and symbolgen options */
   %let oldLogOptions = %sysfunc(getoption(mlogic)) %sysfunc(getoption(symbolgen));

   %if %sysevalf(%superq(inAnalysisDataKey) eq, boolean) %then %do;
      %if(%sysevalf(%superq(key) eq, boolean) and %sysevalf(%superq(objectFilter) eq, boolean)) %then %do;
         %put ERROR: Analysis data key or name is required.;
         %abort;
      %end;

      %if %sysevalf(%superq(key) eq, boolean) and %sysevalf(%superq(objectFilter) ne, boolean) %then %do;
         %core_rest_get_analysis_data(
            outds = analysis_data_info
            , filter = &objectFilter.
            , solution = &solution.
            , outVarToken =&outVarToken.
            , outSuccess = &outSuccess.
            , outResponseStatus = &outResponseStatus.
         );

         %if(not &httpSuccess.) %then %do;
            %put ERROR: Could not request the analysis data information;
            %abort;
         %end;

         %if (not %rsk_dsexist(analysis_data_info)) or (%rsk_attrn(analysis_data_info, nlobs) eq 0 ) %then %do;
            %put ERROR: Could not find the analysis data information using the filter: &objectFilter.;
            %abort;
         %end;
         
         %if (%rsk_attrn(analysis_data_info, nlobs) > 1 ) %then %do;
            %put ERROR: Your filter: &objectFilter. is returning more than one analaysis run.;
            %abort;
         %end;

         data _null_;
            set analysis_data_info;
            call symputx("key", key, "L");
         run;

      %end;

      %if(%sysevalf(%superq(outview) eq, boolean)) %then
         %let outview=analysis_data_view;

      /* Set the required log options */
      %if(%length(&logOptions.)) %then
         options &logOptions.;
      ;

      %let libref = WORK;
      %let view = &outview.;
      %if %sysfunc(find(&outview., %str(.))) %then %do;
         %let libref = %scan(&outview., 1, %str(.));
         %let view = %scan(&outview., 2, %str(.));
      %end;

      /* Create a fileref to the view for risk-data to write the response to */
      filename viewRef "%sysfunc(pathname(&libref.))/&view..sas7bvew";

      /* Set the base Request URL */
      %core_set_base_url(host=&host, server=&server., port=&port.);
      %let requestUrl = &baseUrl./&server./objects/&key./view;

      %if(%sysevalf(%superq(dataFilter) ne, boolean) or %sysevalf(%superq(fields) ne, boolean) or %sysevalf(%superq(dataMapKey) ne, boolean)) %then
         %let requestUrl = &requestUrl.%str(?); 
      
      %if(%sysevalf(%superq(dataFilter) ne, boolean)) %then 
         %let requestUrl = &requestUrl.%str(&)filter=&dataFilter.;

      %if(%sysevalf(%superq(fields) ne, boolean)) %then
         %let requestUrl = &requestUrl.%str(&)fields=&fields.;

      %if(%sysevalf(%superq(dataMapKey) ne, boolean)) %then
         %let requestUrl = &requestUrl.%str(&)dataMapKey=&dataMapKey.;
      
      %let method = GET;
   
   %end;
   %else %do;
         %if (not %rsk_dsexist(&inAnalysisDataKey.)) or (%rsk_attrn(&inAnalysisDataKey., nlobs) eq 0 ) %then %do;
            %put ERROR: The input table with the list of analysis data to extract does not exist or is empty.;
            %abort;
         %end;

         %if (%sysevalf(%superq(locationType) eq, boolean)) %then %do;
            %put ERROR: The 'locationType' parameter is required. Available values : DIRECTORY, LIBNAME;
            %abort;
         %end;

         %if(%sysevalf(%superq(location) eq, boolean)) %then %do;
            %put ERROR: location is required;
            %abort;
         %end;

         data _null_;
            set &inAnalysisDataKey.;
            call symputx(cats("key_",put(_N_, 8.)), key, "L");
            call symputx(cats("outputName_",put(_N_, 8.)), outputName, "L");
            call symputx(cats("dataFilter_",put(_N_, 8.)), dataFilter, "L");
            call symputx(cats("fields_",put(_N_, 8.)), fields, "L");
            call symputx(cats("filter_",put(_N_, 8.)), filter, "L");
            call symputx(cats("dataMapKey_",put(_N_, 8.)), dataMapKey, "L");
            call symputx("tot_analysisData", _N_, "L");
         run;

         filename _body_ temp;
         data _null_;
            file _body_;
            put "{";
            put "    ""items"": [";
            %do i=1 %to &tot_analysisData.;
               put "       {";
               put "         ""key"": ""&&key_&i.."",";
               put "         ""name"": ""&&outputName_&i..""";
			   %if &&fields_&i.. ne %then %do;
               		put "         ,""fields"": ""&&fields_&i..""";
			   %end;
			   %if &&filter_&i.. ne %then %do;
               		put "         ,""filter"": ""&&filter_&i..""";
			   %end;
			   %if &&dataMapKey_&i.. ne %then %do;
               		put "         ,""dataMapKey"": ""&&dataMapKey_&i..""";
			   %end;
               put "       }";
               %if &i. ne &tot_analysisData. %then %do;
                  put ",";
               %end;
            %end;
            put "    ]";
            put "}";
         run;
         
         %if &locationType. = LIBNAME %then %do;
			   %let location = %sysfunc(pathname(&location.));
			   %let locationType = DIRECTORY;
		   %end;

         /* Set the base Request URL */
         %core_set_base_url(host=&host, server=&server., port=&port.);
         %let requestUrl = &baseUrl./&server./objects/views;
         %let requestUrl = &requestUrl.%str(?);
         %let requestUrl = &requestUrl.%str(&)locationType=&locationType.;
         %let requestUrl = &requestUrl.%str(&)location=&location.;

         %let method = POST;
		 filename viewRef temp;
		 
		 filename _hin_ temp;
	    data _null_;
	        file _hin_;
	        put 'Accept: application/json';
	    run;

   %end;

   /* Temporary disable mlogic and symbolgen options to avoid printing of userid/pwd to the log */
   option nomlogic nosymbolgen;
   /* Send the REST request */
   %core_rest_request(url = &requestUrl.
                     , method = &method.
                     , logonHost = &logonHost.
                     , logonPort = &logonPort.
                     , username = &username.
                     , password = &password.
                     , authMethod = &authMethod.
				%if %sysevalf(%superq(inAnalysisDataKey) ne, boolean) %then %do;
					 , body = _body_
					 , headerIn = _hin_
					 , contentType = application/json
				%end;
                     , parser =
                     , fout = viewRef
                     , outVarToken = &outVarToken.
                     , outSuccess = &outSuccess.
                     , outResponseStatus = &outResponseStatus.
                     , debug = &debug.
                     , logOptions = &oldLogOptions.
                     , restartLUA = &restartLUA.
                     , clearCache = &clearCache.
                     );
                     
   %if %sysfunc(fexist(viewRef)) %then %do; 
		filename viewRef clear;
   %end;
   %if %sysfunc(fexist(_body_)) %then %do; 
		filename _body_ clear;
   %end;
   %if %sysfunc(fexist(_hin_)) %then %do; 
		filename _hin_ clear;
   %end;

   /* Exit in case of errors */
   %if not &&&outSuccess.. %then
      %abort;

%mend;
