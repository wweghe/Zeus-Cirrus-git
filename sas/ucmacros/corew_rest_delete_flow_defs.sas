/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor corew_rest_delete_flow_defs

   \brief   Delete multiple flow definition(s) registered in SAS Process Orchestration

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
   \param [in] filter Filters to apply on the GET request when no value for flow_id is specified. (e.g. contains(name,'hello'))
   \param [in] start Specify the starting point of the records to get. Start indicate the starting index of the subset. Start SHOULD be a zero-based index. The default start SHOULD be 0. Applicable only when a filter is used.
   \param [in] limit Limit controls the maximum number of items to get from the start position (Default = 1000). Applicable only when a filter is used.
   \param [in] debug True/False. If True, debugging informations are printed to the log (Default: false)
   \param [in] logOptions Logging options (i.e. mprint mlogic symbolgen ...)
   \param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y)
   \param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y)
   \param [out] outVarToken Name of the output macro variable which will contain the access token (Default: accessToken)
   \param [out] outSuccess Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess)
   \param [out] outResponseStatus Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus)

   \details
   This macro wraps around core_rest_delete_flow_definition to remove a number of flow definitions


   <b>Example:</b>

   1) Set up the environment (set SASAUTOS and required LUA libraries).  Assumes the spre folder is under /riskcirruscore/core/code_libraries/release-core-{cadence-version}
   \code
        %let cadence_version=2023.11;
        %let core_root_path=/riskcirruscore/core/code_libraries/release-core-&cadence_version.;

        options insert = (
           SASAUTOS = (
              "&core_root_path./sas/ucmacros"
              )
           );
        filename LUAPATH ("&core_root_path./lua");
   \endcode

   2) Send a Http GET request and parse the JSON response into the output table WORK.flow_definitions
   \code
        %let accessToken=;
        %corew_rest_delete_flow_defs(outVarToken =accessToken
                                          , outSuccess = httpSuccess
                                          , outResponseStatus = responseStatus
                                          );
        %put &=accessToken;
        %put &=httpSuccess;
        %put &=responseStatus;

   \endcode


   \ingroup coreRestUtils

   \author  SAS Institute Inc.
   \date    2023
*/




%macro corew_rest_delete_flow_defs(host =
                                 , server = processOrchestration
                                 , solution =
                                 , port =
                                 , logonHost =
                                 , logonPort =
                                 , username =
                                 , password =
                                 , authMethod = bearer
                                 , client_id =
                                 , client_secret =
                                 , wait_flg = Y
                                 , filter =
                                 , pollInterval = 1
                                 , maxWait = 3600
                                 , timeoutSeverity = ERROR
                                 , outVarToken =accessToken
                                 , outSuccess = httpSuccess
                                 , outResponseStatus = responseStatus
                                 , debug = false
                                 , logOptions =
                                 , restartLUA = Y
                                 , clearCache = Y
                                 )  / minoperator;

%let accessToken=;
%core_rest_get_flow_definitions(outds = flow_definitions
                              , outVarToken =accessToken
							  , filter = &filter.
                              , outSuccess = httpSuccess
                              , outResponseStatus = responseStatus
                              );
%put &=accessToken;
%put &=httpSuccess;
%put &=responseStatus;

%if (not %rsk_dsexist(flow_definitions)) %then %do;
     %put WARNING: Could not find any flow definitions;
     %goto EXIT;
%end;
%else %do;
	
	%let tot_objs = 0;
	data _null_;
		set flow_definitions;
	    call symputx(catx('_', 'flowid', _N_),id,'L');
	    call symputx('tot_objs', _N_ , 'L');
	run;
	
	%do k=1 %to &tot_objs;

		%put WARNING: deleting flow with id &&flowid_&k.;
		%core_rest_delete_flow_definition(host = &host.
		                                 , server = &server.
		                                 , solution = &solution.
		                                 , port = &port.
		                                 , logonHost = &logonHost.
		                                 , logonPort = &logonPort.
		                                 , username = &username.
		                                 , password = &password.
		                                 , authMethod = &authMethod.
		                                 , client_id = &client_id.
		                                 , client_secret = &client_secret.
		                                 , flow_id = &&flowid_&k.
		                                 , wait_flg = &wait_flg.
		                                 , pollInterval = &pollInterval.
		                                 , maxWait = &maxWait.
		                                 , outVarToken = &outVarToken.
		                                 , outSuccess = &outSuccess.
		                                 , outResponseStatus = &outResponseStatus.
		                                 , debug = &debug.
		                                 , logOptions = &logOptions.
		                                 , restartLUA = &restartLUA.
		                                 , clearCache = &clearCache.
		                                 );

	%end;	
%end;

%EXIT:
%mend;