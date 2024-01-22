/*
 Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/**
\file
\anchor corew_create_cell_based_rules

   \brief   Create rule table from cell based adjustments

   \param [in] host (Optional) Host url, including the protocol.
   \param [in] port (Optional) Server port.
   \param [in] server Name that provides the REST service (Default: riskCirrusObjects).
   \param [in] logonHost (Optional) Host/IP of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app host/ip is the same as the host/ip in the url parameter.
   \param [in] logonPort (Optional) Port of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app port is the same as the port in the url parameter.
   \param [in] username (Optional) Username credentials
   \param [in] password (Optional) Password credentials: it can be plain text or SAS-Encoded (it will be masked during execution).
   \param [in] authMethod Authentication method (accepted values: BEARER). (Default: BEARER).
   \param [in] client_id The client id registered with the Viya authentication server. If blank, the internal SAS client id is used (only if GRANT_TYPE = password).
   \param [in] client_secret The secret associated with the client id.
   \param [in] solution The solution short name from which this request is being made. This will get stored in the createdInTag and sharedWithTags attributes on the object (Default: 'blank').
   \param [in] debug True/False. If True, debugging informations are printed to the log (Default: false).
   \param [in] logOptions Logging options (i.e. mprint mlogic symbolgen ...).
   \param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y).
   \param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y).
   \param [in] analysisDataKey UUID of the input analysis data.
   \param [in] aggregationFilterVar String indicating the name of the flag on the data definition which indicates that a column is aggregated.
   \param [in] crossClassVars Comma separated list of cross class variables (analysis data column names)
   \param [in] performWeightedAggregation (Y/N). Controls whether weighted aggregation is to be performed or not. (Default: N).
   \param [in] weightedVarName Specifies the variable for weighted aggregation.
   \param [in] allocationMethod Specifies the allocation method to use (Default: EVEN).
   \param [in] inLibref Specifies the libref of the input adjustment data (Default: WORK).
   \param [in] inAdjustmentTable Specifies the table name of the adjustment data.
   \param [out] outLibref Specifies the libref of the output rule data (Default: WORK).
   \param [out] outRuleTable Specifies the table name of the output rule data (Default: rule_table).
   \param [out] outVarToken Name of the output macro variable which will contain the access token (Default: accessToken).
   \param [out] outSuccess Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess).
   \param [out] outResponseStatus Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus).

   \details
     This macro performs the following operations on the input adjustment data:
        1. Retrieves the linked data definition.
        2. Identifies aggregatable columns.
        3. Creates a table to store rules in compliance with the meta of an allocation rule set.
        4. Whites the table out to the outRuleTable variable in the outLibref library.

   <b>Example:</b>

   1) Set up the environment (set SASAUTOS and required LUA libraries).  Assumes the spre folder is under /riskcirruscore/core/code_libraries/release-core-{cadence-version}
   \code
      %let cadence_version=2023.11;
      %let core_root_path=/riskcirruscore/core/code_libraries/release-core-&cadence_version.;
      option insert = (
         SASAUTOS = (
            "&core_root_path./spre/sas/ucmacros"
            )
         );
      filename LUAPATH ("&core_root_path./spre/lua");
   \endcode

   2) Send a Http GET request and parse the JSON response into the output table work.load_results
   \code

        %let accessToken =;
        %corew_create_cell_based_rules(solution=ACL,
                                       analysisDataKey=&key.,
                                       aggregationFilterVar=attributable,
                                       inAdjustmentTable=adjustments,
                                       crossClassVars=%quote(PRODUCT_CD,GEOGRAPHY_CD,LOB));
        %put &=httpSuccess;
        %put &=responseStatus;
   \endcode

   \ingroup coreRestUtils

   \author  SAS Institute Inc.
   \date    2023
*/

%macro corew_create_cell_based_rules(solution = 
								   , analysisDataKey =
								   , aggregationFilterVar = 
								   , crossClassVars =
								   , performWeightedAggregation = N
								   , weightedVarName = 
								   , allocationMethod = EVEN
								   , inLibref = WORK
								   , inAdjustmentTable = 
                                   , server = riskCirrusObjects
                                   , host =
                                   , port =
                                   , logonHost =
                                   , logonPort =
                                   , username =
                                   , password =
                                   , authMethod = token
                                   , client_id =
                                   , client_secret =
								   , outLibref = WORK
								   , outRuleTable = rule_table
                                   , outVarToken = accessToken
                                   , outSuccess = httpSuccess
                                   , outResponseStatus = responseStatus
                                   , debug = false
                                   , logOptions =
                                   , restartLUA = Y
                                   , clearCache = Y);

	%local 
		agg_columns
		filter
		andVar
		curr_var
		col
	;

	/* Input checks */
	%if %sysevalf(%superq(aggregationFilterVar) eq, boolean) %then %do;
		%put ERROR: Aggregation filter variable must be provided;
		%abort;
	%end;
	%if %sysevalf(%superq(crossClassVars) eq, boolean) %then %do;
		%put ERROR: Cross class variables must be provided;
		%abort;
	%end;
	%if %sysevalf(%superq(analysisDataKey) eq, boolean) %then %do;
		%put ERROR: Analysis data key must be provided;
		%abort;
	%end;

	/* Get data definition */
	%core_rest_get_data_def(host = &host.
						  , port = &port.
						  , server = &server.
	         			  , logonHost = &logonHost.
	         			  , logonPort = &logonPort.
				          , username = &username.
				          , password = &password.
				          , authMethod = &authMethod.
				          , client_id = &client_id.
				          , client_secret = &client_secret.
				          , solution = &solution.
				  		  , filter = hasObjectLinkTo("RCC", "analysisData_dataDefinition", "&analysisDataKey.", 2)
				  		  , outds = data_definitions
				          , outds_columns = data_definitions_columns
				          , outVarToken = &outVarToken.
				          , outSuccess = &outSuccess.
				          , outResponseStatus = &outResponseStatus.
				          , debug = &debug.
				          , logOptions = &LogOptions.
				          , restartLUA = &restartLUA.
				          , clearCache = &clearCache.);

	/* Get aggregated vars */
	proc sql;
		select upcase(name) into :agg_columns separated by "," from data_definitions_columns where &aggregationFilterVar. = "true";
	quit;

	/* Create rule table */
	data &outLibref..&outRuleTable.;
		set &inLibref..&inAdjustmentTable.;
		/* Static rule set fields */
		RULE_DESC = "Auto generated rule for cell based adjustments";
		/* Only the "sum" aggregation is supported at the moment */
		RULE_METHOD = "value";
		ADJUSTMENT_TYPE = "INCREMENT";
		ALLOCATION_METHOD = "&allocationMethod.";
		AGGREGATION_METHOD = "SUM";
		
		/* Weighted aggregation specified in UI */
		%if &performWeightedAggregation. eq Y %then %do;
			WEIGHTED_AGGREGATION_FLG = "&performWeightedAggregation.";
			WEIGHT_VAR_NM = "&weightedVarName.";
		%end;
		%else %do;
			WEIGHTED_AGGREGATION_FLG = "N";
			WEIGHT_VAR_NM = "";
		%end;
	
		/* Create filter for each cross class set of vars */
		%let filter =;
		%let andVar=;
		%do i = 1 %to %sysfunc(countw(%quote(&crossClassVars.), %str(,)));
		%if &i. ne 1 %then %do;
			%let andVar=||" and "||;
		%end;
			%let curr_var = %scan(%quote(&crossClassVars.), &i., %str(,));
			%let filter = &filter.&andVar."&curr_var.="||"'"||strip(&curr_var.)||"'";
		%end;
		FILTER_EXP = "("||&filter.||")";
	
		/* Create a row for each aggregated variable */
		%if %sysfunc(countw(%quote(&agg_columns.), %str(,))) gt 0 %then %do;
			%do i = 1 %to %sysfunc(countw(%quote(&agg_columns.), %str(,)));
				%let col = %scan(%quote(&agg_columns.), &i., %str(,));
				RULE_ID = "rule_"||strip(_n_)||strip("_&col.");
				RULE_NAME = RULE_ID;
				MEASURE_VAR_NM = strip("&col.");
				if &col._adj ne &col. then do;
					ADJUSTMENT_VALUE = &col._adj - &col.;
					output;
				end;
			%end;
		%end;
        /* Variables included in the allocation rules meta which can probably be reduced */
		keep RULE_ID RULE_NAME RULE_DESC RULE_METHOD ADJUSTMENT_VALUE MEASURE_VAR_NM ADJUSTMENT_TYPE ALLOCATION_METHOD AGGREGATION_METHOD WEIGHTED_AGGREGATION_FLG WEIGHT_VAR_NM FILTER_EXP;
	run;
	
%mend;