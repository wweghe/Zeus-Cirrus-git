/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor core_rest_create_rule_set

   \brief   Create a rule set

   \param [in] host Host url, including the protocol
   \param [in] server Name of the Web Application Server that provides the REST service (Default: riskCirrusObjects)
   \param [in] solution Solution identifier (Source system code) for Cirrus Core content packages (Default: currently blank)
   \param [in] port (Optional) Server port.
   \param [in] logonHost (Optional) Host/IP of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app host/ip is the same as the host/ip in the url parameter
   \param [in] logonPort (Optional) Port of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app port is the same as the port in the url parameter
   \param [in] username Username credentials
   \param [in] password Password credentials: it can be plain text or SAS-Encoded (it will be masked during execution).
   \param [in] authMethod: Authentication method (accepted values: BEARER). (Default: BEARER).
   \param [in] client_id The client id registered with the Viya authentication server. If blank, the internal SAS client id is used (only if GRANT_TYPE = password).
   \param [in] client_secret The secret associated with the client id.
   \param [in] objectId The objectId of the new rule set
   \param [in] name The name of the new rule set
   \param [in] description A description of the new rule set
   \param [in] typeCd The type of rule set defined by the type dropdown (Default: ALLOCATION_RULES)
   \param [in] statusCd The status of the rule set defined by the status dropdown (Default: PROD)
   \param [in] categoryCd The category of the rule set defined by the category dropdown (Default: ADJ)
   \param [in] inputRuleSetDataLibrary The input rule set data table library (Default: work)
   \param [in] inputRuleSetData The input rule set data table name (and library). This defines all of the rules as visible on the "Rules" tab of the rule set object (Default: rules)
   \param [in] dataDefinitionKey (Optional) The key to a data definition to be linked to the new rule set
   \param [in] debug True/False. If True, debugging informations are printed to the log (Default: false)
   \param [in] logOptions Logging options (i.e. mprint mlogic symbolgen ...)
   \param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y)
   \param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y)
   \param [out] outds Name of the output table that contains the rule set information (Default: ruleSet)
   \param [out] outRuleData Name of the output table that contains the rule information (Default: ruleSetData)
   \param [out] outVarToken Name of the output macro variable which will contain the access token (Default: accessToken)
   \param [out] outSuccess Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess)
   \param [out] outResponseStatus Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus)

   \details
   This macro sends a POST request to <b><i>\<host\>:\<port\>/riskCirrusObjects/objects/ruleSets</i></b> and creates a rule set in Cirrus  \n
   See \link core_rest_request.sas \endlink for details about how to send POST requests and parse the response.
   \n
      <b>Example:</b>

   1) Set up the environment (set SASAUTOS and required LUA libraries).  Assumes the spre folder is under /riskcirruscore/core/code_libraries/release-core-{cadence-version}
   \code
      %let cadence_version=2022.11;
      %let core_root_path=/riskcirruscore/core/code_libraries/release-core-&cadence_version.;
      option insert = (
         SASAUTOS = (
            "&core_root_path./spre/sas/ucmacros"
            )
         );
      filename LUAPATH ("&core_root_path./spre/lua");
   \endcode

   2) Call the macro to create the new rule set with require parameters.
   \code
      %let accessToken =;
      %core_rest_create_ruleset(solution = <solution>
                              , objectId = new-rule-set
                              , name = new-rule-set
                              , description = This is a new rule set
                              , inputRuleSetData = rule_set_data
                              , dataDefinitionKey = <uuid>
                              , outds = ruleSet
                              , debug = true
                              , outRuleData = ruleSetData
                              , outVarToken = accessToken
                              , outSuccess = httpSuccess
                              , outResponseStatus = responseStatus
                              );
      %put &=accessToken;
      %put &=httpSuccess;
      %put &=responseStatus;
   \endcode

   \author  SAS Institute Inc.
   \date    2023
*/

%macro core_rest_create_ruleset(host =
                              , port =
                              , server = riskCirrusObjects
                              , solution =
                              , logonHost =
                              , logonPort =
                              , username =
                              , password =
                              , authMethod = bearer
                              , client_id =
                              , client_secret =
                              , objectId =
                              , name = 
                              , description = 
                              , typeCd = ALLOCATION_RULES
                              , categoryCd = ADJ
                              , statusCd = PROD
                              , inputRuleSetDataLibrary = work
                              , inputRuleSetData = rules
                              , dataDefinitionKey = 
                              , outds = ruleSet
                              , outRuleData = ruleSetData
                              , outVarToken = accessToken
                              , outSuccess = httpSuccess
                              , outResponseStatus = responseStatus
                              , debug = false
                              , logOptions =
                              , restartLUA = Y
                              , clearCache = Y
                              );

    /* Set the required log options */
    %if(%length(&logOptions.)) %then
        options &logOptions.;
    ;

    /* Get the current value of mlogic and symbolgen options */
    %local oldLogOptions;
    %let oldLogOptions = %sysfunc(getoption(mlogic)) %sysfunc(getoption(symbolgen));

    /* Get base URL for requests */
    %core_set_base_url(host=&host, server=&server., port=&port.);
    %let requestUrl = &baseUrl./&server./objects/ruleSets;

    /* Set headers file */
    filename _hin_ temp;
    data _null_;
        file _hin_;
        put 'Accept: application/json';
    run;

    /* Get the columns and types for the rule set data (The meta changes depending on the rule set type) */
    %let columns=;
    %let types=;
    proc sql noprint;
        select name into : columns separated by ' '
        from dictionary.columns
        where lowcase(libname) = lowcase("&inputRuleSetDataLibrary.") and lowcase(memname) = lowcase("&inputRuleSetData.");

        select type into : types separated by ' '
        from dictionary.columns
        where lowcase(libname) = lowcase("&inputRuleSetDataLibrary.") and lowcase(memname) = lowcase("&inputRuleSetData.");
    quit;

    /* Get the number of columns */
    %let colCount = %sysfunc(countw(%superq(columns),%str( )));

    /* Open a file for the request body */
    filename _body_ temp;

    /* Populate the body file with the rule set content */
    data _null_;
        set &inputRuleSetDataLibrary..&inputRuleSetData. end=last;
        length tmp_char $ 32000;
        file _body_ nopad;
        if _n_=1 then do;
            put "{";
            put "   ""changeReason"": ""creating rule set"",";
            put "   ""objectId"": ""&objectId."",";
            put "   ""name"": ""&name."",";
            put "   ""description"": ""&description."",";
            put "   ""sourceSystemCd"": ""%upcase(&solution.)"",";
            put "   ""createdInTag"": ""%upcase(&solution.)"",";
            put "   ""customFields"": {";
            put "      ""typeCd"": ""&typeCd."",";
            put "      ""categoryCd"": ""&categoryCd."",";
            put "      ""statusCd"": ""&statusCd."",";
            put "      ""ruleData"": {";
            put "         ""items"": [";
        end;

        /* Go through all of the columns for each rule */
        put "            {";
        %do i = 1 %to %sysfunc(countw(&columns.));
            %let col = %scan(&columns., &i.);
            %let type = %scan(&types., &i.);
            /* Set a comma variable */
            %if &i. ne &colCount. %then %do;
                %let comma=,;
            %end;
            %else %do;
                %let comma=;
            %end;
            /* Handle puts based on the var type */
            %if &type. eq num %then %do;
				tmp_num = trim(cat("""&col.""", ": ", &col., "&comma."));
				put "               " tmp_num;
            %end;
            %else %do;
                %if &col. eq config %then %do;
                    if strip(&col.) ne "" then do;
                        put "               ""&col."": "%bquote(&col.)"&comma.";
                    end;
                %end;
                %else %do;
                    if strip(&col.) eq "" then do;
                        put "               ""&col."": """"&comma.";
                    end;
                    else do;
                        tmp_char = trim(cat("""&col.""", ": ", '"', tranwrd(strip(&col.),'"',"'"),'"', "&comma."));
                        put "               " tmp_char;
                    end;
                %end;
            %end;
        %end;
        /* If this is the last rule, do not add a comma */
        if not last then do;
            put "            },";
        end;
        else do;
            put "            }";
        end;

        /* Finish the json when the last rule is complete */
        if last then do;
            put "         ]";
            put "      }";
            put "   }";
            put "}";
        end;
    run;

    /* Send request to create the new rule set */
    %let &outSuccess. = 0;
    option nomlogic nosymbolgen;
    %core_rest_request(url = &requestUrl.
                     , method = POST
                     , logonHost = &logonHost.
                     , logonPort = &logonPort.
                     , username = &username.
                     , password = &password.
                     , authMethod = &authMethod.
                     , client_id = &client_id.
                     , client_secret = &client_secret.
                     , body = _body_
                     , headerIn = _hin_
                     , contentType = application/json
                     , parser = sas.risk.cirrus.core_rest_parser.coreRestRuleSet
                     , outds = &outds.
                     , arg1 = &outRuleData.
                     , outVarToken = &outVarToken.
                     , outSuccess = &outSuccess.
                     , outResponseStatus = &outResponseStatus.
                     , debug = &debug.
                     , logOptions = &logOptions.
                     , restartLUA = &restartLUA.
                     , clearCache = &clearCache.
                     );

    /* Exit in case of errors */
    %if(not &&&outSuccess..) %then
        %abort;

    /* Clear files */
    filename _body_ clear;
    filename _hin_ clear;

    /*  */
    %if %sysevalf(%superq(dataDefinitionKey) ne,boolean) %then %do;
        /* Link rule set to the data definition if one is provided */
        %let ruleSetKey=;
        data _null_;
            set &outds.;
            call symputx("ruleSetKey", key, "L");
        run;

        /* Get the keys required to build a link */
        %let keyPrefixRuleSet = %substr(&ruleSetKey., 1, 7);
        %let keyPrefixDataDef = %substr(&dataDefinitionKey., 1, 7);

        /* Send request to create a link */
        %let &outSuccess. = 0;
        %core_rest_create_link_inst(host = &host.
                                  , port = &port.
                                  , logonHost = &logonHost.
                                  , logonPort = &logonPort.
                                  , username = &username.
                                  , password = &password.
                                  , authMethod = &authMethod.
                                  , client_id = &client_id.
                                  , client_secret = &client_secret.    
                                  , link_instance_id = ruleSet_dataDef_&keyPrefixRuleSet._&keyprefixdatadef.
                                  , linkSourceSystemCd = &solution.
                                  , link_type = ruleSet_dataDefinition
                                  , solution = &solution.
                                  , business_object1 = &ruleSetKey.
                                  , business_object2 = &dataDefinitionKey.
                                  , collectionObjectKey = &ruleSetKey.
                                  , collectionName = ruleSets
                                  , outds = link_instance
                                  , outVarToken = &outVarToken.
                                  , outSuccess = &outSuccess.
                                  , outResponseStatus = &outResponseStatus.
                                  );

        /* Exit in case of errors */
        %if(not &&&outSuccess..) %then
            %abort;    
    %end;

%mend;