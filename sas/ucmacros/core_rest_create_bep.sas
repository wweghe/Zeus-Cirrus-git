/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor core_rest_create_bep

   \brief   Create a Business Evolution Plan (BEP) instance in SAS Risk Cirrus objects.

   \param [in] host (Optional) Host url, including the protocol.
   \param [in] port (Optional) Server port.
   \param [in] server Name that provides the REST service (Default: riskData).
   \param [in] logonHost (Optional) Host/IP of the sas-logon-app service or ingress. If blank, it is assumed that the sas-logon-app host/ip is the same as the host/ip in the url parameter.
   \param [in] logonPort (Optional) Port of the sas-logon-app service or ingress. If blank, it is assumed that the sas-logon-app port is the same as the port in the url parameter.
   \param [in] username (Optional) Username credentials
   \param [in] password (Optional) Password credentials: it can be plain text or SAS-Encoded (it will be masked during execution).
   \param [in] authMethod: Authentication method (accepted values: BEARER). (Default: BEARER).
   \param [in] client_id The client id registered with the Viya authentication server. If blank, the internal SAS client id is used (only if GRANT_TYPE = password).
   \param [in] client_secret The secret associated with the client id.
   \param [in] sourceSystemCd The source system code to assign to the object when registering it in Cirrus Objects (Default: <solution>)
   \param [in] solution The solution short name from which this request is being made. This will get stored in the createdInTag and sharedWithTags attributes on the object (Default: 'blank').
   \param [in] inDsBepSummaryData a SAS dataset containing the BEP summary information. Used to assign mvars for BEP main customFields attributes.
   \param [in] inDsBepSpreadsheetData a SAS dataset containing the BEP detail data with figures to be included in the BEP to be created.
   \param [in] inDsBepLinkInstances a SAS dataset containing the link instances to set the objectLinks object of new BEP.
   \param [in] inDsBepTargetAuxData a SAS dataset containing the target auxiliary data to set the targetVars object of new BEP. If not provided, the macro will try to collect the information from inDsBepSpreadsheetData.
   \param [in] newBepObjectId Name and ObjectId to be provided to the new BEP. If not provided, BEP attributes assume 'BEP created by script at <datetime>'.
   \param [in] statusCd The status of the bep defined by the status dropdown (Default: PROD)
   \param [in] segVarsTotCount Total number of segmentation variables available in current BEP spreadsheet object (Default: 5).
   \param [in] debug True/False. If True, debugging informations are printed to the log (Default: false).
   \param [in] logOptions Logging options (i.e. mprint mlogic symbolgen ...).
   \param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y).
   \param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y).
   \param [out] outds Name of the output table that contains the BEP information (Default: outBepInfo)
   \param [out] outds_configTablesData Name of the output table data contains the schema of the analysis data structure (Default: config_tables_data).
   \param [out] outVarToken Name of the output macro variable which will contain the access token (Default: accessToken).
   \param [out] outSuccess Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess).
   \param [out] outResponseStatus Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus).

   \details
   This macro sends a POST request to <b><i>\<host\>:\<port\>/riskCirrusObjects/objects/businessEvolutionPlans. \n
   See \link core_rest_request.sas \endlink for details about how to send GET/POST requests and parse the response.

   <b>Example:</b>

   1) Set up the environment (set SASAUTOS and required LUA libraries).  Assumes the spre folder is under /riskcirruscore/core/code_libraries/release-core-{cadence-version}
   \code
      %let cadence_version=2023.12;
      %let core_root_path=/riskcirruscore/core/code_libraries/release-core-&cadence_version.;
      option insert = (
         SASAUTOS = (
            "&core_root_path./spre/sas/ucmacros"
            )
         );
      filename LUAPATH ("&core_root_path./spre/lua");
   \endcode

   2) Send a Http GET request and parse the JSON response into the outputs tables work.configuration_tables_info and work.configuration_tables_data
   \code

      %let accessToken =;
      %core_rest_create_bep(server = riskCirrusObjects
                              , authMethod = bearer
                              , solution = ST
                              , sourceSystemCd = ST
                              , inDsBepSummaryData =
                              , inDsBepSpreadsheetData =
                              , inDsBepLinkInstances =
                              , inDsBepTargetAuxData =
                              , statusCd = PROD
                              , outds = outBepInfo
                              , outVarToken = accessToken
                              , outSuccess = httpSuccess
                              , outResponseStatus = responseStatus
                              , debug = true);
      %put &=httpSuccess;
      %put &=responseStatus;
   \endcode

   \ingroup coreRestUtils

   \author  SAS Institute Inc.
   \date    2023
*/

%macro core_rest_create_bep(host =
                            , port =
                            , server = riskCirrusObjects
                            , logonHost =
                            , logonPort =
                            , username =
                            , password =
                            , authMethod = bearer
                            , client_id =
                            , client_secret =
                            , solution =
                            , sourceSystemCd =
                            , inDsBepSummaryData = bep_summary
                            , inDsBepSpreadsheetData =
                            , inDsBepLinkInstances =
                            , inDsBepTargetAuxData =
                            , newBepObjectId =
                            , newBepName =
                            , statusCd = PROD
                            , segVarsTotCount = 5
                            , outds = outBepInfo
                            , outVarToken = accessToken
                            , outSuccess = httpSuccess
                            , outResponseStatus = responseStatus
                            , debug = false
                            , logOptions =
                            , restartLUA = Y
                            , clearCache = Y) / minoperator;

    %local
        columns
        types
        colCount
        _segVarFlg_
        targetVar targetVarsCount
        ibep
        jbep
        zbep
        comma1
        comma2
        tot_target_vars
        objectLinksDataKeys
        tot_obj_links
        intervalCount
        planningCurrency
        intervalType
        initType
        auxData_flg
        varsToTranspose_comma varsToTranspose_space
        _seg_vars_
        _seg_vars_flg_
        seg_vars_count
        _comma_seg_vars_
    ;

    /* Set the required log options */
    %if(%length(&logOptions.)) %then
        options &logOptions.;
    ;

    /* Get the current value of mlogic and symbolgen options */
    %local oldLogOptions;
    %let oldLogOptions = %sysfunc(getoption(mlogic)) %sysfunc(getoption(symbolgen));

    %if (%sysevalf(%superq(solution) eq, boolean)) %then %do;
        %put ERROR: Parameter 'solution' is required.;
        %abort;
    %end;

    %if (%sysevalf(%superq(sourceSystemCd) eq, boolean)) %then %do;
        %let sourceSystemCd = &solution.;
    %end;

    %if (%sysevalf(%superq(inDsBepSummaryData) eq, boolean)) %then %do;
        %put ERROR: A Summary table with BEP main attributes must be provided.;
        %abort;
    %end;

    %if (%sysevalf(%superq(inDsBepSpreadsheetData) eq, boolean)) %then %do;
        %put ERROR: An auxiliary table with projection values must be provided.;
        %abort;
    %end;

    %if (%sysevalf(%superq(newBepObjectId) eq, boolean)) %then %do;
        %let newBepObjectId = BEP created by script at %sysfunc(tranwrd(%sysfunc(datetime(),nldatm.),%str(:),%str(_)));
    %end;
    %let newBepName = %sysfunc(coalescec(&newBepName., &newBepObjectId.));
    %let newBepObjectId = %sysfunc(tranwrd(&newBepObjectId.,%str( ),%str(_)));

    /* assign mvars from bep summary table for BEP main customFields attributes */
    proc transpose data=&inDsBepSummaryData.(obs=1) out=nums prefix=num;
       var _numeric_;
    run;
    data _null_;
        set nums;
        call symputx(_name_, num1, "L");
    run;
    proc transpose data=&inDsBepSummaryData.(obs=1) out=chars prefix=char;
       var _character_;
    run;
    data _null_;
        set chars;
        char1=prxchange('s/(\n|\r)/\n/', -1, char1);
        call symputx(_name_, char1, "L");
    run;
    proc sql noprint;
        select distinct(targetVar) as targetVar
                into :targetVar separated by ','
        from &inDsBepSpreadsheetData.;
    ;quit;

    /* Set headers file */
    filename _hin_ temp;
    data _null_;
        file _hin_;
        put 'Accept: application/json';
    run;

    /* generate all segVars for the tot number that is set in macro program param */
    %let _seg_vars_=;
    %let _seg_vars_flg_=;
    %let seg_vars_count=&segVarsTotCount.;

    %do _seg_vars_count_=1 %to &seg_vars_count.;
        %if &_seg_vars_count_.=1 %then %let _comma_seg_vars_=;
        %else %let _comma_seg_vars_=,;
        %let _seg_vars_ = &_seg_vars_. &_comma_seg_vars_. segVar&_seg_vars_count_.;
        %let _seg_vars_flg_ = &_seg_vars_flg_. &_comma_seg_vars_. segVarFlg&_seg_vars_count_.;
    %end;

    /* Declare which variables are required for output in bep json structure data */
    /* keep these variables in this order of targetVar,segment, key,level, */
    %let varsToTranspose_comma=targetVar,segment, key,level,&_seg_vars_.,
                        targetType,targetDefinition,hasChildren,hierarchy,horizon0,operator,segMethod,
                        segmentFilter, scenario, scenarioSet, relativePct, relativeValue, absoluteValue,
                        intervalName,&_seg_vars_flg_.;

    %let varsToTranspose_space=%sysfunc(prxchange(s/[%str(,)]/ /,-1,%superq(varsToTranspose_comma)));

    /* to get details data order to merge at the end with new BEP and keep it likewise */
    data WORK.BEP_DETAILS_DATA_ORDER(drop=key rename=(key_aux=key));
        set WORK.&inDsBepSpreadsheetData.(keep=key);
        length key_aux $200;
        retain key_aux "" count 0;
        if key_aux ne key then do;
            count+1;
            key_aux = key;
            output;
        end;
    run;

    /* Column value calculation to transpose in each IntervalName by varsToTranspose mvar */
    proc sql noprint;
        create table WORK.BEP_DETAILS_SORT as
        select *,
                case
                    when (targetType = "" or targetType = "PREVIOUS" or targetType = "FIXED") then absoluteValue
                    when (targetDefinition = "PERCENTAGE") then relativePct
                    when (targetDefinition = "ABSOLUTE") then relativeValue
                end as final_value
        from &inDsBepSpreadsheetData.(keep=&varsToTranspose_space.)
        where intervalName ne "%trim(&intervalType.)_0"
        order by &varsToTranspose_comma.
    ;quit;

    %let varsToTranspose_space=%sysfunc(prxchange(s/intervalName|relativePct|relativeValue|absoluteValue//, -1, %superq(varsToTranspose_space)));
    proc transpose data=WORK.BEP_DETAILS_SORT (drop=absoluteValue relativePct relativeValue)
                    out=WORK.BEP_DETAILS_TRANS(drop=_name_);
        by &varsToTranspose_space.;
        id intervalName;
        var final_value;
    run;

    /* Get the columns and types for the bep data (for the case of bepData.data structure changes) */
    %let columns=;
    %let types=;

    %let _seg_vars_flg_ = %upcase(%sysfunc(prxchange(s/(\w+)/'\1'/, -1, %superq(_seg_vars_flg_))));
    proc sql noprint;
        select name, type into
                        :columns separated by ' ',
                        :types separated by ' '
        from dictionary.columns
        where lowcase(libname) = lowcase("WORK")
                and lowcase(memname) = lowcase("BEP_DETAILS_TRANS")
                /* remove segVarFlg<1-n> since is not needed in json file */
                and upcase(name) not in (&_seg_vars_flg_.)
    ;quit;

    %if &solution eq ST %then %do;
        %let columns = %sysfunc(prxchange(s/targetType/growthInputType/, -1, %superq(columns)));
        %let columns = %sysfunc(prxchange(s/targetDefinition/growthInputParam/, -1, %superq(columns)));
    %end;

    /************************************************************** */
    /* Get the columns and values for the level = 0 aux_data object */
    /************************************************************** */
    proc sql noprint;
        create table WORK.BEP_DETAILS_AUX_DATA as
        select distinct key, auxDataKey as aux_data_key, auxDataName as aux_data_name, auxDataObjectId as aux_data_objectId, "Y" as auxData_flg
        from &inDsBepSpreadsheetData.
        where auxDataKey is not missing
        order by 1,2;

        create table WORK.BEP_DETAILS_TRANS_JOIN_AUX_DATA as
        select t1.*, t3.aux_data_key, t3.aux_data_name, t3.aux_data_objectId, t3.auxData_flg
        from WORK.BEP_DETAILS_TRANS as t1
            left join WORK.BEP_DETAILS_DATA_ORDER as t2 on t1.key = t2.key
            left join WORK.BEP_DETAILS_AUX_DATA as t3 on t1.key = t3.key
        order by t2.count;
    ;quit;

    %let tot_target_vars=;
    /************************************************************** */
    /* Get the columns and values for the targetVars portfolio data */
    /************************************************************** */
    %if (%sysevalf(%superq(inDsBepTargetAuxData) ne, boolean)) %then %do;
        data _null_;
            set &inDsBepTargetAuxData.;
            call symputx(catx('_', 'tgt_aux_data_key', _N_), key,'L');
            call symputx(catx('_', 'tgt_aux_data_name', _N_), name,'L');
            call symputx(catx('_', 'tgt_aux_data_objectId', _N_), objectId,'L');
            call symputx(catx('_', 'tgt_aux_data_value', _N_), value,'L');
            call symputx(catx('_', 'tgt_aux_data_label', _N_), label,'L');
            call symputx('tot_target_vars', _N_ , 'L');
        run;

    %end;
    %else %do;
            proc sql noprint;
                create table WORK._TGT_VARS_ as
                select targetVar, auxDataKey, auxDataName, auxDataObjectId, segment
                from &inDsBepSpreadsheetData.
                where auxDataKey is not missing
                order by 1,2
            ;quit;
            data _null_;
                set WORK._TGT_VARS_;
                by targetVar auxDataKey;
                retain counter 0;
                if first.auxDataKey then do;
                    counter + 1;
                    call symputx(catx('_', 'tgt_aux_data_key', counter), auxDataKey,'L');
                    call symputx(catx('_', 'tgt_aux_data_name', counter), auxDataName,'L');
                    call symputx(catx('_', 'tgt_aux_data_objectId', counter), auxDataObjectId,'L');
                    call symputx(catx('_', 'tgt_aux_data_value', counter), targetVar,'L');
                    call symputx(catx('_', 'tgt_aux_data_label', counter), segment,'L');
                    call symputx('tot_target_vars', counter , 'L');
                end;
            run;
        %end;
    /************************************************************** */

    /************************************************************** */
    /* Get the LinkType to add it to new BEP object objectLinks   */
    /************************************************************** */
    %let objectLinksDataKeys=;
    %let tot_obj_links=;
    %if (%sysevalf(%superq(inDsBepLinkInstances) ne, boolean)) %then %do;
        data _null_;
        set &inDsBepLinkInstances. end=last;
            length objectLinkDataKeys objectLinkTypeDataKeys $200;
            retain tot_obj_links 0 objectLinkDataKeys objectLinkTypeDataKeys;
            if _n_ =1 then do;
                tot_obj_links = 0;
                objectLinkDataKeys = "";
            end;
            if auxiliaryDataKey ne "" then do;
                objectLinkDataKeys = catx(' ', objectLinkDataKeys, auxiliaryDataKey);
                objectLinkTypeDataKeys = catx(' ', objectLinkTypeDataKeys, linkTypeAuxiliaryDataKey);
                tot_obj_links + 1;
            end;
            if hierarchyDataKey ne "" then do;
                objectLinkDataKeys = catx(' ',objectLinkDataKeys,hierarchyDataKey);
                objectLinkTypeDataKeys = catx(' ', objectLinkTypeDataKeys, linkTypeHierarchyDataKey);
                tot_obj_links + 1;
            end;
            if planningDataKey ne "" then do;
                objectLinkDataKeys = catx(' ',objectLinkDataKeys,planningDataKey);
                objectLinkTypeDataKeys = catx(' ', objectLinkTypeDataKeys, linkTypePlanningDataKey);
                tot_obj_links + 1;
            end;
            if last then do;
                call symputx("tot_obj_links",tot_obj_links,'L');
                call symputx("objectLinkTypeDataKeys",objectLinkTypeDataKeys,'L');
                call symputx("objectLinkDataKeys",objectLinkDataKeys,'L');
            end;
        run;
    %end;

    /************************************************************** */

    /* Get the number of columns */
    %let colCount = %sysfunc(countw(%superq(columns),%str( )));

    filename _body_ temp;

    data _null_;
        set WORK.BEP_DETAILS_TRANS_JOIN_AUX_DATA
        %if &solution eq ST %then %do;
            (rename=(targetDefinition=growthInputParam targetType=growthInputType ))
        %end;
        end=last;
            length tmp_char $ 2000;
            file _body_ nopad;
            if _n_=1 then do;
                put "{"; /* open Items object */
                put "   ""changeReason"": ""creating bep object"",";
                put "   ""objectId"": ""&newBepObjectId."",";
                put "   ""sourceSystemCd"": ""%upcase(&solution.)"",";
                put "   ""name"": ""&newBepName."",";
                put "   ""description"": ""&description."",";
                put "   ""createdInTag"": ""%upcase(&solution.)"",";
                put "   ""customFields"": {";
            %if (%sysevalf(%superq(intervalType) ne, boolean)) %then %do;
                tmp_char = '"intervalType": "'||strip("&intervalType.")||'",';
                put "    " tmp_char;
            %end;
            %if (%sysevalf(%superq(intervalCount) ne, boolean)) %then %do;
                tmp_char = '"intervalCount": '||strip("&intervalCount.")||',';
                put "    " tmp_char;
            %end;
            %if (%sysevalf(%superq(planningCurrency) ne, boolean)) %then %do;
                tmp_char = '"planningCurrency": "'||strip("&planningCurrency.")||'",';
                put "    " tmp_char;
            %end;
            %if (%sysevalf(%superq(statusCd) ne, boolean)) %then %do;
                put "      ""statusCd"": ""&statusCd."",";
            %end;
            %if (%sysevalf(%superq(initType) ne, boolean)) %then %do;
                tmp_char = '"initType": "'||strip("&initType.")||'",';
                put "    " tmp_char;
            %end;
                put "      ""bepData"": {"; /* open bepData object */
                put "         ""data"": ["; /* open bepData.data object array */
            end;

            put "            {";
            /* set auxData properties */
            if auxData_flg eq "Y" then do;
                put "               ""auxData"": {";
                put "                     ""restPath"": ""analysisData"",";
                put "                     ""type"": ""cirrusObjectArray"",";
                put "                     ""value"": [";
                put "                         {";
                tmp_char = trim(cat("""name""",": """,strip(aux_data_name),""","));
                put "                           " tmp_char;
                put "                           ""sourceSystemCd"": ""%upcase(&solution.)"",";
                tmp_char = trim(cat("""key""",": """,strip(aux_data_key),""","));
                put "                           " tmp_char;
                tmp_char = trim(cat("""objectId""",": """,strip(aux_data_objectId),""""));
                put "                           " tmp_char;
                put "                         }";
                put "                     ]";
                put "               },";
            end;

            %do ibep = 1 %to %sysfunc(countw(&columns.));
                %let col = %scan(&columns., &ibep.);
                %let type = %scan(&types., &ibep.);
                /* Set a comma variable */
                %if &ibep. ne &colCount. %then %do;
                    %let comma1=,;
                %end;
                %else %do;
                    %let comma1=;
                %end;
                %if &type. eq num %then %do;
                    if &col. eq . then do;
                        tmp_num = trim(cat("""&col.""", ": ", "null", "&comma1."));
                        put "               " tmp_num;
                    end;
                    else do;
                        tmp_num = trim(cat("""&col.""", ": ", &col., "&comma1."));
                        put "               " tmp_num;
                    end;
                %end;
                %else %do;
                        %do _segVarFlg_=1 %to &seg_vars_count.;
                            if upcase("&col.") eq ("SEGVAR&_segVarFlg_.") then do;
                                if segvarflg&_segVarFlg_. ne "Y" then do;
                                    &col. = "";
                                end;
                            end;
                        %end;
                        if strip(&col.) eq "" then do;
                            put "               ""&col."": """"&comma1.";
                        end;
                        else do;
                                if upcase(&col.) = "TRUE" or upcase(&col.) = "FALSE" or substr(&col.,1,1) = "[" then do;
                                    tmp_char = trim(cat("""&col.""", ": ", strip(&col.), "&comma1."));
                                    put "               " tmp_char;
                                end;
                                else do;
                                        if "&col." = "segmentFilter" then do;
                                            tmp_char = trim(cat("""&col.""", ": ", '"', tranwrd(strip(&col.),'"','\"'),'"', "&comma1."));
                                            put "               " tmp_char;
                                        end;
                                        else do;
                                            tmp_char = trim(cat("""&col.""", ": ", '"', strip(&col.),'"', "&comma1."));
                                            put "               " tmp_char;
                                        end;
                                    end;
                            end;
                    %end;

            %end;

            /* If this is not the last data item, add a comma */
            if not last then do;
                put "            },";
            end;
            else do; /* if last obs */
                    put "            }"; /* close bep.data last item */
                    put "         ]"; /* close bepData.data object array */
                    put "      }"; /* close bepData object */
                    %if (%sysevalf(%superq(tot_target_vars) ne, boolean)) %then %do;
                        put "      ,""targetVars"": {"; /* open targetVars object */
                        put "          ""data"": ["; /* open targetVars.data object array */
                        %do jbep = 1 %to &tot_target_vars.;
                            %if &jbep. ne &tot_target_vars. %then %do;
                                %let comma2=,;
                            %end;
                            %else %do;
                                %let comma2=;
                            %end;
                            put "              {";
                            put "                 ""auxData"": {";
                            put "                       ""name"": ""&&tgt_aux_data_name_&jbep."",";
                            put "                       ""sourceSystemCd"": ""%upcase(&solution.)"",";
                            put "                       ""key"": ""&&tgt_aux_data_key_&jbep."",";
                            put "                       ""objectId"": ""&&tgt_aux_data_objectId_&jbep."" ";
                            put "                 },";
                            put "                 ""label"": ""&&tgt_aux_data_label_&jbep."",";
                            put "                 ""value"": ""&&tgt_aux_data_value_&jbep.."" ";
                            put "              }&comma2.";
                        %end;
                        put "           ],"; /* close targetVars.data object array */
                        put "           ""formattedValues"": ""&targetVar."" ";
                        put "      }"; /* close targetVars object */
                    %end;
                    put "   }"; /* close customFields object */
                    %if (%sysevalf(%superq(tot_obj_links) ne, boolean)) %then %do;
                        put ",""objectLinks"": [";  /* open objectLinks */
                        %do zbep = 1 %to &tot_obj_links.;
                            %let obj_link_key = %scan(&objectLinkDataKeys., &zbep.,%str( ));
                            %let obj_link_type_key = %scan(&objectLinkTypeDataKeys., &zbep.,%str( ));
                            %if &zbep. ne &tot_obj_links. %then %do;
                                    %let comma3=,;
                                %end;
                                %else %do;
                                    %let comma3=;
                                %end;
                            put "                   {";
                            put "                      ""sourceSystemCd"": ""%upcase(&solution.)"",";
                            put "                      ""linkType"":""&obj_link_type_key."",";
                            put "                      ""businessObject2"":""&obj_link_key.""";
                            put "                   }&comma3.";
                        %end;
                        put "]                  "; /* close objectLinks */
                    %end;
                    put "}"; /* close Items object */
                end;
    run;

    /* Base Request URL for objects temporary files */
    %core_set_base_url(host=&host, server=&server., port=&port.);
    %let requestUrl = &baseUrl./&server./objects/businessEvolutionPlans;

    filename bep_resp temp;

    /* Temporary disable mlogic and symbolgen options to avoid printing of userid/pwd to the log */
    option nomlogic nosymbolgen;
    /* Send the REST request */
    %let &outSuccess. = 0;
    %core_rest_request(url = &requestUrl.
                        , method = POST
                        , logonHost = &logonHost.
                        , logonPort = &logonPort.
                        , username = &username.
                        , password = &password.
                        , authMethod = &authMethod.
                        , body = _body_
                        , headerIn = _hin_
                        , contentType = application/json
                        , fout = bep_resp
                        , parser = coreRestPlain
                         , outds = &outds.
                        , outVarToken = &outVarToken.
                        , outSuccess = &outSuccess.
                        , outResponseStatus = &outResponseStatus.
                        , debug = &debug.
                        , logOptions = &oldLogOptions.
                        , restartLUA = &restartLUA.
                        , clearCache = &clearCache.
                        );

    libname bep_resp json fileref=bep_resp noalldata;

    /* Exit in case of errors */
    %if( (not &&&outSuccess..) or not(%rsk_dsexist(&outds.)) or %rsk_attrn(&outds., nobs) = 0 ) %then %do;
        %put ERROR: Unable to accomplish the service: Create new Business Evolution Plan;
        data _null_;
            set bep_resp.root(keep=message);
            call symputx("resp_message",message);
        run;
        %put ERROR: &resp_message.;
        %abort;
    %end; /* (not &&&outSuccess..) */

    /* Clear references if we're not debugging */
    %if %upcase(&debug) ne TRUE %then %do;
        /* Clear files */
        filename _body_ clear;
        filename _hin_ clear;

        filename bep_resp clear;
        libname bep_resp;
    %end;

%mend core_rest_create_bep;