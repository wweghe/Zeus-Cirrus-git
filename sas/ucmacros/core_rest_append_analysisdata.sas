/*
 Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor core_rest_append_analysisdata

   \brief   Appends a sas dataset or a cas table to an existing analysis data

   \param [in] host (Optional) Host url, including the protocol.
   \param [in] port (Optional) Server port.
   \param [in] server Name that provides the REST service (Default: riskData).
   \param [in] solution The solution short name from which this request is being made. This will get stored in the createdInTag and sharedWithTags attributes on the object (Default: 'blank').
   \param [in] logonHost (Optional) Host/IP of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app host/ip is the same as the host/ip in the url parameter.
   \param [in] logonPort (Optional) Port of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app port is the same as the port in the url parameter.
   \param [in] username (Optional) Username credentials
   \param [in] password (Optional) Password credentials: it can be plain text or SAS-Encoded (it will be masked during execution).
   \param [in] authMethod: Authentication method (accepted values: BEARER). (Default: BEARER).
   \param [in] casHost (Optional) Host/IP of the sas-cas-server-default-client.
   \param [in] casPort (Optional) Port of the cas-server port.
   \param [in] casSessionName The name of a CAS session to use for local CAS actions.  If one doesn't exist, a new session with this name will be created.
   \param [in] client_id The client id registered with the Viya authentication server. If blank, the internal SAS client id is used (only if GRANT_TYPE = password).
   \param [in] client_secret The secret associated with the client id.
   \param [in] locationType The type of server location from which data will be imported. LIBNAME has tables in filename, FOLDER has csv files in filename and DIRECTORY has <tables>.sas7bdat in filename to be used through a libname.
   \param [in] location The server location from which data will be imported. Interpretation of this parameter varies based on the value of locationType. When DIRECTORY, the filesystem path on the server where the import data is located. When LIBNAME, the name of the library in which the import data may be found.
   \param [in] fileName Name of the file or table from which data will be imported.
   \param [in] cycle_key Cycle key to use for the analysis data instance.
   \param [in] analysis_run_key Analysis Run key to use for the analysis data instance.
   \param [in] key Instance key of the Analysis data object to append.
   \param [in] inputDataFilter Generic filter to apply over input data filename. Use sas syntax to compose where clause. e.g. INSTID eq 'CI_00001' | RATING_GRADE in ('BBB' 'A').
   \param [in] enableResultAttr Enable or disable a feature to add specific columns to result analysis data object (Default: 'N').
   \param [in] debug True/False. If True, debugging informations are printed to the log (Default: false).
   \param [in] logOptions Logging options (i.e. mprint mlogic symbolgen ...).
   \param [in] restartLUA. Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y).
   \param [in] clearCache Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y).
   \param [out] outds_configTablesData Name of the output table data contains the schema of the analysis data structure (Default: config_tables_data).
   \param [out] outVarToken Name of the output macro variable which will contain the access token (Default: accessToken).
   \param [out] outSuccess Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess).
   \param [out] outResponseStatus Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus).

   \details
   This macro sends a PUT request to <b><i>\<host\>:\<port\>/riskData/objects?locationType=&locationType.&location=&location.&fileName=&filename/</i></b> and collects the results in the output table. \n
   See \link core_rest_request.sas \endlink for details about how to send GET/POST requests and parse the response.

   <b>Example:</b>

   1) Set up the environment (set SASAUTOS and required LUA libraries).  Assumes the spre folder is under /riskcirruscore/core/code_libraries/release-core-{cadence-version}
   \code
      %let cadence_version=2023.0;
      %let core_root_path=/riskcirruscore/core/code_libraries/release-core-&cadence_version.;
      option insert = (
         SASAUTOS = (
            "&core_root_path./spre/sas/ucmacros"
            )
         );
      filename LUAPATH ("&core_root_path./spre/lua");
   \endcode

      %let accessToken =;
      %core_rest_append_analysisdata(host = <host>
                              , port = <port>
                              , server = riskData
                              , logonHost =
                              , logonPort =
                              , username =
                              , password =
                              , authMethod = bearer
                              , client_id =
                              , client_secret =
                              , casHost =
                              , casPort =
                              , casSessionName =
                              , cycle_key =
                              , analysis_run_key =
                              , key =
                              , locationType = LIBNAME
                              , location =
                              , fileName =
                              , inputDataFilter =
                              , enableResultAttr =
                              , logSeverity = WARNING
                              , outVarToken = accessToken
                              , outSuccess = httpSuccess
                              , outResponseStatus = responseStatus
                              , debug = true
                              , logOptions =
                              , restartLUA = Y
                              , clearCache = Y);
      %put &=httpSuccess;
      %put &=responseStatus;
   \endcode

   \ingroup rgfRestUtils

   \author  SAS Institute Inc.
   \date    2022
*/

%macro core_rest_append_analysisdata(host =
                            , port =
                            , server = riskData
                            , solution =
                            , logonHost =
                            , logonPort =
                            , username =
                            , password =
                            , authMethod = bearer
                            , casHost =
                            , casPort =
                            , casSessionName =
                            , client_id =
                            , client_secret =
                            , key =
                            , cycle_key =
                            , analysis_run_key =
                            , locationType = LIBNAME
                            , location =
                            , fileName =
                            , inputDataFilter =
                            , enableResultAttr = N
                            , logSeverity = WARNING
                            , outVarToken = accessToken
                            , outSuccess = httpSuccess
                            , outResponseStatus = responseStatus
                            , debug = false
                            , logOptions =
                            , restartLUA = Y
                            , clearCache = Y
                            ) / minoperator;
    %local
        marked_to_terminate
        location_tmp
        marked_to_delete
        cas_libname
        casSessionTag
        dataCategoryCd
        verifyIfColumnsExist
    ;

    %let marked_to_terminate = N;
    %let marked_to_delete = N;
    %let cas_libname = Y;
    %let dataCategoryCd =;

   /* Set the required log options */
   %if(%length(&logOptions.)) %then
      options &logOptions.;
   ;

   /* Get the current value of mlogic and symbolgen options */
   %local oldLogOptions;
   %let oldLogOptions = %sysfunc(getoption(mlogic)) %sysfunc(getoption(symbolgen));

    %if (%sysevalf(%superq(key) eq, boolean)) %then %do;
        %put ERROR: The analysis data key parameter is required.;
        %return;
    %end;

    %if (%sysevalf(%superq(location) eq, boolean)) %then %do;
        %put ERROR: The location parameter is required.;
        %return;
    %end;

    %if (%sysevalf(%superq(fileName) eq, boolean)) %then %do;
        %put ERROR: The fileName parameter is required.;
        %return;
    %end;

    %if (%sysevalf(%superq(locationType) eq, boolean)) %then %do;
        %put ERROR: The locationType parameter is required. Available values : DIRECTORY, LIBNAME;
        %return;
    %end;

    %if (%sysevalf(%superq(solution) eq, boolean)) %then %do;
        %put ERROR: The solution parameter is required.;
        %return;
    %end;

    /* Get dataCategoryCd 'RESULTS' via Data Definition attributes info to feed extra columns if needed */
    %if (%upcase("&enableResultAttr.") eq "Y" ) %then %do;
        %core_rest_get_link_instances( host = &host.
                                    , port = &port.
                                    , server = riskCirrusObjects
                                    , logonHost = &logonHost.
                                    , logonPort = &logonPort.
                                    , username = &username.
                                    , password = &password.
                                    , authMethod = &authMethod.
                                    , client_id = &client_id.
                                    , client_secret = &client_secret.
                                    , solution = &solution.
                                    , objectType = analysisData
                                    , objectKey = &key.
                                    , linkType = analysisData_dataDefinition
                                    , outds = _tmp_dd_link_instances_
                                    , outVarToken = &outVarToken.
                                    , outSuccess = &outSuccess.
                                    , outResponseStatus = &outResponseStatus.
                                    , debug = &debug.
                                    , logOptions = &logOptions.
                                    , restartLUA = &restartLUA.
                                    , clearCache = &clearCache.
                                    );
        /* Check for errors */
        %if( not &&&outSuccess.. or not %rsk_dsexist(_tmp_dd_link_instances_) or %rsk_attrn(_tmp_dd_link_instances_, nobs) = 0 ) %then %do;
            %put ERROR: Unable to get the dataCategory code information;
            %abort;
        %end;
        %else %do;
                /* Get the analysis run key */
                data _null_;
                    set _tmp_dd_link_instances_;
                    call symputx("data_definition_key", businessObject2, "L");
                run;

                %core_rest_get_data_def(server = riskCirrusObjects
                                , solution = &solution.
                                , authMethod = &authMethod.
                                , key = &data_definition_key.
                                , outds = _tmp_dd_
                                , outVarToken = &outVarToken.
                                , outSuccess = &outSuccess.
                                , outResponseStatus = &outResponseStatus.
                                , debug = &debug.
                                , logOptions = &logOptions.
                                , restartLUA = &restartLUA.
                                , clearCache = &clearCache.
                                );
                /* Check for errors */
                %if(not &&&outSuccess.. or not %rsk_dsexist(_tmp_dd_) or %rsk_attrn(_tmp_dd_, nobs) = 0) %then %do;
                    %put ERROR: Unable to get the dataCategory code information;
                    %abort;
                %end;
                %else %do;
                        /* Get the cycle name key */
                        data _null_;
                            set _tmp_dd_;
                            call symputx("dataCategoryCd", dataCategoryCd, "L");
                        run;
                        %if (%upcase("&dataCategoryCd.") ne "RESULTS") %then %do;
                            %put WARNING: To enable 'enableResultAttr' parameter the Data Definition category must be of type 'RESULTS'.;
                        %end;
                    %end;
            %end;
    %end; /* %if (%upcase("&enableResultAttr.") eq "Y" ) */

    /* get AR and Cycle info to feed extra columns if needed */
    %if (%upcase("&enableResultAttr.") eq "Y" and (%upcase("&dataCategoryCd.") eq "RESULTS")) %then %do;

        %if (%sysevalf(%superq(analysis_run_key) eq, boolean)) %then %do;
            %put ERROR: Parameter 'analysis_run_key' is required to enrich Results table;
            %return;
        %end;

        %if (%sysevalf(%superq(cycle_key) eq, boolean)) %then %do;
            %put ERROR: Parameter 'cycle_key' is required to enrich Results table;
            %return;
        %end;

        %core_rest_get_analysis_run( host = &host.
                                    , port = &port.
                                    , server = riskCirrusObjects
                                    , logonHost = &logonHost.
                                    , logonPort = &logonPort.
                                    , username = &username.
                                    , password = &password.
                                    , authMethod = &authMethod.
                                    , client_id = &client_id.
                                    , client_secret = &client_secret.
                                    , solution = &solution.
                                    , key = &analysis_run_key.
                                    , outds = _tmp_analysis_run_
                                    , outVarToken = &outVarToken.
                                    , outSuccess = &outSuccess.
                                    , outResponseStatus = &outResponseStatus.
                                    , debug = &debug.
                                    , logOptions = &logOptions.
                                    , restartLUA = &restartLUA.
                                    , clearCache = &clearCache.
                                    );
        /* Check for errors */
        %if( not &&&outSuccess.. or not %rsk_dsexist(_tmp_analysis_run_) or %rsk_attrn(_tmp_analysis_run_, nobs) = 0 ) %then %do;
            %put ERROR: Unable to get the analysis run information;
            %abort;
        %end;
        %else %do;
                /* Get the analysis run name key */
                data _null_;
                    set _tmp_analysis_run_;
                    call symputx("analysis_run_name", name, "L");
                    call symputx("analysis_run_object_id", objectId, "L");
                run;
            %end;

        %core_rest_get_cycle(server = riskCirrusObjects
                        , solution = &solution.
                        , authMethod = &authMethod.
                        , key = &cycle_key.
                        , outds = _tmp_cycle_
                        , outVarToken = &outVarToken.
                        , outSuccess = &outSuccess.
                        , outResponseStatus = &outResponseStatus.
                        , debug = &debug.
                        , logOptions = &logOptions.
                        , restartLUA = &restartLUA.
                        , clearCache = &clearCache.
                        );
        /* Check for errors */
        %if(not &&&outSuccess.. or not %rsk_dsexist(_tmp_cycle_) or %rsk_attrn(_tmp_cycle_, nobs) = 0) %then %do;
            %put ERROR: Unable to get the cycle information;
            %abort;
        %end;
        %else %do;
                /* Get the cycle name key */
                data _null_;
                    set _tmp_cycle_;
                    call symputx("cycle_name", name, "L");
                    call symputx("runTypeCd", runTypeCd, "L");
                run;
            %end;

    %end; /* %if (%upcase("&enableResultAttr.") eq "Y" and (%upcase("&dataCategoryCd.") eq "RESULTS")) */

    /* Create a CAS session if needed */
    %if (%sysevalf(%superq(casSessionName) ne, boolean)) %then %do;
        %if not %sysfunc(sessfound(&casSessionName.)) %then %do;
            /* start a cas session, if needed */
            %core_cas_initiate_session(cas_host = &casHost.
                , cas_port = &casPort.
                , cas_session_name = &casSessionName.
                , cas_session_options = casdatalimit=ALL
                , cas_assign_librefs = Y);

            %let marked_to_terminate = Y;
        %end;
    %end;

    /* Get part of UUId to use in potencial tables names concurrency */
    %let casSessionTag =;
    %if %symexist(_IOCASUUID_) %then %do;
        %if	(%sysevalf(%superq(_IOCASUUID_) ne, boolean)) %then %do;
            %let casSessionTag = %scan(&_IOCASUUID_., 1,'-');
        %end;
        %else %do;
                %let casSessionTag = %substr(%sysfunc(uuidgen()), 1, 7);
            %end;
    %end;
    %else %if %symexist(analysis_run_key) %then %do;
            %if	(%sysevalf(%superq(analysis_run_key) ne, boolean)) %then %do;
               %let casSessionTag = %substr(&analysis_run_key., 1, 7);
            %end;
        %end;
        %else %do;
               %let casSessionTag = %substr(%sysfunc(uuidgen()), 1, 7);
            %end;

    /* this macro will check if the input data for append has new columns that are not in the selected data definition */
    %macro check_new_vars_for_append(lib=, ds_input=, ds_data_definition_cols=);
        proc contents data=&lib.."&ds_input."n out=work._tmp_tab1_(keep=name) NOPRINT;
        run;

        data _null_;
            if _N_ = 1 then do;
            /* Create the scenario map lookup hash */
            /* WORK.DATA_DEFINITIONS_COLUMNS is coming from data definition rest call */
            declare hash verifyIfColumnsExist(dataset: "WORK.DATA_DEFINITIONS_COLUMNS");
            verifyIfColumnsExist.defineKey("name");
            verifyIfColumnsExist.defineDone();
        end;

        set work._tmp_tab1_;
            length list_of_vars $5000;
            retain list_of_vars "";

            _name_lower=upcase(name);
            _rc_not_there_ = verifyIfColumnsExist.find(key:_name_lower);

            /* Keep only the records that are not in hash table */
            if _rc_not_there_ ne 0 then do;
                list_of_vars = catx(" ",list_of_vars,name);
                call symputx("verifyIfColumnsExist",list_of_vars);
            end;
        run;
    %mend check_new_vars_for_append;

    /* get filename info about, locationType and location */
    %if ( %upcase("&locationType.") eq "LIBNAME" ) %then %do;
        /* assign a temporary libref to CAS due to 8 characters limitation - to allow execution of datastep in the script further ahead */
        %let location_tmp = &location.;
        %if ( %length(&location.) > 8 or %index(%superq(location),%str( )) > 0 or %sysfunc(findc(%superq(location),' ',kn)) > 0 ) %then %do; /* CAS Lib if location > 8 or < 8 but with spaces */
            libname tmpcas cas caslib="&location.";
            %let location_tmp = tmpcas;
        %end;

        %if ( not %rsk_dsexist(&location_tmp.."&filename."n) ) %then %do;
            %put ERROR: Filename &filename. is not available in this &location..;
            %abort;
        %end;

        %if ( %rsk_get_lib_engine(&location_tmp.) ne CAS ) %then %do; /* libname BASE v9 */
            %if ( %sysevalf(%superq(inputDataFilter) ne, boolean) or %upcase("&enableResultAttr.") eq "Y" ) %then %do;

                %check_new_vars_for_append(lib=WORK
                                        , ds_input=&filename.
                                        , ds_data_definition_cols=WORK.DATA_DEFINITIONS_COLUMNS
                                        );

                %if (%sysevalf(%superq(verifyIfColumnsExist) ne, boolean)) %then %do;
                    %put NOTE: Input table structure to APPEND has new columns comparing to target table. Columns will be removed.;
                %end;

                proc sql noprint;
                    create view WORK._view_riskdata_&casSessionTag. as
                    select *
                    %if ( %upcase("&enableResultAttr.") eq "Y" and (%upcase("&dataCategoryCd.") eq "RESULTS") ) %then %do;
                        , "&cycle_key." as cycle_id
                        , "&cycle_name." as cycle_name
                        , "&analysis_run_object_id." as analysis_run_id
                        , "&analysis_run_name." as analysis_run_name
                        , "&runTypeCd." as cycle_run_type length=100
                    %end;
                    from &location_tmp.."&filename."n
                    (drop=cycle_id cycle_name analysis_run_id analysis_run_name cycle_run_type
                    /* drop new append columns that are not in base table*/
                    %if (%sysevalf(%superq(verifyIfColumnsExist) ne, boolean)) %then %do;
                        &verifyIfColumnsExist.
                    %end;
                    )
                    %if (%sysevalf(%superq(inputDataFilter) ne, boolean)) %then %do;
                        where &inputDataFilter.
                    %end;
                    using libname _tmp_ "%sysfunc(pathname(&location_tmp.))";
                quit;

                %if ( %rsk_attrn(WORK."_view_riskdata_&casSessionTag."n, nobs) = 0) %then %do;
                    %put WARNING: No data retrieved from &location_tmp..;
                %end;

                %let locationType = &locationType.;
                %let location = WORK;
                %let filename = _view_riskdata_&casSessionTag.;
            %end;
            %let marked_to_delete = Y;
            %let cas_libname = N;
        %end; /* %if ( %rsk_get_lib_engine(&location_tmp.) ne CAS ) */
        %else %do; /* libname CAS */
                %if ( %sysevalf(%superq(inputDataFilter) ne, boolean) or %upcase("&enableResultAttr.") eq "Y" ) %then %do;
                    %core_cas_drop_table(cas_session_name = &casSessionName.
                    , cas_libref = CASUSER
                    , cas_table = append_cas_riskdata_&casSessionTag.
                    , delete_table = Y
                    , delete_table_options = quiet=TRUE
                    , verify_table_deleted = Y
                    , delete_source = N
                    );

                    %check_new_vars_for_append(lib=&location_tmp.
                                            , ds_input=&filename.
                                            , ds_data_definition_cols=WORK.DATA_DEFINITIONS_COLUMNS
                                            );

                    %if (%sysevalf(%superq(verifyIfColumnsExist) ne, boolean)) %then %do;
                        %put NOTE: Input table structure to APPEND has new columns comparing to target table. Columns will be removed.;
                    %end;

                    /* subset the input data source and upload */
                    data CASUSER.APPEND_CAS_RISKDATA_&casSessionTag. (promote=yes);
                        set &location_tmp.."&filename."n
                        /* drop new append columns that are not in base table*/
                        %if (%sysevalf(%superq(verifyIfColumnsExist) ne, boolean)) %then %do;
                            (drop=&verifyIfColumnsExist.)
                        %end;
                        ;
                        %if ( %upcase("&enableResultAttr.") eq "Y" and (%upcase("&dataCategoryCd.") eq "RESULTS") ) %then %do;
                            CYCLE_ID = "&cycle_key.";
                            CYCLE_NAME = "&cycle_name.";
                            ANALYSIS_RUN_ID = "&analysis_run_object_id.";
                            ANALYSIS_RUN_NAME = "&analysis_run_name.";
                            CYCLE_RUN_TYPE = "&runTypeCd.";
                        %end;
                        %if ( %sysevalf(%superq(inputDataFilter) ne, boolean) ) %then %do;
                            where &inputDataFilter.;
                        %end;
                    run;

                    %let locationType = &locationType.;
                    %let location = CASUSER;
                    %let filename = append_cas_riskdata_&casSessionTag.;
                    %let marked_to_delete = Y;
                %end;
            %end; /*%else ( %rsk_get_lib_engine(&location_tmp.) eq CAS ) */
    %end; /*%upcase("&locationType.") eq "LIBNAME" ) */

    %if ( %upcase("&locationType.") in ("DIRECTORY") ) %then %do;
        %if (%sysevalf(%superq(inputDataFilter) ne, boolean) or %upcase("&enableResultAttr.") eq "Y") %then %do;
            /* turn into libname|table in order to enable filters over input data */
            %rsk_libname(STMT=libname _tmp_ base "&location.");

            %check_new_vars_for_append(lib=_tmp_
                                    , ds_input=&filename.
                                    , ds_data_definition_cols=WORK.DATA_DEFINITIONS_COLUMNS
                                    );

            %if (%sysevalf(%superq(verifyIfColumnsExist) ne, boolean)) %then %do;
                %put NOTE: Input table structure to APPEND has new columns comparing to target table. Columns will be removed.;
            %end;

            proc sql noprint;
                create view _tmp_._view_riskdata_&casSessionTag. as
                select *
                %if (%upcase("&enableResultAttr.") eq "Y" and (%upcase("&dataCategoryCd.") eq "RESULTS")) %then %do;
                    , "&cycle_key." as cycle_id
                    , "&cycle_name." as cycle_name
                    , "&analysis_run_object_id." as analysis_run_id
                    , "&analysis_run_name." as analysis_run_name
                    , "&runTypeCd." as cycle_run_type length=100
                %end;
                from _tmp_.&filename.
                (drop=cycle_id cycle_name analysis_run_id analysis_run_name cycle_run_type
                /* drop new append columns that are not in base table*/
                %if (%sysevalf(%superq(verifyIfColumnsExist) ne, boolean)) %then %do;
                    &verifyIfColumnsExist.
                %end;
                )
                %if (%sysevalf(%superq(inputDataFilter) ne, boolean)) %then %do;
                    where (&inputDataFilter.)
                %end;
                using libname _tmp_ "&location.";
            quit;

            %if ( %rsk_attrn(_tmp_."_view_riskdata_&casSessionTag."n, nobs) = 0) %then %do;
                %put WARNING: No data retrieved from &location_tmp..;
            %end;

            %let filename = _view_riskdata_&casSessionTag.;
        %end;
        %let cas_libname = N;
        %let marked_to_delete = Y;
    %end;

    %let session = %nrstr(&)sessionId=&_IOCASUUID_.;
    %if (&cas_libname. eq N) %then %do;
        %let session =;
        %if ( %upcase("&locationType.") ne "DIRECTORY" ) %then
            %let session = %nrstr(&)computeSessionId=&SYS_COMPUTE_SESSION_ID.;
    %end;

     /* URL encoded to REST request */
    %let location=%sysfunc(urlencode(%bquote(&location.)));
    %let fileName=%sysfunc(urlencode(%bquote(&fileName.)));
    %core_set_base_url(host=&host, server=&server., port=&port.);
    %let requestUrl = &baseUrl./&server./objects/&key./data?locationType=&locationType.%nrstr(&)location=&location.%nrstr(&)fileName=&fileName.;
    %let requestUrl = &requestUrl.&session.;

    filename _resp temp;

    /* Temporary disable mlogic and symbolgen options to avoid printing of userid/pwd to the log */
    option nomlogic nosymbolgen;
    /* Send the REST request */
    %core_rest_request(url = &requestUrl.
                        , method = PUT
                        , logonHost = &logonHost.
                        , logonPort = &logonPort.
                        , username = &username.
                        , password = &password.
                        , authMethod = &authMethod.
                        , headerIn = Accept:application/json
                        , contentType = application/json
                        , fout = _resp
                        , outds = rest_request_put_response
                        , outVarToken = &outVarToken.
                        , outSuccess = &outSuccess.
                        , outResponseStatus = &outResponseStatus.
                        , debug = &debug.
                        , logOptions = &oldLogOptions.
                        , restartLUA = &restartLUA.
                        , clearCache = &clearCache.
                        );

    libname _resp json fileref=_resp noalldata;

    /* Exit in case of errors */
    %if( (not &&&outSuccess..) or not(%rsk_dsexist(rest_request_put_response)) or %rsk_attrn(rest_request_put_response, nobs) = 0 ) %then %do;
        %put ERROR: Unable to accomplish the service: Append Analysis Data;
        data _null_;
            set _resp.root(keep=message);
            call symputx("resp_message",message);
        run;
        %put ERROR: &resp_message.;

        %GOTO FINALSTEPS;

    %end; /* (not &&&outSuccess..) */

    /* Get Job Id for monitoring the execution status */
    data _null_;
        set _RESP.root(keep=appendJobId);
        call symputx("appendJobId",appendJobId);
    run;

    %let jobState=;
    %core_rest_wait_job_execution(jobID = &appendJobId.
                                , wait_flg = Y
                                , pollInterval = 1
                                , maxWait = 3600
                                , timeoutSeverity = ERROR
                                , outJobStatus = jobState
                                , outVarToken = accessToken
                                , outSuccess = httpSuccess
                                , outResponseStatus = responseStatus
                                , debug = false
                                );

    %if (not &&&outSuccess..) %then
        %PUT ERROR: Could not get the status of the job execution process (&appendJobId.);

    %if "&jobState." ne "COMPLETED" or (not &&&outSuccess..) %then %do;
        %PUT ERROR: The risk data process to append the data completed with errors;
        %let &outSuccess. = 0;
        %GOTO FINALSTEPS;
    %end;

    %FINALSTEPS:

    /* Delete the temp data for filtering */
    %if ( "&marked_to_delete." eq "Y" ) %then %do;
        %if ( %upcase("&locationType.") eq "LIBNAME" ) %then %do;
            %if ( "&cas_libname." = "N" and  ) %then %do;
                %rsk_delete_ds(work.&filename.);
            %end;
            %else %do;
                    %core_cas_drop_table(cas_session_name = &casSessionName.
                                        , cas_libref = CASUSER
                                        , cas_table = &filename.
                                        );
                %end;
        %end;
        %else %do;
                %rsk_delete_ds(_tmp_.&filename.);
            %end;
    %end;

    /* Terminate the cas session if we created it */
    %if (&marked_to_terminate. eq Y) %then
            *%core_cas_terminate_session(cas_session_name = &casSessionName.);

    filename _resp clear;
    libname _resp clear;

%mend core_rest_append_analysisdata;