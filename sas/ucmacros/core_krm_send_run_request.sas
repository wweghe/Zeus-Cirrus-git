/*
 Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/**
    \file
    \anchor core_krm_send_run_request

    \brief   This macro generates and sends the necessary HTTP request for starting a krm run

    \param [in] current_dt Current Date corresponds to the day of the execution
    \param [in] cur_dt Current Date, corresponds to the field CUR_DT of the RUN_PARAM table
    \param [in] market_dt Latest data dates from KRM market tables
    \param [in] val_dt Valuation Date
    \param [in] calc_type Calculation Type: 1 Market Value, 2 Multi Period Forecast, 3 Break Cost, 4 Transfer Pricing, 5 Option Adjusted Spread and 6 Fair Value Pricing
    \param [in] risk_method Method for Capturing Risk: 0 Default no risk, 1 Instantaneous Shifts, 2 Multi Period Shifts, 3 Stress Test, 4 Date Based Interest Rate Shift, 5 Valuation Date Shift
    \param [in] sim_method Simulation Method: 1 Matrix VAR, 2 Monte Carlo, 3 Historical Values, 4 Historical Simulation (sequential), 5 Historical Simulation (Random), 6 Regulatory Market Risk
    \param [in] prod_set_id Product Set Id
    \param [in] analysis_data_id Portfolio ADO Key
    \param [in] base_currency Base Currency
    \param [in] reg_id Regulator ID
    \param [in] cur_dt2 iF calc_type = 1 second date for analysis, if calc_type = 4 Port Table date for overrides.
    \param [in] und_port_flag Select source(s) of information (i.e., product characteristics) for underlying securities and collateral: 1 = Use UND_PORT, 2 = Use UND_PORT as well as PORT defined for the Run ID 
    \param [in] fltr_id Filter ID applied to PORT table records
    \param [in] yc_set_id Yield Curve Set ID
    \param [in] rpt_set_id Reporting Period Set ID (primarily for Multi-period forecasts)
    \param [in] shift_set_id Risk factor shift point ID (linked to DATE_SHIFT.SHIFT_SET_ID). Applicable to CALC_TYPE = 1,2 and RISK_METHOD = 2
    \param [in] mc_set_id Monte Carlo Set ID
    \param [in] st_set_id Stress Test Set ID
    \param [in] collat_id Collateral ID used for securitized assets
    \param [in] hdg_set_id Hedge Set ID
    \param [in] div_id Dividend Payout ID (used for Autobalancing)
    \param [in] sector_corr_id Sector Correlation ID (REF_NAME_CHAR.CATEGORY_VAL)
    \param [in] src_run_id Results from this Source Run ID will be used as input for one of the other calculations
    \param [in] tp_run_id Reference Run ID in Forecasting:  used to retrieve calculated TP from TP_RT_TXN.
    \param [in] neg_rt_flag Flag for allowing negative rates: 0 No Negative Interest Rates, 1 Allow Negative Interest Rates.
    \param [in] duration_flag Parameters for Effective Duration calculation: 0 = Do not Calculate  (default), 1 = Use Yield Shift,  2 = Use Par Coupon Shift
    \param [in] duration_shift Shift amount used for duration and convexity (when DURATION_FLAG = 1,2), in percentage points
    \param [in] cut_flag Use Cuts for calculations: 0 = (default) do not use CUT files (All transactions will be reported as CUT #1), 1 = Use CUT files (requires CUT_SET_ID)
    \param [in] cut_set_id Cut ID
    \param [in] hier_id (NOT IMPLEMENTED)  Hierarchy ID
    \param [in] cr_flag Default risk flag for stochastic processes and/or default-based valuation method
    \param [in] cr_model_id Credit Model ID
    \param [in] dflt_ref_flag Reference Entity default flag: 0 = Reference Entity cannot default, 1 = Reference Entity can default  [default selection]
    \param [in] dflt_issuer_flag Issuer default flag: 0 = Issuer cannot default, 1 = Issuer Entity can default  [default selection]
    \param [in] dflt_cp_flag Counterparty default flag: 0 = Counterparty cannot default, 1 = Counterparty can default  [default selection]
    \param [in] dflt_self_flag NOT IMPLEMENTED
    \param [in] tp_mast_id TP Master ID to define "alternative" assumptions used for Transfer Pricing (CALC_TYPE=4 or CALC_TYPE=2 with forecasted TP)
    \param [in] tp_hist_dt Historical reference date used for Transfer Pricing (CALC_TYPE=4) to determine which records to process (i.e., PORT.MAT_DT must be > TP_HIST_DT, even if TP_HIST_DT < Current date for the Run)
    \param [in] fcst_param_id Multi-period Forecast ID to determine how certain forecast measures should be calculated
    \param [in] fcst_type_id Multi-period Forecast ID to determine which calculations should be included in the forecast
    \param [in] fcst_ph_flag Flag for portfolio balances in the forecast: 0 = Do not include portfolio holdings, 1 = Include portfolio holdings (default)
    \param [in] fcst_rl_flag Flag for Rollover balances in the forecast: 0 = Do not include Rollovers  (default), 1 = Include Rollovers
    \param [in] fcst_nb_flag Flag for New Business balances in the forecast: 0 = Do not include New Business  (default), 1 = Include New Business
    \param [in] grp_mast_id (NOT IMPLEMENTED)  Group Master ID
    \param [in] fcst_mast_id Forecasting Master ID
    \param [in] fcst_limit_id Forecasting forecast Limit ID
    \param [in] rebal_set_id Rebalancing Set ID
    \param [in] fcst_cpn_id Coupon ID (for rollover, new business, autobalancing)
    \param [in] non_int_id Non Interest Set ID
    \param [in] val_dt2 Specify dates for Rollover or Theta analysis: Rollover Horizon for Forecasts, Second Valuation Date for Theta analysis  
    \param [in] fcst_buffer Number of portfolio records to be buffered in memory (applicable only for Stress Test Forecasts, Stochastic Forecasts, and  Stochastic Instantaneous Shock Forecasts): 0 or NULL = records are not buffered (default setting), -1 = use all records in PORT (up to 50,000 records in 32-bit environment), 1 - NNNNN = user input value
    \param [in] buffer_id When FCST_BUFFER = 1, used by Forecasts to identify TXN_IDs to be buffered for forecasts.  Corresponds to FLTR_DEF.FLTR_ID 
    \param [in] base_calc_flag Base Case reporting in Forecasts: 0 = Calculate and report base case results  (default),  1 = Do not calculate/report base case results 
    \param [in] yc_flag Yield Curve shift in Forecasts for base case and non-base for yield curves that are not risk factors: 0 = Use implied forward rates from Current yield curve for all future Periods (default), 1 = Use the Current yield curve for all future Periods (horizontal shift)  
    \param [in] yc_fltr_id Yield Curve Filter ID
    \param [in] risk_set_id Risk Factor Set ID
    \param [in] risk_const_flag Risk Factor flag:  0 = All Risk Factor shifts, 1 = Constant Risk Factors. Applies to the folllowing calculation:  CALC_TYPE=2, RISK_METHOD = 1,2  
    \param [in] risk_grp_flag Define whether or not KRM should report results by RISK_GRP (as defined in RISK_DEF table): 0 = Do not report by RISK_GRP, 1 = Report by RISK_GRP  (default) 
    \param [in] risk_measure Define type of risks to be measured and reported for VAR calculations: 0 = All risk factors  (default), 1 = Ignore all risk factors and simulate only time-to-default, 2 = All risk factors, isolate and report default risk (RISK_GRP = DEFAULT), 3 = For various VAR measures (all relevant output tables), calculate contribution of each RISK_GRP<>ALL (including DEFAULT) to RISK_GRP = ALL  [based on market value correlations among RISK_GRPs], 4 = Same as RISK_MEASURE = 3 except for output in VAR_SUPP_OUT:  for each RISK_GRP (including ALL and DEFAULT), calculate contribution of each TXN_ID to COMP_VAR and COMP_ES [based on VAR_CUT_OUT and market value correlations among TXN_IDs] 
    \param [in] risk_threshold Define how risk factor thresholds (if specified in RISK_SET) should be applied:  0 = (NOT IMPLEMENTED) Ignore all thresholds 1 = Apply all min (RISK_MIN) and max (RISK_MAX) thresholds  (default)
    \param [in] var_mv_flag Determine date when market values are calculated for VAR: 0 = Use market value at valuation date  (default),  1 = Use market value at horizon date 
    \param [in] comp_var_method Method for calculating Component VAR (SIM_METHOD = 2,4,5): 1 = Calculate correlations based on all scenarios  (default), 2 = Calculate correlations for scenarios within the range of CONF_LEVEL_L and CONF_LEVEL_U,  3 = Use ratios of market-value averages for scenarios within the range of CONF_LEVEL_L and CONF_LEVEL_U
    \param [in] var_conf_level VAR, Stochastic Forecasts, Date-based Stress Test (MV): Confidence Level in %
    \param [in] var_conf_level_u If COMP_VAR_METHOD=2 or 3: Upper Confidence Level: Works in conjunction with CONF_LEVEL_L to identify scenarios in that band for VAR-related calculations
    \param [in] var_conf_level_l If COMP_VAR_METHOD=2 or 3: Lower Confidence Level: Works in conjunction with CONF_LEVEL_U to identify scenarios in that band for VAR-related calculations
    \param [in] risk_conf_level (NOT IMPLEMENTED)  Confidence level in % to be used for the risk factor tests
    \param [in] num_scenario VAR, Stochastic Forecasts, and distributed mode Date-based Stress Forecasts: Number of scenario 
    \param [in] scenario_step For stochastic processes on a single server, subdivide total number of scenarios (NUM_SCENARIO) into incremental processes with a subset of scenarios, each = SCENARIO_STEP.
    \param [in] mc_style Flag to determine how KRM should draw risk factor values for random Monte Carlo scenarios: 0 = Single random draw for each row in RISK_VAL_OUT (default), 1 = Multiple random draws for each row in RISK_VAL_OUT
    \param [in] hist_start_dt Historical simulation start date
    \param [in] hist_end_dt Historical simulation end date
    \param [in] hist_data_term Historical data retrieval range for market data (equity, FX, rates, PD, etc.), and TP spread data. NULL = All historic data loaded, 0 = No historic data retrieval    
    \param [in] hist_data_term_u Unit for HIST_DATA_TERM: D = Day, W = Week, M = Month, Q = Quarter, S = Semiannual, Y = Year 
    \param [in] risk_shift_term Specify various types of Intervals: - VAR - Matrix: Holding Period - VAR or Stochastic Forecasts (Instantaneous Shock) using Monte Carlo: Holding Period - VAR or Stochastic Forecasts (Instantaneous Shock) using Historical simulations: Interval of the historical simulation -  VaR Historical Absolute: Holding Period between dates used in Historical valuations (default: 1D) - Correlated Stress Test (ST_MAST.RISK_SHIFT_TYPE=2): Shift term
    \param [in] risk_shift_term_u Unit for RISK_SHIFT_TER: D = Day, W = Week, M = Month, Q = Quarter, S = Semiannual, Y = Year 
    \param [in] cr_horizon_term Specify the length of term during which external credit risk events (i.e., defaults) can occur.  Applicable for - CR_FLAG = 1 with PROD_DEF.CR_DFLT_TYPE = 11 - CR_FLAG > 1   
    \param [in] cr_horizon_term_u Unit for CR_HORIZON_TERM: D = Day, W = Week, M = Month, Q = Quarter, S = Semiannual, Y = Year 
    \param [in] hist_skip For stochastic analysis based on sequential historical returns (SIM_METHOD=4):  The number of RISK_RETURN / EQT_RETURN records to be skipped between scenarios
    \param [in] num_trials Monte Carlo (SIM_METHOD=2) VaR and Stochastic Forecasts: The number of times that KRM should re-generate the Monte Carlo scenarios specified in NUM_SCENARIO.  These results will be used for the VAR standard deviation calculation. Accounting-based or Time-based Stress Forecasts with CR_FLAG=(1, 2, or 3):  Number of Default scenarios
    \param [in] var_calc_src Define data source to be used for calculating VAR: 0 = From Memory  (default), 1 = From Tables
    \param [in] dist_run_flag Type of distributed process. 1 = Local processing, 2 = Distributed Sub process, 3 = Distributed Master process, 4 = Consolidation only, 5 = Distributed Master process that waits for completion of all sub-processes to consolidate results. 6 = Stand-alone Consolidator (reads the outputs of master run MV_RUN_ID)
    \param [in] reg_calc_id Regulatory Calculation ID
    \param [in] num_threads Number of Threads to be used
    \param [in] num_db_ins Number of threads used for bulk insert
    \param [in] msg_error_flag Flag indicating whether Error messages should be reported to MSG_MAST_RUN: 0 = Do not Report Messages, 1 = Report Messages  (default)
    \param [in] msg_warn_flag Flag indicating whether Warning messages should be reported to MSG_MAST_RUN: 0 = Do not Report Messages, 1 = Report Messages  (default)
    \param [in] msg_info_flag Flag indicating whether Informational messages should be reported to MSG_MAST_RUN: 0 = Do not Report Messages, 1 = Report Messages  (default)
    \param [in] msg_diag_flag Flag indicating whether Diagnostic messages should be reported to MSG_MAST_RUN: 0 = Do not Report Messages,  1 = Report Messages  (default)
    \param [in] msg_sql_flag Flag indicating whether failed Database SQL commands should be reported to MSG_DETL_RUN: 0 = Do not Report Messages  (default), 1 = Report Messages
    \param [in] template_run_id run_id of an existing template run in the RUN_PARAM table.
    \param [in] b2_port ADO Key for the B2_PORTFOLIO
    \param [in] b3_port ADO Key for the B3_PORTFOLIO
    \param [in] port_table Name of an already existing portfolio table, in the krm db, to use for the run
    \param [in] run_status Run Status defaults to 0 when not specified
    \param [in] run_id Run Id used for identfying the run
    \param [in] analysis_id The identifier of the analysis run object that this run wraps. This is a system assigned identifier of the analysis run object.
    \param [in] keep_Krm_run Flag to indicate if the KRM Run and its outputs should be kept after is finishes executing. When not specified, defaults to false.
    \param [in] discard_KrmADO_output_tables Flag to indicate if the KRM output tables should be discarded. If this is set to true, the KRM output tables will not be persisted as Cirrus analysis data objects. When not specified, defaults to false.
    \param [in] reports_ds Post-execution report scripts for this run
    \param [in] host (Optional) Host url, including the protocol.
    \param [in] port (Optional) Server port.
    \param [in] server Name that provides the REST service (Default: riskData).
    \param [in] debug True/False. If True, debugging informations are printed to the log (Default: false).
    \param [in] logonHost (Optional) Host/IP of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app host/ip is the same as the host/ip in the url parameter.
    \param [in] logonPort (Optional) Port of the sas-logon-app service or ingress.  If blank, it is assumed that the sas-logon-app port is the same as the port in the url parameter.
    \param [in] username (Optional) Username credentials.
    \param [in] password (Optional) Password credentials: it can be plain text or SAS-Encoded (it will be masked during execution).
    \param [in] authMethod (optional): Authentication method (accepted values: BEARER). (Default: BEARER).
    \param [in] client_id (optional) The client id registered with the Viya authentication server. If blank, the internal SAS client id is used (only if GRANT_TYPE = password).
    \param [in] client_secret (optional) The secret associated with the client id.
    \param [in] logOptions (optional) Logging options (i.e. mprint mlogic symbolgen ...).
    \param [in] restartLUA (optional). Flag (Y/N). Resets the state of Lua code submission for a SAS session if set to Y (Default: Y)
    \param [in] clearCache (optional) Flag (Y/N). Controls whether the connection cache is cleared across multiple proc http calls. (Default: Y)
    \param [in] solution (optional) Solution identifier (Source system code) for Cirrus Core content packages (Default: currently blank)
    \param [in] debug (optional) True/False. If True, debugging informations are printed to the log (Default: false).
    \param [out] outVarToken (optional) Name of the output macro variable which will contain the access token (Default: accessToken).
    \param [out] outSuccess (optional) Name of the output macro variable that indicates if the request was successful (&outSuccess = 1) or not (&outSuccess = 0). (Default: httpSuccess).
    \param [out] outResponseStatus (optional) Name of the output macro variable containing the HTTP response header status: i.e. HTTP/1.1 200 OK. (Default: responseStatus).


    \details
        This macro generates and sends the necessary HTTP request for starting a krm run. For examples check the scripts: IRRBB and Template-ALM-Calculation in the risk-cirrus-alm repository.
    <b>Example:</b>

    1) Example using an already existing run for template-runs. 
    \code
        %let cur_dt = 2021-11-08T00:00:00Z;
        %let calc_type = 1;
        %let risk_method = 1;
        %let val_dt = 2021-11-08T00:00:00Z;
        %let data_dt = 2021-11-08T00:00:00Z;
        %let base_currency = USD;
        %let prod_set_id = SEG;
        %let analysis_data_id = c20ee7f9-8a33-4416-8189-fb90ec9bb18d; 
        %let fltr_id = ;
        %let cur_dt = 2021-11-08T00:00:00Z;
        %let template_run_id = run11;
        %let b2_port = ;
        %let b3_port = ;
        %let run_id = run12;
        %let analysis_id = 16372757e-011d-454d-905b-eb10b4a9e5a9; 
        %let keepKrmRun = true;
        %let discard_KrmADO_output_tables = true;
        %let accessToken = ;
        %let httpSuccess = ;
        %let responseStatus = ;
        ${function:GetSASDSCode(AnalysisRun.customFields.scriptParameters.reportSelector.data, 'reports_ds')} 

        %core_krm_send_run_request(current_dt = &cur_dt.
                                , calc_type = &calc_type.
                                , risk_method = &risk_method.
                                , val_dt = &val_dt.
                                , market_dt = &data_dt.
                                , base_currency = &base_currency.
                                , prod_set_id = &prod_set_id.
                                , analysis_data_id = &analysis_data_id.
                                , fltr_id = &fltr_id.
                                , cur_dt = &cur_dt.
                                , template_run_id = &template_run_id.
                                , b2_port = &b2_port.
                                , b3_port = &b3_port.
                                , run_id = &run_id.
                                , analysis_id = &analysis_id.
                                , keep_Krm_run = &keepKrmRun.
                                , discard_KrmADO_output_tables = &discard_KrmADO_output_tables.
                                , reports_ds = &reports_ds.
                                , solution = ALM
                                );  
    \endcode

    \ingroup coreRestUtils

    \author  SAS Institute Inc.
    \date    2023
*/


%macro core_krm_send_run_request(current_dt = 
                            , cur_dt = 
                            , market_dt = 
                            , val_dt = 
                            , calc_type = 
                            , risk_method = 
                            , sim_method = 
                            , prod_set_id = 
                            , analysis_data_id = 
                            , base_currency = 
                            , reg_id = 
                            , cur_dt2 = 
                            , und_port_flag = 
                            , fltr_id = 
                            , yc_set_id = 
                            , rpt_set_id = 
                            , shift_set_id = 
                            , mc_set_id = 
                            , st_set_id = 
                            , collat_id = 
                            , hdg_set_id = 
                            , div_id = 
                            , sector_corr_id = 
                            , src_run_id = 
                            , tp_run_id = 
                            , neg_rt_flag = 
                            , duration_flag = 
                            , duration_shift = 
                            , cut_flag = 
                            , cut_set_id = 
                            , hier_id = 
                            , cr_flag = 
                            , cr_model_id = 
                            , dflt_ref_flag = 
                            , dflt_issuer_flag = 
                            , dflt_cp_flag = 
                            , dflt_self_flag = 
                            , tp_mast_id = 
                            , tp_hist_dt = 
                            , fcst_param_id = 
                            , fcst_type_id = 
                            , fcst_ph_flag = 
                            , fcst_rl_flag = 
                            , fcst_nb_flag = 
                            , grp_mast_id = 
                            , fcst_mast_id = 
                            , fcst_limit_id = 
                            , rebal_set_id = 
                            , fcst_cpn_id = 
                            , non_int_id = 
                            , val_dt2 = 
                            , fcst_buffer = 
                            , buffer_id = 
                            , base_calc_flag = 
                            , yc_flag = 
                            , yc_fltr_id = 
                            , risk_set_id = 
                            , risk_const_flag = 
                            , risk_grp_flag = 
                            , risk_measure = 
                            , risk_threshold = 
                            , var_mv_flag = 
                            , comp_var_method = 
                            , var_conf_level = 
                            , var_conf_level_u = 
                            , var_conf_level_l = 
                            , risk_conf_level = 
                            , num_scenario = 
                            , scenario_step = 
                            , mc_style = 
                            , hist_start_dt = 
                            , hist_end_dt = 
                            , hist_data_term = 
                            , hist_data_term_u = 
                            , risk_shift_term = 
                            , risk_shift_term_u = 
                            , cr_horizon_term = 
                            , cr_horizon_term_u = 
                            , hist_skip = 
                            , num_trials = 
                            , var_calc_src = 
                            , dist_run_flag = 
                            , reg_calc_id = 
                            , num_threads = 
                            , num_db_ins = 
                            , msg_error_flag = 
                            , msg_warn_flag = 
                            , msg_info_flag = 
                            , msg_diag_flag = 
                            , msg_sql_flag = 
                            , template_run_id = 
                            , b2_port = 
                            , b3_port = 
                            , port_table = 
                            , run_status = 
                            , run_id =
                            , analysis_id =
                            , keep_Krm_run =
                            , discard_KrmADO_output_tables =
                            , reports_ds = 
                            , host = 
                            , port =
                            , server = 
                            , logonHost =
                            , logonPort =
                            , username = 
                            , password =
                            , authMethod = bearer
                            , client_id =
                            , client_secret = 
                            , logOptions =
                            , restartLUA = Y
                            , clearCache = Y
                            , solution =
                            , debug = true
                            , outVarToken = accessToken
                            , outSuccess = httpSuccess
                            , outResponseStatus = responseStatus
                            );
    filename request temp;
    /* create the request json */
    proc json out=request pretty nosastags;
        write open object;
        %nobstr(key=currentDate, val=&current_dt);
        %nobstr(key=curDt, val=&cur_dt);
        %nobstr(key=marketDate, val=&market_dt);
        %nobstr(key=valuationDate, val=&val_dt);
        %nobnum(key=calculationType, val=&calc_type);
        %nobnum(key=riskMethod, val=&risk_method);
        %nobnum(key=simMethod, val=&sim_method);
        %nobstr(key=productSet, val=&prod_set_id);
        %nobstr(key=analysisDataId, val=&analysis_data_id);
        %nobstr(key=baseCurrency, val=&base_currency);
        %nobstr(key=regId, val=&reg_id);
        %nobstr(key=curDt2, val=&cur_dt2);
        %nobnum(key=undPortFlag, val=&und_port_flag);
        %nobstr(key=fltrId, val=&fltr_id);
        %nobstr(key=ycSetId, val=&yc_set_id);
        %nobstr(key=rptSetId, val=&rpt_set_id);
        %nobstr(key=shiftSetId, val=&shift_set_id);
        %nobstr(key=mcSetId, val=&mc_set_id);
        %nobstr(key=stSetId, val=&st_set_id);
        %nobstr(key=collatId, val=&collat_id);
        %nobstr(key=hdgSetId, val=&hdg_set_id);
        %nobstr(key=divId, val=&div_id);
        %nobstr(key=sectorCorrId, val=&sector_corr_id);
        %nobstr(key=srcRunId, val=&src_run_id);
        %nobstr(key=tpRunId, val=&tp_run_id);
        %nobnum(key=negRtFlag, val=&neg_rt_flag);
        %nobnum(key=durationFlag, val=&duration_flag);
        %nobnum(key=durationShift, val=&duration_shift);
        %nobnum(key=cutFlag, val=&cut_flag);
        %nobstr(key=cutSetId, val=&cut_set_id);
        %nobnum(key=hierId, val=&hier_id);
        %nobnum(key=crFlag, val=&cr_flag);
        %nobstr(key=crModelId, val=&cr_model_id);
        %nobnum(key=dfltRefFlag, val=&dflt_ref_flag);
        %nobnum(key=dfltIssuerFlag, val=&dflt_issuer_flag);
        %nobnum(key=dfltCpFlag, val=&dflt_cp_flag);
        %nobnum(key=dfltSelfFlag, val=&dflt_self_flag);
        %nobstr(key=tpMastId, val=&tp_mast_id);
        %nobstr(key=tpHistDt, val=&tp_hist_dt);
        %nobstr(key=fcstParamId, val=&fcst_param_id);
        %nobstr(key=fcstTypeId, val=&fcst_type_id);
        %nobnum(key=fcstPhFlag, val=&fcst_ph_flag);
        %nobnum(key=fcstRlFlag, val=&fcst_rl_flag);
        %nobnum(key=fcstNbFlag, val=&fcst_nb_flag);
        %nobstr(key=grpMastId, val=&grp_mast_id);
        %nobstr(key=fcstMastId, val=&fcst_mast_id);
        %nobstr(key=fcstLimitId, val=&fcst_limit_id);
        %nobstr(key=rebalSetId, val=&rebal_set_id);
        %nobstr(key=fcstCpnId, val=&fcst_cpn_id);
        %nobstr(key=nonIntId, val=&non_int_id);
        %nobstr(key=valDt2, val=&val_dt2);
        %nobnum(key=fcstBuffer, val=&fcst_buffer);
        %nobstr(key=bufferId, val=&buffer_id);
        %nobnum(key=baseCalcFlag, val=&base_calc_flag);
        %nobnum(key=ycFlag, val=&yc_flag);
        %nobstr(key=ycFltrId, val=&yc_fltr_id);
        %nobstr(key=riskSetId, val=&risk_set_id);
        %nobnum(key=riskConstFlag, val=&risk_const_flag);
        %nobnum(key=riskGrpFlag, val=&risk_grp_flag);
        %nobnum(key=riskMeasure, val=&risk_measure);
        %nobnum(key=riskThreshold, val=&risk_threshold);
        %nobnum(key=varMvFlag, val=&var_mv_flag);
        %nobnum(key=compVarMethod, val=&comp_var_method);
        %nobnum(key=varConfLevel, val=&var_conf_level);
        %nobnum(key=varConfLevelU, val=&var_conf_level_u);
        %nobnum(key=varConfLevelL, val=&var_conf_level_l);
        %nobnum(key=riskConfLevel, val=&risk_conf_level);
        %nobnum(key=numScenario, val=&num_scenario);
        %nobnum(key=scenarioStep, val=&scenario_step);
        %nobnum(key=mcStyle, val=&mc_style);
        %nobstr(key=histStartDt, val=&hist_start_dt);
        %nobstr(key=histEndDt, val=&hist_end_dt);
        %nobnum(key=histDataTerm, val=&hist_data_term);
        %nobstr(key=histDataTermU, val=&hist_data_term_u);
        %nobnum(key=riskShiftTerm, val=&risk_shift_term);
        %nobstr(key=riskShiftTermU, val=&risk_shift_term_u);
        %nobnum(key=crHorizonTerm, val=&cr_horizon_term);
        %nobstr(key=crHorizonTermU, val=&cr_horizon_term_u);
        %nobnum(key=histSkip, val=&hist_skip);
        %nobnum(key=numTrials, val=&num_trials);
        %nobnum(key=varCalcSrc, val=&var_calc_src);
        %nobnum(key=distRunFlag, val=&dist_run_flag);
        %nobstr(key=regCalcId, val=&reg_calc_id);
        %nobnum(key=numThreads, val=&num_threads);
        %nobnum(key=numDbIns, val=&num_db_ins);
        %nobnum(key=msgErrorFlag, val=&msg_error_flag);
        %nobnum(key=msgWarnFlag, val=&msg_warn_flag);
        %nobnum(key=msgInfoFlag, val=&msg_info_flag);
        %nobnum(key=msgDiagFlag, val=&msg_diag_flag);
        %nobnum(key=msgSqlFlag, val=&msg_sql_flag);
        %nobstr(key=runIdTemplate, val=&template_run_id);
        %nobstr(key=b2AnalysisDataId, val=&b2_port);
        %nobstr(key=b3AnalysisDataId, val=&b3_port);
        %nobstr(key=portTable, val=&port_table);
        %nobnum(key=runStatus, val=&run_status);
        
        %if %length(&run_id.) %then %do;
            write value "runId" &run_id;
        %end;
        
        write value "analysisId" "&analysis_id";
        write value "keepKrmRun" &keep_Krm_run;
        write value "discardKrmADOOutputTables" &discard_KrmADO_output_tables;
        write value "reports";
        write open array;
        export &reports_ds.;
        write close;
        write close;
    run;

    %let base_url = %sysfunc(getoption(servicesbaseurl))/riskCirrusKrm/runs;
    /* Print the request URL to the log */
    %if (%upcase(&debug.) eq TRUE) %then %do;
        /* log json request */
        data _null_;
            infile request;
            input;
            put _infile_;
        run;
        %put &base_url;
    %end;
   
    /* call the midtier service */   
    filename resp temp;
    %core_rest_request(url = &base_url.
                    , method  = POST
                    , body = request
                    , fout = respJobs
                    , debug = &debug.
                    , logonHost = &logonHost.
                    , logonPort = &logonPort.
                    , username = &username.
                    , password = &password.
                    , authMethod = &authMethod.
                    , client_id = &client_id.
                    , outds = outds_jobExecutionInfo
                    , outVarToken = &outVarToken.
                    , outSuccess = &outSuccess.
                    , outResponseStatus = &outResponseStatus.
                    , logOptions = &logOptions.
                    , restartLUA = &restartLUA.
                    , clearCache = &clearCache.                    
    );
    
    /* Print the request URL to the log */
    %if (%upcase(&debug.) eq TRUE) %then %do;
        /* log response */
        data _null_;
            infile respJobs;
            input;
            put _infile_;
        run;
    %end;

    /* check for success */
    %if &&&outSuccess.. ne 1 %then %do;
        %put ERROR: There was an error submitting the HTTP Request to KRM.;
        %abort;
    %end;

    /* Clear references if we're not debugging */
    %if %upcase(&debug.) ne TRUE %then %do;
        filename respJobs clear;
    %end;
    
%mend;