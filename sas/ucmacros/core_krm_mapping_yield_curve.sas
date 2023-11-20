/*************************************************************************
 * Copyright 2023, SAS Institute Inc., Cary, NC, USA. All Rights Reserved.
 *
 * NAME:        core_krm_mapping_yield_curve
 *
 * PURPOSE:     Mapping RFM variable definitions to KRM for yield curves
 *
 * PARAMETERS: 
 *              rfm_variable_table 
 *                  <required> - Variables table for RFM variable export end point
 *              rfm_customattributes_table 
 *                  <required> - customAttributes table for RFM variable export end point
 *              rfm_associations_table 
 *                  <required> - Associations table for RFM variable export end point 
 *              krmdb_libref 
 *                  <required> - Libref for KRM database
 *              output_code_user 
 *                  <required> - output CODE_USER data set for currency  
 *              output_yc_def 
 *                  <required> - output YC_DEF data set 
 *
 * EXAMPLE:     %core_krm_mapping_yield_curve(rfm_variable_table=rfm_variable_table,
 *                                            rfm_customattributes_table=rfm_customattributes_table,
 *                                            rfm_associations_table=rfm_associations_table,
 *                                            krmdb_libref=KRMDB,
 *                                            output_code_user=_code_user,
 *                                            output_yc_def=yc_def);
 **************************************************************************/
%macro core_krm_mapping_yield_curve(rfm_variable_table=,
                                    rfm_customattributes_table=,
                                    rfm_associations_table=,
                                    krmdb_libref=,
                                    output_code_user=,
                                    output_yc_def=);
   %local var_tbl_col_name_len cust_tbl_col_variable_len cust_tbl_col_name_len dflt_cust_tbl_col_name_len assct_tbl_col_members_len ;

   %let var_tbl_col_name_len = %core_get_varLen(ds=&rfm_variable_table, var=name);
   %let cust_tbl_col_variable_len = %core_get_varLen(ds=&rfm_customattributes_table, var=variable);
   %let cust_tbl_col_name_len = %core_get_varLen(ds=&rfm_customattributes_table, var=name);
   %let assct_tbl_col_members_len = %core_get_varLen(ds=&rfm_associations_table, var=members);
   %let dflt_cust_tbl_col_name_len = 13;

   %rsk_varlist_nm_only(DS=&krmdb_libref..YC_DEF);

   data &output_yc_def;
      if 0 then set &krmdb_libref..YC_DEF;
      length yield_curve $%sysfunc(max(&var_tbl_col_name_len, &assct_tbl_col_members_len))  
             variable $%sysfunc(max(&var_tbl_col_name_len, &cust_tbl_col_variable_len))
             name $%sysfunc(max(&dflt_cust_tbl_col_name_len, &cust_tbl_col_name_len)) 
             ;  
  
      set &rfm_variable_table(rename=(name=curve_name));
      where upcase(type) = "CURVE";

      if 0 then set &rfm_associations_table(keep=name rename=(name=yield_curve_class));
      if 0 then set &rfm_customattributes_table(keep=value);
      
      if _n_=1 then do;
         dcl hash ycl(dataset: " &rfm_associations_table(rename=(name=yield_curve_class members=yield_curve))");
         ycl.definekey('yield_curve');
         ycl.definedata('yield_curve_class');
         ycl.definedone();

         dcl hash custattr(dataset: "&rfm_customattributes_table");
         custattr.definekey('variable', 'name');
         custattr.definedata('value');
         custattr.definedone(); 

         call missing(yield_curve, variable, name);
      end;

      /*yc_class*/
      yc_id=curve_name;
      yc_name = label;
      if ycl.find(key:yc_id) eq 0;
      yc_class = yield_curve_class;

      /*data_type*/
      select(lowcase(ValueType));
         when ("discount bond yield") data_type = 0;
         when ("discount bond price") data_type = 1;
         when ("same series par bond") data_type = 2;
         when ("same series non-par bond") data_type = 3;
         when ("different series bond") data_type = 4;
         when ("periodic zero rate") data_type = 5;
         otherwise do;
            put 'ERROR: Invalid valueType "' valueType '" is found for the curve ' yc_id '.'; 
            abort;
         end;
      end;

      /*mat_type*/
      if lowcase(maturityType) = 'years' then mat_type = 1;
      else if lowcase(maturityType) = 'unit' then mat_type =2;
      else do;
         put 'ERROR: Invalid maturityType "' maturityType '" is found for the curve ' yc_id '.'; 
         abort;
      end; 

      /*smth_method*/
      select(lowcase(Interpolation));
         when ("maximumsmoothnessforwardrate") smth_method = 0;
         when ("cubicsplineofprices") do; 
            if lowcase(Extrapolation) = "linear" then smth_method = 1;
            else if lowcase(Extrapolation) = "constant" then smth_method = 2;
            else smth_method = 2;
         end;
         when ("cubicsplineofyields") do; 
            if lowcase(Extrapolation) = "linear" then smth_method = 3;
            else if lowcase(Extrapolation) = "constant" then smth_method = 4;
            else smth_method = 4;
         end;
         when ("linearsplineofyields") do;  
            if lowcase(Extrapolation) = "constant" then smth_method = 5;
            else if lowcase(Extrapolation) = "linear" then smth_method = 6;
            else smth_method = 5;
         end;
         when ("linearsplineofprices") do;  
            if lowcase(Extrapolation) = "constant" then smth_method = 7;
            else if lowcase(Extrapolation) = "linear" then smth_method = 8;
            else smth_method = 7;
         end;
         when ("vasicek") smth_method = 9;   
         when ("nelsonsiegel") smth_method = 10;
         when ("nelsonsiegelsvensson") smth_method = 11;
         otherwise do;
            smth_method = 0;
            put 'WARING: Invalid Interpolation ' Interpolation 'is found for the curve ' yc_id '.'; 
            put 'WARING: smth_method is set to 0 for the curve ' yc_id '.'; 
         end;
      end;

      /*Spread_type and base_class*/
      base_class = "";
      select(lowcase(spreadType));
         when ("base yield curve (no spread)")  spread_type = 1;
         when ("spread-by security (constant)")  spread_type = 2;
         when ("spread-by security and term structure")  do; spread_type = 3; base_class = baseClass; end;
         when ("spread-by term structure")  do; spread_type = 4; base_class = baseClass; end;
         when ("spread-by default and recovery")  spread_type = 5;
         when ("spread-by hazard function")  spread_type = 6;
         when ("spread-by term structure (no base)")  spread_type = 7;
         when ("user-adjusted yield curve")  do; spread_type = 8; base_class = baseClass; end;
         when ("synthetic yield curve (no spread)")  do;
            spread_type = 9;
            if custattr.find(key:yc_id, key:"FORMULA") eq 0 then base_class = value;
         end;
         otherwise do;
            put 'ERROR: Invalid spreadType "' spreadType '" is found for the curve ' yc_id '.';  
            abort;
         end;
      end;

      /*Custom attributes*/
      if custattr.find(key:yc_id, key:"DFLT_FLAG") eq 0 then DFLT_FLAG = input(value, 1.0);
      else DFLT_FLAG = .;

      if custattr.find(key:yc_id, key:"FX_FLAG") eq 0 then FX_FLAG = input(value, 1.0);
      else FX_FLAG = .;

      if custattr.find(key:yc_id, key:"RISK_FLAG") eq 0 then RISK_FLAG = input(value, 1.0);
      else RISK_FLAG = .;

      if custattr.find(key:yc_id, key:"REINVEST_FLAG") eq 0 then REINVEST_FLAG = input(value, 1.0);
      else REINVEST_FLAG = .;

      if custattr.find(key:yc_id, key:"CONST") eq 0 then CONST = input(value, best12.);
      else CONST = .;

      if custattr.find(key:yc_id, key:"COEFF") eq 0 then COEFF = input(value, best12.);
      else COEFF = .;

      if custattr.find(key:yc_id, key:"ALPHA") eq 0 then ALPHA = input(value, best12.);
      else ALPHA = .;

      if custattr.find(key:yc_id, key:"SIGMA") eq 0 then SIGMA = input(value, best12.);
      else SIGMA = .;

      if custattr.find(key:yc_id, key:"AUX01") eq 0 then AUX01 = input(value, best12.);
      else AUX01 = .;

      if custattr.find(key:yc_id, key:"AUX02") eq 0 then AUX02 = input(value, best12.);
      else AUX02 = .;

      if custattr.find(key:yc_id, key:"AUX03") eq 0 then AUX03 = strip(value);
      else AUX03 = "";
   
      keep &VARLIST_NM;
   run;

   proc sort data=&output_yc_def(keep=currency) out=&output_code_user nodupkey;
      by currency;
   run;

   %rsk_varlist_nm_only(DS=&krmdb_libref..CODE_USER);
   data &output_code_user;
      if 0 then set &krmdb_libref..code_user;
      retain type_id "CUR" CODE "001";

      set &output_code_user;
      description=upcase(currency);
      keep &VARLIST_NM;
   run;   
%mend core_krm_mapping_yield_curve;