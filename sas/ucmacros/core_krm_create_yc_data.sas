/*************************************************************************
 * Copyright 2023, SAS Institute Inc., Cary, NC, USA. All Rights Reserved.
 *
 * NAME:        core_krm_create_yc_data
 *
 * PURPOSE:     Create KRM YC_DATA table from RFM market history data
 *
 * PARAMETERS: 
 *              rfm_variable_table 
 *                  <required> - Variables table for RFM variable export end point
 *              rfm_associations_table 
 *                  <required> - Associations table for RFM variable export end point
 *              rfm_history_data 
 *                  <required> - Data from RFM market variable history end point
 *              krmdb_libref 
 *                  <required> - Libref for KRM database
 *              output_yc_data 
 *                  <required> - output YC_DATA data set 
 *
 * EXAMPLE:     %core_krm_create_yc_data(rfm_variable_table=rfm_variable_table,
 *                                       rfm_associations_table=rfm_associations_table,
 *                                       rfm_history_data=market_history,
 *                                       krmdb_libref=krmdb,
 *                                       output_yc_data=yc_data);
 **************************************************************************/
%macro core_krm_create_yc_data(rfm_variable_table=,
                               rfm_associations_table=,
                               rfm_history_data=,
                               krmdb_libref=,
                               output_yc_data=);
   %local temp_table_to_delete var_tbl_col_name_len assct_tbl_col_members_len assct_tbl_col_name_len;
   proc contents data=&rfm_history_data out=_history_content(keep=name) NOPRINT;
   run;
   %let temp_table_to_delete=&temp_table_to_delete _history_content;

   /*Get factors of yc_id*/
   proc sql noprint;
      create table _krm_yc as 
      select yc_id 
      from &krmdb_libref..YC_DEF;

      create table _yc_factors as
      select name as yield_curve_nm, members as factor
      from &rfm_associations_table
      where upcase(name) in ( select upcase(yc_id) from _krm_yc)
            and
            upcase(members) in (select upcase(name) from _history_content); 
   quit;
   %let temp_table_to_delete=&temp_table_to_delete _krm_yc _yc_factors;
   %if &sqlobs eq 0 %then %goto exit;

   %let var_tbl_col_name_len = %core_get_varLen(ds=&rfm_variable_table, var=name);
   %let assct_tbl_col_members_len = %core_get_varLen(ds=&rfm_associations_table, var=members);
   %let assct_tbl_col_name_len = %core_get_varLen(ds=&rfm_associations_table, var=name);

   proc sort data=_yc_factors;
      by yield_curve_nm;
   run;

   data _yc_factors;
      set _yc_factors;
      by yield_curve_nm;

      length name $%sysfunc(max(&var_tbl_col_name_len, &assct_tbl_col_members_len, &assct_tbl_col_name_len));
      if 0 then set krmdb.yc_data(keep=MAT_TERM MAT_TERM_U PMT_FREQ PMT_FREQ_U DAY_COUNT);
      if 0 then set &rfm_variable_table(keep=type Coordinate1_Unit Coordinate1_Value PaymentFreq_Unit PaymentFreq_Value DayCount CouponRate valueType maturityType dayCount);
   
      if _n_=1 then do;
         dcl hash vardef(dataset:"&rfm_variable_table(where=(upcase(type)='FACTOR'))");
         vardef.defineKey('name');
         vardef.defineData('Coordinate1_Unit', 'Coordinate1_Value', 'PaymentFreq_Unit', 'PaymentFreq_Value', 'DayCount', 'CouponRate');
         vardef.defineDone();

         dcl hash curdef(dataset:"&rfm_variable_table(where=(upcase(type)='CURVE'))");
         curdef.defineKey('name');
         curdef.defineData('valueType', 'maturityType', 'dayCount');
         curdef.defineDone();
      end;

      if first.yield_curve_nm then do;
         rc = curdef.find(key:yield_curve_nm);
      end;

      if vardef.find(key:factor) = 0 then do;
         MAT_TERM = max(0, Coordinate1_Value);
         MAT_TERM_U = "";
         if lowcase(maturityType) = 'unit' then do;
            select(upcase(Coordinate1_Unit));
               when ("DAY") MAT_TERM_U = "D";
               when ("WEEK") MAT_TERM_U = "W";
               when ("MONTH") MAT_TERM_U = "M";
               when ("SEMIANNUAL") MAT_TERM_U = "S";
               when ("YEAR") MAT_TERM_U = "Y";
               when ("") MAT_TERM_U = "";
               otherwise do;
                  put 'ERROR: Invalid Coordinate1_Unit "' Coordinate1_Unit '" is found for the factor ' factor '.'; 
                  abort;
               end;
            end;
         end;
      
         PMT_FREQ = PaymentFreq_Value;
         select(upcase(PaymentFreq_Unit));
            when ("DAY") PMT_FREQ_U = "D";
            when ("WEEK") PMT_FREQ_U = "W";
            when ("MONTH") PMT_FREQ_U = "M";
            when ("SEMIANNUAL") PMT_FREQ_U = "S";
            when ("YEAR") PMT_FREQ_U = "Y";
            when ("") PMT_FREQ_U = "";
            otherwise do;
               put 'ERROR: Invalid PaymentFreq_Unit "' PaymentFreq_Unit '" is found for the factor ' factor '.'; 
               abort;
            end;
         end;
      
         select(DayCount);
           when ("ACT/360")      day_count = 1;
           when ("ACT/365")      day_count = 2;
            when ("30/360")       day_count = 3;
           when ("30E/360")      day_count = 4;
           when ("ACT/365L")     day_count = 5;
           when ("30E+/360")     day_count = 6; 
            when ("30/365")       day_count = 7; 
           when ("ACT/ACT/ISDA") day_count = 8;
           when ("30E/360/ISDA") day_count = 9;
            when ("ACT/ACT/ICMA") day_count = 10;
           when ("ACT/365A")     day_count = 11;
           when ("NL/360")       day_count = 12;
           when ("NL/365")       day_count = 13;
            when ("ACT/365.25")   day_count = 14;
            when ("")             day_count = .;
            /* 15 */
            otherwise do;
               put 'ERROR: Invalid DayCount "' DayCount '" is found for the factor ' factor '.'; 
               abort;
            end;
         end;
 
         CouponRate = 100*CouponRate;
      end;
      else do;
         put 'ERROR: The definition details of the factor "' factor '" was not able to be found.'; 
         abort;
      end;

      if missing(MAT_TERM_U) then maturityInYear = MAT_TERM;
      else do;
         if MAT_TERM_U = "D" then maturityInYear = MAT_TERM/365;
         else if MAT_TERM_U = "W" then maturityInYear = MAT_TERM/52;
         else if MAT_TERM_U = "M" then maturityInYear = MAT_TERM/12;
         else if MAT_TERM_U = "S" then maturityInYear = MAT_TERM/2;
         else if MAT_TERM_U = "Y" then maturityInYear = MAT_TERM;
      end;
      factor=upcase(factor);

      keep yield_curve_nm factor valueType maturityType MAT_TERM MAT_TERM_U PMT_FREQ PMT_FREQ_U day_count CouponRate maturityInYear;
   run;

   proc sort data=_yc_factors;
      by yield_curve_nm maturityInYear;
   run;

   %rsk_varlist_nm_only(DS=&krmdb_libref..YC_DATA);
   data &output_yc_data;
      if 0 then set &krmdb_libref..YC_DATA;
      set &rfm_history_data(keep=date %core_get_values(ds=_yc_factors, column=factor));
      array his_val{*} %core_get_values(ds=_yc_factors, column=factor);;  
      length var_name $&assct_tbl_col_members_len;
  
      if 0 then set _yc_factors(keep=yield_curve_nm factor valueType maturityType MAT_TERM MAT_TERM_U PMT_FREQ PMT_FREQ_U day_count CouponRate maturityInYear);
      if _n_ = 1 then do;
         dcl hash factors(dataset:'_yc_factors(keep=yield_curve_nm factor valueType maturityType MAT_TERM MAT_TERM_U PMT_FREQ PMT_FREQ_U day_count CouponRate maturityInYear)');
         factors.defineKey('factor');
         factors.defineData('yield_curve_nm','valueType','maturityType','MAT_TERM','MAT_TERM_U','PMT_FREQ','PMT_FREQ_U','day_count','CouponRate', 'maturityInYear');
         factors.defineDone();
      end;
   
      data_dt = dhms(input(date, yymmdd10.),0,0,0);
      do i = 1 to dim(his_val);
         var_name = upcase(vname(his_val[i]));
         value = his_val[i]*100;
         if missing(value) then continue;

         if factors.find(key:var_name) eq 0 then do;
            cpn_rt=.;
            price_yield=.;
            mat_dt=.;
            yc_id = yield_curve_nm;
            select(lowcase(ValueType));
               when ("discount bond yield", "discount bond price", "periodic zero rate") 
                  do;
                     if mat_term = 0 and lowcase(maturityType) = 'unit' then cpn_rt = value;
                     else price_yield = value;
                  end;
               when ("same series par bond") cpn_rt = value;
               when ("same series non-par bond", "different series bond")
                  do;
                     if mat_term = 0 then cpn_rt = value;
                     else do;
                        cpn_rt = CouponRate;
                        price_yield = value;
                     end;               
                  end;
               otherwise do;
                  put 'ERROR: Invalid valueType ' valueType 'is found for the curve ' yc_id; 
                  abort;
               end;
            end;
            if mat_term = 0 and lowcase(maturityType) = 'unit' then mat_dt = data_dt;
            output;
         end;
     end;
      keep &VARLIST_NM;
   run;

   %exit:
   %if not %core_is_blank(temp_table_to_delete) %then %do;
      proc delete data=&temp_table_to_delete;
      run;
   %end;
%mend core_krm_create_yc_data;
