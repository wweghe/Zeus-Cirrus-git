/*************************************************************************
 * Copyright 2023, SAS Institute Inc., Cary, NC, USA. All Rights Reserved.
 *
 * NAME:        core_krm_create_eqt_index
 *
 * PURPOSE:     Create KRM EQT_INDEX table from RFM market history data
 *
 * PARAMETERS: 
 *              rfm_variable_table 
 *                  <required> - Variables table for RFM variable export end point
 *              rfm_history_data 
 *                  <required> - Data from RFM market variable history end point
 *              krmdb_libref 
 *                  <required> - Libref for KRM database
 *              output_eqt_index 
 *                  <required> - output EQT_INDEX data set 
 *
 * EXAMPLE:     %core_krm_create_eqt_index(rfm_variable_table=rfm_variable_table,
 *                                         rfm_history_data=market_history,
 *                                         krmdb_libref=krmdb,
 *                                         output_eqt_index=eqt_index);
 **************************************************************************/
%macro core_krm_create_eqt_index(rfm_variable_table=,
                                 rfm_history_data=,
                                 krmdb_libref=,
                                 output_eqt_index=);

   %local temp_table_to_delete var_tbl_col_name_len;
   proc contents data=&rfm_history_data out=_history_content(keep=name) noprint;
   run;
   %let temp_table_to_delete=&temp_table_to_delete _history_content;

   proc sql;
      create table _equit_index_factors as 
      select upcase(name) as name, currency
      from &rfm_variable_table
      where lowcase(type) = "factor" and lowcase(roleCategory) = "equity_index"
            and  
            upcase(name) in (%upcase(%unquote(%core_get_values(ds=_history_content, column=name, dlm=%str(,), quote=single)))); 
   quit;
   %let temp_table_to_delete=&temp_table_to_delete _equit_index_factors;
   %if &sqlobs eq 0 %then %goto exit;

   %let var_tbl_col_name_len = %core_get_varLen(ds=&rfm_variable_table, var=name);
   %rsk_varlist_nm_only(DS= &krmdb_libref..EQT_INDEX);
   data &output_eqt_index;
      if 0 then set &krmdb_libref..EQT_INDEX;
      set &rfm_history_data(keep=date %core_get_values(ds=_equit_index_factors, column=name));
      array his_val{*} %core_get_values(ds=_equit_index_factors, column=name);;
      length var_name $&var_tbl_col_name_len;
 
      if 0 then set _equit_index_factors;
      if _n_ = 1 then do;
         dcl hash eqt(dataset: "_equit_index_factors");
         eqt.defineKey('name');
         eqt.defineData('currency');
         eqt.defineDone();
      end;     

      data_dt = dhms(input(date, yymmdd10.),0,0,0);
      do i = 1 to dim(his_val);
         symbol = vname(his_val[i]);
         index_val = his_val[i];
         if missing(index_val) then continue;
         var_name=upcase(symbol);
         if eqt.find(key:var_name) ne 0 then currency = "";
         output;
      end;

      keep &VARLIST_NM;
   run;

   %exit:
   %if not %core_is_blank(temp_table_to_delete) %then %do;
      proc delete data=&temp_table_to_delete;
      run;
   %end;
%mend core_krm_create_eqt_index;