/*************************************************************************
 * Copyright 2023, SAS Institute Inc., Cary, NC, USA. All Rights Reserved.
 *
 * NAME:        core_krm_create_mf_data
 *
 * PURPOSE:     Create KRM MF_DATA table from RFM market history data
 *
 * PARAMETERS: 
 *              rfm_variable_table 
 *                  <required> - Variables table for RFM variable export end point
 *              rfm_history_data 
 *                  <required> - Data from RFM market variable history end point
 *              krmdb_libref 
 *                  <required> - Libref for KRM database
 *              output_mf_data 
 *                  <required> - output MF_DATA data set 
 *
 * EXAMPLE:     %core_krm_create_mf_data(rfm_variable_table=rfm_variable_table,
 *                                       rfm_history_data=market_history,
 *                                       krmdb_libref=krmdb,
 *                                       output_mf_data=mf_data);
 **************************************************************************/
%macro core_krm_create_mf_data(rfm_variable_table=,
                               rfm_history_data=,
                               krmdb_libref=,
                               output_mf_data=);

   %local temp_table_to_delete;

   proc contents data=&rfm_history_data out=_history_content(keep=name) noprint;
   run;
   %let temp_table_to_delete=&temp_table_to_delete _history_content;

   proc sql;
      create table _mf_factors as 
      select name
      from &rfm_variable_table
      where lowcase(type) = "factor" and lowcase(roleCategory) = "macro_factor"
            and 
            upcase(name) in (%upcase(%unquote(%core_get_values(ds=_history_content, column=name, dlm=%str(,), quote=single)))); 
   quit;
   %let temp_table_to_delete=&temp_table_to_delete _mf_factors;
   %if &sqlobs eq 0 %then %goto exit;

   %rsk_varlist_nm_only(DS= &krmdb_libref..MF_DATA);
   data &output_mf_data;
      if 0 then set &krmdb_libref..MF_DATA;
      set &rfm_history_data(keep=date %core_get_values(ds=_mf_factors, column=name));
      array his_val{*} %core_get_values(ds=_mf_factors, column=name);;

      data_dt = dhms(input(date, yymmdd10.),0,0,0);
      do i = 1 to dim(his_val);
         var_name = vname(his_val[i]);
         var_val = his_val[i];
         if not missing(var_val) then output;
      end;

      keep &VARLIST_NM;
   run;

   %exit:
   %if not %core_is_blank(temp_table_to_delete) %then %do;
      proc delete data=&temp_table_to_delete;
      run;
   %end;
%mend core_krm_create_mf_data;