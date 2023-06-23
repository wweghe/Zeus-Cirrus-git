/*************************************************************************
 * Copyright 2023, SAS Institute Inc., Cary, NC, USA. All Rights Reserved.
 *
 * NAME:        core_krm_create_index_rt
 *
 * PURPOSE:     Create KRM INDEX_RT table from RFM market history data
 *
 * PARAMETERS: 
 *              rfm_variable_table 
 *                  <required> - Variables table for RFM variable export end point
 *              rfm_history_data 
 *                  <required> - Data from RFM market variable history end point
 *              krmdb_libref 
 *                  <required> - Libref for KRM database
 *              output_index_rt 
 *                  <required> - output INDEX_RT data set 
 *
 * EXAMPLE:     %core_krm_create_index_rt(rfm_variable_table=rfm_variable_table,
 *                                        rfm_history_data=market_history,
 *                                        krmdb_libref=krmdb,
 *                                        output_index_rt=index_rt);
 **************************************************************************/
%macro core_krm_create_index_rt(rfm_variable_table=,
                                rfm_history_data=,
                                krmdb_libref=,
                                output_index_rt=);

   %local temp_table_to_delete;

   proc contents data=&rfm_history_data out=_history_content(keep=name) noprint;
   run;
   %let temp_table_to_delete=&temp_table_to_delete _history_content;
 
   proc sql;
      create table _index_factors as
      select index_id
      from &krmdb_libref..INDEX_DEF
      where upcase(index_id) in (%upcase(%unquote(%core_get_values(ds=_history_content, column=name, dlm=%str(,), quote=single)))); 
   quit;
   %let temp_table_to_delete=&temp_table_to_delete _index_factors;
   %if &sqlobs eq 0 %then %goto exit;

   %rsk_varlist_nm_only(DS= &krmdb_libref..INDEX_RT);
   data &output_index_rt;
      if 0 then set &krmdb_libref..INDEX_RT;
      set &rfm_history_data(keep=date %core_get_values(ds=_index_factors, column=index_id));
      array his_val{*} %core_get_values(ds=_index_factors, column=index_id);;

      data_dt = dhms(input(date, yymmdd10.),0,0,0);
      do i = 1 to dim(his_val);
         index_id = vname(his_val[i]);
         int_rt_val = his_val[i];
         if not missing(int_rt_val) then output;
      end;

      keep &VARLIST_NM;
   run;

   %exit:
   %if not %core_is_blank(temp_table_to_delete) %then %do;
      proc delete data=&temp_table_to_delete;
      run;
   %end;
%mend core_krm_create_index_rt;