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

   %local temp_table_to_delete max_num_factor_processed num_factors num_factors_processed n_ds;

   proc contents data=&rfm_history_data out=_history_content(keep=name) noprint;
   run;
   %let temp_table_to_delete=&temp_table_to_delete _history_content;
 
   proc sql;
      create table _index_factors as
      select index_id
      from &krmdb_libref..INDEX_DEF
      where upcase(index_id) in (select upcase(name) from _history_content); 
   quit;
   %let temp_table_to_delete=&temp_table_to_delete _index_factors;
   %let num_factors=&SQLOBS;
   %if &num_factors eq 0 %then %goto exit;

   %rsk_varlist_nm_only(DS= &krmdb_libref..INDEX_RT);
   %let max_num_factor_processed=1000;
   %let num_factors_processed=0;
   %let n_ds=1;    
   %do %while (&num_factors_processed < &num_factors);
      %if &n_ds = 1 %then %let _out_ds_= &output_index_rt;
      %else %let _out_ds_=_out_ds_;
       
      %let firstObs = %eval(&num_factors_processed + 1);
      %let Obs = %sysfunc(min(&num_factors_processed+&max_num_factor_processed, &num_factors));

      data &_out_ds_;
         if 0 then set &krmdb_libref..INDEX_RT;
         set &rfm_history_data(keep=date %core_get_values(ds=_index_factors, column=index_id, firstObs=&firstObs, Obs=&Obs));
         array his_val{*} %core_get_values(ds=_index_factors, column=index_id, firstObs=&firstObs, Obs=&Obs);;

/*          data_dt = dhms(input(date, yymmdd10.),0,0,0); */
         data_dt = input(date, yymmdd10.);
         do i = 1 to dim(his_val);
            index_id = vname(his_val[i]);
            int_rt_val = his_val[i];
            if not missing(int_rt_val) then output;
         end;

         keep &VARLIST_NM;
      run;
      %if &n_ds > 1 %then %do;
         proc append base=&output_index_rt data=&_out_ds_;
         run;
      %end;

      %let num_factors_processed = &Obs;
      %let n_ds=%eval(&n_ds+1);
   %end;
   %if &n_ds > 2 %then %let temp_table_to_delete=&temp_table_to_delete &_out_ds_;

   %exit:
   %if not %core_is_blank(temp_table_to_delete) %then %do;
      proc delete data=&temp_table_to_delete;
      run;
   %end;
%mend core_krm_create_index_rt;