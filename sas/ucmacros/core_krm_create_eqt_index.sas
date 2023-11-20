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
            upcase(name) in (select upcase(name) from _history_content); 
   quit;
   %let temp_table_to_delete=&temp_table_to_delete _equit_index_factors;
   %let num_factors=&SQLOBS;
   %if &num_factors eq 0 %then %goto exit;

   %let var_tbl_col_name_len = %core_get_varLen(ds=&rfm_variable_table, var=name);
   %rsk_varlist_nm_only(DS= &krmdb_libref..EQT_INDEX);
   %let max_num_factor_processed=1000;
   %let num_factors_processed=0;
   %let n_ds=1;    
   %do %while (&num_factors_processed < &num_factors);
      %if &n_ds = 1 %then %let _out_ds_= &output_eqt_index;
      %else %let _out_ds_=_out_ds_;
       
      %let firstObs = %eval(&num_factors_processed + 1);
      %let Obs = %sysfunc(min(&num_factors_processed+&max_num_factor_processed, &num_factors));

      data &_out_ds_;
         if 0 then set &krmdb_libref..EQT_INDEX;
         set &rfm_history_data(keep=date %core_get_values(ds=_equit_index_factors, column=name, firstObs=&firstObs, Obs=&Obs));
         array his_val{*} %core_get_values(ds=_equit_index_factors, column=name, firstObs=&firstObs, Obs=&Obs);;
         length var_name $&var_tbl_col_name_len;
 
         if 0 then set _equit_index_factors;
         if _n_ = 1 then do;
            dcl hash eqt(dataset: "_equit_index_factors(firstObs=&firstObs Obs=&Obs)");
            eqt.defineKey('name');
            eqt.defineData('currency');
            eqt.defineDone();
         end;     

/*          data_dt = dhms(input(date, yymmdd10.),0,0,0); */
         data_dt = input(date, yymmdd10.);
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

      %if &n_ds > 1 %then %do;
         proc append base=&output_eqt_index data=&_out_ds_;
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
%mend core_krm_create_eqt_index;