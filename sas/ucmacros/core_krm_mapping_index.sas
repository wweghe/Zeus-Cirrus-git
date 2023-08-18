/*************************************************************************
 * Copyright 2023, SAS Institute Inc., Cary, NC, USA. All Rights Reserved.
 *
 * NAME:        core_krm_mapping_index
 *
 * PURPOSE:     Mapping RFM variable definitions to KRM for index rates
 *
 * PARAMETERS: 
 *              rfm_variable_table 
 *                  <required> - Variables table for RFM variable export end point
 *              rfm_customattributes_table 
 *                  <required> - customAttributes table for RFM variable export end point
 *              krmdb_libref 
 *                  <required> - Libref for KRM database
 *              output_index_def 
 *                  <required> - output INDEX_DEF data set 
 *
 * EXAMPLE:     %core_krm_mapping_index(rfm_variable_table=rfm_variable_table,
 *                                      rfm_customattributes_table=rfm_customattributes_table,
 *                                      krmdb_libref=KRMDB,
 *                                      output_index_def=index_def);
 **************************************************************************/
%macro core_krm_mapping_index(rfm_variable_table=,
                              rfm_customattributes_table=,
                              krmdb_libref=,
                              output_index_def=);
   %local var_tbl_col_name_len cust_tbl_col_variable_len cust_tbl_col_name_len dflt_cust_tbl_col_name_len;

   %let var_tbl_col_name_len = %core_get_varLen(ds=&rfm_variable_table, var=name);
   %let cust_tbl_col_variable_len = %core_get_varLen(ds=&rfm_customattributes_table, var=variable);
   %let cust_tbl_col_name_len = %core_get_varLen(ds=&rfm_customattributes_table, var=name);
   %let dflt_cust_tbl_col_name_len = 10;

   %rsk_varlist_nm_only(DS=&krmdb_libref..index_def);

   data &output_index_def;
      if 0 then set &krmdb_libref..index_def;
      length variable $%sysfunc(max(&var_tbl_col_name_len, &cust_tbl_col_variable_len))
             name $%sysfunc(max(&dflt_cust_tbl_col_name_len, &cust_tbl_col_name_len)) 
             ;  

      set &rfm_variable_table(rename=(name=factor_name));
      where upcase(type) = "FACTOR" and upcase(roleCategory) = "INDEX";

      if 0 then set &rfm_customattributes_table(keep=value);
      if _n_=1 then do;
         dcl hash custattr(dataset: "&rfm_customattributes_table");
         custattr.definekey('variable', 'name');
         custattr.definedata('value');
         custattr.definedone(); 
         call missing(variable, name);
      end;

      variable = factor_name;
      index_id = factor_name;
      index_name = label; 

      if custattr.find(key:variable, key:"YC_ID") eq 0 then yc_id = value;
      else yc_id = "";

      if custattr.find(key:variable, key:"INDEX_FLAG") eq 0 then index_flag = input(value, 1.0);
      else index_flag = .;

      if custattr.find(key:variable, key:"HLDY_ID") eq 0 then HLDY_ID = value;
      else HLDY_ID = "";

      if custattr.find(key:variable, key:"HLDY_CONV") eq 0 then HLDY_CONV = input(value, 1.0);
      else HLDY_CONV = .;

      if custattr.find(key:variable, key:"FORMULA") eq 0 then FORMULA = value;
      else FORMULA = "";

      keep &VARLIST_NM; 
   run;
%mend core_krm_mapping_index;