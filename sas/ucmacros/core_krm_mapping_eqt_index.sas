/*************************************************************************
 * Copyright 2023, SAS Institute Inc., Cary, NC, USA. All Rights Reserved.
 *
 * NAME:        core_krm_mapping_eqt_index
 *
 * PURPOSE:     Mapping RFM variable definitions to KRM for equity index
 *
 * PARAMETERS: 
 *              rfm_variable_table 
 *                  <required> - Variables table for RFM variable export end point
 *              krmdb_libref 
 *                  <required> - Libref for KRM database
 *              output_code_user 
 *                  <required> - output CODE_USER data set for currency 
 *              output_index_symbol 
 *                  <required> - output INDEX_SYMBOL data set 
 *
 * EXAMPLE:     %core_krm_mapping_eqt_index(rfm_variable_table=rfm_variable_table,
 *                                          krmdb_libref=KRMDB,
 *                                          output_code_user=code_user,
 *                                          output_index_symbol=index_symbol);
 **************************************************************************/
%macro core_krm_mapping_eqt_index(rfm_variable_table=,
                                  krmdb_libref=,
                                  output_code_user=,
                                  output_index_symbol=);
   %local symbol_var_list;
   %rsk_varlist_nm_only(DS=&krmdb_libref..index_symbol); 
   %let symbol_var_list =  &VARLIST_NM;
   %rsk_varlist_nm_only(DS=&krmdb_libref..code_user);

   data &output_code_user(keep=&VARLIST_NM)
        &output_index_symbol(keep=&symbol_var_list);

      if 0 then set &krmdb_libref..code_user;
      if 0 then set &krmdb_libref..index_symbol;

      retain type_id "CUR" CODE "001";
      set &rfm_variable_table (keep=name label type roleCategory currency);
      where upcase(type) = "FACTOR" and upcase(roleCategory) = "EQUITY_INDEX";

      description=upcase(currency);

      symbol = name;
      symbol_name = label;
   run;
%mend core_krm_mapping_eqt_index;