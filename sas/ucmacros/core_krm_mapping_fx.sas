/*************************************************************************
 * Copyright 2023, SAS Institute Inc., Cary, NC, USA. All Rights Reserved.
 *
 * NAME:        core_krm_mapping_fx
 *
 * PURPOSE:     Mapping RFM variable definitions to KRM for foreign exchange spot rates
 *
 * PARAMETERS: 
 *              rfm_variable_table 
 *                  <required> - Variables table for RFM variable export end point
 *              krmdb_libref 
 *                  <required> - Libref for KRM database
 *              output_code_user 
 *                  <required> - output CODE_USER data set for currency 
 *
 * EXAMPLE:     %core_krm_mapping_fx(rfm_variable_table=rfm_variable_table,
 *                                   krmdb_libref=KRMDB,
 *                                   output_code_user=code_user);
 **************************************************************************/
%macro core_krm_mapping_fx(rfm_variable_table=,
                           krmdb_libref=,
                           output_code_user=);

   %rsk_varlist_nm_only(DS=&krmdb_libref..code_user);
   data &output_code_user;
      if 0 then set &krmdb_libref..code_user;
      retain type_id "CUR" CODE "001";

      set &rfm_variable_table (keep=type role currency fromCurrency);
      where upcase(type) = "FACTOR" and upcase(role) = "FX_SPOT";

      description=upcase(currency); output;
      description=upcase(fromCurrency); output;
      keep &VARLIST_NM;
   run;

   proc sort data=&output_code_user nodupkey;
      by description;
   run;
%mend core_krm_mapping_fx;