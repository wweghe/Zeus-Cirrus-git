/*************************************************************************
 * Copyright 2023, SAS Institute Inc., Cary, NC, USA. All Rights Reserved.
 *
 * NAME:        core_krm_mapping_yield_class
 *
 * PURPOSE:     Mapping RFM variable definitions to KRM for yield curve classes
 *
 * PARAMETERS: 
 *              rfm_variable_table 
 *                  <required> - Variables table for RFM variable export end point
 *              krmdb_libref 
 *                  <required> - Libref for KRM database
 *              output_code_user 
 *                  <required> - output CODE_USER data set for currency 
 *
 * EXAMPLE:     %core_krm_mapping_yield_class(rfm_variable_table=rfm_variable_table,
 *                                            krmdb_libref=KRMDB,
 *                                            output_code_user=code_user);
 **************************************************************************/
%macro core_krm_mapping_yield_class(rfm_variable_table=,
                                    krmdb_libref=,
                                    output_code_user=);
   %rsk_varlist_nm_only(DS=&krmdb_libref..code_user);
   data &output_code_user;
      if 0 then set &krmdb_libref..code_user;
      retain type_id "YCL" CODE "001";

      set &rfm_variable_table (keep=name type isClass);
      where upcase(type) = "GROUP" and isClass= 1;

      description=name;
      keep &VARLIST_NM;
   run;
%mend core_krm_mapping_yield_class;