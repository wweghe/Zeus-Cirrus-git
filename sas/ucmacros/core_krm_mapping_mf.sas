/*************************************************************************
 * Copyright 2023, SAS Institute Inc., Cary, NC, USA. All Rights Reserved.
 *
 * NAME:        core_krm_mapping_mf
 *
 * PURPOSE:     Mapping RFM variable definitions to KRM for macro factors
 *
 * PARAMETERS: 
 *              rfm_variable_table 
 *                  <required> - Variables table for RFM variable export end point
 *              krmdb_libref 
 *                  <required> - Libref for KRM database
 *              output_variable_def 
 *                  <required> - output VARIABLE_DEF data set 
 *
 * EXAMPLE:     %core_krm_mapping_mf(rfm_variable_table=rfm_variable_table,
 *                                   krmdb_libref=KRMDB,
 *                                   output_variable_def=variable_def);
 **************************************************************************/
%macro core_krm_mapping_mf(rfm_variable_table=,
                           krmdb_libref=,
                           output_variable_def=);

   %rsk_varlist_nm_only(DS=&krmdb_libref..variable_def);
   data &output_variable_def;
      if 0 then set &krmdb_libref..variable_def;
      retain var_type "M";

      set &rfm_variable_table (keep=name label type roleCategory);
      where upcase(type) = "FACTOR" and upcase(roleCategory) = "MACRO_FACTOR";

      var_name = name;
      description = label;
      var_id = var_name;

      keep &VARLIST_NM;
   run;
%mend core_krm_mapping_mf;