/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
\file
\anchor rsk_dsexist_CAS
\brief Verifies whether a CAS data set exists.

\details

\param [in]  LIB : CAS libname. (Default value: CASLib 'Public')
\param [in]  DS : given data set.

Output
------
tableExists returns value of 1 for session-scope table and 2 for a global-scope table (cas-table format: <caslib><table>)
- Returns out_var = 0 if the given DS does not exist in a specific CAS Lib
- Returns out_var = 1 if the given DS exists in a specific CAS Lib

\n

\ingroup CommonAnalytics utilities
\author  SAS Institute Inc.
\date    2022
*/
%macro rsk_dsexist_cas(cas_lib = Public
                      ,cas_table =
                      ,cas_session_name =
                      ,out_var=cas_table_exists);

   %local exist_flg;

   /* out_var cannot be missing. Set a default value */
   %if(%sysevalf(%superq(out_var) =, boolean)) %then
      %let out_var = cas_table_exists;

   /* Declare the output variable as global if it does not exist */
   %if(not %symexist(&out_var.)) %then
      %global &out_var.;

   %let &out_var.=0;
   %let exist_flg=0;
   proc cas;
      %if(%sysevalf(%superq(cas_session_name) ne, boolean)) %then %do;
         session &cas_session_name.;
      %end;
      table.tableExists result=r1 /
         caslib="&cas_lib." name="&cas_table.";
      if r1.exists ne 0 then CALL SYMPUTX("exist_flg", 1, "L");
      run;
   quit;
   %let &out_var.= &exist_flg.;

%mend rsk_dsexist_cas;
