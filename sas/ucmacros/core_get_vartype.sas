/*****************************************************************************
   Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA.

   NAME: core_get_vartype.sas

   PURPOSE: Check the type of a variable: Character (C) or Numeric (N)

   PARAMETERS:
         - ds: input data set
         - var: name of the variable to be checked

   USAGE:
      data test;
         length str $1.  num 8.;
         stop;
      run;
      %put %core_get_vartype(test, str);
      %put %core_get_vartype(test, num);

   NOTES:

 *****************************************************************************/
%macro core_get_vartype(ds, var);
   %local dsid vnum vtype rc;
   /* Open data set */
   %let dsid = %sysfunc(open(%str(&ds.)));
   %if &dsid. > 0 %then %do;
      /* Get variable number */
      %let vnum = %sysfunc(varnum(&dsid., &var));
      %if(&vnum. > 0) %then
         /* Get variable type (C/N) */
         %let vtype = %sysfunc(vartype(&dsid., &vnum.));
      %else
         /* Variable does not exist */
         %let vtype = %str( );
      /* Close data set */
      %let rc = %sysfunc(close(&dsid));
   %end;
   /* Return variable type */
   &vtype.
%mend;