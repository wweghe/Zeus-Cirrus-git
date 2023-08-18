/*************************************************************************
 * Copyright 2023, SAS Institute Inc., Cary, NC, USA. All Rights Reserved.
 *
 * NAME:        core_get_varLen
 *
 * PURPOSE:     Get length of a variable
 *
 *
 * PARAMETERS: 
 *              ds 
 *                  <required> - input data set
 *              var 
 *                  <required> - name of the variable to be checked
 *
 * EXAMPLE:     %core_get_varLen(ds=test, var=col1);
 **************************************************************************/
%macro core_get_varLen(ds=, var=);
   %local dsid vnum vLen rc;
   /* Open data set */
   %let dsid = %sysfunc(open(%str(&ds.)));
   %if &dsid. > 0 %then %do;
      /* Get variable number */
      %let vnum = %sysfunc(varnum(&dsid., &var));
      %if(&vnum. > 0) %then
         /* Get variable type (C/N) */
         %let vLen = %sysfunc(varLen(&dsid., &vnum.));
      %else
         /* Variable does not exist */
         %let vLen = %str( );
      /* Close data set */
      %let rc = %sysfunc(close(&dsid));
   %end;
   /* Return variable type */
   &vLen.
%mend core_get_varLen;
