%macro rsk_varattr(ds, var, attr);
   %local
      dsid
      varnum
      varattr
      rc
   ;

   %if not %sysfunc(prxmatch(/^(length|type|label|format|informat)$/i, %superq(attr))) %then %do;
      %put ERROR: input parameter ATTR = &attr. is invalid. Valid options are: length|type|label|format|informat;
      %abort;
   %end;

   %let attr = %sysfunc(prxchange(s/format/fmt/i, -1, &attr.));
   %let attr = %sysfunc(prxchange(s/length/len/i, -1, &attr.));

   /* Open dataset */
   %let dsid = %sysfunc(open(&ds.));
   /* Look for the variable */
   %let varnum = %sysfunc(varnum(&dsid., &var.));
   /* Check for errors */
   %if %sysevalf(&varnum. <= 0, boolean) %then %do;
      %put ERROR: variable &var. does not exist in table &ds.;
      %abort;
   %end;

   /* Get the requested variable attribute */
   %let varattr = %sysfunc(var&attr.(&dsid., &varnum.));

   /* Close Dataset */
   %let rc = %sysfunc(close(&dsid.));

   /* Return the attribute value */
   &varattr.
%mend;
