/*************************************************************************
 * Copyright 2023, SAS Institute Inc., Cary, NC, USA. All Rights Reserved.
 *
 * NAME:        core_get_values
 *
 * PURPOSE:     Get a list of values for a column in a data set
 *
 *
 * PARAMETERS: 
 *              ds 
 *                  <required> - input data set
 *              column 
 *                  <required> - column name
 *              dlm 
 *                  <optional> - delimiter of the list
 *              quote 
 *                  <optional> - single/double quote 
 *              firstObs
 *                  <optional> - Specifies the first observation to process
 *              oobs
 *                  <optional> - Specifies the last observation to process
 *
 * EXAMPLE:     %core_get_values(ds=in_ds, column=value, dlm=%str(,), quote=single);
 **************************************************************************/
%macro core_get_values(ds=, column=, dlm=, quote=, firstObs=, obs=);  
   %local dsid rc value row nobs;
       
   %if %lowcase(&quote) = double %then %let quote=%str(%");
   %else %if %lowcase(&quote) = single %then %let quote=%str(%');  
   %else %let quote=;
   
   %let dsid=%sysfunc(open(&ds, i));  
   %if &dsid <= 0 %then %do;
       %put ER%str(R)OR: (core_get_values) Can not open data set %upcase(&ds.).;
       %return;
   %end;  

   %let nobs = %sysfunc (attrn(&dsid.,NOBS));
   %if %core_is_blank(firstObs) %then %let firstObs = 1;
   %else %if &firstObs < 1 %then firstObs = 1;

   %if %core_is_blank(obs) %then %let obs = &nobs;
   %else %if &obs > &nobs %then %let obs = &nobs; 

   %do row = &firstObs %to &obs;    
      %let rc=%sysfunc(fetchobs(&dsid, &row));  
      %if &rc ne 0 and &rc ne -1 %then %do;
          %put ER%str(R)OR: (core_get_values) %sysfunc(sysmsg());
          %return;
      %end;       
                                                                                  
      %let value=%sysfunc(getvarc(&dsid, %sysfunc(varnum(&dsid, &column)))); 
      %if &row > &firstObs %then &dlm&quote&value&quote;
      %else &quote&value&quote;    
   %end;

   %let rc=%sysfunc(close(&dsid));
%mend core_get_values; 