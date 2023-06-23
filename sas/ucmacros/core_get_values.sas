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
 *                  <required> - single/double quote 
 *
 * EXAMPLE:     %core_get_values(ds=in_ds, column=value, dlm=%str(,), quote=single);
 **************************************************************************/
%macro core_get_values(ds=, column=, dlm=, quote=);  
   %local dsid rc value row;
       
   %if %lowcase(&quote) = double %then %let quote=%str(%");
   %else %if %lowcase(&quote) = single %then %let quote=%str(%');  
   %else %let quote=;
   
   %let dsid=%sysfunc(open(&ds, i));    
   %let rc = 0;
   %let row = 1;
   %let rc=%sysfunc(fetch(&dsid));   
   %do %while (&rc = 0);                                                                                             
      %let value=%sysfunc(getvarc(&dsid, %sysfunc(varnum(&dsid, &column)))); 
      %if &row > 1 %then &dlm&quote&value&quote;
      %else &quote&value&quote;    
      %let rc=%sysfunc(fetch(&dsid));                                                                                                  
      %let row = %eval(&row + 1);
   %end;
                                                                                                     
   %if &rc ne -1 %then %do;                                                                                                                    
      %put %sysfunc(sysmsg());                                                                                                              
   %end; 

   %let rc=%sysfunc(close(&dsid));
%mend core_get_values; 