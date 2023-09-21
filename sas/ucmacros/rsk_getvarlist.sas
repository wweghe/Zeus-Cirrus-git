/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/*!
\file
\anchor rsk_getvarlist
\brief   Retrieves the names of variables (columns) from a given data set as a list of space separated varnames.

\param [dsName] IN Name of the input table
\param [pattern] IN (Optional) RegEx pattern used to get only the list of variables that match the specified pattern
\param [type] IN (Optional) Specifies the types of variables to look for: C (Character) or N (Numeric)
\param [format] IN (Optional) Specifies the pipe-separated formats of variables to look for (e.g. MONYY|DATE|DAY)


\details
This macro will return the names of the columns of a table of a given type (Character, Numeric or both) and whose name matches a given pattern and format matches a given format.

\ingroup misc
\author  SAS Institute Inc.
\date    2016
*/

%macro rsk_getvarlist(dsName, pattern =, type =, format =) / minoperator;

   %local
      dsid
      regex_pattern
      varlist
      varname
      vartype
      i
   ;

   %if %bquote(&dsName.) eq %bquote() %then %do;
      %put getVariableList > Data set name is missing.;
      %return;
   %end;

   /* If no type is selected then look for both character and numeric variables */
   %if(%length(&type.) = 0) %then
      %let type = N C;

   /* If no specific pattern is provided then look for all variables of the chosen type */
   %if(%sysevalf(%superq(pattern)=, boolean)) %then
      %let pattern = *;

   /* If no specific format is provided then look for all variables of the chosen type */
   %if(%sysevalf(%superq(format)=, boolean)) %then
      %let format = *;

   /* Convert the pattern to regex style */
   %let regex_pattern = %sysfunc(tranwrd(&pattern., *, (.*)));

   /* Convert the format to regex style */
   %let regex_format = %sysfunc(tranwrd(&format., *, (.*)));

   %let dsid = %sysfunc(open(&dsName.));
   %if &dsid. > 0 %then %do;
      %do i = 1 %to %rsk_getattr(&dsName., NVARS);
         %let varname = %sysfunc(varname(&dsid.,&i.));
         %let vartype = %sysfunc(vartype(&dsid.,&i.));
         %let varfmt = %sysfunc(varfmt(&dsid.,&i.));
         /* Return the variable if it matches the required type/pattern/format */
         %if(&vartype. in &type.
             and %sysfunc(prxmatch(/&regex_pattern./i, &varname.))
             and %sysfunc(prxmatch(/^&regex_format./i, &varfmt.))
            ) %then &varname.;
      %end;
      %let dsid = %sysfunc(close(&dsid.));
   %end;
   %else %do;
      %rsk_print_msg(rsk_ds_cannot_open_nr, &dsName);
      %put %sysfunc(sysmsg());
   %end;

%mend;