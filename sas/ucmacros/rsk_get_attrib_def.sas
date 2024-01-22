/*
 Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/*!
\file
\anchor rsk_get_attrib_def
\brief   Retrieve columns definition from a dataset

\param [in] ds_in Input table
\param [in] keep_vars (Optional) list of variables to keep
\param [in] drop_vars (Optional) list of variables to drop
\param [in] rename_vars (Optional) Rename statment. Syntax: old_name1 = new_name1 [old_nameN = new_nameN]

\details

This macro will query the provided input table and return a string containing the columns definition (name, label, length, format, informat).
The returned string can be used with the Data Step ATTRIB statement.

Example:

The following code will create a table with a structure derived from SASHELP.CARS

\code
option mprint;
data test;
   attrib
      %rsk_get_attrib_def(ds_in = sashelp.cars
                          , keep_vars = make model driveTrain
                          , rename_vars = model = car_model
                          )
   ;
   stop;
run;

\endcode

Here is the execution log:

\verbatim
   1933  data test;
   1934     attrib
   1935        %rsk_get_attrib_def(ds_in = sashelp.cars
   1936                            , keep_vars = make model driveTrain
   1937                            , rename_vars = model = car_model
   1938                            )
   MPRINT(RSK_GET_ATTRIB_DEF):   Make length = $13.
   MPRINT(RSK_GET_ATTRIB_DEF):   car_model length = $40.
   MPRINT(RSK_GET_ATTRIB_DEF):   DriveTrain length = $5.
   1939     ;
   1940     stop;
   1941  run;
\endverbatim


\author  SAS Institute Inc.
\date    2018
*/
%macro rsk_get_attrib_def(ds_in =, keep_vars =, drop_vars =, rename_vars =) / minoperator;
   %local
      attrib_def
      nvars
      process_var_flg
      varname
      vartype
      varlen
      varlabel
      varfmt
      varinfmt
      dsid
      rc
      i
   ;

   /* Get number of variables */
   %let nvars = %rsk_attrn(&ds_in., nvars);

   %if(&nvars. > 0) %then %do;
      /*  Open Dataset */
      %let dsid = %sysfunc(open(&ds_in.));

      /* Loop through all variables */
      %do i = 1 %to &nvars.;
         /* Get variable name */
         %let varname = %sysfunc(varname(&dsid., &i.));
         %let process_var_flg = N;
         /* Check if the KEEP_VARS list has been specified */
         %if(%sysevalf(%superq(keep_vars) ne, boolean)) %then %do;
            /* Process only variables that are included in the KEEP_VARS list */
            %if(%lowcase(&varname.) in %lowcase(&keep_vars.)) %then
               %let process_var_flg = Y;
         %end;
         /* Check if the DROP_VARS list has been specified */
         %else %if(%sysevalf(%superq(drop_vars) ne, boolean)) %then %do;
            /* Process only variables that are not included in the DROP_VARS list */
            %if(not (%lowcase(&varname.) in %lowcase(&drop_vars.))) %then
               %let process_var_flg = Y;
         %end;
         %else
            /* No Keep/Drop statement: process all variables */
            %let process_var_flg = Y;

         %if(&process_var_flg. = Y) %then %do;

            /* Check if we need to do any renaming */
            %if(%sysevalf(%superq(rename_vars) ne, boolean)) %then %do;
               /* Rename the variable if it is in the RENAME_VARS list */
               %if(%sysfunc(prxmatch(/\b&varname.\s*=\s*(\w+)/i, &rename_vars.))) %then
                  %let varname = %sysfunc(prxchange(s/.*\b&varname.\s*=\s*(\w+).*/$1/i, -1, &rename_vars.));
            %end;

            /* Get variable info (special prcessing for VARLABEL which may contain quotes) */
            %let vartype = %sysfunc(vartype(&dsid., &i.));
            %let varlen = %sysfunc(varlen(&dsid., &i.));
            %let varlabel = "%sysfunc(varlabel(&dsid., &i.))";
            %let varfmt = %sysfunc(varfmt(&dsid., &i.));
            %let varinfmt = %sysfunc(varinfmt(&dsid., &i.));

            /* Add variable definition to the output */
            %let attrib_def = &varname. length = %sysfunc(ifc(&vartype. = C, $&varlen.., &varlen..));
            %if(&varlabel. ne "") %then
               %let attrib_def = &attrib_def. label = &varlabel.;
            %if(%sysevalf(%superq(varfmt) ne, boolean)) %then
               %let attrib_def = &attrib_def. format = &varfmt.;
            %if(%sysevalf(%superq(varinfmt) ne, boolean)) %then
               %let attrib_def = &attrib_def. informat = &varinfmt.;

            &attrib_def.
         %end;
      %end;
      /* Close Dataset */
      %let rc = %sysfunc(close(&dsid.));

   %end;
   %else
      %put WARNING: Could not retrieve the number of variables from input dataset &ds_in..;
%mend;
