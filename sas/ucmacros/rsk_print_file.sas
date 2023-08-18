/*
 Copyright (C) 2020-2023 SAS Institute Inc. Cary, NC, USA
*/

/*!
\file 
\anchor rsk_print_file
\brief   Prints the content of a file to the log

\param [in] file Path to the file 
\param [in] title (Optional) Title printed at the top. Use AUTO for an auto generated title (Default: AUTO)
\param [in] logSeverity The log severity (NOTE/WARNING/ERROR) for the case here there are issues with opening or reading the file. (Default: WARNING)

\author  SAS Institute Inc.
\date    2023
*/
%macro rsk_print_file(file =
                      , title = AUTO
                      , logSeverity = WARNING
                      );
   %local
      rc_fread
      fref
      fid
      str
      rc
   ;
   
   /* Set the default title */
   %if %sysevalf(%superq(title) = AUTO, boolean) %then
      %let title = File: &file.;
      
   /* Assign the filename */
   %let fref = __fref__;
   %let rc = %sysfunc(filename(fref, &file.));
   %if(&rc. = 0) %then %do;
      /* Open the file */
      %let fid = %sysfunc(fopen(&fref.));
      %if(&fid. > 0) %then %do;
         /* Print the title to the log */
         %put NOTE: - ---------------------------------------------------- -;
         %if %sysevalf(%superq(title) ne, boolean) %then %do;
            %put NOTE: &title.;
         %end;
         %put NOTE: - ---------------------------------------------------- -;
         /* Set the file separator to be CR('0D'x) or LF('0A'x), forcing fread to read the entire line */
         %let rc = %sysfunc(fsep(&fid.,0D0A,x));
         %let rc_fread = 0;
         /* Loop through all records */
         %do %while(&rc_fread. = 0);
            /* Read a record to the file data buffer */
            %let rc_fread = %sysfunc(fread(&fid.));
            %if(&rc_fread. = 0) %then %do;
               %let str =;
               /* Copy the content of the file data buffer to the STR variable */
               %let rc = %sysfunc(fget(&fid., str));
               /* Print the content of the STR variable to the log */
               %put %superq(str);
            %end; /* %if(&rc_fread. = 0) */
         %end; /* Loop through all records */
         /* Close the file */
         %let rc = %sysfunc(fclose(&fid.));
         %put NOTE: - ---------------------------------------------------- -;
         %put;
         %put;
      %end; /* %if(&fid. > 0) */
      %else %do;
         %put &logSeverity.: Could not open file &file.;
      %end;
      /* Deassign the filename */
      %let rc = %sysfunc(filename(fref));
   %end; /* %if(&rc. = 0) */
   %else %do;
      %put &logSeverity.: Could not assign a filename to &file.;
   %end;
%mend;