/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor rsk_get_unique_filename

   \brief   Provides a unique filename within a given OS path.

   \param [in] path:                The path to search for a unique filename.
   \param [in] ext [optional]:      File extension.
   \param [in] exclude [optional]:  List of names excluded from potential output (space delimited).
   \param [in] return [optional]:   Whether to return the <filename> only (FILENAME) or the full name <path><filename><ext>
                                       (FULLNAME). Default: FILENAME.

   \details
   Given a OS path returns a unique valid <filename> (return=FILENAME) or full name <path><filename><ext> (return=FULLNAME).
   If <ext> is not empty tests for <filename>.<ext> .
   Fills in directory separator and dot before the extension automatically if not present.
   Does NOT test if the given path exists. Does NOT create the file.
   Excludes any <filename> ocuring as exclude parameter.

   <b>Example:</b>

   \code
      %let nm1 = %rsk_get_unique_filename(path = /tmp/, ext = txt, return = fullname);
      %let nm2 = %rsk_get_unique_filename(path = /tmp/, ext = txt, exclude=&nm1, return = fullname);
   \endcode

   \ingroup mrm

   \author  SAS Institute Inc.
   \date    2022
*/

%macro rsk_get_unique_filename(path=, ext=, exclude=, return=filename) / minoperator;
   %local __filename __spath __sext __fullname;

   %let __spath = %sysfunc(strip(%superq(path)));
   %if %sysevalf(%superq(__spath) eq, boolean) %then %do;
      %put ERROR: Empty path.;
      %return;
   %end;

   %if not %sysfunc(prxmatch(/^(?i)[\s]*(filename|fullname)[\s]*$/, %superq(return))) %then %do;
      %put ERROR: Wrong RETURN parameter. [FILENAME | FULLNAME] allowed.;
      %return;
   %end;

   %let __sext = %sysfunc(strip(%superq(ext)));
   %if %sysevalf(%superq(__sext) ne, boolean) %then %do;
      %if %substr(%superq(__sext), 1, 1) ne %str(.) %then %let __sext = %str(.)%superq(__sext);
   %end;

   %if &SYSSCP. = WIN %then %let __sep = \;
   %else %let __sep = /;

   %if %qsubstr(%superq(__spath), %length(%superq(__spath))) ne %superq(__sep) %then %let __spath = %superq(__spath)&__sep.;

   %let __filename = _%sysfunc(putn(%sysfunc(rand(integer, 0, 2147483647)), hex8.));
   %let __fullname = %superq(__spath)%superq(__filename)%superq(__sext);
   %if %sysevalf(%superq(exclude) ne, boolean) %then %do;
      %do %while((%rsk_fileexist(%superq(__fullname))) or (%superq(__filename) in %superq(exclude)));
         %let __filename = _%sysfunc(putn(%sysfunc(rand(integer, 0, 2147483647)), hex8.));
      %end;
   %end;
   %else %do;
      %do %while(%rsk_fileexist(%superq(__fullname)));
         %let __filename = _%sysfunc(putn(%sysfunc(rand(integer, 0, 2147483647)), hex8.));
      %end;
   %end;

   %if %qupcase(&return.) = FILENAME %then &__filename.;
   %else &__fullname.;

%mend;