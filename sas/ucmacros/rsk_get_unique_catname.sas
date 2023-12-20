/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor rsk_get_unique_catname

   \brief   Provides a unique SAS dataset name.

   \param [in] path:                Path to catalog to be unique.
   \param [in] exclude [optional]:  List of names excluded from potential output (space delimited).
   \param [in] return:              Whether to return complete catalog path (PATH)
                                       or just the generated unique suffix (NAME).

   \details
   Given a path to catalog returns a unique valid catalog name. Excludes any name ocuring as exclude parameter.


   <b>Example:</b>

   \code
      %let nm1 = %rsk_get_unique_catname(WORK);
      %let nm2 = %rsk_get_unique_catname(WORK, exclude=&nm1);
   \endcode

   \ingroup mrm

   \author  SAS Institute Inc.
   \date    2022
*/

%macro rsk_get_unique_catname(path, exclude=, return=PATH) / minoperator;

   %if not %sysfunc(prxmatch(/^(?i)[\s]*(path|name)[\s]*$/, %superq(return))) %then %do;
      %put ERROR: Wrong RETURN parameter. [PATH | NAME] allowed.;
      %return;
   %end;

   %local __catnm;
   %let __catnm = _%sysfunc(putn(%sysfunc(rand(integer, 0, 2147483647)), hex8.));
   %if %sysevalf(%superq(exclude) ne, boolean) %then %do;
      %do %while((%rsk_dsexist(&path..&__catnm.)) or (&__catnm. in %superq(exclude)));
         %let __catnm = _%sysfunc(putn(%sysfunc(rand(integer, 0, 2147483647)), hex8.));
      %end;
   %end;
   %else %do;
      %do %while(%rsk_dsexist(&path..&__catnm.));
         %let __catnm = _%sysfunc(putn(%sysfunc(rand(integer, 0, 2147483647)), hex8.));
      %end;
   %end;

   %if %qupcase(&return.) = PATH %then &path..&__catnm.;
   %else &__catnm.;
%mend;
