/*
 Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/**
\file 
\anchor rsk_varlist_nm_only
\brief Queries in a base data set for given column names.

\details

\param [in]  DS        : base data set where to look for the column information.
\param [in]  EXCEPTION : columns to be excluded, separated by space.
\param [in]  DLM       : delimiter used to separate var name in the output string list; default value is space.

OUTPUT
------

Macro variable VARLIST_NM with the list of the variables but the exception column name.

USE
---

%rsk_varlist_nm_only(ds=test, exception=INSTID);

%put &varlist_nm; (this gives the list "var1 var2 ")

\n

\ingroup CommonAnalytics utilities
\author  SAS Institute Inc.
\date    2014
*/
%macro rsk_varlist_nm_only(DS       =,
                           EXCEPTION=,
                           DLM      =%str( ));
%rsk_trace(Entry);

/*if exception is not empty, quote it*/
%if &EXCEPTION NE %then %do;
   %let EXCEPTION=%rsk_quote_list(list=&EXCEPTION, case=UP);
%end;

%global VARLIST_NM;

%if %index(&DS,.) %then
%do;
    %let LIBNAME=%scan(%upcase(&DS),1,.);
    %let MEMNAME=%scan(%upcase(&DS),2,.);
%end;
%else
%do;
   %let LIBNAME=WORK;
   %let MEMNAME=%scan(%upcase(&DS),1,.);
%end;

%let VARLIST_NM=;

proc sql noprint;
    select distinct name  into :VARLIST_NM separated by "&DLM."
    from dictionary.columns
    where libname="%upcase(&LIBNAME)" and memname="%upcase(&MEMNAME)"
       %if &EXCEPTION NE %then %do;
          and upcase(name) NOT IN (&EXCEPTION);
        %end;
        %else %do;
        ;
        %end;
quit;

%rsk_trace(Exit);
%mend rsk_varlist_nm_only;
