/*
 Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/**
\file 
\anchor rsk_chg_string_delimter
\brief Changes the delimiter within a string.

\details

\param [in]  LIST         : a list of words.
\param [in]  EXISTING_DLM : delimiter used in the input string LIST.
\param [in]  NEW_DLM      : delimiter to be used, default is comma; if the space delimiter is wanted, do new_dlm=%str( )
\param [in]  CASE         : make words upper/lower case (UP/LOW/), default is UP; if no change in case is wanted, leave the variable CASE as blank, i.e. case=

\n

\ingroup CommonAnalytics utilities
\author  SAS Institute Inc.
\date    2008
*/
%macro rsk_chg_string_delimter(list=, existing_dlm=%str( ), new_dlm=%str(,), case=UP);

    %local i tlist qlist;

    %let tlist=%bquote(&list);

    %let i=1;

    %do %while(%length(%qscan(&tlist, &i, &existing_dlm.)) GT 0);

        %let qlist=&qlist.%qscan(&tlist, &i, &existing_dlm.);

        %let i=%eval(&i + 1);

        %if %length(%qscan(&tlist, &i, &existing_dlm.)) GT 0 %then %do;
            %let qlist=&qlist.&new_dlm;
        %end;
    %end;

    %if %upcase(&case) = UP %then
        %let qlist = %qupcase(&qlist);
    %else %if %upcase(&case) = LOW %then
        %let qlist = %qlowcase(&qlist);

    %unquote(&qlist)

%mend rsk_chg_string_delimter;
