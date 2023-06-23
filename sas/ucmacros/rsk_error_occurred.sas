/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
\file 
\anchor rsk_error_occurred
\brief Verifies whether an error occurred in the process.

\details

Output
------

- Returns 1 if an error occurred
- Returns 0 otherwise

\n

\ingroup CommonAnalytics utilities
\author  SAS Institute Inc.
\date    2014
*/
%macro rsk_error_occurred;
   %if %symexist(rsk_term_message) %then %do;
      %if "%trim(%left(&rsk_term_message))" ne ""  %then %do;
         1
       %return;
     %end;
   %end;
0
%mend;
