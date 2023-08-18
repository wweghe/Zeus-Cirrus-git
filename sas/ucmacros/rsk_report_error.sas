/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
\file
\anchor rsk_report_error
\brief Reports when an occur occurs.

\details

\param [in]  ERROR : error key.
\param [in]  DATA  : data.

\n

\ingroup CommonAnalytics utilities
\author  SAS Institute Inc.
\date    2014
*/
%macro rsk_report_error(ERROR=, DATA=);

   %if "&ERROR" ne "" %then %do;
      %if %symexist(rsk_term_message) %then %do;
         %let rsk_term_message = &ERROR;
      %end;
      %abort;
   %end;

%mend;
