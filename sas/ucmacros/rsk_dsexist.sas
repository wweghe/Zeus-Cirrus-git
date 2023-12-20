/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
\file 
\anchor rsk_dsexist
\brief Verifies whether a SAS data set or view exists.

\details

\param [in]  DS : given data set.

Output
------

- Returns 0 if the given DS does not exist
- Returns 1 if the given DS exists

\n

\ingroup CommonAnalytics utilities
\author  SAS Institute Inc.
\date    2014
*/
%macro rsk_dsexist(DS);


   %if (%sysfunc(exist(&DS)) ne 1)
   and (%sysfunc(exist(&DS, VIEW)) ne 1) %then %do;
      0
   %end;
   %else %do;
      1
   %end;


%mend rsk_dsexist;
