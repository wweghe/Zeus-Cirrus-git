/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
\file 
\anchor rsk_fileexist
\brief Verifies whether a file exists.

\details

\param [in]  FILE : given file.

Output
------

- Returns 0 if the given file does not exist
- Returns 1 if the given file exists

\n

\ingroup CommonAnalytics utilities
\author  SAS Institute Inc.
\date    2020
*/
%macro rsk_fileexist(FILE);


   %if (%sysfunc(fileexist(&FILE)) ne 1) %then %do;
      0
   %end;
   %else %do;
      1
   %end;


%mend rsk_fileexist;
