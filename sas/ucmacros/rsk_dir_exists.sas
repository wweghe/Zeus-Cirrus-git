/*
 Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/**
\file 
\anchor rsk_dir_exists
\brief Verifies whether a given directory exists.

\details

\param [in]  DIR : directory.

Returned values
---------------

- Returns 1 if the given directory exists, 0 otherwise.
- If no directory is given, 0 is returned.
- Blank is not interpreted as any kind of "current directory".

\n

\ingroup CommonAnalytics utilities
\author  SAS Institute Inc.
\date    2014
*/
 %macro rsk_dir_exists(DIR=);

   %sysfunc(fileexist(&DIR))

%mend;
