/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
\file 
\anchor rsk_delete_dir
\brief Deletes a specified directory.

\details

\param [in]  DIR : directory to be deleted.

\n

\ingroup CommonAnalytics utilities
\author  SAS Institute Inc.
\date    2014
*/
%macro rsk_delete_dir_cb(ID     =,
                         TYPE   =,
                         MEMNAME=,
                         LEVEL  =,
                         PARENT =,
                         CONTEXT=,
                         ARG    =);

   %rsk_delete_file(&CONTEXT/&MEMNAME);

%mend;

%macro rsk_delete_dir(DIR=);

      /*only if dir exists*/
      %if %rsk_dir_exists(dir=&DIR.) %then %do;
       /* delete subdirectories/files */
       %rsk_dirtree_walk(&DIR, maxdepth=-1, callback=rsk_delete_dir_cb);
       /* delete top folder */
       %rsk_delete_file(&DIR);
    %end;

%mend;
