/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
\file
\anchor rsk_delete_file
\brief Deletes a specified file.

\details

\param [in]  FILENAME : file to be deleted.

\n

\ingroup CommonAnalytics utilities
\author  SAS Institute Inc.
\date    2014
*/
%macro rsk_delete_file(FILENAME);
%rsk_trace(Entry);
    %local rc;
    %let rc=9999;

    filename _T "&FILENAME";
    %let rc = %sysfunc(fdelete(_T));
    filename _T clear;

    %if &rc. NE 0 %then %do;
        %local err_msg;
        %let err_msg=;
        %let err_msg= %sysfunc(sysmsg());
        %put &err_msg.;
        %put ERROR: Directory %bquote(&FILENAME.) cannot be deleted successfully.;
        %abort;
    %end;

%rsk_trace(Exit);

%mend;




