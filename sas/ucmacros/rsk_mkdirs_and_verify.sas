/*
 Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/**
\file
\anchor rsk_mkdirs_and_verify
\brief

Creates the directory corresponding to a given pathname, including any intermediate non-existent parent directory,
and verifies whether the directory actually exists.

\details

\param [in]  DIR : relative or absolute pathname.

ERROR Message
-------------

When the directory associated with the given pathname fails to be created
(i.e. when the '<em>created</em>' directory '<em>does not exist</em>')
the macro issues an error message and terminates the SAS run.

\n

\ingroup CommonAnalytics utilities
\author  SAS Institute Inc.
\date    2014
*/
%macro rsk_mkdirs_and_verify(DIR);
   %rsk_mkdirs(&DIR);
   %if (%sysfunc(fileexist(&DIR)) = 0) %then %do;
      %put ERROR: Could not create &DIR directory.;
      %abort;
   %end;

%mend rsk_mkdirs_and_verify;
