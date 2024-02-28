/*
 Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/**
\file
\anchor rsk_alloc_lib_to_dir
\brief Allocates a library to the provided directory.

\details

\param [in]  LIBREF             : library name.
\param [in]  DIR                : directory.
\param [in]  LIB_OPTIONS        : library option.
\param [in]  ENSURE_DIR         : verification of directory flag.
\param [in]  ONLY_IF_DIR_EXISTS : directory exist flag.

\n

\ingroup CommonAnalytics utilities
\author  SAS Institute Inc.
\date    2014
*/
%macro rsk_alloc_lib_to_dir(LIBREF            =,
                            DIR               =,
                            LIB_OPTIONS       =,
                            ENSURE_DIR        =N,
                            ONLY_IF_DIR_EXISTS=N);

%rsk_trace(Entry);

   %if %upcase(&ONLY_IF_DIR_EXISTS) eq Y %then %do;
      %if %rsk_DIR_exists(DIR=&DIR) eq 0 %then %do;
         %return;
      %end;

      %let ENSURE_DIR=N;
   %end;

   %if &ENSURE_DIR eq Y %then %do;
      %rsk_mkdirs_and_verify(&DIR);
      %if %rsk_error_occurred %then %abort;
   %end;

   %if &LIBREF ne %then %do;
      %rsk_libname(stmt=libname &LIBREF.  "&DIR" &LIB_OPTIONS.);
      %if %rsk_error_occurred %then %abort;
   %end;

%rsk_trace(Exit);

%mend;
