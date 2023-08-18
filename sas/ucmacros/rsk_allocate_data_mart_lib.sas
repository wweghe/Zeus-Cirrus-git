/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
\file 
\anchor rsk_allocate_data_mart_lib
\brief Allocates a a libname to a directory.

\details

\param [in]  LIBREF   : library name.
\param [in]  DIR      : directory.
\param [in]  ALLOCHOW : rights assigned to the library.

\n

Possible values for the parameter ALLOCHOW
------------------------------------------

 - RW        : allocate libref read-write
 - RO        : allocate libref read-only (does not ensure the directory exists)
 - RO_ENSURE : allocate libref read-only (ensures the directory exists)
 - NA        : not allocated for this usage

\n

\ingroup CommonAnalytics utilities
\author  SAS Institute Inc.
\date    2014
*/
%macro rsk_allocate_data_mart_lib(libref   =,
                                  dir      =,
                                  allochow =);

   %rsk_trace(Entry);

   %if &allochow. eq NA %then %do;
      %rmi_sii_terminate(key=rsk_lib_prop_not_specified);
   %end;

   %local lib_options ensure_dir;
   %if &allochow. eq RO %then %do;  /* allocate read-only, do not ensure the dir */
      %let lib_options=%str(access=readonly);
      %let ensure_dir=N;  /* we only ensure the dir when we alloc RW or RO_ENSURE */
   %end;
   %else %if &allochow. eq RO_ENSURE %then %do;  /* allocate read-only, do not ensure the dir */
      %let lib_options=%str(access=readonly);
      %let ensure_dir=Y;
   %end;
   %else %do;  /* allocate lib read/write, so ensure the dir */
      %let lib_options=%str( );
      %let ensure_dir=Y;
   %end;

   %rsk_alloc_lib_to_dir(LIBREF     = &libref.,
                         DIR        = &dir.,
                         LIB_OPTIONS= &lib_options.,
                         ENSURE_DIR = &ensure_dir.);

   %rsk_trace(Exit);

   %mend;
