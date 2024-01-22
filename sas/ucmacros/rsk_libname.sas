/*
 Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/**
\file
\anchor rsk_libname
\brief Runs a given libname statement and performs a call to \link rsk_abend.sas \endlink if the libname fails to be assigned.

\details

\param [in]  STMT : libname statement to run.

\note In order to avoid calling the macro \link rsk_abend.sas \endlink every time the libname statement fails,
      the libname statement must be manually executed - without using the macro \link rsk_libname.sas \endlink.

\n

\ingroup CommonAnalytics utilities
\author  SAS Institute Inc.
\date    2014
*/
%macro rsk_libname(STMT=);

   /* Say that we're going to run the libname statement then run it. */
   %rsk_trace();
   %rsk_trace(Attempting libname statement: %bquote(&stmt;));


   /* Now execute the given statement */
   &stmt;

   /* If the libname statement failed, then put out a msg */
   %if &syslibrc. ne 0 %then %do;
      %put ERROR: The following libref statement failed: %bquote(&STMT.).;
      %abort;
   %end;

%mend;
