/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
\file 
\anchor rsk_copy_conf_table
\brief Filters a given data set.

\details

\param [in]  DS           : mapping data set name.
\param [in]  FILTER_VAR   : name of the active filter variable.
\param [in]  FILTER_VALUE : value of the active filter variable.
\param [in]  OUTLIB       : the output library; default value is RD_CONF.

\n

\ingroup CommonAnalytics utilities
\author  SAS Institute Inc.
\date    2014
*/
%macro rsk_copy_conf_table(DS           = ,
                           FILTER_VAR   = ,
                           FILTER_VALUE = ,
                           OUTLIB       = RD_CONF);

   %rsk_trace(Entry);

   data &OUTLIB..&DS;
      set RD_CONF.&DS;
      %if "&FILTER_VAR" NE "" %then %do;
        where &FILTER_VAR= "&FILTER_VALUE"
      %end;
   ;
   run;

   %rsk_trace(Exit);

%mend rsk_copy_conf_table;
