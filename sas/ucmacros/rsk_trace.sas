/*
 Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file 
\anchor rsk_trace
   \brief The macro writes macro call stack and date/time information to the log.

   \details

   <b> Identified Inputs </b>
   \param[in]  msg              Any message, such as Entry or Exit

   \ingroup CommonAnalytics

   \author SAS Institute INC.
   \date 2015

 */
%macro rsk_trace(MSG);

   %global TRACE;
   %if (%bquote(&TRACE) ne Y) %then
      %return;

   %if %upcase(&MSG) eq ENTRY %then
    %put NOTE: Macro started execution at %sysfunc(time(),timeampm.) on %sysfunc(date(),worddate.).;
   %if %upcase(&MSG) eq EXIT %then
    %put NOTE: Macro ended execution at %sysfunc(time(),timeampm.) on %sysfunc(date(),worddate.).;

   /* Save settings of options we care about */

   %local SAVEMLOGIC SAVENEST;

   %let SAVEMLOGIC=%sysfunc(getoption(MLOGIC));
   %let SAVENEST=%sysfunc(getoption(MLOGICNEST));

   /* Force call stack to be included */

   options MLOGIC MLOGICNEST;

   /* Print the message in the log */

   %rsk_trace1(&MSG);

   /* Restore options we may have changed */

   options &SAVEMLOGIC &SAVENEST;

%mend rsk_trace;

/*- a dummy macro in order to trigger the trace -*/
%macro rsk_trace1(MSG);
   %put &MSG;
%mend rsk_trace1;
