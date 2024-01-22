/* Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA */

/*!
\file
\anchor rsk_set_logging_options
\brief   The macro rsk_set_logging_options .sas sets SAS session logging based on the log_level macrovariable

\details
<b> Log verbosity </b>
Depending on the value of the <i>LOG_LEVEL</i> macro variable, the solution applies a specific verbosity level to the node log (via the SAS OPTIONS statement)
- LOG_LEVEL &leq; 1 -> notes
- LOG_LEVEL = 2 -> notes source2 mprint
- LOG_LEVEL = 3 -> notes source2 mprint symbolgen.  &outDebugVar. is set to true.
- LOG_LEVEL = 4 -> notes source2 mprint symbolgen mlogic. &outDebugVar. is set to true.
- LOG_LEVEL &geq; 5 -> notes source2 mprint symbolgen mlogic mprintnest. &outDebugVar. is set to true.

No option is set if the macro variable <i>LOG_LEVEL</i> is empty or doesn't exist.


\ingroup macro utility
\author  SAS Institute Inc.
\date    2018
*/
%macro rsk_set_logging_options (outDebugVar = debug);

   /* outDebugVar cannot be missing. Set a default value */
   %if(%sysevalf(%superq(outDebugVar) =, boolean)) %then
      %let outDebugVar = debug;

   /* Declare the output variable as global if it does not exist */
   %if(not %symexist(&outDebugVar.)) %then
      %global &outDebugVar.;

   %let &outDebugVar. = false;

   /* Check if the LOG_LEVEL macro variable exists */
   %if %symexist(log_level) %then %do;
      option linesize = max;
      /* Check if the LOG_LEVEL macro variable is set */
      %if %sysevalf(%superq(log_level) ne, boolean) %then %do;
         %if &log_level. >= 4 %then
            options notes source2 mprint symbolgen MSGLEVEL=I mlogic fullstimer;
         %else %if &log_level. = 3 %then
            options notes source2 mprint symbolgen MSGLEVEL=I;
         %else %if &log_level. = 2 %then
            options notes source2 macrogen MSGLEVEL=I;
         %else
            options notes;
         ;

         %if &log_level. >= 4 %then
            %let &outDebugVar. = true;
      %end;

   %end;
   %else %do;
      %put NOTE: No logging options applied;
   %end;

%mend;
