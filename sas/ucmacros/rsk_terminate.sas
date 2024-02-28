/* Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA. */


/*!
\file
\anchor rsk_terminate


\brief   Causes SAS job to exit with an error but does not terminate the stored process server or workspace server (if running in that environment).

\ingroup utilities

\author  SAS Institute Inc.

\date    2015

\details
Causes SAS job to exit with an error but does not terminate the stored process server or workspace server (if running in that environment).
*/

%macro rsk_terminate(msg =);

   %if %sysevalf(%superq(msg) =, boolean) %then
      %let msg = A system fatal error has occurred. No further processing will be executed.;

   /* Print in the log that a fatal error has occurred and we are stopping execution. */
   %put ERROR: &msg.;

   /* Get the macro call stack to put in the log before aborting. */
   %local stack;
   %let stack=;
   %rsk_terminate_get_macro_stack(o_call_stack_var=stack);

   /* we are about to abort this sas session, help this error msg show up well in the log by disabling some logging options */
   %put >>>--------------------------------------------------------------------------;
   %put >>> Abending at macro call stack.; ;
   %put >>> &stack. ;
   %put >>>--------------------------------------------------------------------------;
   %let SYSCC=9999;
   /* Cause this SAS code to exit. */
   %abort;

%mend;


/*****************************************************************************
 * NAME:          rsk_terminate_get_macro_stack
 *
 * PURPOSE: Returns the call stack trace of the macro calls made at this point.
 *
 * USAGE: Only used within rsk_terminate -- do not use outside cirr_terminate.
 *
 * NOTES:   An important BAD side-effect of this macro, is that the
 *          SAS log is diverted to a temporary location and can never be
 *          reliably returned to it's original location.
 *          So, only use this macro in cases where that is not a problem.
 *
 *****************************************************************************/
%macro rsk_terminate_get_macro_stack(O_CALL_STACK_VAR=);

   /* Save settings of options we care about  */

   %local savemlogic savenest savenotes savesource savesource2 savels;

   %let savemlogic=%sysfunc(getoption(MLOGIC));
   %let savenest=%sysfunc(getoption(MLOGICNEST));
   %let savenotes=%sysfunc(getoption(NOTES));
   %let savesource=%sysfunc(getoption(SOURCE));
   %let savesource2=%sysfunc(getoption(SOURCE2));
   %let savels=%sysfunc(getoption(LINESIZE, KEYWORD));

   options mlogic mlogicnest nonotes nosource nosource2 ls=256;

   /* Send the log to a temporary file in the work directory */

   filename temp temp lrecl=1024;

   proc printto log=temp new;
   run;

   /* Invoke a dummy macro so that the MLOGICNEST option has
      something to work with (has no effect on current macro) */

   %core_term_get_macro_stack_x;

   proc printto;
   run;

   /* Read the log to get the call stack information */

   data _NULL_;
      infile temp truncover lrecl=1024;
      input record $1024.;
      if (_N_ = 1) then do;
         re = prxparse("/MLOGIC\((.*)\.CIRR_TERMINATE_GET_MACRO_STACK\./");
         retain re;
      end;
      if (prxmatch(re, record)) then do;
         length callstack $512;
         length dtstring $24;
         callstack = lowcase(prxposn(re, 1, record));
         callstack = TRANSTRN(callstack, ".", "-> ");
         call symput("&O_CALL_STACK_VAR", callstack);
         stop;
      end;
   run;

   /* Delete the temporary file */

   filename temp clear;

   /* Restore options we may have changed */

   options &savemlogic &savenest &savenotes &savesource &savesource2 &savels;

%mend ;

/* Dummy macro definition - do not delete this - needed by the main macro */
%macro core_term_get_macro_stack_x;
%mend ;

