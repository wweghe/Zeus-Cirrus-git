/*
 Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/**
\file 
\anchor rsk_get_msg
\brief Looks up a message by key in the message data set and writes it to the input buffer.

\details

\param [in]  KEY : key determining the look up for conditions.
\param [in]  S1  : substitution parameter.
\param [in]  S2  : substitution parameter.
\param [in]  S3  : substitution parameter.
\param [in]  S4  : substitution parameter.
\param [in]  S5  : substitution parameter.
\param [in]  S6  : substitution parameter.
\param [in]  S7  : substitution parameter.

\note The substitution parameters s1-s7 are all optional.

\n

\ingroup CommonAnalytics utilities
\author  SAS Institute Inc.
\date    2014
*/
%macro rsk_get_msg(KEY, s1, s2, s3, s4, s5, s6, s7);

   %* Force the key to be lowercase ;

   %let KEY=%lowcase(&KEY);

   %* Figure out the message file name ;

   %local msgfile;
   %if (%substr(&KEY, 1, 7) = core_rpt ) %then %do;
      %let msgfile = SASHELP.IRMLABELS;
   %end;
   %else %if (%substr(&KEY, 1, 5) eq irmif) %then %do;
      %let msgfile = IRMHELP.IRMIFUTILMSG;
   %end;
   %else %if (%substr(&key, 1, 3) eq irm) %then %do;
      %let msgfile = SASHELP.IRMCALCMSG;
   %end;
   %else %if (%substr(&KEY, 1, 3) eq rsk) %then %do;
      %let msgfile = SASHELP.IRMUTILMSG;
   %end;
   %else %do;
      %put WARNING: Cannot determine message file for KEY=&KEY;
      %return;
   %end;

   %* Verify that the message file exists ;

   %if (%sysfunc(exist(&msgfile)) = 0) %then %do;
      %put WARNING: Message file &msgfile does not exist;
      %return;
   %end;

   %* Retrieve the message text ;

   %local text;
   %if (%bquote(&S7) ne ) %then %do;
      %let text = %sysfunc(sasmsg(&msgfile, &KEY, NOQUOTE, &s1, &s2, &s3, &s4, &s5, &s6, &s7));
   %end;
   %else %if (%bquote(&s6) ne ) %then %do;
      %let text = %sysfunc(sasmsg(&msgfile, &KEY, NOQUOTE, &s1, &s2, &s3, &s4, &s5, &s6));
   %end;
   %else %if (%bquote(&s5) ne ) %then %do;
      %let text = %sysfunc(sasmsg(&msgfile, &KEY, NOQUOTE, &s1, &s2, &s3, &s4, &s5));
   %end;
   %else %if (%bquote(&s4) ne ) %then %do;
      %let text = %sysfunc(sasmsg(&msgfile, &KEY, NOQUOTE, &s1, &s2, &s3, &s4));
   %end;
   %else %if (%bquote(&s3) ne ) %then %do;
      %let text = %sysfunc(sasmsg(&msgfile, &KEY, NOQUOTE, &s1, &s2, &s3));
   %end;
   %else %if (%bquote(&s2) ne ) %then %do;
      %let text = %sysfunc(sasmsg(&msgfile, &KEY, NOQUOTE, &s1, &s2));
   %end;
   %else %if (%bquote(&s1) ne ) %then %do;
      %let text = %sysfunc(sasmsg(&msgfile, &KEY, NOQUOTE, &s1));
   %end;
   %else %do;
      %let text = %sysfunc(sasmsg(&msgfile, &KEY, NOQUOTE));
   %end;

   %* Write the text to the input buffer ;

   &text

%mend rsk_get_msg;