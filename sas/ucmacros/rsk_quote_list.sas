/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file 
\anchor rsk_quote_list
   \brief The macro quotes a list of words separated by spaces.

   <b> Identified Inputs </b>

   \param[in]  list              a list of words delimited by spaces

   \param[in]  quote             the quote character to be used
                                 (default is single quote)

   \param[in]  dlm               the delimiter to be used (default is comma)

   \param[in]  case              make words upper/lower case (UP/LOW/) (default is UP)

   \ingroup CommonAnalytics

   \author SAS Institute INC.
   \date 2015

 */
%macro rsk_quote_list(LIST  =,
                      QUOTE =%str(%'),
                      DLM   =%str(,),
                      CASE  =UP);

   %local i TLIST QLIST;

   %let TLIST=%bquote(&LIST);

   %if %upcase(&CASE) = UP %then
      %let TLIST = %qupcase(&TLIST);
   %else %if %upcase(&CASE) = LOW %then
      %let TLIST = %qlowcase(&TLIST);

   %let i=1;

   %do %while(%length(%qscan(&TLIST, &i, %str( ))) GT 0);
      %if %length(&QLIST) EQ 0 %then %do;
         %let QLIST=&QUOTE.%qscan(&TLIST, &i, %str( ))&QUOTE;
      %end;
      %else %do;
         %let QLIST=&QLIST.&QUOTE.%qscan(&TLIST, &i, %str( ))&QUOTE;
      %end;

      %let i=%eval(&i + 1);

      %if %length(%qscan(&TLIST, &i, %str( ))) GT 0 %then %do;
         %let QLIST=&QLIST.&DLM;
      %end;

   %end;

   %unquote(&QLIST)

%mend rsk_quote_list;
