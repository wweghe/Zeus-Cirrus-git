/*
 Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file 
\anchor rsk_varexist
   \brief The macro checks if a variable exists in a data set.

   \details Returns 0 if the variable does NOT exist, and return the position of the var if it does.

   <b> Identified Inputs </b>

   \param[in]  ds              data set name

   \param[in]  var             variable name

   \ingroup CommonAnalytics

   \author SAS Institute INC.
   \date 2015

 */

%macro rsk_varexist(DS, VAR);

    %local DSID RC;

    %if %sysevalf(%superq(VAR)=,boolean) %then %do;
            0
            %goto exit;
    %end;

    %let DSID=%sysfunc(open(&DS,is));

    %if &DSID EQ 0 %then %do;
          %put %sysfunc(sysmsg());
        0
    %end;
    %else %do;
        %sysfunc(varnum(&DSID,&VAR))
        %let RC=%sysfunc(close(&DSID));
    %end;
%EXIT:
%mend;
