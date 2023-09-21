 /*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file 
\anchor rsk_attrn
   \brief The macro returns a numeric attribute of a data set..

   <b> Identified Inputs </b>

   \param[in]  DS                 data set name
   \param[in]  ATTRIB             data set attribute, attrib value in the macro call is returned
                                  POSSIBLE attributes: NOBS - see attrn documentation.
   \param[in]  MSGON              message flag

 \ingroup CommonAnalytics

 \author SAS Institute INC.
 \date 2015

 */
%macro rsk_attrn(DS,
                 ATTRIB,
                 MSGON =YES);

/*returns a numeric ATTRIBute of a data set*/

    %local DSID RC;

    %let DSID=%sysfunc(open(&ds,is));

    %if &DSID EQ 0 %then %do;
        %if &MSGON EQ YES %then %do;
            %local msg;
            %let msg = %sysfunc(sysmsg());
            %put WARNING: Data set %trim(&ds) was not opened due to the following reason:;
            %put &msg;
        %end;
    %end;
    %else %do;
        %sysfunc(attrn(&DSID,&ATTRIB))
        %let RC=%sysfunc(close(&DSID));
    %end;

%mend rsk_attrn;
