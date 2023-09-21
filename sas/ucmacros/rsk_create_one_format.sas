 /*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file 
\anchor rsk_create_one_format
   \brief The macro creates a format from a mapping data set, using parameters to determine the format name, the         ;
          key variable(s), the target column, etc. Based on core_create_format.

   <b> Identified Inputs </b>

   \param[in]  dsname            The name of the data set from which the format is built

   \param[in]  fmtname           The format name, default=base table name

   \param[in]  type              Format type (C=Character,N=Numeric) default=C

   \param[in]  key               One or more column names in a comma-separated list; If there are multiple names,
                                 use %str to enclose them in the calling macro, like %str(col1,col2,col3)

   \param[in]  target            The target column, which supplies the values

   \param[in]  other             The label to be used for any other values

   \param[in]  fmtlib            The library in which formats are written

   \param[in]  compress          (Optional) Removes spaces from the source

   \param[in]  key_length        (Optional) Sets lengths of start and end variables (default, $64)

   \ingroup CommonAnalytics

   \author SAS Institute INC.
   \date 2015

 */

%macro rsk_create_one_format(
                        dsname=,
                        fmtname=,
                        type=C,
                        key=,
                        target=,
                        other=MISSING,
                        fmtlib=RD_TEMP,
                        compress=Y,
                        key_length=64
                     );

%rsk_trace(Entry);

   %*------------------------------------------------------------------;
   %* Validate input parameters                                        ;
   %*------------------------------------------------------------------;

   %if (%bquote(&dsname) eq ) %then %do;
      %put ERROR: No DSNAME parameter specified;
      %put ERROR: Data set &dsname is not specified.;
      %rsk_terminate();
   %end;
   %if (%bquote(&fmtname) eq ) %then %do;
      %let fmtname = %scan(&dsname, -1, %str(.));
   %end;
   %if (%sysfunc(exist(&DSNAME)) = 0) %then %do;
      %put ERROR: &DSNAME data set does not exist. Format &FMTNAME not created;
      %put ERROR: Cannot find data set &dsname..;
      %rsk_terminate();
   %end;
   %if (%bquote(&key) eq ) %then %do;
      %put ERROR: No KEY parameter specified;
      %put ERROR: Data set &key key is not found.;
      %rsk_terminate();
   %end;
   %let KEY = %upcase(&KEY);
   %if (%bquote(&target) eq ) %then %do;
      %put ERROR: No TARGET parameter specified;
      %put ERROR: The input parameter &target cannot be left empty.;
      %rsk_terminate();
   %end;
   %let TARGET = %upcase(&TARGET);
   %if (%sysfunc(libref(&FMTLIB)) ne 0) %then %do;.
      %put ERROR: Format library "&FMTLIB" is not found.;
      %rsk_terminate();
   %end;

   %*------------------------------------------------------------------;
   %* Verify that the key name(s) and target column name specified     ;
   %* actually exist in this data set                                  ;
   %*------------------------------------------------------------------;

   %local i rc dsid nvars varname ncommas nkeys keyvar nerrors;
   %let ncommas   = %sysfunc(count(%bquote(&KEY), %str(,)));
   %let nkeys     = %eval(&ncommas + 1);
   %let dsid      = %sysfunc(open(&DSNAME));
   %let NVARS     = %sysfunc(attrn(&DSID, NVARS));
   %let NERRORS   = 0;
   %if (%sysfunc(varnum(&DSID, &TARGET)) = 0) %then %do;
      %let NERRORS = %eval(&NERRORS + 1);
      %put ERROR: &TARGET variable not found in &DSNAME;
   %end;
   %do i = 1 %to &NKEYS;
      %let keyvar = %trim(%scan(%bquote(&KEY), &i, %str(,)));
      %if (%sysfunc(varnum(&DSID, &KEYVAR)) = 0) %then %do;
         %let NERRORS = %eval(&NERRORS + 1);
         %put ERROR: &KEYVAR variable not found in &DSNAME;
      %end;
   %end;
   %let rc = %sysfunc(close(&DSID));
   %if (&NERRORS gt 0) %then %do;
      %put ERROR: Format "&FMTNAME." is not created. ;
      %rsk_terminate();
   %end;

   %*------------------------------------------------------------------;
   %* Create the key string, which is either a simple variable name    ;
   %* or COMPRESS(var1 || "_" || var2 || ... || varn)                  ;
   %*------------------------------------------------------------------;

   %if (&nkeys gt 1) %then %do;
      %local SB;
      %do i = 1 %to &nkeys;
         %if (&i > 1) %then %do;
            %let sb = &sb || "_" ||;
         %end;
         %let keyvar = %trim(%scan(%bquote(&KEY), &i, %str(,)));
         %let sb = &sb &keyvar;
      %end;
      %if (&compress. eq Y) %then
        %let KEY=compress(&sb);
      %else
          %let KEY=&sb;
   %end;
   %else %do;
         %if (&compress. eq Y) %then
          %let KEY=compress(&KEY);
      %else
          %let KEY=strip(&KEY);
   %end;

   %*------------------------------------------------------------------;
   %* Create the format CNTLIN data set                                ;
   %*------------------------------------------------------------------;

   %local COUNT;
   %let COUNT = 0;
   data cntlin(keep=fmtname type start end label hlo);
      attrib   fmtname  length=$64
               type     length=$1
               start    length=$&key_length.
               end      length=$&key_length.
               label    length=$256
               hlo      length=$1
               ;

      retain fmtname "&FMTNAME";
      retain type "&TYPE";
      retain count 0;

      set &DSNAME end=eof;

      start = &KEY;
      if (not missing(start)) then do;
         end = start;
         label = &TARGET;
         hlo = ' ';
         output;
         count + 1;
      end;

      if (eof) then do;
         start = "**OTHER**";
         end = start;
         label = "&OTHER";
         HLO = "O";
         output;
         count + 1;
         call symput("COUNT", put(count, 8.));
      end;
   run;

   %*------------------------------------------------------------------;
   %* If there were no records written, do not create the format       ;
   %*------------------------------------------------------------------;

   %if (&count eq 0) %then %do;
      %put NOTE: No data in &DSNAME.  Dummy &FMTNAME format will be created;
      proc sql noprint;
         insert into cntlin
            values("&FMTNAME",
                   "&TYPE",
                   "**OTHER**",
                   "**OTHER**",
                   "N/A",
                   "O");
      quit;
   %end;

   proc sort data=cntlin nodupkey;
      by start;
   run;

   %*------------------------------------------------------------------;
   %* Create the format from the CNTLIN data set                       ;
   %*------------------------------------------------------------------;

   proc format cntlin=cntlin lib=&FMTLIB;
   run;

%rsk_trace(Exit);

%mend rsk_create_one_format;
