/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor rsk_col_to_macvars

   \brief   Produces a file of sas code generating a set of macro variables containing values of a column of a dataset.

   \param [in] tab:                name of the input dataset.
   \param [in] var:                name of the variable.
   \param [in] outvar:             prefix of output macro variables.
   \param [in] qchar [optional]:   quoting string. Default: quotation mark.
   \param [in] sep [optional]:     list separator char. Default: comma.
   \param [in] cut [optional]:     maximal length of an output macro variable contents. Default: 32762, max: 32762.
   \param [in] fileref:            fileref of the output sas code file.
   \param [in] scope [optional]:   scope of generated macro variables. Values L, F, G according to call symputx. Default: L.
   \param [in] errflg [optional]:  name of an error flaging macro variable set to 1 in case of an error.

   \details
   The macro generates a file of SAS code containing a datastep that rewrites contents of a dataset column to quoted and
   delimited strings of bounded length assigned to a set of macrovariables.
   It uses an externally defined fileref (physical file or catalog only, no TEMP), that needs to be included afterwards.
   The macrovariables are created in a designated scope (L, F ,G) with the LOCAL being the scope of invoking %include. If
   the filename is skipped SAS code is executed within rsk_col_to_macvars making this the LOCAL context.
   The quotation and separator chars can be set according to preferences.

   The names of the created macro variables have the form of:
   - <outvar>_<n> - contain the generated strings, with n being cosecutive integers starting from 0,
   - <outvar> - extra variable containing the generated string, when it's a single one,
   - <outvar>_no - contains the number of generated variables.

   <b>Example:</b>

   \code
      data s;
        do i=10 to 13; output; end;
      run;

      filename __f catalog "work.temp.source";
      %rsk_col_to_macvars(s, i, nums, fileref=__f, cut=10);
      %include __f;

      %put &nums_1;
      %put &nums_2;
      %put &nums_no;

      "10","11"
      "12","13"
      2
   \endcode

   \ingroup mrm

   \author  SAS Institute Inc.
   \date    2022
*/

%macro rsk_col_to_macvars( tab,
                           var,
                           outvar,
                           qchar=%str(%"),
                           sep=%str(,),
                           cut=32762,
                           fileref=,
                           scope=L,
                           errflg=) / minoperator;
   %local qqchar qsep tofile;

   %if %sysevalf(%superq(errflg) eq, boolean) %then %do;
      %local __err;
      %let errflg = __err;
   %end;
   %else %if not %symexist(%superq(errflg)) %then %do;
      %put WARNING: Specified ERRFLG macro variable does not exist;
      %local __err;
      %let errflg = __err;
   %end;

   %if %sysevalf(%superq(qchar)=,boolean) %then %do;
       %let qqchar = "";
   %end;
   %else %do;
      %let qqchar = %sysfunc(quote(&qchar));
   %end;

   %if %sysevalf(%superq(sep)=,boolean) %then %do;
       %let qsep = "";
   %end;
   %else %do;
      %let qsep = %sysfunc(quote(&sep));
   %end;

   %let tofile = %sysevalf(%superq(fileref) ne,boolean);

   %if not (%superq(scope) in L F G) %then %do;
      %put ERROR: Invalid SCOPE argument;
      %let &errflg. = 1;
   %end;

   %if %sysfunc(prxmatch(/^0$|^[1-9][0-9]*$/, %superq(cut))) %then %do;
      %let cut = %sysfunc(inputn(%superq(cut), best32.));
      %if &cut. < 1 or &cut. > 32762 %then %do;
         %put ERROR: Invalid CUT argument. Valid values: [1, 32762];
         %let &errflg. = 1;
      %end;
   %end;
   %else %do;
      %put ERROR: Invalid CUT argument. Valid values: [1, 32762];
      %let &errflg. = 1;
   %end;


   %if %sysevalf(%superq(&errflg) eq 1, boolean) %then %goto exit;

   %rsk_check_ds_sanity(&tab., &var, errflg = &errflg);
   %if %sysevalf(%superq(&errflg) eq 1, boolean) %then %goto exit;

   %let str = %rsk_get_unique_varname(tab);
   %let cnt = %rsk_get_unique_varname(tab, exclude = &str.);

   %if &tofile. %then %do;
      proc stream outfile = &fileref.; begin &streamdelim;
   %end;
      data _null_;
         if _n_ = 1 and last then call symputx("&outvar._no", 0, "&scope.");
         set &tab. end=last;
         length &str. $32762 &cnt. 8;
         retain &str. &cnt.;
         if _n_ = 1 then do;
            &str.="";
            &cnt.=0;
         end;

         if (length(&str.) + (not missing(&str.))*%length(&sep.) + length(strip(&var.)) + 2*%length(&qchar.)) le &cut then do;
             &str. = catx(&qsep., &str., cats(&qqchar., &var., &qqchar.));
         end;
         else do;
            &cnt. = &cnt.+1;
            call symputx(cats("&outvar._", &cnt.), &str., "&scope.");

            if length(strip(&var.)) + 2*%length(&qchar.) gt &cut then do;
               putlog "ERROR: contents will not fit into &outvar.";
               call symputx("&errflg.", 1, 'F');
               abort;
            end;
            &str.=cats(&qqchar., &var., &qqchar.);
         end;

         if last then do;
            &cnt. = &cnt.+1;
            call symputx(cats("&outvar._", &cnt.), &str., "&scope.");

            if &cnt. eq 1 then do;
               call symputx("&outvar", &str., "&scope.");
            end;
            call symputx("&outvar._no", &cnt., "&scope.");
         end;
      run;
   %if &tofile. %then %do;
      ;;;;
   %end;

   %exit:
%mend;
