/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor rsk_check_ds_sanity

   \brief   Checks a dataset for sanity conditions.

   \param [in] tab:              name of the dataset checked.
   \param [in] var [optional]:   names of variables checked.
   \param [in] check [optional]: conditions for variables. Accepts: KEY (is UUId key), ORD (is ordinal number),
                                    EKEY (is UUID key or empty), EORD (is ordinal number or empty),
                                    NEMPTY (is not empty), NONE (is anything).
   \param [in] card [optional]:  checks the number of rows of the dataset. Accepts <ordinal> as exact expected
                                    number of rows, <ordinal>+ as a minimal expected number of rows,
                                    <ordinal_min>-<ordinal_max> as a range of expected number of rows.
   \param [in] errflg [optional]:name of an error flaging macro variable set to 1 in case of an error.

   \details
   The macro checks a given dataset for a number of sanity conditions:
      - if the dataset <tab> exists.
      - if each of <var> variables exist.
      - if each of <var> variables has a specified contents:
         KEY (is UUId key),
         ORD (is ordinal number),
         EKEY (is UUID key or empty),
         EORD (is ordinal number or empty),
         NEMPTY (is not empty),
         NONE (is anything, always true).
      - if the number of rows meets given condition. Accepts:
         <ordinal> - exact expected number of rows (0, 1, 2, ...),
         <ordinal>+ - minimal expected number of rows (0+, 1+, 2+, ...)
         <ordinal_min>-<ordinal_max> - range of expected number of rows (0-1, 1-10, etc.)

   If any of the conditions is not met an ERROR message is put to log and <errflg> macro variable is set to 1.

   <b>Example:</b>

   \code
      %rsk_check_ds_sanity(ds, name key number, NEMPTY KEY ORD, 1+)
   \endcode

   \ingroup mrm

   \author  SAS Institute Inc.
   \date    2022
*/

%macro rsk_check_ds_sanity( tab,
                              var,
                              check=,
                              card=,
                              errflg=) / minoperator;
   %local _obs _dataCheck _vt _i _v _t _c _dc _ptrn _card_cond _c1 _c2 _c3;

   %if %sysevalf(%superq(errflg) eq, boolean) %then %do;
      %local __err;
      %let errflg = __err;
   %end;
   %else %if not %symexist(%superq(errflg)) %then %do;
      %put WARNING: Specified ERRFLG macro variable does not exist;
      %local __err;
      %let errflg = __err;
   %end;
   
   %if not %rsk_dsexist(&tab.) %then %do;
      %put ERROR: %superq(tab) does not exist;
      %if %sysevalf(%superq(errflg) ne, boolean) %then %let &errflg. = 1;
      %return;
   %end;

   %if %sysevalf(%superq(var) eq, boolean) %then %goto exit;

   %let _card_cond = 1;
   %if %sysevalf(%superq(card) ne, boolean) %then %do;   
      %let _ptrn = %sysfunc(prxparse(/^([\s]*)(0|[1-9][0-9]*)([\s]*)(|\+|-([\s]*)(0|[1-9][0-9]*))([\s]*)$/));
      %if %sysfunc(prxmatch(&_ptrn., %superq(card))) %then %do;
         %let _c1 = %sysfunc(prxposn(&_ptrn., 2, %superq(card)));
         %let _c2 = %sysfunc(prxposn(&_ptrn., 4, %superq(card)));
         %if %sysevalf(%superq(_c2) eq, boolean) %then %let _card_cond = %nrstr(&_obs. = )&_c1.;
         %else %do;
            %if %sysevalf(%superq(_c2) eq %bquote(+), boolean) %then %let _card_cond = %nrstr(&_obs. ge )&_c1.;
            %else %do;
               %let _c3 = %sysfunc(prxposn(&_ptrn., 6, %superq(card)));
               %if %sysevalf(%superq(_c1) le %superq(_c3), boolean) %then
                  %let _card_cond = (%nrstr(&_obs. ge )&_c1.) and (%nrstr(&_obs. le )&_c3.);
               %else %goto card_err;
            %end;
         %end;
      %end;
      %else %goto card_err;
   %end;

   %let _dc = 0;
   %let _i = 1;
   %if %sysevalf(%superq(check) ne, boolean) %then %do; %rsk_validation_funcs %end;

   %do %while (%qscan(&var., &_i.) ne );
      %let _v = %qscan(&var., &_i.);
      %if not %rsk_varexist(&tab., &_v.) %then %do;
         %put ERROR: %superq(_v) does not exist in &tab.;
         %if %sysevalf(%superq(errflg) ne, boolean) %then %let &errflg. = 1;
         %return;
      %end;

      %if %sysevalf(%superq(check) ne, boolean) %then %do;

         %let _t = %qscan(&check., &_i.);
         %if not (%superq(_t) in KEY ORD EKEY EORD NEMPTY NONE) %then %do;
            %put ERROR: Wrong CHECK argument [KEY ORD EKEY EORD NEMPTY NONE];
            %if %sysevalf(%superq(errflg) ne, boolean) %then %let &errflg. = 1;
            %return;
          %end;
   
         %let _vt = %rsk_varattr(&tab., &_v., type);
         %if (&_t. eq KEY) %then %do;
            %if (&_vt. eq C) %then %do; %let _dc = &_dc + (%isValidKeyInline(&_v.) eq 0); %end;
            %else %do; %let _dc = &_dc + 1; %end;
         %end;
         %if (&_t. eq ORD) %then %do;
            %if (&_vt. eq N) %then %do; %let _dc = &_dc + (%isValidOrdNInline(&_v.) eq 0); %end;
            %else %do; %let _dc = &_dc + 1; %end;
         %end;
         %if (&_t. eq EKEY) %then %do;
            %if (&_vt. eq C) %then %do; %let _dc = &_dc + (%isValidKeyInline(&_v.) eq 0 and not missing(&_v.)); %end;
            %else %do; %let _dc = &_dc + 1; %end;
         %end;
         %if (&_t. eq EORD) %then %do;
            %if (&_vt. eq N) %then %do; %let _dc = &_dc + (%isValidOrdNInline(&_v.) eq 0 and not missing(&_v.)); %end;
            %else %do; %let _dc = &_dc + 1; %end;
         %end;
         %if (&_t. eq NEMPTY) %then %do; %let _dc = &_dc + (missing(&_v.)); %end;
         %if (&_t. eq NONE) %then %do; %end;

      %end;
      %let _i = %eval(&_i + 1);
   %end;
   
   proc sql noprint;
      select
         count(*), sum(&_dc.) into :_obs, :_dataCheck
      from &tab.;
   quit;
   %if (&_dataCheck. > 0) %then %do;
      %put ERROR: %superq(tab) contains invalid data;
      %if %sysevalf(%superq(errflg) ne, boolean) %then %let &errflg. = 1;
      %return;
   %end;
   %if not (%unquote(&_card_cond.)) %then %do;
      %put ERROR: %superq(tab) violates cardinality condition [OBS = &_obs. / CARD = &card.];
      %if %sysevalf(%superq(errflg) ne, boolean) %then %let &errflg. = 1;
      %return;
   %end;

   %exit:
      %if %sysevalf(%superq(errflg) ne, boolean) %then %let &errflg. = 0;
      %return;

   %card_err:
      %put ERROR: Wrong CARD argument;
      %if %sysevalf(%superq(errflg) ne, boolean) %then %let &errflg. = 1;
      %return;

%mend;


