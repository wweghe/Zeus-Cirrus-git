/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
\file 
\anchor rsk_fx_conversion
\brief Converts amounts to the report currency unit for given FX spot rates.

\details

\param [in]  REPORT_CCY          : reporting currency.
\param [in]  IN_DS               : input data set name.
\param [in]  IN_SPOT_FX_DS       : FX spot rate data set.
\param [in]  OUT_DS              : output data set name.
\param [in]  IN_SRC_ORG_CCY_COL  : input original currency column name.
\param [in]  IN_SRC_COL_LIST     : list of source column name that requires to perform FX conversion.

\param [out] OUT_TGT_COL_LIST    : list of target column name which FX conversion process writes to.
\param [out] OUT_CCY_UNIT_COL_NM : output report currency column name.

\note This macro requires input data enriched by execution of macro \link rsk_get_fx_optimized_quote.sas \endlink.

\n

\ingroup CommonAnalytics
\author  SAS Institute Inc.
\date    2014
*/
%macro rsk_fx_conversion(   REPORT_CCY          =,
                            IN_DS               =,
                            IN_SPOT_FX_DS       =,
                            OUT_DS              =,
                            IN_SRC_ORG_CCY_COL  =currency,
                            IN_SRC_COL_LIST     =,
                            OUT_TGT_COL_LIST    =,
                            OUT_CCY_UNIT_COL_NM =currency);

   %rsk_trace(Entry);

   /*Check if &IN_SPOT_FX_DS data set is empty*/
      %if %rsk_attrn(&IN_SPOT_FX_DS, nlobs) eq 0 %then %do;
      %put WARNING: The FX spot rate data set &IN_SPOT_FX_DS contains 0 observations. Failed to perform currency conversion.;
        data &OUT_DS;
            set &IN_DS;
            BASECASE_VAL=.;
            RISK_VALUE=.;
         run;
        %return;
   %end;

   %local report_currency;
   %let report_currency=%sysfunc(compress(%upcase(&REPORT_CCY)));

   %local original_currency_col;
   %let original_currency_col=_ORIG_CCY;

   %if %upcase(&IN_SRC_ORG_CCY_COL) eq &original_currency_col %then
      %let original_currency_col=_&original_currency_col;

   /*check if required variables exist*/
   %local required_inst_var_list missing_inst_var success_flg;

   %let required_inst_var_list=&IN_SRC_ORG_CCY_COL &IN_SRC_COL_LIST;

   %rsk_verify_ds_col(      REQUIRED_COL_LIST    =&required_inst_var_list,
                            IN_DS_LIB            =%scan(&IN_DS,1),
                            IN_DS_NM             =%scan(&IN_DS,2),
                            OUT_SUCCESS_FLG      =success_flg,
                            OUT_MISSING_VAR      =missing_inst_var);

   %if %upcase(&success_flg) eq N %then %do;
      /*throw error*/
      %put ERROR: Required variable "&missing_inst_var" is missing in data set "&IN_DS".;
      %rsk_terminate();
   %end;

   /*filter optimized FX quote by REPORT_CCY*/
   data opt_fx_quote_&REPORT_CCY;
      set &IN_SPOT_FX_DS;
      where(upcase(QUOTE_CURRENCY)="&report_currency");
   run;

   /*check if opt_fx_quote_&REPORT_CCY is empty*/
   %if %rsk_attrn(opt_fx_quote_&REPORT_CCY, nlobs) eq 0 %then %do;
      %put WARNING: FX quote for converting to "&report_currency" is missing. Failed to perform FX conversion for data set "&IN_DS".;
      data &OUT_DS;
         set &IN_DS;
         BASECASE_VAL=.;
         RISK_VALUE=.;
      run;
      %return;
   %end;

   /*left join with FX spot rate*/
   proc sql noprint;
      create table enriched_ptf_x_fx_quote as
         select t1.*, t2.quote
         from &IN_DS as t1
            left join opt_fx_quote_&REPORT_CCY as t2
               on t1.&IN_SRC_ORG_CCY_COL = t2.BASE_CURRENCY;
   quit;

   /*check if OUT_CCY_UNIT_COL_NM exists*/
   %local  missing_var isOUTCCYEXist;

   %rsk_verify_ds_col(     REQUIRED_COL_LIST    =&OUT_CCY_UNIT_COL_NM,
                           IN_DS_LIB            =work,
                           IN_DS_NM             =enriched_ptf_x_fx_quote,
                           OUT_SUCCESS_FLG      =isOUTCCYExist,
                           OUT_MISSING_VAR      =missing_var);


   %local index item_src_col item_tgt_col;
   %let index = 1;

   data &OUT_DS(drop = quote);
      set enriched_ptf_x_fx_quote;
      %if %upcase(&isOUTCCYExist) eq N %then %do;
      length &OUT_CCY_UNIT_COL_NM $3.;
      %end;
      length &original_currency_col $3.;

      %do %while (%scan(&IN_SRC_COL_LIST, &index) ne );

         %let item_src_col = %scan(&IN_SRC_COL_LIST, &index);

         %let item_tgt_col = %scan(&OUT_TGT_COL_LIST, &index);

      if &IN_SRC_ORG_CCY_COL ne "&report_currency" then do;

         if missing(quote) then do;
            &item_tgt_col = .;
         end;
         else do;
            &item_tgt_col = &item_src_col * quote;
         end;

      end;
      else do;

         /*no currency conversion is needed*/
         &item_tgt_col = &item_src_col;

      end;
         %let index = %eval( &index + 1 );
      %end;

      &original_currency_col=&IN_SRC_ORG_CCY_COL;

      &OUT_CCY_UNIT_COL_NM = "&report_currency";

      output;

   run;

   /*Check if there is any missing quote data*/
   %let index = 1;
   data fx_conversion_checking;
      set &OUT_DS;
      where(
      %do %while (%scan(&IN_SRC_COL_LIST, &index) ne );

         %let item_src_col = %scan(&IN_SRC_COL_LIST, &index);

         %let item_tgt_col = %scan(&OUT_TGT_COL_LIST, &index);

         %if &index eq 1 %then %do;
            &item_tgt_col eq . and &item_src_col ne .
         %end;
         %else %do;
            or &item_tgt_col eq . and &item_src_col ne .
         %end;

         %let index = %eval( &index + 1 );

      %end;
      );
   run;

   /*print warning message*/
   %if %rsk_attrn(fx_conversion_checking, nlobs) ne 0 %then %do; 
   
      %local base_currency;

      data _null_;
         set fx_conversion_checking(obs=1);
         call symput("base_currency",trim(&original_currency_col));
      run;
      %put WARNING: FX quote for converting to "&base_currency/&report_currency" is missing. Failed to perform FX conversion for data set "&IN_DS".;

   %end;

   data &OUT_DS(drop=&original_currency_col);
      set &OUT_DS;
   run;

   %rsk_trace(Exit);

%mend;
