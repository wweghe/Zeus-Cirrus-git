/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
\file 
\anchor rsk_verify_ds_col
\brief Verifies whether a list of required column names exists in a given data set.

\details

\param [in]  REQUIRED_COL_LIST : input list of required column name.
\param [in]  IN_DS_LIB         : input data set library.
\param [in]  IN_DS_NM          : input data set name.

\param [out] OUT_SUCCESS_FLG   : output flag; returns Y if all exists.
\param [out] OUT_MISSING_VAR   : output missing variable; returns first missing column.

\n

\ingroup CommonAnalytics utilities
\author  SAS Institute Inc.
\date    2014
*/
%macro rsk_verify_ds_col(REQUIRED_COL_LIST    =,
                         IN_DS_LIB            =,
                         IN_DS_NM             =,
                         OUT_SUCCESS_FLG      =,
                         OUT_MISSING_VAR      =);

%local i  REQUIRED_COL_LIST_ITEM _FLAG_ _MISSING_COL_;
%let i=1;
%let _FLAG_=Y;
%do %while( %upcase(&_FLAG_) eq Y and %scan(&REQUIRED_COL_LIST,&i) ne );
   %let required_col_list_item=%scan(&REQUIRED_COL_LIST,&i);
   %if %rsk_varexist(&IN_DS_LIB..&IN_DS_NM,&REQUIRED_COL_LIST_ITEM) = 0 %then %let _FLAG_=N;
   %let i=%eval(&i+1);
%end;

%if &_FLAG_ eq Y %then %do;
   data _null_;
      call symput("&OUT_SUCCESS_FLG","Y");
      call symput("&OUT_MISSING_VAR","");
   run;
%end;
%else %do;
   %let _MISSING_COL_=%scan(&REQUIRED_COL_LIST,&i-1);
   data _null_;
      call symput("&OUT_SUCCESS_FLG","N");
      call symput("&OUT_MISSING_VAR","&_MISSING_COL_");
   run;
%end;

%mend;
