/*************************************************************************
 * Copyright 2023, SAS Institute Inc., Cary, NC, USA. All Rights Reserved.
 *
 * NAME:        core_check_http_success
 *
 * PURPOSE:     Check if a http call success
 *
 *
 * PARAMETERS: 
 *              expect_return_code 
 *                  <required> - Expected HTTP return code if the call is success
 *              respose 
 *                  <required> - Response body of the HTTP call
 *
 * EXAMPLE:     %core_check_http_success(expect_return_code=200, respose=resp);
 **************************************************************************/
%macro core_check_http_success(expect_return_code=,
                               respose=);
   /* check for success */
   %if &SYS_PROCHTTP_STATUS_CODE ne &expect_return_code %then %do;
      /* log response */
      data _null_;
         infile &respose;
         input;
         put _infile_;
      run;
      %abort 9999;
   %end;
%mend core_check_http_success;