/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor core_krm_getrunid

   \brief   Retrieve the KRM run ID of a given Analysis Run

   \param [in] analysis_run_key Key of the Analysis Run
   \param [out] out_run_id Name of the output variable that contains the run_id

   \details
   This macro sends a GET request to <b><i>\<host\>:\<port\>/riskCirrusKrm/runId</i></b> with the analysis run ID as a parameter and gets the KRM run ID. \n

   <b>Example:</b>

   1) Send a Http GET request and save the response in the variable run_id
   \code
      %let analysis_run_key = ${AnalysisRun.key};
      %let run_id = ;
      %core_krm_getrunid(analysis_run_key = &analysis_run_key.
                        , out_run_id = run_id);
      %put &=run_id;
   \endco

   \ingroup coreRestUtils

   \author  SAS Institute Inc.
   \date    2023
*/

%macro core_krm_getrunid(analysis_run_key =
                        , out_run_id =);
                        
   %let base_url = %sysfunc(getoption(servicesbaseurl))/riskCirrusKrm/runId?analysisRunId=&analysis_run_key.;
   %put &base_url;

    /* Creating the HTTP request */
   filename resp temp;
   proc http
      url="&base_url"
      out = resp
      ct = "application/json"
      oauth_bearer = SAS_SERVICES;
   run;

   /* check for success */
   %if &SYS_PROCHTTP_STATUS_CODE. ne 200 %then %do;
      /* check if there is an analysis run in progress */
      %if &SYS_PROCHTTP_STATUS_CODE. eq 409 %then %do;
         %PUT ERROR: KRM run execution conflict - the analysis run cannot be executed until the last run finishes execution;
      %end;
      %else %do;
         %PUT ERROR: Unable to get KRM run ID;
      %end;
      %abort 9999;
   %end;
   
   /* log response */
   data _null_;
      infile resp;
      input;
      call symputx("&out_run_id.", _infile_);
   run;

%mend;