/*************************************************************************
 * Copyright 2023, SAS Institute Inc., Cary, NC, USA. All Rights Reserved.
 *
 * NAME:        core_krm_getkrmcaslibname
 *
 * PURPOSE:     Assign a CAS libref for KRM database
 *
 *
 * EXAMPLE:     %core_krm_getkrmcaslibname();
 **************************************************************************/
%macro core_krm_getkrmcaslibname();
    %let base_url = %sysfunc(getoption(servicesbaseurl))/riskCirrusKrm/krmDbLibname?type=CAS;
    filename resp TEMP;
    proc http
        url="&base_url"
        method = "GET"
        out = resp
        oauth_bearer = SAS_SERVICES;
    run;

    %if &SYS_PROCHTTP_STATUS_CODE. ne 200 %then
    %do;
        %put ERROR: Could not obtain libname information. Are you authorized?;
        %abort 9999;
    %end;

    data _null_;
        infile resp;
        input;
        call symputx('krmLibname', _infile_,'G');
    run;

    &krmLibname.;
%mend core_krm_getKRMCASLibname;
