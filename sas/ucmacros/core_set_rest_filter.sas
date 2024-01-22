/* Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA */

/*!
\file
\anchor core_set_rest_filter
\brief   The macro core_set_rest_filter.sas determines the filter to use for GET requests.

\param [in] key (optional) Key of the resource.
\param [in] solution (optional) Risk Cirrus Objects solution (createdInTag/sharedWithTags value).  Only applicable for risk-cirrus-objects requests
\param [in] filter (optional) additional filter to use for the request
\param [in] customFilter (optional) additional filter to use for the request - this is useful for macros that both take an input filter but also
want to specify a custom filter of their own.
\param [in] start (optional) Starting point of the records to get (zero-based index)
\param [in] limit (optional) Maximum number of items to get from the start position
\param [out] outUrlVar Name of the output macro variable that holds the URL on which to append the filters.  The macrovariable name must exist
   outside the scope of this macro and must resolve to a non-missing value (Default: requestUrl)

\details
This macro determines the filter to use for REST requests to the risk-cirrus-objects service.  All input filters are included in the
final filter that is built using the and() function.  The filters corresponding to each variable are:

   key: eq(key,<key>)
   solution: or(eq(createdInTag,<solution>),contains(sharedWithTags,<solution>))
   filter: <filter>

<b>Example:</b>

Get the first 5 analysis data objects in the ECL solution with name=MyObjectName:
\code
   %let requestUrl = https://sas-risk-cirrus-objects:443/riskCirrusObjects/objects/analysisData;
   %core_set_rest_filter(solution=ECL, filter=eq(name,%27MyObjectName%27), limit=5, outUrlVar=requestUrl);
   %put &=requestUrl;
      // https://sas-risk-cirrus-objects:443/riskCirrusObjects/objects/analysisData?filter=and(or(eq(createdInTag,%27ECL%27),contains(sharedWithTags,%27ECL%27)),eq(name,%27MyObjectName%27))&limit=5
\endcode

\ingroup macro utility
\author  SAS Institute Inc.
\date    2022
*/

%macro core_set_rest_filter(key=, solution=, filter=, customFilter=, start=, limit=, outUrlVar=requestUrl);

   %local restFilter;

   /* Make sure output variable outUrlVar is set */
   %if %sysevalf(%superq(outUrlVar) =, boolean) %then %do;
      %put ERROR: outUrlVar must be the name of an existing macrovariable holding the URL;
      %abort;
   %end;

   /* Make sure output variable outUrlVar has a value */
   %if %sysevalf(%superq(&outUrlVar.) =, boolean) %then %do;
      %put ERROR: &outUrlVar. must resolve to a URL.;
      %abort;
   %end;

   /* Remove trailing slash from the URL, if needed */
   %let &outUrlVar. = %sysfunc(prxchange(s/\/$//, -1, %superq(&outUrlVar.)));

   /* Remove filter= from the filter and the custom filter, if needed */
   %let filter = %sysfunc(prxchange(s/\bfilter=\b//i, -1, %superq(filter)));
   %let customFilter = %sysfunc(prxchange(s/\bfilter=\b//i, -1, %superq(customFilter)));


   /**********************************************/
   /* Set the filter= parameter on the requestUrl*/
   /**********************************************/
   /* Add key filter */
   %if %sysevalf(%superq(key) ne, boolean) %then
      %let restFilter=eq(key,%27&key.%27);

   /* Add solution filter */
   %if %sysevalf(%superq(solution) ne, boolean) %then %do;
      %let solution = %upcase(&solution.);
      %if "&restFilter." ne "" %then
         %let restFilter=and(&restFilter.,or(eq(createdInTag,%27&solution.%27),contains(sharedWithTags,%27&solution.%27)));
      %else
         %let restFilter=or(eq(createdInTag,%27&solution.%27),contains(sharedWithTags,%27&solution.%27));
   %end;

   /* Add filter */
   %if %sysevalf(%superq(filter) ne, boolean) %then %do;
      %if "&restFilter." ne "" %then
         %let restFilter=and(&restFilter.,%superq(filter));
      %else
         %let restFilter=%superq(filter);
   %end;

   /* Add customFilter */
   %if %sysevalf(%superq(customFilter) ne, boolean) %then %do;
      %if "&restFilter." ne "" %then
         %let restFilter=and(&restFilter.,%superq(customFilter));
      %else
         %let restFilter=%superq(customFilter);
   %end;

   /* In the rare event that there are no filters, just use a dummy filter: eq(1,1) */
   %if "%superq(restFilter)" ne "" %then
      %let restFilter = filter=%superq(restFilter);
   %else
      %let restFilter = filter=eq(1,1);

   /* Add the filter to the URL.  Use '&' if the URL already has filters.  Otherwise, use '?' */
   %if(%index(%superq(&outUrlVar.),?) = 0) %then
      %let &outUrlVar. = %superq(&outUrlVar.)?%superq(restFilter);
   %else
      %let &outUrlVar. = %superq(&outUrlVar.)%str(&)%superq(restFilter);


   /************************************************/
   /* Set Start and Limit options on the requestUrl*/
   /************************************************/
   %if(%sysevalf(%superq(start) ne, boolean)) %then
      %let &outUrlVar. = %superq(&outUrlVar.)%str(&)start=&start.;

   %if(%sysevalf(%superq(limit) ne, boolean)) %then
      %let &outUrlVar. = %superq(&outUrlVar.)%str(&)limit=&limit.;

   %put NOTE: Request URL with filters added: %superq(&outUrlVar.);

%mend;