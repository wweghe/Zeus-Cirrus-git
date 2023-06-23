%macro corew_prepare_re_scenarios(inScenarios =
                                 , inCasLib = Public
                                 , outHistorical =
                                 , outCurrent =
                                 , outFuture =
                                 , outCasLib = Public
                                 , casSessionName = casauto
                                 , asOfDate =
                                 , dateBasedFormat = false
                                 );

   %local   outCasLibref
            outHistoricalCasLib outCurrentCasLib outFutureCasLib inScenariosCasLib
            outHistoricalCasTable outCurrentCasTable outFutureCasTable inScenariosCasTable
            ;

   options MSGLEVEL=I;

   /**************************/
   /* Process the parameters */
   /**************************/

   %if(%sysevalf(%superq(outHistorical) eq, boolean)) %then
      %let outHistorical = historical_&asOfDate_ymdn.;

   %if(%sysevalf(%superq(outCurrent) eq, boolean)) %then
      %let outCurrent = current_&asOfDate_ymdn.;

   %if(%sysevalf(%superq(outFuture) eq, boolean)) %then
      %let outFuture = future_&asOfDate_ymdn.;

   %let outHistoricalCasLib = &outCasLib.;
   %let outHistoricalCasTable = &outHistorical.;
   /*%let outHistorical = &outCasLib..&outHistorical.;*/

   %let outCurrentCasLib = &outCasLib.;
   %let outCurrentCasTable = &outCurrent.;
   /*%let outCurrent = &outCasLib..&outCurrent.;*/

   %let outFutureCasLib = &outCasLib.;
   %let outFutureCasTable = &outFuture.;
   /*%let outFuture = &outCasLib..&outFuture.;*/

   %let inScenariosCasLib = &inCasLib.;
   %let inScenariosCasTable = &inScenarios.;
   /*%let inScenarios = &outCasLib..&inScenarios.;*/

   %if(%sysevalf(%superq(asOfDate) eq, boolean)) %then %do;
      %put ERROR: asOfDate is required.;
      %abort;
   %end;

   %if(%sysevalf(%superq(dateBasedFormat) eq, boolean)) %then
      %let dateBasedFormat = false;

   /* Create the following from the exported scenarios (&outCasLib..&inScenarios.) for Risk Engines:
      &outCasLib..&outHistorical.  - historical data (h<0)     - RE needs this in date-based format
      &outCasLib..&outCurrent.     - current data (h=0)        - RE needs this in date-based format
      &outCasLib..&outFuture.      - scenario data (h>0)       - RE needs this in expanded format
   */

   /* Drop the RE CAS scenario tables before recreating them */
   %core_cas_drop_table(cas_session_name = &casSessionName.
                        , cas_libref = &outFutureCasLib.
                        , cas_table = &outFutureCasTable.);
   %core_cas_drop_table(cas_session_name = &casSessionName.
                        , cas_libref = &outHistoricalCasLib.
                        , cas_table = &outHistoricalCasTable.);
   %core_cas_drop_table(cas_session_name = &casSessionName.
                        , cas_libref = &outCurrentCasLib.
                        , cas_table = &outCurrentCasTable.);

   %let outCasLibref = %rsk_get_unique_ref(type = lib, engine = cas, args = caslib="&outCasLib." sessref=&casSessionName.);

   libname futCas cas caslib="&outFutureCasLib.";
   libname histCas cas caslib="&outHistoricalCasLib.";
   libname currCas cas caslib="&outCurrentCasLib.";
   libname scenCas cas caslib="&inScenariosCasLib.";

   /* if the scenario data is in expanded format, we need to transpose the current/historical data into date-based format */
   %if "&dateBasedFormat." = "false" %then %do;

      data &outCasLibref.._tmp_historical_and_curr(keep=date variable_name change_value)
         futCas."&outFutureCasTable."n(rename=(variable_name=_name_ change_value=_value_ change_type=_type_) drop=_priority_ promote=yes);
         format date YYMMDD10.;
         set scenCas."&inScenariosCasTable."n;
         if date <= &asOfDate. then output &outCasLibref.._tmp_historical_and_curr;
         else output futCas."&outFutureCasTable."n;
      run;

      proc transpose data=&outCasLibref.._tmp_historical_and_curr out=&outCasLibref.._tmp_historical_and_curr_trans;
         var change_value;
         id variable_name;
         by date;
      run;

      data histCas."&outHistoricalCasTable."n (promote=yes) currCas."&outCurrentCasTable."n (promote=yes);
         set &outCasLibref.._tmp_historical_and_curr_trans;
         if date < &asOfDate. then output histCas."&outHistoricalCasTable."n;
         else output currCas."&outCurrentCasTable."n;
         drop _name_;
      run;

   %end;

   /* if the scenario data is in date-based format, we need to transpose the future data into expanded format */
   %if "&dateBasedFormat." = "true" %then %do;

      data  histCas."&outHistoricalCasTable."n (drop=scenario_name interval _type_ horizon promote=yes)
            currCas."&outCurrentCasTable."n (drop=scenario_name interval _type_ horizon promote=yes)
            &outCasLibref.._tmp_future;

         set scenCas."&inScenariosCasTable."n;

         if date < &asOfDate. then output histCas."&outHistoricalCasTable."n;
         else if date = &asOfDate. then output currCas."&outCurrentCasTable."n;
         else output &outCasLibref.._tmp_future;
      run;

      proc transpose data=&outCasLibref.._tmp_future out=&outCasLibref.._tmp_future_trans prefix=_ suffix=_;
         id _type_;
         by _type_ interval scenario_name date horizon;
      run;

      data futCas."&outFutureCasTable."n(promote=yes);
         set &outCasLibref.._tmp_future_trans;
      run;

   %end;

   libname &outCasLibref. clear;

%mend;