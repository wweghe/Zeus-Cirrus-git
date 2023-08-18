/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
\file 
\anchor rsk_functions_utilities
\brief Library of rsk functions and subroutines.

\details

Functions and subroutines defined in this macro
-----------------------------------------------

- function rsk_find
- function rsk_find_left
- function rsk_find_right
- subroutine rsk_print_msg_subr
- function rsk_check_num_missing_pf
- function rsk_check_array_pf
- function rsk_daycount
- function rsk_intpolate2
- function rsk_get_fwd_curve_dis_and_pv
- function rsk_pv_cshflw_dis

Details about these functions and subroutines are provided within the code of the macro.

\n

\ingroup CommonAnalytics utilities
\author  SAS Institute Inc.
\date    2012
*/
%macro rsk_functions_utilities();

 /***********************************************************************************
   Copyright (c) 2022-2023 by SAS Institute Inc., Cary, NC, USA.

   NAME: rsk_array_utilities.sas

   PURPOSE: set of simple algorithms for arrays.  "rsk_find" will find the index of
      a value in an array or will return 0 if it isn't there.  "rsk_find_left" will find
      index i such that arr[i] <= value < arr[i+1] or will return 0 if the value precedes
      the first element in the array.  "rsk_find_right" will find index i such that
      arr[i-1] < value <= arr[i] or will return one greater than the size of the array
      if the value is greater than the last element of the array.


   USAGE: index = rsk_find(value,arr);
          index = rsk_find_left(value,arr);
          index = rsk_find_right(value,arr);

   INPUTS:
      value : numeric value to search for
      arr   : presorted numeric array

   EXAMPLE:
      array arr[*] 1 3 7 8;
      index = rsk_find(4,arr);        * returns zero;
      lindex = rsk_find_left(4,arr);  * returns 2;
      rindex = rsk_find_right(4,arr); * returns 3;

   NOTES:
      arr[*] must be sorted (ascending) before input.


 ***********************************************************************************/

function rsk_find(value,arr[*]) kind="Array utility" label = "Finds an element of a sorted array";
   up = dim(arr)+1;
   lp = 1;

   do while( up-lp > 0 );
      mp = floor((up+lp)/2);
      if arr[mp] > value then up = mp;
      else if arr[mp] < value then lp = mp+1;
      else return(mp);
   end;

   if arr[mp] eq value then return(mp);
   else return(0);

endsub;


function rsk_find_left(value,arr[*]) kind="Array Utility"
   label = "Finds closest element of a sorted array which is less than or equal to the input value";
   cap = dim(arr);
   up = cap+1;
   lp = 1;

   do while( up-lp > 0 );
      mp = floor((up+lp)/2);
      if arr[mp] > value then up = mp;
      else if mp eq cap then return(cap);
      else if arr[mp+1] le value then lp = mp + 1;
      else return(mp);
   end;

   return(0);
endsub;


function rsk_find_right(value,arr[*]) kind="Array Utility"
   label = "Finds closest element of a sorted array which is greater than or equal to the input value";
   cap1 = dim(arr)+1;
   up = cap1;
   lp = 1;

   do while( up-lp > 0 );
      mp = floor((up+lp)/2);
      if arr[mp] < value then lp = mp + 1;
      else if mp eq 1 then return(1);
      else if arr[mp-1] ge value then up = mp;
      else return(mp);
   end;

   return(cap1);
endsub;




/***********************************************************************************
   Copyright (c) 2022-2023 by SAS Institute Inc., Cary, NC, USA.

   NAME: rsk_print_msg_subr.sas

   PURPOSE: Subroutine that calls macro rsk_print_msg via function run_macro and macro
      rsk_print_msg_runmacro.  This will be an fcmp/compile/risk runtime call.

   INPUTS:
      key - RMB/RMI message key
      s1-s7 - message inputs 1 to 7, may be missing (see notes below)

   USAGE (in fcmp/compile):
      call rsk_print_msg_subr( 'rsk_calc_nxt_default', 'rsk_calc_next_workday2', '',
         '', '', '', '', '' );

   NOTES:
      Although this function will initialize RSK_LOG_MESSAGE_COUNT to zero, it is
         strongly recommended that the user initialize RSK_LOG_MESSAGE_COUNT to
         zero in the init block of a project method.
      Similar logic is found here as is found in rsk_get_msg.  Anyone looking to
         edit either this or rsk_get_msg should consider editing both.
      Assumes that s1-s7 are populated in that order (e.g. s5 being populated
         implies that s1-s4 are populated and s2 being populated implies that
         s1 is populated )

***********************************************************************************/


   subroutine rsk_print_msg_subr( key $, s1 $, s2 $, s3 $, s4 $, s5 $, s6 $, s7 $ )
      kind = "Utility functions"
      label = "Prints a message to the log";

      /* Set the maximum length of the message */
      length msg $1024;

      /* hard code the maximum allowed messages to fifty */
      maxputs = 50;

      /* Get how many messages have been printed so far, default to zero */
      /* Note: although this function will set RSK_LOG_MESSAGE_COUNT to zero, it is strongly recommended
         that the user initialize RSK_LOG_MESSAGE_COUNT to zero in the init block of a project method */
      array message_count[1] / nosym;
      call dynamic_array(message_count,.);
      if missing(message_count[1]) then message_count[1]=1;
      else message_count[1]=message_count[1]+1;

      numputs=message_count[1];

      /* If we have not exceeded the number of allowed messages, then get and print this message */
      if numputs le maxputs then do;

         /* Figure out the message file from the key name, otherwise output a warning */
         /* Note: this is similar to logic found in rsk_get_msg */
         length msgfile $64 key3 $3 key7 $7;
         key3 = lowcase(substr( key, 1, 3 ));
         key7 = lowcase(substr( key, 1, 7 ));
         if key7 eq 'rmi_rpt' then msgfile = 'SASHELP.RMILABELS';
         else if key3 eq 'rmi' then msgfile = 'SASHELP.RMICALCMSG';
         else if key3 in ( 'alm', 'rmb', 'cra', 'fra', 'mra' ) then msgfile = 'SASHELP.RMBCALCMSG';
         else if key3 eq 'rsk' then msgfile = "%sysfunc(compress(SASHELP.&PRODUCT.UTILMSG))";         else do;
            file log;
            put 'WARN' 'ING: Cannot determine message file for ' key=;
            file print;
            return;
         end;

         /* Depending on how many inputs were supplied, call sasmsgl differently to get the message */
         /* We are assuming that s1-s7 are populated in that order (i.e. s2 is not populated when s1 is missing ) */
         if not missing(s7) then msg = sasmsgl( msgfile, key, 'en',
            'NOQUOTE', trim(s1), trim(s2), trim(s3), trim(s4), trim(s5), trim(s6), trim(s7) );
         else if not missing(s6) then msg = sasmsgl( msgfile, key, 'en',
            'NOQUOTE', trim(s1), trim(s2), trim(s3), trim(s4), trim(s5), trim(s6) );
         else if not missing(s5) then msg = sasmsgl( msgfile, key, 'en',
            'NOQUOTE', trim(s1), trim(s2), trim(s3), trim(s4), trim(s5) );
         else if not missing(s4) then msg = sasmsgl( msgfile, key, 'en',
            'NOQUOTE', trim(s1), trim(s2), trim(s3), trim(s4) );
         else if not missing(s3) then msg = sasmsgl( msgfile, key, 'en',
            'NOQUOTE', trim(s1), trim(s2), trim(s3) );
         else if not missing(s2) then msg = sasmsgl( msgfile, key, 'en',
            'NOQUOTE', trim(s1), trim(s2) );
         else if not missing(s1) then msg = sasmsgl( msgfile, key, 'en',
            'NOQUOTE', trim(s1) );
         else msg = sasmsgl( msgfile, key, 'en','NOQUOTE' );

         /* Print the message */
         file log;
         put msg;
         file print;

      end;

      /* If we have reached the maximum messages, send the message to let the user know that no more messages will be output. */
      if numputs eq maxputs then do;
         msg =  sasmsgl( "%sysfunc(compress(SASHELP.&PRODUCT.UTILMSG))", 'rsk_exceed_msg_limit_warning', 'en', 'NOQUOTE' );
         file log;
         put msg;
         file print;
      end;


   endsub;






/******************************************************************************
   Copyright (c) 2022-2023 by SAS Institute Inc., Cary, NC, USA.

   NAME: rsk_check_num_missing_pf.sas

   PURPOSE: Checks if a variable is missing.  If it is missing, outputs an
      error to the log and returns 1.  Otherwise, returns the value of ErrorFoundFlag.
      This is so many calls to this function can be made in a row without
      repeatedly checking the output (see USAGE).

   INPUTS:
      VariableValue - the numeric value to be checked
      FunctionName - the function name this variable is called from
      VariableNum - the number (as a string) of the variable in the input list
         to the function named by FunctionName
      VariableName - the variable name in the called function
      ErrorFoundFlag - either 1, to indicate than an error has been found
         already in other variables, or 0, otherwise.

   USAGE (in another function):
      ReturnMissingFlg = 0;
      Fname = 'rsk_reset_option2_pf';
      ReturnMissingFlg = rsk_check_num_missing_pf( RiskFreeRate, Fname, '5', 'RiskFreeRate', ReturnMissingFlg );
      ReturnMissingFlg = rsk_check_num_missing_pf( YieldParam, Fname, '6', 'YieldParam', ReturnMissingFlg );
      if ReturnMissingFlg eq 1 then return(.);

   NOTES:

 ******************************************************************************/



   function rsk_check_num_missing_pf( VariableValue, FunctionName $, VariableNum $, VariableName $, ErrorFoundFlag )
      kind = "Utility functions"
      label = "Checks a value to verify that it is nonmissing";

      /* Check missing: if found, print error message and return 1 */
      if missing(VariableValue) then do;
         call rsk_print_msg_subr( 'rsk_func_missing_inputs_error', FunctionName, VariableNum, VariableName, '', '', '', '' );
         return(1);
      end;

      /* If no errors found, return previous error status,
         or if no previous error status given, then set the status to zero (no error) */
      return(coalesce(ErrorFoundFlag,0));

   endsub;




/******************************************************************************
   Copyright (c) 2022-2023 by SAS Institute Inc., Cary, NC, USA.

   NAME: rsk_check_array_pf.sas

   PURPOSE: Checks if an array has all positive elements.  If any element is
      nonpositive, outputs an error to the log and returns 1.  Otherwise,
      returns the value of ErrorFoundFlag.  This is so many calls to this
      function can be made in a row without repeatedly checking the output (see USAGE).

   INPUTS:
      InputArray - the array to be checked
      ArraySize - size of the array
      CheckType - NONPOSITIVE, NEGATIVE, MISSING or UNORDERED
      FunctionName - the function name this variable is called from
      VariableNum - the number (as a string) of the variable in the input list
         to the function named by FunctionName
      VariableName - the variable name in the called function
      ErrorFoundFlag - either 1, to indicate than an error has been found
         already in other variables, or 0, otherwise.

   USAGE (in another function):
      ReturnMissingFlg = 0;
      Fname = 'rsk_reset_option2_pf';
      ReturnMissingFlg = rsk_check_num_missing_pf( RiskFreeRate, Fname, '5', 'RiskFreeRate', ReturnMissingFlg );
      ReturnMissingFlg = rsk_check_num_missing_pf( YieldParam, Fname, '6', 'YieldParam', ReturnMissingFlg );
      if ReturnMissingFlg eq 1 then return(.);

   NOTES:

 ******************************************************************************/


function rsk_check_array_pf( InputArray[*], ArraySize, CheckType $, FunctionName $,
   VariableNum $, VariableName $, ErrorFoundFlag )
   kind = "Utility functions"
   label = "Checks the values of a one-dimensional array to verify they meet expected criteria";

   /* Check array: if offense found, print error message and return 1 */
   newsize = min(ArraySize,dim(InputArray));
   i = .;
   tempvalue = coalesce(InputArray[1],0);
   if CheckType eq 'NONPOSITIVE' then do i = 1 to newsize while( InputArray[i] > 0 );
   end;
   else if CheckType eq 'NEGATIVE' then do i = 1 to newsize while( InputArray[i] ge 0 );
   end;
   else if CheckType eq 'MISSING' then do i = 1 to newsize while( not missing(InputArray[i]) );
   end;
   else if CheckType eq 'UNORDERED' then do i = 1 to newsize while( tempvalue le InputArray[i] );
      tempvalue = InputArray[i];
   end;
   if i le newsize and not missing(i) then do;
      if CheckType eq 'NONPOSITIVE' then do;
         call rsk_print_msg_subr( 'rsk_func_nonpos_array_error', FunctionName,
            VariableNum, VariableName, '', '', '', '' );
      end;
      else if CheckType eq 'NEGATIVE' then do;
         call rsk_print_msg_subr( 'rsk_func_neg_array_error', FunctionName,
            VariableNum, VariableName, '', '', '', '' );
      end;
      else if CheckType eq 'MISSING' then do;
         call rsk_print_msg_subr( 'rsk_func_miss_array_error', FunctionName,
            VariableNum, VariableName, '', '', '', '' );
      end;
      else if CheckType eq 'UNORDERED' then do;
         call rsk_print_msg_subr( 'rsk_func_order_array_error', FunctionName,
            VariableNum, VariableName, '', '', '', '' );
      end;
      return(1);
   end;

   /* If no errors found, return previous error status,
      or if no previous error status given, then set the status to zero (no error) */
   return(coalesce(ErrorFoundFlag,0));

endsub;





/*****************************************************************************
   Copyright (c) 2022-2023 by SAS Institute Inc., Cary, NC, USA.

   NAME: rsk_daycount.sas

   PURPOSE: Calculates the number of years between two dates using
      different date/year conventions

   USAGE:

   NOTES:
      If either date is missing or difference is negative, will return zero.


 *****************************************************************************/



function rsk_daycount(Convention $,BeginDate,EndDate)

   kind  = "Date management"
   label = "Calculates the number of years between two dates, given a day counting convention";

   /*
   Actual/actual: The actual number of days between two dates is used.
   Leap years count for 366 days, non-leap years count for 365 days. If more than 1 year then
   actual year use actual year rsk_daycount - hence the loop and frac return.

   Actual/360: The actual number of days between two dates is used as the numerator.
   A year is assumed to have 12 months of 30 days each.

   Actual/365: The actual number of days between two dates is used as the numerator.
   All years are assumed to have 365 days. BASECASE

   Actual/366: The actual number of days between two dates is used as the numerator.
   All years are assumed to have 366 days.

   30/360: All months are assumed to have 30 days, resulting in a 360 day year.
   If the first date falls on the 31st, it is changed to the 30th.
   If the second date falls on the 31th, it is changed to the 30th,
   but only if the first date falls on the 30th or the 31st.
   */

   /* Create a local copy of this pass-through character value */
   /* This will make the function more efficient */
   length local_convention $32;
   local_convention = trim(Convention);

   /* Default value to ACT/365 */
   if missing(local_convention) then local_convention = 'ACT/365';

   /* Return zero if EndDate before BeginDate or missing either date */
   if EndDate le BeginDate or missing(BeginDate) then return(0);

   /* Handle cases, for each convention */
   select(local_convention);

      /* ACT/360 must be computed by this function */
      /* Otherwise, use yrdif function */
      when ('ACT/366') do;
         num = EndDate - BeginDate ;
         den = 366;
         returnval = num / den;
      end;
      otherwise do;
         returnval = yrdif( BeginDate, EndDate, trim(local_convention) );
      end;

   end;

   /* Return the value */
   return(returnval);

endsub;




/*****************************************************************************
   Copyright (c) 2022-2023 by SAS Institute Inc., Cary, NC, USA.

   NAME: rsk_intpolate2.sas

   PURPOSE: Interpolation on a curve with maturities

   INPUTS:
      Period - the time, in years, to interpolate to
      Curve - array of curve values
      CurveMat - array of curve maturities, in years
      IntpMethod - interpolation method
         LOG - loglinear interpolation
         CUBIC - natural cubic spline interpolation
         STEP - right-continuous step interpolation
         LINEAR (default) - linear interpolation

   NOTES:
      If Period is before the first maturity in CurveMat, the first value in
         curve is used.
      If Period is after the first maturity in CurveMat, the last value in
         curve is used.

 *****************************************************************************/



function rsk_intpolate2( Period, Curve[*], CurveMat[*], IntpMethod $)
   kind = "Curve interpolation"
   label = "Interpolates on a curve, given the target maturity in years";

   /**************************************************************************/
   /* ERROR CHECKING */
   /**************************************************************************/

   dim = dim(Curve);

   /* Initialize the return missing flag to zero
      If we find an error in one of the inputs, we will return missing */
   /* Set the function name for error reporting */
   ReturnMissingFlg = 0;
   Fname = 'rsk_intpolate2';

   /* Make sure 'Period' is nonmissing */
   /* Missing values for 'Period' may cause problems
      if input into another function (e.g. the logarithm function) */
   ReturnMissingFlg = rsk_check_num_missing_pf( Period, Fname, '1', 'Period', ReturnMissingFlg );

   /* Make sure CurveMat array is nonmissing and nondecreasing */
   ReturnMissingFlg = rsk_check_array_pf( CurveMat,
      dim, 'MISSING', Fname, '3',
      'CurveMat', ReturnMissingFlg );

   ReturnMissingFlg = rsk_check_array_pf( CurveMat,
      dim, 'UNORDERED', Fname, '3',
      'CurveMat', ReturnMissingFlg );

   /* Return missing, if errors are found */
   if ReturnMissingFlg eq 1 then return(.);

   /************************************************************/
   /* RESUME INTERPOLATION */
   /************************************************************/

   if IntpMethod eq 'LOG' then do;       /* Interpolation method = LOGLINEAR*/
      j = rsk_find_right( period, CurveMat );
      if j eq 1 then IntVal = Curve{1};
      else if j > dim then  IntVal = Curve{dim};
      else do;
         if Curve{j-1} then do;
            IntVal = Curve{j-1} * (    ( Curve{j} / Curve{j-1} ) ** ( ( period - CurveMat{j-1} ) / ( CurveMat{j} - CurveMat{j-1} ) )   );
         end;
         else do;
            IntVal = 0;
         end;
      end;
   end;
   else if IntpMethod eq 'CUBIC' then do;       /* Interpolation method = CUBIC (calculation of natural cubic spline coefficients)*/
      /* linear interpolation at endpoints used */
      if ( period <= CurveMat{1} ) then IntVal = Curve{1};
      else if ( period >= CurveMat{dim} ) then  IntVal = Curve{dim};
      else do;
         Array M_temp[1] /nosym;
         Array N_temp[1] /nosym;
         Array Q_temp[1] /nosym;
         Array A_temp[1] /nosym;
         Array B_temp[1] /nosym;
         Array D_temp[1] /nosym;
         Array AA_temp[1] /nosym;
         Array BB_temp[1] /nosym;
         Array CC_temp[1] /nosym;
         CALL DYNAMIC_ARRAY(M_temp,dim);
         CALL DYNAMIC_ARRAY(N_temp,dim);
         CALL DYNAMIC_ARRAY(Q_temp,dim);
         CALL DYNAMIC_ARRAY(A_temp,dim);
         CALL DYNAMIC_ARRAY(B_temp,dim);
         CALL DYNAMIC_ARRAY(D_temp,dim);
         CALL DYNAMIC_ARRAY(AA_temp,dim);
         CALL DYNAMIC_ARRAY(BB_temp,dim);
         CALL DYNAMIC_ARRAY(CC_temp,dim);
         /* Initialize */
         M_temp[1]=CurveMat[2] - CurveMat[1];
         N_temp[1]=Curve[2] - Curve[1];
         A_temp[1]=1;
         B_temp[1]=0;
         D_temp[1]=0;
         /* end initialize */
         do i=2 to dim-1;
            M_temp[i]= CurveMat[i+1] - CurveMat[i];
            N_temp[i]= Curve[i+1] - Curve[i];
            Q_temp[i]= 3*( (N_temp[i] / M_temp[i] )  - (N_temp[i-1] / M_temp[i-1] ) );
            A_temp[i]= 2* (M_temp[i-1] + M_temp[i]) - M_temp[i-1] * B_temp[i-1];
            B_temp[i]= M_temp[i] / A_temp[i];
            D_temp[i]= ( Q_temp[i] - M_temp[i-1] * D_temp[i-1] ) / A_temp[i];
         end;
         /* Initialize */
         A_temp[dim]=0;
         BB_temp[dim]=0;
         D_temp[dim]=0;
         /* end initialize */
         do i=(dim-1) to 1 BY -1;
            BB_temp[i]= D_temp[i] - B_temp[i] * BB_temp[i+1];
            AA_temp[i]= (N_temp[i] / M_temp[i] )  - ( M_temp[i] / 3* (  BB_temp[i+1] + 2*BB_temp[i]     )    );
            CC_temp[i]= (BB_temp[i+1]  - BB_temp[i]) / (3*M_temp[i]);
         end;
         j = rsk_find_right( period, CurveMat );
         if j eq 1 then IntVal = Curve{1};
         else if j > dim then IntVal = Curve{dim};
         else do;
            j = j - 1;
            IntVal = Curve{j} + ( AA_temp{j} * ( period - CurveMat{j} ) )  + ( BB_temp{j} * ( period - CurveMat{j} )**2 )
                     + ( CC_temp{j} * ( period - CurveMat{j} )**3 );
         end;
      end;
   end;
   else if IntpMethod eq 'STEP' then do;
      j = rsk_find_left( period, CurveMat );
      j = max(j,1);
      IntVal = Curve[j];
   end;
   else if IntpMethod eq 'FORWARD_RATE' then do;
      j = rsk_find_left( period, CurveMat );
      if j < 1 then IntVal = Curve[1];
      else if j eq dim(CurveMat) then IntVal = Curve[j];
      else do;
         timediff = period - CurveMat[j];
         if round(timediff,1e-15) eq 0 then IntVal = Curve[j];
         else do;
            fwdperiod = CurveMat[j+1] - CurveMat[j];
            r1plus1 = 1+Curve[j];
            forward_rate_plus1 = (1+Curve[j+1])**(CurveMat[j+1]/fwdperiod) / r1plus1**(CurveMat[j]/fwdperiod);
            IntVal = r1plus1**(CurveMat[j]/period) * forward_rate_plus1 ** (timediff/period)-1;
         end;
      end;
   end;
   else do;    /* Interpolation method = LINEAR*/
      j = rsk_find_right( period, CurveMat );
      if j eq 1 then IntVal = Curve{1};
      else if j > dim then  IntVal = Curve{dim};
      else IntVal = Curve{j-1} + ( ( Curve{j} - Curve{j-1} ) * ( period - CurveMat{j-1} ) / ( CurveMat{j} - CurveMat{j-1} ) );
   end;


 return ( IntVal );
endsub;




/*****************************************************************************
   Copyright (c) 2022-2023 by SAS Institute Inc., Cary, NC, USA.

   NAME: rsk_get_fwd_curve_dis_and_pv.sas

   PURPOSE: Generates forward curve from spot curve and discounts future cash
      flows to a forward date using discrete annual compounding.

   INPUTS:
      Now_Date - Effective date (i.e. today)
      CFNum - Number of valid dates in MatDate.
      Forward_Date - Forward date of valuation
      Day_Basis - Day counting convention for counting years between two dates.
      Spot_Curve - Array of continuous spot interest rates.
      Spot_Curve_Mat - Array of maturities for the rates in Spot_Curve.
      Interpolation_Method - Interpolation method for the spot interest rate curve.
      MatDate - Array of cash flow dates.
      MatCash - Array of cash flow amounts.

   USAGE:

   FUNCTION DEPENDENCY:
      rsk_daycount
      rsk_intpolate2

   NOTES:

 *****************************************************************************/



function rsk_get_fwd_curve_dis_and_pv( Now_Date, CFNum, Forward_Date,
   Day_Basis $, Spot_Curve[*], Spot_Curve_Mat[*], Interpolation_Method $,
   MatDate[*], MatCash[*] )

   kind  = "Cash flow valuation"
   label = "Values a series of cash flows by interpolating forward interest rates from spot annual interest rates";

   /**************************************************************************/
   /* ERROR CHECKING */
   /**************************************************************************/

   /* Initialize the return missing flag to zero
      If we find an error in one of the inputs, we will return missing */
   /* Set the function name for error reporting */
   ErrorFoundFlg = 0;
   Fname = 'rsk_get_fwd_curve_dis_and_pv';

   /* Check inputs */
   ErrorFoundFlg = rsk_check_num_missing_pf( Now_Date, Fname, '1',
   'Now_Date', ErrorFoundFlg );
   ErrorFoundFlg = rsk_check_num_missing_pf( CFNum, Fname, '2',
   'CFNum'
   , ErrorFoundFlg );
   ErrorFoundFlg = rsk_check_num_missing_pf( Forward_Date, Fname, '3',
   'Forward_Date'
   , ErrorFoundFlg );
   ErrorFoundFlg = rsk_check_array_pf( Spot_Curve,
      dim(Spot_Curve), 'MISSING', Fname, '5',
      'Spot_Curve'
      , ErrorFoundFlg );
   ErrorFoundFlg = rsk_check_array_pf( Spot_Curve_Mat,
      dim(Spot_Curve_Mat), 'UNORDERED', Fname, '6',
      'Spot_Curve_Mat'
      , ErrorFoundFlg );
   ErrorFoundFlg = rsk_check_array_pf( MatDate,
      CFNum, 'MISSING', Fname, '8',
      'MatDate'
      , ErrorFoundFlg );
   ErrorFoundFlg = rsk_check_array_pf( MatCash,
      CFNum, 'MISSING', Fname, '9',
      'MatCash'
      , ErrorFoundFlg );

   /* Return missing, if errors are found */
   if ErrorFoundFlg eq 1 then return(.);


   /************************************************************/
   /* RESUME STANDARD PRICING */
   /************************************************************/

   pv_delivery = 0;
   t_f = rsk_daycount(Day_Basis, Now_Date, Forward_Date);
   _rate2_ = rsk_intpolate2( t_f, Spot_Curve, Spot_Curve_Mat, Interpolation_Method );
   numer = ( 1 + _rate2_ )**t_f;
   do i = 1 to CFNum;
      t_i = rsk_daycount(Day_Basis, Now_Date, MatDate[i]);
      if MatDate[i] > Forward_Date then do;
         _rate1_ = rsk_intpolate2( t_i, Spot_Curve, Spot_Curve_Mat, Interpolation_Method );
         _temp_ = numer / ( 1 + _rate1_ )**t_i;
         pv_delivery = pv_delivery + MatCash[i] * _temp_;
      end;
   end;

   return (pv_delivery);

endsub;



 /*****************************************************************************
   Copyright (c) 2022-2023 by SAS Institute Inc., Cary, NC, USA.

   NAME: rsk_pv_cshflw_dis.sas

   PURPOSE: Iterates through the cash flow amounts, calculating the time from
      the valuation date to the payment date, then interpolating the risk-free
      rate and discounting with discrete calculations.

   INPUTS:
      N - Number of cash flow dates
      ValDate - Valuation date
      Date - Array of cash flow payment dates
      Amount - Array of cash flow payment amounts
      Convention - Convention for counting the years between two dates
      ZCCurve - Array of discrete discounting rates
      ZCCMAT - Array of discounting rate maturities
      SPREAD - Spread to add to the discounting rate
      IntpMethod - Interpolation method for the discounting rates curve.
         See function rsk_intpolate2 for more details.

   FUNCTION DEPENDENCY:
      rsk_check_array_pf
      rsk_check_num_missing_pf
      rsk_daycount
      rsk_intpolate2

 *****************************************************************************/



function rsk_pv_cshflw_dis( N, ValDate, Date[*], Amount[*], Convention $,
   ZCCurve[*], ZCCMAT[*], SPREAD, IntpMethod $)
    kind="Cash flow valuation"
    label = "Calculates the present value of a series of cash flows using discrete annual rates";

   /**************************************************************************/
   /* ERROR CHECKING */
   /**************************************************************************/

   /* Initialize the return missing flag to zero
      If we find an error in one of the inputs, we will return missing */
   /* Set the function name for error reporting */
   ErrorFoundFlg = 0;
   Fname = 'rsk_pv_cshflw_dis';

   /* Check inputs */
   ErrorFoundFlg = rsk_check_num_missing_pf( N, Fname, '1', 'N', ErrorFoundFlg );
   ErrorFoundFlg = rsk_check_num_missing_pf( ValDate, Fname, '2', 'ValDate', ErrorFoundFlg );
   ErrorFoundFlg = rsk_check_array_pf( Date,
      N, 'MISSING', Fname, '3',
      'Date', ErrorFoundFlg );
   ErrorFoundFlg = rsk_check_array_pf( Amount,
      N, 'MISSING', Fname, '4',
      'Amount', ErrorFoundFlg );
   ErrorFoundFlg = rsk_check_array_pf( ZCCurve,
      dim(ZCCurve), 'MISSING', Fname, '6',
      'ZCCurve', ErrorFoundFlg );
   ErrorFoundFlg = rsk_check_array_pf( ZCCMAT,
      dim(ZCCMAT), 'UNORDERED', Fname, '7',
      'ZCCMAT', ErrorFoundFlg );
   ErrorFoundFlg = rsk_check_num_missing_pf( Spread, Fname, '8', 'Spread', ErrorFoundFlg );

   /* Return missing, if errors are found */
   if ErrorFoundFlg eq 1 then return(.);


   /************************************************************/
   /* RESUME STANDARD PRICING */
   /************************************************************/

   /* Create a local copy of this pass-through character value */
   /* This will make the function more efficient */
   length local_convention $32;
   local_convention = Convention;

    PresentVal=0;

    do i = 1 to N;
        p = Date[i] - ValDate;
        p_year=rsk_daycount(local_convention,ValDate,Date[i]);

        if p gt 0 then do;
            r = rsk_intpolate2( p_year,ZCCURVE,ZCCMAT,IntpMethod) + SPREAD;
            d = 1/(1+r)**p_year;
            PresentVal = PresentVal + d*(Amount[i]);
        end;
    end;

    return (PresentVal);
endsub;



%mend;
