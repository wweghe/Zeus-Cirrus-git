/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
\file 
\anchor rsk_call_riskexec
\brief Wrapper of proc riskexec.

\details

\param [in]  ENTRYPOINT_MODULE   : entry point module.
\param [in]  ENTRYPOINT_FUNCTION : function invoked.
\param [in]  INVOKER             : function invoker.
\param [in]  RESTARTLUA          : restart LUA or not.
\param [in]  ARG1                : argument.
\param [in]  ARG2                : argument.
\param [in]  ARG3                : argument.
\param [in]  ARG4                : argument.
\param [in]  ARG5                : argument.
\param [in]  ARG6                : argument.
\param [in]  ARG7                : argument.
\param [in]  ARG8                : argument.
\param [in]  ARG9                : argument.

\n

Executing LUA Functions
-----------------------

This macro is a convenience wrapper around proc riskexec to allow for easy passing of arguments and return value between a SAS macro and a Lua function.
Arguments are passed to the Lua entry point specified, see the example below:

ENTRYPOINT_MODULE   = entry point module                       \n
ENTRYPOINT_FUNCTION = create_playpen_having_pedm               \n
INVOKER             = sas.risk.rmx.rsk_module_function_invoker \n
RESTARTLUA          = Y                                        \n
ARG1                = name                                     \n
ARG2                = application                              \n
ARG3                = description                              \n
ARG4                = cfg_set_id                               \n
ARG5                = config_datetime                          \n
ARG6                = result                                   \n \n

ALL empty string values passed to this macro are converted to nil values prior to being given to the given Lua entry point function.

A single return value may be retrieved from Lua by referencing the _RISKEXEC_RETURN_VALUE1_ global variable when this macro returns.
This value is only available if _RISKEXEC_RC_ is 0.

The global variable _RISKEXEC_RC_  takes the following values:

 - 0 when NO error was caught when the function was run
 - 1 when an error WAS caught when the function was run

\n

\ingroup CommonAnalytics
\author  SAS Institute Inc.
\date    2014
*/
%macro rsk_call_riskexec(entrypoint_module  =,
                         entrypoint_function=,
                         invoker            =sas.risk.rmx.rsk_module_function_invoker,
                         restartlua         = Y,
                         arg1               =,
                         arg2               =,
                         arg3               =,
                         arg4               =,
                         arg5               =,
                         arg6               =,
                         arg7               =,
                         arg8               =,
                         arg9               =,
                         arg10              =
                         );


  /* Lua input variables passed to sas_macro_entrypoint and
     whose arguments are then passed to the specified Lua entry point. */
  %local _riskexec_entrypoint_module
         _riskexec_entrypoint_function
         _riskexec_arg1
         _riskexec_arg2
         _riskexec_arg3
         _riskexec_arg4
         _riskexec_arg5
         _riskexec_arg6
         _riskexec_arg7
         _riskexec_arg8
         _riskexec_arg9
         _riskexec_arg10;

  %let _riskexec_entrypoint_module=&entrypoint_module;
  %let _riskexec_entrypoint_function=&entrypoint_function;
  %let _riskexec_arg1 = %bquote(&arg1);
  %let _riskexec_arg2 = %bquote(&arg2);
  %let _riskexec_arg3 = %bquote(&arg3);
  %let _riskexec_arg4 = %bquote(&arg4);
  %let _riskexec_arg5 = %bquote(&arg5);
  %let _riskexec_arg6 = %bquote(&arg6);
  %let _riskexec_arg7 = %bquote(&arg7);
  %let _riskexec_arg8 = %bquote(&arg8);
  %let _riskexec_arg9 = %bquote(&arg9);
  %let _riskexec_arg10= %bquote(&arg10);

  /* prime the rc.
     Lua functions sas.symput() and sas.gsymput() only seem to work right now when the already variable exists */
  %global _RISKEXEC_RC_  _RISKEXEC_RETURN_VALUE1_;

/* -2=failed to enter proc riskexec
   -1=entered proc riskexec but failed to call requested lua ftn,
    0=executed requested lua function without throwing an error,
    1=error thrown from requested lua ftn */
  %let _RISKEXEC_RC_=-2;
  %let _RISKEXEC_RETURN_VALUE1_=;

  %global _RISKEXEC_INVOKER;
  %let _RISKEXEC_INVOKER=&invoker;

  /* It is not possible to use the INFILE option on PROC LUA to call directly to a Lua module that has
     a dotted package name unless the module's directory is listed explicitly on the LUAPATH
     (unlike require() which is the functionality we want). So we have to use an "entry point caller" until INFILE search logic is changed.*/

  %local _riskexec_entrypoint; %let _riskexec_entrypoint=sas.risk.utils.sas_macro_entrypoint;



  %put NOTE: restartLUA was set to &restartLUA.;
  %put NOTE: _RISKEXEC_RC_ (before lua call) is &_RISKEXEC_RC_;

  proc lua infile='entry_point_caller' %if &restartlua = Y %then %do; restart %end;;  run;

  %put NOTE: _RISKEXEC_RC_ (after lua call) is &_RISKEXEC_RC_;
  %if &_RISKEXEC_RC_ ne 0 and %rsk_error_occurred eq 0 %then %do;
      /* if rc is empty, the sas session may be in syntax checking mode */
      %put ERROR: Calling Lua functions "&entrypoint_function" in module "&entrypoint_module".;
      %rsk_terminate();
  %end;
%mend;
