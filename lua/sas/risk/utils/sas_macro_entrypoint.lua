--[[
/* Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA */

/*!
\file    sas_macro_entrypoint.lua

\brief   The code in this module exists as a single entrypoint for the %rsk_call_riskexec() macro.
         It calls out to get the arguments sent from SAS macro-land, calls the given Lua module.function
         with those arguments, and returns any results to SAS macro-land.

\ingroup commonAnalytics

\author  SAS Institute Inc.

\date    2015

\requirement    require rsk_init before anything else in this module is done
                require 'sas.risk.rmx.rsk_init'

\detail         Note that all passed (length=0) empty strings are converted to nil values prior to
                calling the Lua entry point. Lua code that wants empty strings instead of nils can convert as wished.

                A function that returns the arguments specified on the other side of a PROC RISKEXEC invocation via
                the macro %rsk_call_riskexec.  All empty string values "" passed from SAS macro are converted to
                nil values.

                @return (1) argument count
                @return (2) argument table (possibly with some nil values in it)
*/

]]


local get_args = function()

   local in_args={}
   in_args[1] = sas.symget('_riskexec_arg1') or sas.MISSING
   in_args[2] = sas.symget('_riskexec_arg2') or sas.MISSING
   in_args[3] = sas.symget('_riskexec_arg3') or sas.MISSING
   in_args[4] = sas.symget('_riskexec_arg4') or sas.MISSING
   in_args[5] = sas.symget('_riskexec_arg5') or sas.MISSING
   in_args[6] = sas.symget('_riskexec_arg6') or sas.MISSING
   in_args[7] = sas.symget('_riskexec_arg7') or sas.MISSING
   in_args[8] = sas.symget('_riskexec_arg8') or sas.MISSING
   in_args[9] = sas.symget('_riskexec_arg9') or sas.MISSING
   in_args[10]= sas.symget('_riskexec_arg10') or sas.MISSING

   --print("Arguments received by sas_macro_entrypoint:\n" .. tableutils.to_string(in_args));

   -- Remove any ending empty strings.  Nil values will be passed automatically by Lua for any unsufficient
   -- number of arguments.
   while in_args[#in_args] == "" do
      table.remove(in_args)
   end

   -- Now replace empty strings with nil values.
   -- Lua code that wants empty strings instead of nils can convert as wished.
   -- This puts nil "holes" in the args table so care must be taken prior to calling the user's function
   -- (the length operator is wrong after putting a hole in the table, etc.)
   local arg_count = #in_args  -- get the "real" argument count

   local args = {}
   for i,v in ipairs(in_args) do
     if v ~= "" then  -- we convert empty strings to nil values when calling lua from SAS
        args[i] = v  -- always store in ith value, not #args+1 since # operator is bogus when nils are in the table
     end
   end

   return arg_count, args
end


--[[                          ]]--
--[[ START OF EXECUTABLE CODE ]]--
--[[                          ]]--

-- Put out the Entering Lua <version> message
-- use print() so that this message stays nicely formatted when lua tracing is turned on
print('--------------------')
print('  Entering ' .. _VERSION)
print('--------------------')


-- We want a max linesize so that any tracebacks get formatted as best they can when put in the SAS log.
-- They still get "hiccups" in their printing at 256 char boundaries (which is the max linesize).
-- There is a bug.
-- linesize set outside Lua does not pass to Lua and vice versa, set it to the max here for this Lua call.
sas.submit[[options linesize=max;]]


-- Get the module and function that is supposed to be called
local module_name = tostring(sas.symget("_RISKEXEC_ENTRYPOINT_MODULE"))
local function_name = tostring(sas.symget("_RISKEXEC_ENTRYPOINT_FUNCTION"))

if module_name == "" then
   error("No entry point module name found in SAS global variable _RISKEXEC_ENTRYPOINT_MODULE")
end

if function_name == "" then
   error("No entry point function name found in SAS global variable _RISKEXEC_ENTRYPOINT_FUNCTION")
end


local invoker_name = sas.symget("_RISKEXEC_INVOKER")
local arg_count, args = get_args()


if (invoker_name==nil or invoker_name=="") then
   invoker_name = "sas.risk.utils.generic_module_function_invoker"
end

local invoker = require(invoker_name)
invoker.invoke(module_name, function_name, table.unpack(args, 1, arg_count))
