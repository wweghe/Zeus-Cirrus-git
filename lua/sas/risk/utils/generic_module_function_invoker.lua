--[[
/* Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA */

/*!
\file    generic_module_function_invoker.lua

\brief   Generic invoker. The function that calls the user's real module.function()

\ingroup commonAnalytics

\author  SAS Institute Inc.

\date    2015

*/

]]

local errors = require 'sas.risk.utils.errors'

local function args_string(args_table)
   local out = ''
   for k,v in pairs(args_table) do  -- use pairs instead of ipairs so that empty (nil) parameters are shown
      out=out .. '\n' .. '[' .. tostring(k) .. ']=' .. tostring(v)
   end
   if out=='' then out='(none)' end
   return out
end


local invoke = function(mod_name, ftn_name, ...)
   sas.gsymput("_RISKEXEC_RC_", "-1")  -- did not even get to call the program
   local mod = require(mod_name)

   local function_to_call = mod[ftn_name]
   if not function_to_call then
      error("Function " .. tostring(mod_name) .. "." .. tostring(ftn_name) .. "() could not be found")
   end

   -- use print() so that this message stays nicely formatted when lua tracing is turned on
   local args_str = args_string({...})  -- get this ahead of time to help with the following being nicely formatted when tracing
   print('---------------------------------------------------------------------------')
   print("Calling Lua function " .. mod_name .. "." .. ftn_name .. "() with arguments: " .. args_str)
   print('---------------------------------------------------------------------------')

   local start = sas.datetime()
   local ok, error_obj = errors.protected_call(function_to_call, ...)

   if ok then
      sas.gsymput("_RISKEXEC_RC_", "0")
      local rval1 = tostring(error_obj or "")
      sas.gsymput("_RISKEXEC_RETURN_VALUE1_", rval1) -- error_obj is the returned value #1 when no error

      -- use print() so that this message stays nicely formatted when lua tracing is turned on
      print('---------------------------------------------------------------------------')
      print("Returning from Lua function " .. mod_name .. "." .. ftn_name .. "(), seconds spent: " .. tostring(sas.datetime()-start))
      print('          Return value="' .. rval1 ..'"')
      print('---------------------------------------------------------------------------')
   else
      sas.gsymput("_RISKEXEC_RC_", "1")
      -- Report error to midtier when applicable
      sas.submit([[%rsk_report_error(error=@error@);]], {error=error_obj.msg_key})

      sas.print("%1zLua function " .. mod_name .. "." .. ftn_name .. "() ended in error:")
      sas.print("%1z" .. (error_obj.msg or error_obj))
      sas.print("%1z" .. (error_obj.trace or ""))
  end
end

return {invoke=invoke}
