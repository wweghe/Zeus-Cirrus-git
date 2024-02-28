--[[
/* Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA */

/*!
\file    submit_helper.lua

\brief   This module exists to get the error messages from our code when using SUBMIT statements within PROC LUA.
         The reason this is needed is that the errors.throw() function throws a table (with error information) when there is an error,
         not just a error message string.  This code dips into the table to print the error string.
\ingroup commonAnalytics

\author  SAS Institute Inc.

\date    2022-2023

*/

]]

local M={}
local errors=require 'sas.risk.utils.errors'

local submit=function(code)
   local f = load(code)
   local ok, error_obj = errors.protected_call(f)
   if not ok then
      if error_obj.msg == error_obj.msg_key then -- key is same as msg, so msg did not come from msg file, so prepend ERROR:
        sas.print("%1z" .. error_obj.msg)
      else
        print(error_obj.msg)  -- came from msg file, should have ERROR: prepended
      end
   end
end
M.submit=submit

return M
