--[[
/* Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA */

/*!
\file    rsk_module_function_invoker.lua

\brief   this is the function invoker

\ingroup commonAnalytics

\author  SAS Institute Inc.

\date    2015

\details require rsk_init before anything else in this module is done

*/

]]

require 'sas.risk.rmx.rsk_init'


local errors = require 'sas.risk.utils.errors'

---
-- Override assert so that we can capture the msg value, and later set RSK_TERM_MESSAGE with
-- that value (for the mid-tier)
--
local old_assert = assert
--[[assert = function(expr, msg)
            local success, result = require('sas.risk.utils.errors').protected_call(old_assert, expr, msg)
            if not success then
               if type(result)=="table" then
                  result.msg_key = msg
               else
                  print("result = ", result)
               end
               error({msg_key=msg})
            end
          end
          --]]


local function invoke(mod_name, ftn_name, ...)
   sas.gsymput("_RISKEXEC_RC_", "-1")  -- got here but did not even get to call the requested lua code
   _G.rsk = require("sas.risk.rmx.rsk")
   Debug('Recovering Lua state prior to running ' .. mod_name .. '.' ..  ftn_name .. '  ...')
   local real_invoker = require('sas.risk.utils.generic_module_function_invoker')
   real_invoker.invoke(mod_name, ftn_name, ...)
end

return { recover=recover,invoke=invoke }
