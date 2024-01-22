--[[
/* Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA */

/*!
\file    rsk_init.lua

\brief   this module contains rsk-initialization logic (formerly rsk module bootstrap code),
         that is executed before all rsk other logic.

\ingroup commonAnalytics

\author  SAS Institute Inc.

\date    2015

\details Include the product lua code so that we have access to the function get_message_file,
          which is required for messaging

*/

]]

sas.set_separator("@")
local errors = require('sas.risk.utils.errors')

--[[
local prod = string.lower(sas.symget("PRODUCT"))
require('sas.risk.' .. prod .. '.' .. prod)
]]

--- Set RSK up to use the multi-byte string capabilities provided by sasstr -- this allows all of our RMX Lua string code to "just work"
--  in the encoding that the SAS session runs in -- which is what a user expects.
--  The following needs to occur for this to happen completely:
--  1)  All functions provide by the string library are being replaced (when used by RSK/RMX) by multi-byte equivalents.
--      This then causes string.len(mystr) to call into sasstr instead of the single-byte Lua string library.
--   ... the following must be done in our Lua C code ... can't be done from here ...
--  2)  All string functions need to be overridden using the metatable for the string type to point to sasstr. so that s:upper() goes to sasstr
--      This then causes  mystr:len() to call into sasstr instead of the single-byte Lua string library
--  3)  All string operators need to be directed to functions in sasstr
--      This then causes  (str_a .. str_b) to perform the concatenation operation from sasstr instead of the single-byte Lua string library
--
--  Commented out until sasstr is better tested and working as expected
--local sasstr = require 'sasstr'
--sasstr.take_over()  -- there is no undoing this for a Lua VM instance once it's done




DEBUG_ACTIVE=true

-- The log4sas logger provides us little value until the Lua VM sticks around for a SAS session between PROC calls ... commenting out
--local setup_logger = function (file)
--   if _G.logger == nil then
 --     print('Setting up logger to ' .. tostring(file))
      -- Set up the Log4SAS logger that is usable in all RMX code
      -- (all code assumes access to a global logger called "logger")
--      sas.submit ([[filename log4rmx '@file@'  LRECL=10000000 MOD;]], {file=file}) -- large record length to prevent record truncation
--      local appender = log4sas.Log4SasAppender:new("rmx","log4rmx", "trace")
--      _G.logger = appender:logger("rmx_log")
--   end
--end


Debug = function(msg)
   if DEBUG_ACTIVE then
      if logger then
         logger:debug(msg)
      else
         print(msg)
      end
   end
end


local IGNORE_TRACE_POINTS = {
                             ['sasext.lua:tostring']=1,
                             ['sasext.lua:assert']=1,
                             ['sasext.lua:do_token_replace']=1
                             }

-- Returns nil if the calling ftn should not be logged, otherwise returns a string representing the name of the function in its file

local calling_ftn_name = function()
   local out=nil  -- default to tracepoint should not be logged
   local info= debug.getinfo(3,"nS")  -- 3 = get caller of caller of this function
   local ftn_file = info.short_src or ""

   -- only log non-C functions

   if not string.starts_with(ftn_file, '[C]') then
      local ftn_name = info.name or ""
      out = ftn_file .. ':' .. ftn_name
      -- To keep from poluting the log with some functions that provide little value most of the time, we are
      -- removing their trace points from being in the log
      if IGNORE_TRACE_POINTS[out] then
        out = nil
     end
   end

   return out
end

local get_args_str = function(skip_temps)
   local s = ''
   local i = 1
   while true do
      local name, value = debug.getlocal(3, i) -- 3 = get local vars at call time for caller of caller of get_args_str() ... i.e. caller of debug hook
      if not name or (skip_temps and name == '(*temporary)') then
         -- stop going through name value pairs, you are at the end
         break
      end

      -- comma separate the args.  do this even if skipping 1 below to give an indication to the user we skipped a type we don't print

      if #s > 0 then
         s = s .. ', '
      end

      -- Skip printing certain types of variables.  They provide little value (other than their address) and pollute the sas log quite a bit causing a lot of spilling onto next lines because the log is only a fixed length
      local value_type = type(value)
      if value_type ~= 'table'  and value_type ~='function' then
         s = s .. name .. '=' .. tostring(value);
      end
      i = i + 1
    end
    return s
end

local debug_hook_ftn = function(event)
   if event == 'call' or event == 'tail call' then
      local ftn = calling_ftn_name()
      if ftn then
         local s = '    args: ' .. get_args_str(true)
         Debug(sas.putn(sas.time(),"time12.3") .. ' >> ' .. ftn .. s)
      end
   elseif event == 'return' then
      local ftn = calling_ftn_name()
      if ftn then
         local s = get_args_str()
         if #s> 0 then   -- only include if there are any vars at the return
            s = '    vars-at-return: ' .. s
         end
         Debug(sas.putn(sas.time(),"time12.3") .. ' << ' .. ftn .. s)
      end
   end
end

-- override error() to guarantee that RSK_TERM_MESSAGE is always set (even if code wasn't executed via %rsk_call_riskexec)

local my_error = error
error = function(errobj)
           local msg
           if type(errobj)=="table" then
              msg = errobj.msg_key
           else
              msg = tostring(errobj)
           end
           sas.symput("RSK_TERM_MESSAGE", msg)
           my_error(errobj)
        end

-- override assert() to throw error via the errors library in order to get the traceback info dumped to the log

assert = function(expr, msg)
            if not expr then
               errors.throw(msg)
            end
         end

-- enable tracing if the TRACE_LUA global variable is defined to be Y

local TRACE_LUA=sas.symget('TRACE_LUA')
TRACE_LUA=type(TRACE_LUA) == 'string' and TRACE_LUA=='Y' -- turn TRACE_LUA into a boolean

if TRACE_LUA then
   debug.sethook(debug_hook_ftn, 'cr')
end
