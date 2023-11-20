--[[
/* Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA */

/*!
\file    errors.lua

\brief   This module has methods about the error handling occured in lua code

\ingroup commonAnalytics

\author  SAS Institute Inc.

\date    2015

*/

]]

local tableutils = require 'sas.risk.utils.tableutils'
local sas_msg = require 'sas.risk.utils.sas_msg'

local M = {}


---
--     throw():
--
--     Throw an error with arguments specifying pieces of a formatted message.
--     This function will not return but will send an error object
--     to the last pcall() or xpcall() function in the call stack.
--
--     msgkey designates a message in the message file defined by the product (see rmi.lua, etc.)
--
local throw = function(msg_key, ...)

   -- Create and fill in an error object
   local error_obj = {}
   error_obj.msg_key = msg_key
   error_obj.msg = sas_msg.get(msg_key, ...) or msg_key
   error_obj.from_throw=true

   -- Pass the error object to the error() function to do the real throwing of the error.
   -- Control will pass from here to the next outer error handler (e.g. protected_call_handler).
   -- Note that Lua's error() function only pays attention to the 2nd arg (if supplied) if the 1st arg
   -- is a string (to which it can prepend the error location).
   -- In our case the 1st arg is not a string, so it cannot prepend the error location ... but that's not
   -- serious since a traceback gotten in the error handler can get that.
   error(error_obj)
end
M.throw = throw


-- a couple forward declares in order to keep our main functions at the top of the file
local xpcall_with_args, protected_call_handler

---
--   A function that uses xpcall_with_args to call a target function and handles errors using it's own
--   error handler.
--
--   All callers of this function will receive return values as described
--   1st return value =a boolean "success" value
--            true=the call to the given function succeeded without error (just like pcall & xpcall return)
--                 and return values from the called function are returned as the subsequent return values (just like pcall & xpcall return)
--           false=the call to the given function failed with an error and
--                 the error object table described below is returned as a second return value
--
--   An error object (table) with the following fields filled in is returned when false is returned for the 1st return value:
--        @param msg_key   the message key to the error message
--        @param msg  a potentially translated (using server-side locale) & value substituted error message
--        @param trace a traceback - or potentially several tracebacks concatenated together depending on whether errors were rethrown
--
local protected_call = function(func, ...)
   -- local args = { ... }
   -- local n = select("#", ...)
   -- print("Got " .. tostring(n) .. " arguments into protected_call:\n" .. tableutils.to_string(args))
   return xpcall_with_args(func, protected_call_handler, ...)
end
M.protected_call = protected_call



---
--   The xpcall as shipped with Lua 5.1 does not provide the ability to pass arguments to the function it calls
--   in protected mode.  To do this we have to jump through some hoops to build our own version of xpcall
--   to do that.
--
xpcall_with_args = function (func, handler, ...)

   local args = { ... }
   local n = select("#", ...)
   -- print("Got " .. tostring(n) .. " arguments into xpcall_with_args:\n" .. tableutils.to_string(args))
   return xpcall(function()
                    return func(table.unpack(args, 1, n))
                 end, handler)
end




-- The protected call handler creates and returns an "error table with fields" from the given error object.
-- This function gets control whenever a protected call is made that ends with an error being thrown
-- which can occur for a variety of reasons:
--       error() is called (from throw() or otherwise), indexing a nil value, a compile error, etc.
protected_call_handler = function(in_error_obj)
   local out_error_obj = in_error_obj

   -- In the case we got an error string without it being in the usual error table, put it in one
   -- The msg key will not always be a key but it may be sometimes depending on what got passed in
   if type(in_error_obj) ~= "table" then
      out_error_obj = {msg=in_error_obj, msg_key=in_error_obj}
   end


   -- determine the starting level to start the traceback ...
   -- essentially removing clutter from the traceback that one would otherwise have to
   -- mentally "step-over" to see where the real error occurred
   local level=3;  -- 3 to remove this function and the overriding of debug.traceback() in sasext
   if out_error_obj.from_throw then
      level = level+2  -- also remove throw() and it's use of error() from the traceback returned
   end

   -- concatenate the stack trace with the most detailed trace on top
   out_error_obj.trace = debug.traceback(out_error_obj.trace or "", level)

   return out_error_obj
end

--print("'errors' module loaded")

return M
