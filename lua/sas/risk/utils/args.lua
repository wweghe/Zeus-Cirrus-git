--[[
/* Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA */

/*!
\file    args.lua

\brief   This module has methods to help check a function's arguments

\ingroup commonAnalytics

\author  SAS Institute Inc.

\date    2015

\details The intent is that a function's arguments should be checked before using them.
         By doing so, it also helps document *in the code* what the function expects ... which is
         sometimes difficult to see in the code since Lua has dynamic typing.

*/

]]

local errors = require 'sas.risk.utils.errors'


local M = {}

local assert_string = function(arg, argnum)
   argnum = argnum or 1
   local t = type(arg)
   if t ~= "string" then
      errors.throw("utils_arg_not_string",  tostring(argnum), t)
   end
end
M.assert_string = assert_string


local function assert_string_or_nil(arg, argnum)
   argnum = argnum or 1
   local t = type(arg)
   if arg ~= nil and t ~= "string" then
      errors.throw("utils_arg_not_string_or_nil",  tostring(argnum), t)
   end
end
M.assert_string_or_nil = assert_string_or_nil


local assert_nonempty_string = function(arg, argnum)
   argnum = argnum or 1
   assert_string(arg, argnum)
   if arg == "" then
      errors.throw("utils_arg_string_empty",  tostring(argnum), arg)
   end
end
M.assert_nonempty_string = assert_nonempty_string


local assert_number = function(arg, argnum)
   argnum = argnum or 1
   local t = type(arg)
   if t ~= "number" then
      errors.throw("utils_arg_not_number",  tostring(argnum), t)
   end
end
M.assert_number = assert_number


local assert_number_or_nil = function(arg, argnum)
   argnum = argnum or 1
   local t = type(arg)
   if arg ~= nil and t ~= "number" then
      errors.throw("utils_arg_not_number_or_nil",  tostring(argnum), t)
   end
end
M.assert_number_or_nil = assert_number_or_nil


local assert_table = function(arg, argnum)
   argnum = argnum or 1
   local t = type(arg)
   if t ~= "table" then
      errors.throw("utils_arg_not_table",  tostring(argnum), t)
   end
end
M.assert_table = assert_table


local assert_table_or_nil = function(arg, argnum)
   argnum = argnum or 1
   local t = type(arg)
   if arg ~= nil and t ~= "table" then
      errors.throw("utils_arg_not_table_or_nil",  tostring(argnum), t)
   end
end
M.assert_table_or_nil = assert_table_or_nil


local assert_function = function(arg, argnum)
   argnum = argnum or 1
   local t = type(arg)
   if t ~= "function" then
      errors.throw("utils_arg_not_function",  tostring(argnum), t)
   end
end
M.assert_function = assert_function


local assert_function_or_nil = function(arg, argnum)
   argnum = argnum or 1
   local t = type(arg)
   if arg ~= nil and t ~= "function" then
      errors.throw("utils_arg_not_function_or_nil",  tostring(argnum), t)
   end
end
M.assert_function_or_nil = assert_function_or_nil

--print("'args' module loaded")
return M
