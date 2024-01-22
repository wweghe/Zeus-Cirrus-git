--[[
/* Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA */

/*!
\file    stringutil.lua

\brief   This module is about utility functions for strings

\ingroup commonAnalytics

\author  SAS Institute Inc.

\date    2015

*/

]]


local M = {}

--- Split a string using a delimiter
-- @param str String to split
-- @param by delimiter to split by
-- @return a table containing the string segments
M.split = string.split


local split_table_names = function(str)
   local names = {}
   for name in str:gmatch("[_%w]+") do
      table.insert(names, name)
   end
   return names
end
M.split_table_names = split_table_names


M.starts_with = string.starts_with



--- Escape a SAS string value where the characters are given as input (not a general SAS syntax string but a string value)
local escape_sas_string_value = function(in_str)
   if in_str then
      local result = in_str
      result = string.gsub(result, '%%', '%%%%')
      result = string.gsub(result, "'", "%%'")
      result = string.gsub(result, '"', '%%"')
      result = string.gsub(result, '%(', '%%(')
      result = string.gsub(result, '%)', '%%)')
      return '%nrstr(' .. result .. ')'
   else
      return ""
   end
end
M.escape_sas_string_value = escape_sas_string_value


-- already defined in sasext.lua
M.trim = string.trim
M.ends_with = string.ends_with


-- quote a list of strings for use in an sql IN expression
local quote_list = function(list, to_upper)
   if to_upper == nil then to_upper=true end  -- by default uppers the values

   local the_type = type(list)
   if the_type == "string" then
      return '"' .. list .. '"'
   end

   local out=''
      for i,v in ipairs(list) do
         if i ~= 1 then
            out = out .. ', '
         end
         if to_upper then
            v = string.upper(v)
         end
         out = out .. '"' .. v .. '"'
      end
   return out
end
M.quote_list = quote_list


return M
