--[[
/* Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA */

/*!
\file    tableutils.lua

\brief   This module is about utility functions for tables

\ingroup commonAnalytics

\author  SAS Institute Inc.

\date    2015

*/

]]


local M = {}

---
--   Returns the true number of entries in a table, unlike the # operator
--   which does not work for non-array tables (i.e. tables without numeric keys)
--   and does not work when an array has holes, for example:
--   t[1]="x"
--   t[3]="y"
--   print(#t)  -- prints 1
--   print(table.size(t)) -- prints 2

M.size = table.size  -- use function defined in sasext.lua

M.contains_value = table.contains


---
-- Returns a simple union of  tables
local union = function(...)
   local result = {}
   for i,t in ipairs(...) do
      for k,v in pairs(t) do
         result[k] = v
      end
   end
   return result
end
M.union = union



M.to_string=tostring             -- use tostring overridden in sasext.lua

return M
