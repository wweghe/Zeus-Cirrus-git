--[[
/* Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA */

/*!
\file    sas_msg.lua

\brief   This module is about getting message file.
         It returns a formatted message given it's unformatted components.

\ingroup commonAnalytics

\author  SAS Institute Inc.

\date    2015

*/

]]


local M={}

local _get_msg_file = function(msg_key)
   local getMessageFile = get_message_file or function(msgKey) return nil end
   local msg_file = getMessageFile(msg_key)
   if not msg_file then
      sas.print("%2zCould not determine message file for message key '" .. tostring(msg_key) .. "'")
      return msg_key
   end

   msg_file = msg_file:upper()
   if not sas.exists(msg_file) then
      sas.print("%2zMessage file " .. msg_file .. " does not exist.")
      return msg_key
   end

   return msg_file
end

---
--   Returns a formatted message given it's unformatted components.
--
--   Assumptions:  get_message_file() is defined by the product code being run
local get_message = function(msgKey, s1, s2, s3, s4, s5, s6, s7)

   local msg_file = _get_msg_file(msgKey)
   -- The sasmsg function will return the msgKey value when the message is missing from the msg file
   return sas.sasmsg(msg_file, msgKey, "NOQUOTE", s1 or "", s2 or "", s3 or "", s4 or "", s5 or "", s6 or "", s7 or "")
end
M.get_message = get_message
M.get = get_message  -- alias


--- Utility function intended to print NOTE and WARNING messages to the log.  Errors messages should be thrown via errors.throw()
local print = function(msgKey, s1, s2, s3, s4, s5, s6, s7)
  -- TODO: dajack - fix the highlighting of these messages in the sas log
  sas.print(get_message(msgKey, s1, s2, s3, s4, s5, s6, s7))
end
M.print = print

return M
