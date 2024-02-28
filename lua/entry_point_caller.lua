--[[
/* Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA */

/*!
\file    entry_point_caller.lua

\brief   This module exists because currently it is not possible to use the INFILE option on PROC LUA
         to call directly to a Lua module that has a dotted package name unless the module's directory is listed
         explicitly on the LUAPATH (unlike require()).  We will continue to use this indirect means of calling
         a module until INFILE can search via a dotted path to the module.

\ingroup commonAnalytics

\author  SAS Institute Inc.

\date    2015

*/

]]

local target=sas.symget('_riskexec_entrypoint')
print('entry_point_caller is calling:', tostring(target))
require(target)

