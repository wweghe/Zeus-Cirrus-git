--[[
/* Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA */

/*!
\file    metalib.lua

\brief   This module creates a library SASTable and its related functions.

\ingroup commonAnalytics

\author  SAS Institute Inc.

\date    2015

*/

]]



local M = {}

local MetaLibrary = {}
local SASLibrary = {}
local SASTable = {}
M.MetaLibrary = MetaLibrary
M.SASLibrary = SASLibrary
M.SASTable = SASTable



function SASTable:new(library, memname, type)

   -- instantiate the new object and set up its inheritance
   o = {}
   setmetatable(o, self)
   self.__index = self

   o.library = library
   o.memname = string.upper(memname)

   if type then
      type = string.upper(type)
   end
   o.type = type
   o.fullname = library.libname .. "." .. o.memname
   return o
end

function SASTable:exists()
   if self.type then
      return sas.exists(self.fullname, self.type)
   else
      return sas.exists(self.fullname)
   end
end


function SASTable:__tostring()
   local type_str = ""
   if self.type then
      type_str = " (" .. self.type .. ")"
   end
   return self.fullname  .. type_str
end

function SASTable:type()
   if not self.type then
      local dsid = sas.open("sashelp.vstabvw")
      assert(dsid, "Could not open sashelp.vslib")
      sas.where(dsid,"libname='".. library.libname .."' and memname='" .. self.memname .. "'")
      assert(sas.fetch(dsid), "Table is not available: " .. self)
      self.type = sas.get_value(dsid, "type")
      sas.close(dsid)
   end
   return self.type
end


function SASLibrary:new(libname)
   -- instantiate the new object and set up its inheritance
   o = {}
   setmetatable(o, self)
   self.__index = self

   o.libname = string.upper(libname)
   return o
end

function SASLibrary:__tostring()
   return self.libname
end

function SASLibrary:load(libname)
   local dsid = sas.open("sashelp.vlibnam")
   assert(dsid, "Could not open sashelp.vslib")
   sas.where(dsid,"libname='".. libname .."'")
   local paths = {}
   local i = 0
   local last_path = ""
   while(sas.next(dsid)) do
      if i==0 then
         self.readOnly = (sas.get_value(dsid,"readonly") == "yes")
         self.temporary = (sas.get_value(dsid,"temp") == "yes")
         self.engine = (sas.get_value(dsid,"engine")=="yes")
      end
      local path = sas.get_value(dsid,"path")
      if last_path ~= path then
         table.insert(paths, path)
         last_path = path
      end
   end
   self.paths = paths
   self.loaded = true
end

function SASLibrary:tables()
   local sastables = {}
   local memlist = sas.memlist(self.libname, "DATA VIEW")
   for i,v in ipairs(memlist) do
      table.insert(sastables, SASTable:new(self, v.memname, v.memtype))
   end
   return sastables
end

function SASLibrary:paths()
   if not self.loaded then
      self:load(self.libname)
   end
   return self.paths
end

function SASLibrary:isReadOnly()
   if not self.loaded then
      self:load()
   end
   return self.readOnly
end

function SASLibrary:isTemporary()
   if not self.loaded then
      self:load()
   end
   return self.temporary
end

function SASLibrary:engine()
   if not self.loaded then
      self:load()
   end
   return self.engine
end

function MetaLibrary:new()
   -- instantiate the new object and set up its inheritance
   o = {}
   setmetatable(o, self)
   self.__index = self
   return o
end

function MetaLibrary:libraries()
  local dsid = sas.open("sashelp.vslib")
  assert(dsid, "Could not open sashelp.vslib")

  local libs = {}
  while(sas.next(dsid)) do
     local libname = sas.get_value(dsid, "libname")
     local lib = SASLibrary:new(libname)
     table.insert(libs, lib)
  end
  sas.close(dsid)
  return libs
end

return M
