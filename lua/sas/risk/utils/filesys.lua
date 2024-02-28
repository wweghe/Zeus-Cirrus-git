--[[
/* Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA */

/*!
\file    filesys.lua

\brief    This module contains utility functions related to file systems accessed via SAS.
          This utility is meant to fit on top of the SAS API provided by the sasext module
          to make a more user-friendly interface for file systems accessed via SAS.

\ingroup commonAnalytics

\author  SAS Institute Inc.

\date    2015

*/

]]


local stringutils = require 'sas.risk.utils.stringutils'
local args = require 'sas.risk.utils.args'
local errors = require 'sas.risk.utils.errors'

local M = {}

--- Tests whether the given directory path exists or not.
--     This differs from the sas.fileexist() function in that this function ensures that the path
--     is to a directory and not to a file
--     @return boolean value
--
--     Note: as file_exists() is implemented, this function cannot be based on file_exists()

local dir_exists = function(dir_path)
   local rc = false
   local fileref_rc = sas.filename("_tdir", dir_path)  -- assign the fileref
   if fileref_rc==0 then                              -- if successful
      local did = sas.dopen("_tdir")    -- try to open the fileref as a directory
      if did > 0 then                   -- if successful
         sas.dclose(did)                -- close the directory
         rc = true
      end
      sas.filename("_tdir")  -- deassign the fileref
   end
   return rc
end
M.dir_exists = dir_exists


--- Tests whether the given file exists or not.
--  This differs from the sas.fileexist() function in that this function ensures that the path
--  is NOT to a directory.
--  @return boolean value
local file_exists = function(file_path)
   return sas.fileexists(file_path) and not dir_exists(file_path)
end
M.file_exists = file_exists


--- Makes the directories all the way down to the end of the path as necessary
-- @param the path to the directory that was created (or already exists)
local mkdir = function(path)
   if (dir_exists(path)) then
     return path
   end

   local path = string.gsub(path, "\\","/")
   local pathElements = stringutils.split(path,"/")
   local progressingPath  -- progressing path is the path we know that exists
   progressingPath = ""
   if not string.find(pathElements[1], ":") then
      progressingPath = "/"
   end
   for i,v in ipairs(pathElements) do
      if not string.find(v,":") then
         local nextPath = progressingPath .. "/" .. v
         if not sas.fileexists(nextPath) then
            local new_path = sasxx.mkdir(v, progressingPath)
            if new_path == nil then
               print("Failed to create directory: " .. tostring(nextPath))
               errors.throw("utils_filesys_mkdir_error", nextPath)
            end
         end
         progressingPath = nextPath
      else
         progressingPath = v
         v = ""
      end
   end
   if not dir_exists(path) then
      errors.throw("utils_filesys_mkdir_error", path)
   end
   return path
end
M.mkdir = mkdir

--- Utility function to creates the given directory if it does not already exist
local ensure_dir_exists = function(dir)
   if not dir_exists(dir) then
      mkdir(dir)
   end
end
M.ensure_dir_exists=ensure_dir_exists


local dir_tree_walk_p

--  top   directory has           1      0         nil
dir_tree_walk_p = function(dir, level, dir_id, parent_dir_id, maxdepth, callback, arg)

   -- Open the directory given
   local fref = "rsk" .. tostring(level)
   sas.filename(fref, dir)

   local did = sas.dopen(fref)

   if did == 0 then
      -- could not open, so release the fileref
      sas.filename(fref)
      return
   end


   -- Iterate over all the directory entries
   local dnum = sas.dnum(did);
   for i=1,dnum do
      local memname = sas.dread(did, i)

      if memname ~= "" then
         if dir_exists(dir .. "/" .. memname) then
            parent_dir_id = dir_id
            dir_id = dir_id+1

            if maxdepth > level then
               -- recurse further down
               dir_tree_walk_p(dir .. "/" .. memname, level+1, dir_id, parent_dir_id, maxdepth, callback, arg)
            end

            callback(dir_id, "D", memname, level, parent_dir_id, dir, arg)
         else
            callback(nil   , "F", memname, level, parent_dir_id, dir, arg)
         end
      end
   end

   -- Close the directory
   sas.dclose(did)
   sas.filename(fref)
end


local dir_tree_walk_echo_callback = function(dir_id, type, memname, level, parent, context, arg)
   print("inside echo-callback : dir_id=" .. tostring(dir_id) ..
                            ", type=" .. tostring(type) ..
                         ", memname=" .. tostring(memname) ..
                           ", level=" .. tostring(level) ..
                          ", parent=" .. tostring(parent) ..
                         ", context=" .. tostring(context) ..
                             ", arg=" .. tostring(arg))
end


-- no callback is made for the directory that is given... only its recursive contents
-- calls callbacks in an order usable by rmdir (files first, then the directory containing them)
local dir_tree_walk = function(dir, callback, maxdepth, arg)
  args.assert_string(dir)
  args.assert_function_or_nil(callback)
  args.assert_number_or_nil(maxdepth)

  maxdepth = maxdepth or 9999999       -- make non-nil for easier testing
  callback = callback or dir_tree_walk_echo_callback  -- default to echo function

  return dir_tree_walk_p(dir, 1, 0, nil, maxdepth, callback, arg)
end
M.dir_tree_walk = dir_tree_walk


local rm_file_or_empty_dir = function(path)
    local fref = "_T"

    sas.filename(fref, path);
    local rc = sas.fdelete(fref)
    sas.filename(fref) -- clear the fileref

    if rc ~= 0 then
       print(sas.sysmsg())  -- print this in case there's something there ... a lot of times there's not
       errors.throw("utils_filesys_cannot_delete_file_or_dir", path)
    end
end

local rm_file_cb = function(dir_id, type, memname, level, parent, context, arg)
   rm_file_or_empty_dir(context .. "/" .. memname)
end

--- Removes a directory by recursively deleting its contents
local rmdir = function(dir)
   if dir_exists(dir) then
      print("Deleting directory " .. dir)
      dir_tree_walk(dir, rm_file_cb)  -- delete all dir contents first
      rm_file_or_empty_dir(dir)       -- then delete the dir itself

      if dir_exists(dir) then  -- just in case something failed, check it now
         errors.throw('utils_filesys_cannot_complete_delete_dir', dir)
      end
   end
end
M.rmdir = rmdir


--- Removes the given file
local rm = function(path)
   if file_exists(path) then
      rm_file_or_empty_dir(path)
   end
end
M.rm = rm


--- Given a path possibly including directories, returns the
-- base filename including the extension if present
-- @param path the path
-- @return base file name
local basename = function(path)
  local basename = path
  for x = 1, #path do
    local c = path:sub(-x, -x)
    if c == '\\' or c == '/' or c == ':' then
      basename = path:sub(-x+1)
      break
    end
  end
  return basename
end
M.basename=basename


return M
