--[[
/* Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA */

/*!
\file    fileutil.lua

\brief   This module is about utility function for files, including unzip file, read text file,
         save test file to data set

\ingroup commonAnalytics

\author  SAS Institute Inc.

\date    2015

*/

]]

local M={}

local filesys=require 'sas.risk.utils.filesys'
local sasdata=require 'sas.risk.utils.sasdata'
local errors=require 'sas.risk.utils.errors'


--- Unzips a zip file to a directory
-- Recursively unzips up all files and folder in the specified zip file.
-- The archive must have been created using the macro %sst_zip() or equivalent code.
-- The program will run on any windows or unix box. Furthermore
-- the package tools move data sets in a portable manner - indexes are
-- preserved.
--
-- @param zipfile The fully qualified zip file name
-- @param outdir  The fully qualified directory name
--
local unzip_sas_dir = function(zipfile, outdir)

   if filesys.dir_exists(outdir) then
--    if true then
      errors.throw('utils_fileutils_unzip_directory_exists', outdir)
   end

   -- Make sure the macro can create the directory to unzip to, otherwise it won't work
   filesys.mkdir(outdir) -- done to create the directories down to the outdir
   filesys.rmdir(outdir) -- leave outdir itself to be created by the macro
   sas.submit[[%sst_unzip(zipfile=@zipfile@, dir=@outdir@);]]
end
M.unzip_sas_dir=unzip_sas_dir





-- Could not get the Lua-only code to work, so had to resort to macro, ugh
--- Read a text file and return an array of text lines
local read_text_file=function(filepath)
  sas.submit[[
  %macro read_file(filepath=, outvar=);
     filename rdr "&filepath";
     %let fid=%sysfunc(fopen(rdr));
     %local rc  i;  %let i=1;
     %do %while(%sysfunc(fread(&fid.)) eq 0);
        %let rc=%sysfunc(fget(&fid.,line,32767));
      %global &outvar.&i.;
        %let &outvar.&i. = %superq(line);
      %let i = %eval(&i+1);
     %end;
     %if %symexist(&outvar.&i.) %then %do;
       %symdel &outvar.&i.;
     %end;
     %let rc=%sysfunc(fclose(&fid.));
  %mend;
    %read_file(filepath=@filepath@, outvar=_line);]]
  local lines={}
  local i=1
  while sas.symget('_line' .. i)~=nil do
    lines[i]=sas.symget('_line' .. i)
    sas.set_quiet(true)
    sas.submit[[ %symdel _line@i@; ]]
    sas.set_quiet(false)
    i=i+1
  end
  return lines
end
M.read_text_file=read_text_file

--- Reads a properties file and returns a table of key-value pairs
local function read_properties_file(filepath)
   local lines = read_text_file(filepath)
   local props={}
     if lines then
        for i,line in pairs(lines) do
           if line[1] ~= '#' then  -- if not a comment, look for an equals
             for key, value in string.gmatch(line, "(.-)=(.*)") do
                props[key] = value
             end
           end
        end
     end
   return props
end
M.read_properties_file=read_properties_file

--[=[  Could not get the Lua-only code to work, so had to resort to macro, ugh
local read_text_file=function(filepath)
  -- what about encoding?
  sas.submit[[filename rdr "@filepath@" lrecl=1024;]]
  local fid = sas.fopen('rdr')
  local contents=''
  while sas.fread(fid)==0 do
    sas.fget(fid, 'line', 1024)
    contents=contents .. sas.symget('line')
  end
  sas.fclose(fid)
  sas.submit[[filename rdr clear;]]
  return contents
end
M.read_text_file=read_text_file
--]=]

--- TODO: This is a workaround to avoid component conflicts in 9.4m3 Lua.
--        SAS Lua disable sas.io if sasext.lua is loaded.
local function get_sas_io_module()

   if sas.io == nil then
      print ("Lua io module uses sasxx")
      return sasxx
   else
      print("Lua io module uses sas.io")
      return sas.io
   end

end

--- Save text file to SAS data set. Each line is an observation in SAS data set
local function save_file_to_dataset(filepath, dataset, maxLengthOfLine)

   local my_sas_io = get_sas_io_module()

   local fileref = my_sas_io.assign(filepath)

   if not  fileref then return nil end

   if type(fileref) == "string" then
       fileref =  my_sas_io.new(fileref)
   end

   local file_handle = fileref:open()

   if not file_handle then return end

   local table = {}

   if maxLengthOfLine == nil then
   ---Why this magic number? see http://support.sas.com/kb/00/888.html
     maxLengthOfLine = 262
   end

   local vars = { codeline={type='C', length=maxLengthOfLine} }
   table.vars=vars

   for line in file_handle:lines() do
      table[ #table + 1 ]={codeline=line}
   end

   sas.write_ds(table, dataset)

   file_handle:close()

   fileref:deassign()

end
M.save_file_to_dataset = save_file_to_dataset

--- Save SAS data set to text file. Assuming SAS data set containing one column. Each
--- observation writes as string with newline in the end
local function save_dataset_to_file(filepath, dataset)

   local my_sas_io = get_sas_io_module()

   local fileref = my_sas_io.assign(filepath)

   if not  fileref then return false  end

   local file_handle = fileref:open("w-o","v")
   if not  file_handle then
      fileref:deassign()
      return false
   end


   if sas.exists(dataset) then
      local dsid = sas.open(dataset)
      local vars = {}
      local var, vname
      for var in sas.vars(dsid) do
         vars[var.name:lower()] = var
      end

      while sas.next(dsid) do
         local line = ""
         for vname,var in pairs(vars) do
            print (sas.get_value(dsid,vname))
            line = line..sas.get_value(dsid,vname)
         end
         file_handle:write(line)
      end
      sas.close(dsid)
      file_handle:close()
   else
      file_handle:close()
   end

   fileref:deassign()

end

M.save_dataset_to_file = save_dataset_to_file

--- Read a file referenced by fileref into a string.
-- OBSOLETE: the io package is disabled in Viya environments, so can't use this
-- @param fileref - either a string or a fileref from sasxx.assign()
--[[function M.read_fileref( fileref )
   if type(fileref) == "string" then
       fileref =  sasxx.new(fileref)
   end
   local path = fileref:info().path
   if not path then
      fileref:deassign()
      return false, "Couldn't open file referenced by "..tostring(fileref).." for read."
   end

   local BUFSIZE = 2^13
   local f = io.open(path,"rb")
   if not f then
      return false, "Couldn't open file referenced by "..tostring(fileref).." for read."
   end

   local contents = ""
   while true do
      local bufread = f:read(BUFSIZE)
      if not bufread then break end
      contents = contents..bufread
   end
   f:close()
   return contents
end]]

--- Read a file referenced by fileref into a string.
-- WARNING: This can only be used to read files <32767 characters.
--    If there is a need to read in bigger using this function we can adapt this
-- @param fileref - either a string or a fileref from sasxx.assign()
function M.read_fileref( fileref )

   local rc=sas.submit[[
      %let fileContents=;
      data _null_;
         length contents $ 32767;
         retain contents "";
         infile @fileref@ lrecl=32767;
         input;
         contents=catt(contents, _infile_);
         call symputx("fileContents", contents, "L");
      run;
   ]]

   if rc ~= 0 then
      sas.print("%1zFailed to read contents of fileref."..fileref)
      return nil
   end

   local contents = sas.symget("fileContents")
   return contents

end



--- Read a file referenced by a filename into a string.
--- Note: This can only be used to read files <32767 characters.
--    If there is a need to read in bigger using this function we can adapt to read into multiple macrovars
-- @param filename - fully qualified pathname to the file
function M.read_file( filename )
   local fileref = sasxx.assign( filename)
   if not  fileref then return nil end
   -- print(table.tostring(fileref:info()))

  local contents = M.read_fileref(fileref)
  fileref:deassign()

  return contents
end


--- Write a file referenced by fileref from a string with carriage returns
-- OBSOLETE: the io package is disabled in Viya environments, so can't use this
-- @param fileref - either a string or a fileref from sasxx.assign()
-- @param txt  - the string being written to the file
--[[function M.write_fileref( fileref, txt )
    if type(fileref) == "string" then
       fileref =  sasxx.new(fileref)
   end
   local path = fileref:info().path
   if not path then
      fileref:deassign()
      return false, "Couldn't open file referenced by "..tostring(fileref).." for write."
   end
   local f = io.open(path,"wb")
   if not f then
      return false, "Couldn't open file: "..path.." for write."
   end
   f:write(txt)
   f:close()
   return true
end]]

--- Write a file referenced by fileref from a string with carriage returns
-- Warning!  This will fail if the text contains "%mend" (a proc LUA bug).
-- @param fileref - either a string or a fileref from sasxx.assign()
-- @param txt  - the string being written to the file
function M.write_fileref( fileref, txt )

   local txtFmt = string.gsub(txt, "'", "''")
   txtFmt = string.gsub(txtFmt, "\\t", "   ")
   txtFmt = string.gsub(txtFmt, "run;", "run ;") --yes this is necessary - proc lua brilliantly errors out if txt contains "run;"
   txtFmt = string.gsub(txtFmt, "quit;", "quit ;") --or "quit;"

   local rc=sas.submit[[
      data _null_;
         file @fileref@ lrecl=500000;
         put '@txtFmt@';
      run;
   ]]

   if rc ~= 0 then
      print("ERROR: Failed to write text to fileref."..fileref)
      return false
   end

   return true

end

--- Write a file referenced by fileref from a string serialized
-- @param fileref - either a string or a fileref from sasxx.assign()
-- @param txt  - the string being written to the file
function M.write_fileref_serialized( fileref, txt )
    if type(fileref) == "string" then
       fileref =  sasxx.new(fileref)
   end

   local file_handle = fileref:open("w-o","f", 1)
   if not  file_handle then
      fileref:deassign()
      return false, "Couldn't open file for write."
   end

   local lines = string.split(txt, "\n")
   for i, line in ipairs(lines) do
       for i = 1, string.len(line) do
           file_handle:write(string.sub(line, i, i) )
       end
  end

  file_handle:close()
  return true
end

--- Write a file referenced by a filename from a string with carriage returns
-- @param fileref - either a string or a fileref from sasxx.assign()
-- @param txt  - the string being written to the file
function M.write_file( filename, txt )
   local fileref = sasxx.assign( filename)
   if not  fileref then return false, "Couldn't open file location."  end

   local pass, msg = M.write_fileref( fileref, txt )
   fileref:deassign()

   return pass, msg
end

return M
