--[[
/* Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA */

/*!
\file    sasdata.lua

\brief   This module contains general utility functions related to SAS data.
         This module is meant to fit on top of the SAS API provided by the sasext module
         to make a more user-friendly interface to general data-portions of SAS.

\ingroup commonAnalytics

\author  SAS Institute Inc.

\date    2015

*/

]]

local filesys = require 'sas.risk.utils.filesys'
local stringutils = require 'sas.risk.utils.stringutils'
local args = require 'sas.risk.utils.args'
local errors = require 'sas.risk.utils.errors'
local sas_msg = require 'sas.risk.utils.sas_msg'

local M={}


-- returns true or false whether the given libref exists (is allocated in SAS)
local libref_exists = function(libref)
   return sas.libref(libref) == 0   -- rc of 0 from SAS means the libref is assigned
end
M.libref_exists = libref_exists

local require_libref = function(libref)
   if not libref_exists(libref) then
      errors.throw('utils_sasdata_required_library_missing', tostring(libref))
   end
end
M.require_libref = require_libref



local free_one = function(libref)
   if libref_exists(libref) then
      sas.glibname(libref)  -- we should always use sas.glibname() for our libname needs
      sas_msg.print('utils_sasdata_library_deallocated', string.upper(libref))
   end
end

local nextTempNumber=1

---
--   Allocates a SAS library and assigns the given libref to it.
--   The directory specified can be either a directory or a parenthesis-surrounded,
--   space delimited list of librefs to create a concatenated library.
--
--   This function throws an error when the library is not allocated successfully
--
-- @param libref  The SAS libref to allocate to the given directory or nil if any generated libref name will do
-- @param dir     The directory to allocate the libref to or nil if the libref is to be deallocated
-- @param libref_options an libref_options string for the SAS libref/library being defined as documented in the SAS documentation
-- @param options {} of options:  ensure_dir (if true ensures the directory)
-- @return the libref when allocating the library, nil when deallocating the library
local libname = function(libref, dir, libref_options, options)
   -- Dealloc the library first if it exists, so that we can definitively determine
   -- when the libref failed without checking the assorted non-zero RCs considered "OK".
   -- This also ensures that the old library assignment won't exist after this ftn returns.
   if libref then
      if type(libref) == "string" then
         free_one(libref)
      elseif type(libref) == "table" then
         for i,v in ipairs(libref) do
            free_one(v)
         end
         return -- we currently don't define assigning a libref array, only freeing a libref array
      end
      if not dir then
         return
      end
   else
      repeat
         libref="_lib" .. nextTempNumber
         nextTempNumber=nextTempNumber+1
      until not libref_exists(libref)
   end

   if options and options.ensure_dir then
      filesys.ensure_dir_exists(dir)
   end

   sas.glibname(libref, dir, libref_options)  -- we should always use sas.glibname() for our libname needs

   if not libref_exists(libref) then
      errors.throw("utils_sasdata_libref_alloc_failure", libref, dir, libref_options)
   end

   if libref_options and #libref_options > 0 then
      sas_msg.print('utils_sasdata_libref_allocd_with_opts', libref, dir, libref_options)
   else
      sas_msg.print('utils_sasdata_libref_allocd', libref, dir)
   end

   return libref
end
M.libname = libname


local dslist = function(libref)
   return table.concat(sas.memlist(libref,"DATA", true),' ')
end
M.dslist = dslist

--- Wrapper around sas.memlist to return an array of strings of the names of the members of the given library.
--  This function also allows a directory to be given instead of a libref (which will cause a libref to be temporarily allocated)
-- @param libref  SAS libref or directory
-- @param type  DATA, VIEW or ALL values are valid
local member_list= function(libref, type)
  type=type or 'ALL'
  local members
  if not (string.len(libref) <= 8 and libref_exists(libref)) then
    members = sas.memlist(libref, type, true)
  else  -- specified a directory instead of a libref
    local generated_libref = libname(nil, libref, nil)
    members = sas.memlist(generated_libref, type, true)
    libname(generated_libref) -- free it
  end
  return members
end
M.member_list=member_list

---
-- Open a dataset/view, and throw an error if it fails
--
local open = function(dsname, mode)
   local dsid = sas.open(dsname, mode)
   assert(dsid, "Failed to open " .. dsname .. " : " .. sas.sysmsg() )
   return dsid
end
M.open = open


local ds_exists = function(ds)
   return sas.exists(ds, 'DATA')
end
M.ds_exists = ds_exists


local ds_exists_in_dir = function(dir, datasetName)
   args.assert_string(dir)
   args.assert_string(datasetName)

   if not filesys.dir_exists(dir) then
      return false
   end
   local libref = libname(nil, dir)
   local rc = ds_exists(libref .. '.' .. datasetName)
   free_one(libref)
   return rc
end
M.ds_exists_in_dir = ds_exists_in_dir


local require_ds = function(fqDatasetName)
   if ds_exists(fqDatasetName) == false then
      errors.throw('utils_sasdata_required_dataset_missing', tostring(fqDatasetName))
   end
end
M.require_ds = require_ds

--- Convenience method handle nil/empty and non-existent situations
local delete_ds = function(ds)
   if ds and #ds > 0 and ds_exists(ds) then
      sas.delete(ds)
   end
end
M.delete_ds = delete_ds


--- returns boolean indicating whether the var exists or not
local var_exists = function(ds, var_name)

    local rc=false
    --print(ds)
    local dsid = sas.open(ds,"i")

    if dsid == 0 then
       print(sas.sysmsg());  -- print this in case there's something useful in it from the call to open()
    else
       local varnum = sas.varnum(dsid, var_name)
       --print("varnum for " .. tostring(var_name) .. " is " .. tostring(varnum))
       rc = varnum ~= 0  -- the variable exists if told the varnum is not zero
       sas.close(dsid)
    end
    --print("var_exists() is returning " .. tostring(rc))
    return rc
end
M.var_exists = var_exists


--- Enforces that the given variable exists in the given data set.
-- If it does not exist, an error is thrown.
local require_var = function(ds, var_name)
   if not var_exists(ds, var_name) then
      errors.throw("utils_sasdata_required_var_does_not_exist", ds, var_name)
   end
end
M.require_var = require_var


--- Add new variables to data set. Expects fully-qualified dataset name and a table containing tables, one for each variable, as follows:
-- { var1={type='C', length=25, label="this is var1", format= ...},
--   var2={type='N', length=8,   label="this is var2", format= ...}}

local add_vars = function(ds, cols)
   sas.submit_[[
         proc sql noprint;
   ]]
   for name,col in pairs(cols) do
      sas.submit_[[alter table @ds@]]
      local type = 'NUM'
      if col.type=='C' then
         type = 'CHAR'
      end
      local length = col.length or 8
      local label= col.label or ""

      sas.submit_[[ add @name@ @type@ length=@length@ label="@label@" ]]
      if col.format then
         sas.submit_([[ format = @format@]], {format=col.format})
      end
      if col.informat then
         sas.submit_([[ informat=@informat@]], {informat=col.informat})
      end
      sas.submit_(";")
   end
   sas.submit("quit;")
end
M.add_vars = add_vars


--- Deletes the given list of data set names
local delete_datasets = function(libref, ds_names)
   args.assert_string(libref)
   if ds_names == nil then
      return
   end
   local datasets
   if type(ds_names) == "string" then
      datasets = stringutils.split(ds_names," ")
   elseif type(ds_names) == "table" then
      datasets = ds_names
   end
   assert(datasets, "invalid list of data set names")
   for i,v in ipairs(datasets) do
      sas.delete(libref .. "." .. v)
   end
end
M.delete_datasets = delete_datasets


local copy_ds_schema = function(inlib, outlib, ds_name)
      ---
      -- Save the current OBS system option and then set it to zero so
      -- that no data is copied
      --
      -- Use PROC COPY to create an empty data set with exactly the same
      -- attributes, indexes (if possible), and constraints (if possible), then restore the OBS
      -- system option

      sas.submit[[
      %let SAVED_OBS=%sysfunc(getoption(obs, KEYWORD));
      options obs=0;
      proc copy in=@inlib@ out=@outlib@ constraint=yes index=yes;
         select @ds_name@;
      run;
      options &SAVED_OBS;
      ]]
end
M.copy_ds_schema = copy_ds_schema




---
-- Copies a single data set from one library to another, optionally filtering it with a given SQL WHERE clause,
-- and preserving its indexes. Constraints are preserved as best possible per the constraint=yes option
-- documented at  http://support.sas.com/documentation/cdl/en/proc/63079/HTML/default/viewer.htm#p1juxu16zautpxn1dikxecc3kn7w.htm
-- @parm inlib Input libname
-- @parm outlib Output libname
-- @parm ds_name Data set name (source and destination)
-- @parm filter SQL WHERE clause

local copy_and_filter = function(inlib, outlib, ds_name, filter, drop_list)
   if filter == nil then
      sas.submit([[
         proc copy in=@inlib@ out=@outlib@ constraint=yes index=yes;
            select @ds_name@;
         run;
      ]], {inlib=inlib, outlib=outlib, ds_name=ds_name})
   else

      copy_ds_schema(inlib, outlib, ds_name)

      sas.submit([[
      proc sql noprint;
        insert into @outlib@.@ds_name@
           select *
           from @inlib@.@ds_name@
           @filter@
           ;
      quit;
      ]], {inlib=inlib, outlib=outlib, ds_name=ds_name, filter=filter})

   end

   if drop_list then
      local drop_cols = table.concat(drop_list, ' ')
      sas.submit[[
         data @outlib@.@ds_name@;
            set @outlib@.@ds_name@;
            drop @drop_cols@;
         run;]]
   end
end
M.copy_and_filter = copy_and_filter


---
-- Copies an entire library of data sets.
-- @parm from_lib Input libname
-- @parm to_lib Output libname
-- @parm options   a string containing any of these options: <br/>
-- http://support.sas.com/documentation/cdl/en/proc/63079/HTML/default/viewer.htm#p1juxu16zautpxn1dikxecc3kn7w.htm
-- @parm selected_data_sets:  list of data set names
local copy_lib = function(from_lib, to_lib, options, selected_data_sets, excluded_data_sets)
   options = options or 'CONSTRAINT=YES'
   args.assert_table_or_nil(selected_data_sets,4)
   args.assert_table_or_nil(excluded_data_sets,5)

   sas.submit_ [[
         proc copy in=@from_lib@ out=@to_lib@ @options@;
   ]]
   if selected_data_sets then
      local ds_list_str = table.concat(selected_data_sets, " ")
      sas.submit_[[
            select @ds_list_str@;
      ]]
   end
   if excluded_data_sets then
      local ds_list_str = table.concat(excluded_data_sets, " ")
      sas.submit_[[
            exclude @ds_list_str@;
      ]]
   end
   sas.submit("run;")
end
M.copy_lib = copy_lib


local sort_ds = function(in_ds, out_ds, by_cols, keywords)

   keywords = keywords or ""

   -- if cols are not in a string (can be space delimited values), must be in a list (table)
   if type(by_cols) == "table" then
      by_cols = table.concat(by_cols, " ")
   end

   if out_ds==nil then
      sas.submit([[proc sort data=@in_ds@              @keywords@;
                      by @by_cols@;
                   run;]],
                 {in_ds=in_ds,                by_cols=by_cols, keywords=keywords})
   else
      sas.submit([[proc sort data=@in_ds@ out=@out_ds@ @keywords@;
                      by @by_cols@;
                   run;]],
                 {in_ds=in_ds, out_ds=out_ds, by_cols=by_cols, keywords=keywords})
   end
end
M.sort_ds = sort_ds


--- Perform an SQL statement via proc sql.
-- If PROC SQL diagnoses an error, a Lua error is thrown
local sql = function(sql_stmt, substitution_args, proc_sql_options, addl_lookup_level)
   addl_lookup_level=addl_lookup_level or 0
   proc_sql_options = proc_sql_options or "noprint"  -- default to noprint if options not specified

   sas.submit("proc sql " .. proc_sql_options .. ";\n" ..
                   sql_stmt .. ";\n" ..  -- add a benign semicolon in case the caller didn't include one
                "quit;", substitution_args, addl_lookup_level+1) -- add this function as a single level to what caller passed

   -- Now check the SQLRC and see if the proc reported an error, if so throw a Lua error.
   -- SQL automatic variables are described here:
   --     http://support.sas.com/documentation/cdl/en/sqlproc/63043/HTML/default/viewer.htm#p0xlnvl46zgqffn17piej7tewe7p.htm
   local sqlrc = sas.symget("SQLRC") -- returns a number!
   if sqlrc > 4 then
      errors.throw("utils_sasdata_error_running_proc_sql", tostring(sqlrc))
   end
end
M.sql = sql


-- Queries a whole table and returns a lua table of tables (outer table is rows(1..n), inner table is named columns for each for row)
-- Should only be used on tables we expect we can keep in memory
local select = function(selectStmt, substitution_args, trim_string_values, addl_lookup_level)
   addl_lookup_level=addl_lookup_level or 0
   trim_string_values = trim_string_values or true  -- default to true if not specified

   local dsname = "WORK._select"
   delete_ds(dsname)

   sql("create table " .. dsname .. " as\n" ..
        selectStmt .. ";",   -- add a benign semicolon in case the caller didn't include one
        substitution_args, nil, addl_lookup_level+1) -- add this function as a single level to what caller passed

   local dsid = open(dsname)
   local col_names = {}
   local col_count = sas.nvars(dsid)
   for i = 1,col_count do
      local vname = sas.varname(dsid, i)
      col_names[i] = vname
   end

   -- now get the values into a table to return
   local _rows =  {}
   while sas.next(dsid) do
      local _row =  {}
      for col_num = 1,col_count do
         local vname = col_names[col_num]
         local value = sas.get_value(dsid, col_num)
         if trim_string_values and
            type(value) == "string" then
            value = stringutils.trim(value)
         end
         _row[vname] = value
      end
      _rows[#_rows+1] = _row
   end

   sas.close(dsid)
   return _rows
end
M.select = select


local select_one_row = function(selectStmt, substitution_args, trim_string_values, addl_lookup_level)
   addl_lookup_level=addl_lookup_level or 0
   local rows = select(selectStmt, substitution_args, trim_string_values, addl_lookup_level+1) -- add this function as a single level to what caller passed
   if #rows~=1 then
      errors.throw('utils_sasdata_other_than_one_row_returned', tostring(#rows))
   end
   return rows[1] -- return the inner row (set of named columns in a lua table)
end
M.select_one_row = select_one_row

--- Returns an array of column values from the query
local select_one_column = function(selectStmt, substitution_args, trim_string_values, addl_lookup_level)
   addl_lookup_level=addl_lookup_level or 0
   trim_string_values = trim_string_values or true  -- default to true if not specified

   local dsname = "WORK._select_into_array"
   delete_ds(dsname)

   sql("create table " .. dsname .. " as\n" ..
        selectStmt .. ";",   -- add a benign semicolon in case the caller didn't include one
       substitution_args, nil, addl_lookup_level+1) -- add this function as a single level to what caller passed

   local dsid = open(dsname)

   local col_count = sas.nvars(dsid)
   assert(col_count == 1, "Query was expected to return 1 column but got " .. tostring(col_count) .. " columns")

   -- now get the values into a table to return
   local _column =  {}
   while sas.next(dsid) do
      local value = sas.get_value(dsid, 1)
      if trim_string_values and
         type(value) == "string" then
         value = stringutils.trim(value)
      end
      _column[#_column+1] = value
   end
   sas.close(dsid)
   delete_ds(dsname)
   return _column
end
M.select_one_column = select_one_column


local select_one_value = function(selectStmt, substitution_args, trim_string_values, addl_lookup_level)
   addl_lookup_level=addl_lookup_level or 0
   local value = select_one_column(selectStmt, substitution_args, trim_string_values, addl_lookup_level+1) -- add this function as a single level to what caller passed
   if #value == 0 then
     return nil
   elseif #value > 1 then
      errors.throw('utils_sasdata_more_than_one_value_returned', tostring(#value))
   end
   return value[1]
end
M.select_one_value = select_one_value


-- A very common attribute (row count)
local get_row_count = function(ds)
   local dsid = open(ds)
   local out_value = dsid:nobs() -- TODO: fix this: it should be nlobs but that function is not defined yet (nobs really does give back numbers including deleted records)
   dsid:close()
   return out_value
end
M.get_row_count = get_row_count


local set_column_value = function(ds, col, value, filter)
   -- use sql instead of datastep since data step drops integrity constraints
   local value_syntax = value
   if tostring(value) == "string" then
      value_syntax = '"' .. value_syntax .. '"'
   end

   local where_clause = ""
   if filter and #filter > 0 then
      where_clause = 'where ' .. filter
   end

   sql[[update @ds@
         set @col@=@value_syntax@
         @where_clause@
      ]]
end
M.set_column_value = set_column_value



local Rowset = {}
M.Rowset = Rowset

function Rowset:new(o)
   o = o or {}
   setmetatable(o, self)
   self.__index = self
   return o
end
function Rowset:close()
   if self.dsid then
      self.dsid:close()
      sas.delete(self.view,"view")
      self.dsid = nil
   end
end

function Rowset:rows()
   return coroutine.wrap(function()

                            self.view = "WORK.T" .. tostring(sas.datetime()):gsub("%.",""):sub(-4)
                            sas.submit([[
                              proc sql noprint;
                              create view @name@ as @query@;
                              quit;]], {name=self.view, query=self.query})
                            assert(sas.exists(self.view, "view"))
                            local dsid = open(self.view)
                            self.dsid  = dsid
                            for row in dsid:rows() do
                               coroutine.yield(row)
                            end
                            self:close()
                         end)

end

--- Run an SQL query. For example:
-- local rowset = sasdata.query("select age, height from sashelp.class")
-- for row in rowset:rows() do
--    print(row.age, row.height)
-- end
-- rowset:close()
local query = function(query)
   return Rowset:new({query=query})
end
M.query = query

local varlist = function(dsname)
   if not string.find(dsname, "%.") then
      dsname = "WORK." .. dsname
   end
   local dsid = open(dsname,'i')
   local vars = {}
   local nvars = sas.nvars(dsid)
   for n = 1,nvars do
      local vname = sas.varname(dsid, n)
      table.insert(vars, vname)
   end
   dsid:close()
   return vars
end
M.varlist = varlist

--- Deletes all rows from a data set, keeping existing indexes, etc. build on the data set.
-- Note that other ways to delete rows can remove indexes as a side-effect, this way does not.
local delete_all_rows = function(dataset)
   sql([[delete from @dataset@;]])
end
M.delete_all_rows=delete_all_rows


local recursively_delete_all_rows_callback=function(dir_id, type, memname, level, parent, context, arg)
   if type=='D' then
      member_list(libref)
   end
end


local recursively_delete_all_rows = function(dir)
  filesys.dir_tree_walk(dir, recursively_delete_all_rows_callback)
end
M.recursively_delete_all_rows=recursively_delete_all_rows

return M
