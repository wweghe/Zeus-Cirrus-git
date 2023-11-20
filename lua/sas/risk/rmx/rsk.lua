--[[
/* Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA */

/*!
\file    rsk.lua

\brief   This file is to store the Graph library which has all the functions declared.

\ingroup commonAnalytics

\author  SAS Institute Inc.

\date    2015

\details This file first creates class: graph,
         then declare all the functions:  NewGraph getWeight setWeight allEdges allConnectedVertexes
                                          allAdjacentVertexes allVertexes getVertexSet getConnectedVertexSet
                                          setGraphMatrix DijkstraShortestPath BellmanFordShortestPath
                                          getWeight setWeight BreadthFirstSearch Extend_shortest_paths
                                          AllPairsShortestPath FloydWarshallShortestPath
                                          checkAllPairsNegativeCycles


*/

]]

-- require rsk_init before anything else in this module is done
require 'sas.risk.rmx.rsk_init'

local sas_msg = require 'sas.risk.utils.sas_msg'
local filesys = require 'sas.risk.utils.filesys'
local args = require 'sas.risk.utils.args'

local errors = require 'sas.risk.utils.errors'
local sasdata = require 'sas.risk.utils.sasdata'
local tableutils = require 'sas.risk.utils.tableutils'
local lock_manager = require 'sas.risk.utils.lock_manager'

local M = {}


---
--  Common rsk routines
--

local libname = function(libref, dir, options)
  args.assert_string(libref)
  args.assert_string_or_nil(dir,2)
  args.assert_table_or_nil(options,3)

   if options then
      if options.only_if_location_exists and not filesys.dir_exists(dir) then
         return
      elseif options.ensure or not options.readOnly then
         filesys.mkdir(dir)
      end
  end

  local engine_options = ""
  if dir and dir:len()>1 and dir:sub(1,1) ~='(' then
     -- we don't want to check if the dir exists when
     -- issuing LIBNAME FOO (FOO1 FOO2);
     dir = dir:gsub("\\", "/")
     if not filesys.dir_exists(dir) then
        Debug("This directory does not exist: " .. tostring(dir))
        errors.throw("rsk_libname_dir_does_not_exist", libref, dir)
       end

     if options and options.engine then
        engine_options = engine_options .. " " .. options.engine
     end
     if options and options.readOnly then
        engine_options = engine_options .. "  access=readonly"
     end
  end

  sasdata.libname(libref, dir, engine_options)
end
M.libname = libname


local function dump_globals()
   sas.submit[[
         proc sql;
         create view work.globals as
         select name, value
         from sashelp.vmacro
         where scope='GLOBAL' and not (name like '%PASSWORD%')
         order by name;
         quit;
         data _null_;
         set work.globals;
         put name '= ' value;
         run;
         proc sql;
         drop view work.globals;
         quit;
]]
end
M.dump_globals = dump_globals



local alloc_libraries = function(libraries)
   for k,v in pairs(libraries) do
      libname(k, v)
   end
end
M.alloc_libraries = alloc_libraries


-- iterator function that hands out next snippet of submitted code
local next_snippet_iterator = function(code, start_from_index)
   if start_from_index <= code:len() then
      -- print("start_from_index is " .. tostring(start_from_index))
      local i_run  = code:find("run;" , start_from_index, true)
      local i_quit = code:find("quit;", start_from_index, true)

      if i_run == nil and i_quit == nil then
         -- print("found end of string")
         return code:len() + 1, "", code:sub(start_from_index)   -- return last piece
      elseif i_run ~= nil and i_quit ~= nil then
         if i_run < i_quit then
            -- print("found run; at " .. i_run)
            return i_run+4, "run;", code:sub(start_from_index, i_run-1)
         else
            -- print("found quit; at " .. i_quit)
            return i_quit+5, "quit;", code:sub(start_from_index, i_quit-1)
         end
      elseif i_run ~= nil then
         -- print("-found run; at " .. i_run)
         return i_run+4, "run;", code:sub(start_from_index, i_run-1)
      else
         -- print("-found quit; at " .. i_quit)
         return i_quit+5, "quit;", code:sub(start_from_index, i_quit-1)
      end
   end
end

local next_snippet = function(code)
   return next_snippet_iterator, code, 1
end

--- a local function that handles 1 submission from submit()
--  and throws an error if:
--  1) SAS' SYSERR global variable is > 0 (or > the user specified value).  By default
--     we do not allow any warnings, etc in submitted code.  This can be overridden by providing
--     an acceptable syserr value other than 0.
--  2) if rsk_term_message has been set

local submit1 = function(code, substitution_args, max_allowed_syserr_value)
  local syserr = sas.submit(code, substitution_args, 2)  -- add 2 more levels to look for vars (to look above rsk.submit1 and rsk.submit)

  local errmsg = sas.symget("rsk_term_message")
  if (errmsg and (errmsg:len() ~= 0)) then
    sas.print("%1zError occurred executing SAS code")
    error(errmsg)
  end

  if syserr > max_allowed_syserr_value then
     print('Problem submitting code:\n' .. code)
     if substitution_args then
        print(tableutils.to_string(substitution_args))
     end
     errors.throw("utils_syserr_check_failed", tostring(syserr), tostring(max_allowed_syserr_value));
  end
end


-- this custom submit function will throw an error any time rsk_term_message is set
-- It also checks syserr to make sure an error was not encountered in the submitted code.
local submit = function(code, substitution_args, max_allowed_syserr_value)
  args.assert_string(code)
  args.assert_table_or_nil(substitution_args,2)
  max_allowed_syserr_value = max_allowed_syserr_value or 0  -- default to allowing syserr of 0, user can provide higher value to allow warnings, etc. in submitted code

  -- iterate over the code, submitting successive pieces ending in "run;" or "quit;"
  -- users not wanting their code broken up at step boundaries can use "run ;" or "quit ;" instead
  -- The benefit of doing this is that the coder can put multiple steps in the same submit and
  -- the code will stop on the first step error found (via syserr) ... the coder doesn't have to put each step in it's own submit
  for _, end_token, code_snippet in next_snippet(code) do
     submit1(code_snippet .. end_token, substitution_args, max_allowed_syserr_value)
  end
end
M.submit = submit




local ensure_table = function(name, spec)
   if not sas.exists(name) then
      sas.new_table(name,spec)
      assert(sas.exists(name), "Could not create table " .. name)
   end
end
M.ensure_table = ensure_table


--- replaces rsk_alloc_dm_libraries
local alloc_dm_libraries = function(dm_dir, dm_usage, applications)

   args.assert_string(dm_dir)
   args.assert_string(dm_usage,2)
   args.assert_string_or_nil(applications,3)

   dm_usage = dm_usage:lower()
   assert(dm_usage=="playpen_use" or dm_usage=="pull_from_sdm" or dm_usage=="enrichment",
          "invalid value for dm_usage parameter")

   Debug("Allocating data mart libs for use for " .. dm_usage)

   local mart =          {readOnly=true,ensure=false}
   local enriched_data = {readOnly=true,ensure=false}
   local stat =          {readOnly=true,ensure=false}
   local corr_mats =     {readOnly=true,ensure=false}
   local mapping =       {readOnly=true,ensure=false}
   local rd_env =        {readOnly=true,ensure=false}
   local staging =       {readOnly=true,ensure=false}
   local import =        {readOnly=false,ensure=false}
   local portfolio =     {readOnly=false,ensure=false}


   if  "playpen_use" ~= dm_usage then
       mart.readOnly = false
       staging.readOnly = false
       rd_env.readOnly = false

      if "pull_from_sdm" == dm_usage then
         mapping.readOnly = false
         stat.readOnly = false
         corr_mats.ensure=true
         enriched_data = nil
      else
         corr_mats.readOnly = false
         enriched_data.readOnly = false
      end
   end
   libname("RD_MART",  dm_dir,                               mart)
   libname("RD_STAGE", dm_dir .. "/staging",                 staging)
   libname("RD_MAP0",  dm_dir .. "/cfg/mapping",             mapping)
   libname("RD_CONF0", dm_dir .. "/cfg/static",              stat)
   libname("RD_CORR0", dm_dir .. "/cfg/static/corr_mats",    corr_mats)
   libname("RD_IMPRT", dm_dir .. "/imported_positions",      import)

   if enriched_data then
     libname("RD_POS",   dm_dir .. "/enriched_data/instdata",  enriched_data)
     libname("RD_MKT",   dm_dir .. "/enriched_data/mktdata",   enriched_data)
     libname("RD_PARM",  dm_dir .. "/enriched_data/parameters",enriched_data)
     libname("RD_TEMP",  dm_dir .. "/enriched_data/temp",      enriched_data)
     libname("RD_PORT",  dm_dir .. "/enriched_data/portfolios",portfolio)
   end

   sasdata.libname("RD_MAP",  "(RD_MAP0)")
   sasdata.libname("RD_CONFC","(RD_CORR0)")
   sasdata.libname("RD_CONF", "(RD_CONF0 RD_CONFC)")
   sasdata.libname("RD_WORK", "(WORK)")

   _G.PROD:alloc_env_libs_dm(dm_dir, rd_env, applications)
end
M.alloc_dm_libraries = alloc_dm_libraries



local get_column_value = function(dsname, column, first)
   local dsid = sas.open(dsname)
   assert(dsid,"Could not open " .. dsname)

   if first then
      dsid:next()
      local value = dsid:get_value(column)
      dsid:close()
      return value
   end

   local value
   while sas.next(dsid) do
      value = sas.get_value(dsid, column)
   end
   sas.close(dsid)
   return value
end
M.get_column_value = get_column_value



local publish_to_dav = function(report, path, userEntityContext)
   local content_server = userEntityContext.product.content_server
   local dav_connection = content_server:connect(userEntityContext.user)
   local component = report.component_name:lower()
   local publish_path =   dav_connection:get_user_folder() .. "/" ..userEntityContext.entity .. "/" .. component


   dav_connection:ensure_path(publish_path)
   sas.filename("_report", path .. "/" .. report.act_report_nm)

   dav_connection:publish("_report", publish_path, report.mime_type)
   sas.filename("_report")
end
M.publish_to_dav = publish_to_dav

local report_to_dav = function(userEntityContext, report_id)
   local report = userEntityContext.report_registry:get_report(report_id)
   local path = userEntityContext:path_for("reports")

   publish_to_dav(report, path, userEntityContext)

   userEntityContext.report_registry:mark_published(report_id)
end
M.report_to_dav = report_to_dav

local use_entity = function(entity, username, interactive)
   local uec = user.use_entity(_G.PROD, entity, username, interactive)
   -- UEC is another global we can handle, because its useful to reference it
   -- (ala Singleton) from anywhere - especially from rsk code. It contains 'app' within it, so if we
   -- had to choose between _G.UEC and _G.PROD, _G.UEC would be a better choice

   _G.UEC = uec
   return uec
end
M.use_entity = use_entity

local recover_uec = function()
   local username = sas.symget("_RSK_LAST_LOGGED_IN_USERNAME")
   local entity   = sas.symget("_RSK_LAST_ENTITY")
   if not username or #username < 1 then
      errors.throw("cannot recover user-entity context with blank username value")
   end
   if not entity or #entity < 1 then
      entity = "main"
   end

   Debug('---------------------------------------------------------------------------')
   Debug("Recovering user-entity context of user=" .. tostring(username) .. ", entity=" .. tostring(entity))
   Debug('---------------------------------------------------------------------------')
   local uec = use_entity(entity, username)
   if not uec then
      errors.throw("Could not recover the user entity context for", username, entity)
   end

   Debug("Successfully recovered user-entity context of user=" .. tostring(username) .. ", entity=" .. tostring(entity))

   return uec
end
M.recover_uec = recover_uec


local recover_playpen = function()
   local playpen_name = sas.symget("_RSK_LAST_PLAYPEN")
   if playpen_name and playpen_name ~= "" then
      Debug('---------------------------------------------------------------------------')
      Debug("Recovering playpen context for " .. playpen_name)
      Debug('---------------------------------------------------------------------------')
      user.use_playpen(playpen_name)
   end
end
M.recover_playpen = recover_playpen



local get_lib_members = function(library, exceptions)
   local filter_out = {}
   local mems = sas.memlist(library, "DATA VIEW", true)
   if not exceptions then
      return mems
   end

  local result = {}
  for i,v in ipairs(exceptions) do
     filter_out[string.upper(v)]=true
   end
  for i,v in ipairs(mems) do
     if not filter_out[v] then
        table.insert(result, v)
     end
   end
   return result
end
M.get_lib_members = get_lib_members


local xor = function(a, b)
   if a and b then
      return false
   end
   if not a and not b then
      return false
   end
   return true
end
M.xor = xor



local proc_copy_single_dir = function(src, dst, ds_to_copy, except_ds)

   local src_lib_crtd, dst_lib_crtd=false, false
   local src_lib, dst_lib=src, dst
   if not (string.len(src) <= 8 and sasdata.libref_exists(src)) then
      src_lib = '_in'
      libname(src_lib, src)
      src_lib_crtd=true
   end

   Debug("Copying contents from " .. src .. " to " .. dst)

   -- We check if there are members in the library prior to copying only because
   -- a warning is put in the log from PROC COPY when the source lib is empty ... something
   -- we don't want to happen.
   local members = sas.memlist(src_lib, "DATA", true)
   if #members > 0 then
      if not (string.len(dst) <= 8 and sasdata.libref_exists(dst)) then
         dst_lib = '_out'
         libname(dst_lib, dst, {ensure=true})
         dst_lib_crtd=true
      end
      sasdata.copy_lib(src_lib, dst_lib, nil, ds_to_copy, except_ds)
   end

   if src_lib_crtd then
      sasdata.libname(src_lib)
   end
   if dst_lib_crtd then
      sasdata.libname(dst_lib)
   end
end
M.proc_copy_single_dir = proc_copy_single_dir


local varlist = function(dsname, exclude)
   local vars = sasdata.varlist(dsname)
   if not exclude then
      return vars
   end

   local vlist = {}
   for i,v in ipairs(vars) do
      if not exclude[v] then
         table.insert(vlist, v)
      end
   end
   return vlist
end
M.varlist = varlist

--- Our product's lock names, declared global to be used within our code instead of being hardcoded in multiple places
--  ALL lock names should be defined here except for critical code section names which should be unique where used in the code.

--- Lock name for "all promoted configs" meaning all static, mapping & RD environment data that exists in the SDM
M.LOCKNAME_PROMOTED_CONFIGS = 'promoted configs'

--- Lock name for the _published_configs data set inside a user's PSDM
M.LOCKNAME_PSDM_PUBLISHED_CONFIGS='<psdm>._published_configs'



local assert_active_entity = function()
   assert(_G.UEC, "rmx.no.entity.in.use")
end
M.assert_active_entity = assert_active_entity

local assert_active_playpen = function()
   assert_active_entity()
   assert(_G.UEC.current_playpen, "rmx.no.playpen.open")
end
M.assert_active_playpen = assert_active_playpen

local function print_msg(key, ...)
   sas.print(sas_msg.get_message(key, ...))
end

-- print messages for every empty dataset found in lib
local find_empty_data = function(lib)
   local vstable = sas.open('sashelp.vstable')
   vstable:where("libname='" .. lib:upper() .. "'")
   for row in vstable:rows() do
       local memname = row.memname
       local dsid = sas.open(lib .. "." .. memname)
       if dsid:attr('NOBS') < 1 then
          print_msg("rsk_empty_dataset", memname)
       end
       dsid:close()
   end
   vstable:close()
end
M.find_empty_data = find_empty_data


local function print_warning(key, ...)
   sas.print("%2z"..sas_msg.get_message(key, ...))
end

local function print_error(key, ...)
   sas.print("%1z".. sas_msg.get_message(key, ...))
end

local function print_note(key, ...)
   sas.print("%3z"..sas_msg.get_message(key, ...))
end

M.print_msg = print_msg
M.print_note = print_note
M.print_error = print_error
M.print_warning = print_warning





return M
