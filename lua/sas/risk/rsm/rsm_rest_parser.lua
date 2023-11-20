--[[
/* Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA */

/*!
\file

\brief   Module provides JSON parsing functionality for interacting with the SAS Risk Scenario Manager REST API

\details
   The following functions are available:
   - rsmRestPlain Parse a simple (one level) JSON structure
   - ...

\section ..

   Returns the list of scenarios defined in SAS Risk Scenario Manager <br>

   <b>Syntax:</b> <i>(\<filename�fileref\>, output)</i>

   <b>Sample JSON input structure: </b>
   \code

   \endcode

   <b>Sample Output: </b>


\ingroup commonAnalytics

\author  SAS Institute Inc.

\date    2018

*/

]]


local filesys = require 'sas.risk.utils.filesys'
local stringutils = require 'sas.risk.utils.stringutils'
local args = require 'sas.risk.utils.args'
local errors = require 'sas.risk.utils.errors'
local sas_msg = require 'sas.risk.utils.sas_msg'
local tableutils = require 'sas.risk.utils.tableutils'
local json = require 'sas.risk.utils.json'
local json_utils = require 'sas.risk.cirrus.core_rest_parser'

json.strictTypes = true

local M = {}

M.rsmRestPlain = json_utils.coreRestPlain

-- Clone a lua table recursively
clone = function(obj, seen)
  if type(obj) ~= 'table' then return obj end
  if seen and seen[obj] then return seen[obj] end
  local s = seen or {}
  local res = setmetatable({}, getmetatable(obj))
  s[obj] = res
  for k, v in pairs(obj) do res[clone(k, s)] = clone(v, s) end
  return res
end

-----------------------------------------------------------------------------------------
-- General RSM parsing functions
-----------------------------------------------------------------------------------------

-- Process RSM item attributes
rsmProcessAttr = function(item, func)

   -- Initialize output row
   local row = {}

   for attrName, attrValue in pairs(item) do
     if(type(attrValue) == "table") then
        -- Convert table into plain string
        row[attrName] = json:encode(attrValue)
     else
        row[attrName] = attrValue
     end
   end

   -- Use custom function if provided
   if(func ~= nil) then
      row = func(item, row)
   end

   -- Return the processed row
   return row
end


-- General wrapper for REST calls of type rest/<Object>
rsmProcessItems = function(schema, jsonTable, output, func)
   -- Declare output table structure (copy from the input schema)
   local result = schema

   if jsonTable ~= nil then
      -- Check for errors
      if jsonTable.errorCode ~= nil then
         result = {}
         local row = {}
         -- Grab Error details
         row.errorCode = jsonTable.errorCode
         row.message = jsonTable.message
         row.remediation = jsonTable.remediation
         row.httpStatusCode = jsonTable.httpStatusCode
         table.insert(result, row)
         -- Print error info to the log
         sas.print("%1z" .. row.message .. ". " .. row.remediation)
      else
         -- Check if this is a collection of items
         if jsonTable.items ~= nil then
            -- Loop through the items
            for i, row in pairs(jsonTable.items) do
               row.itemsCount = jsonTable.count
               -- Add record to the output table
               table.insert(result, rsmProcessAttr(row, func))
            end
            -- Process boolean attributes
            result = json_utils.processBoolean(result)
         else
            -- Check if this is a single item
            if jsonTable.id ~= nil then
               -- Add record to the output table
               table.insert(result, rsmProcessAttr(jsonTable, func))
               -- Process boolean attributes
               result = json_utils.processBoolean(result)
            end
         end
      end

      --print(table.tostring(result))

      if output ~= nil then
         if #result == 0 then
            -- Make sure there is at least one record
            result[1] = {id = -1}
            -- Write table
            sas.write_ds(result, output)
            -- Now remove the empty record
            sas.submit([[data @output@; set @output@(obs = 0); run;]])
         else
            -- Write result
            sas.write_ds(result, output)
         end
      end
   end

   return result

end
-----------------------------------------------------------------------------------------
-- RSM parser functions
-----------------------------------------------------------------------------------------

-- Attributes of the RSM Scenario object
rsmScenarioAttr = function()
  -- Declare table structure
   local result = {
      vars = {
         -- Standard fields
           id = {type = "C"}
         , name = {type = "C"}
         , label = {type = "C"}
         , scenarioVersion = {type = "C"}
         , periodType = {type = "C"}
         , createdBy = {type = "C"}
         , creationTimeStamp = {type = "C"}
        }
   }

   -- Return table structure
   return result
end


-- Attributes of the RSM Scenario object (more variables)
rsmScenarioAttrFull = function()
  -- Declare table structure
   local result = {
      vars = {
         -- Standard fields
           id = {type = "C"}
         , name = {type = "C"}
         , label = {type = "C"}
         , scenarioVersion = {type = "C"}
         , description = {type = "C"}
         , filename = {type = "C"}
         , dataSourceName = {type = "C"}
         , dataSourceId = {type = "C"}
         , periodType = {type = "C"}
         , shock = {type = "C"}
         , stressTypes = {type = "C"}
         , reason = {type = "C"}
         , asOfDate = {type = "C"}
         , lastHistoryDate = {type = "C"}
         , historyId = {type = "C"}
         , alignment = {type = "C"}
         , createdBy = {type = "C"}
         , createdByName = {type = "C"}
         , creationTimeStamp = {type = "C"}
         , historyDate = {type = "C"}
         , modifiedBy = {type = "C"}
         , modifiedByName = {type = "C"}
         , modifiedTimeStamp = {type = "C"}
         , links = {type = "C", length = 32000}
         , periods = {type = "C", length = 32000}
         , stresses = {type = "C", length = 32000}
         , version = {type = "N"}
        }
   }

   -- Return table structure
   return result
end

-- Attributes of the RSM Scenario Set
rsmScenarioSetAttr = function(details_flg)

   -- Process details_flg parameter
   details_flg = details_flg or "N"
   details_flg = string.upper(details_flg)

  -- Declare table structure
   local result = {
      vars = {
         -- Standard fields
           id = {type = "C"}
         , name = {type = "C"}
         , asOfDate = {type = "C"}
         , alignment = {type = "C"}
         , shockSet = {type = "C"}
         , description = {type = "C"}
         , createdBy = {type = "C"}
         , modifiedBy = {type = "C"}
         , creationTimeStamp = {type = "C"}
         , modifiedTimeStamp = {type = "C"}
         , links = {type = "C", length = 32000}
         , scenarios = {type = "C", length = 32000}
         , version = {type = "N"}
        }
   }

   if details_flg == "Y" then
      -- Add details about each scenario associated with this scenario set
      result.vars.scenarioName = {type = "C"}
      result.vars.scenarioVersion = {type = "C"}
      result.vars.weight = {type = "N"}
      result.vars.scenarioId = {type = "C"}
   end

   -- Return table structure
   return result
end



-----------------------------------------------------------------------------------------
-- RSM parser functions
-----------------------------------------------------------------------------------------

-----------------------------
-- Scenario
-----------------------------

-- Parser for rest/Scenario
rsmRestScenario = function(filename, output)
   -- Parse JSON
   local jsonTable = json_utils.parseJSONFile(filename)
   -- Define Scenario Attributes
   local schema = rsmScenarioAttr()
   -- Process all items
   rsmProcessItems(schema, jsonTable, output)
end
M.rsmRestScenario = rsmRestScenario



-- Parser for rest/Scenario (more variables)
rsmRestScenarioFull = function(filename, output)
   -- Parse JSON
   local jsonTable = json_utils.parseJSONFile(filename)
   -- Define Scenario Attributes
   local schema = rsmScenarioAttrFull()
   -- Process all items
   rsmProcessItems(schema, jsonTable, output)
end
M.rsmRestScenarioFull = rsmRestScenarioFull


-----------------------------
-- Scenario Set
-----------------------------

-- Parser for rest/scenarioSet
rsmRestScenarioSet = function(filename, output, details_flg)

   -- Parse JSON
   local jsonTable = json_utils.parseJSONFile(filename)

   -- Define Scenario Set Attributes
   local schema = rsmScenarioSetAttr(details_flg)

   -- Process all scenario set items
   local init_result = rsmProcessItems(schema, jsonTable, details_flg ~= "Y" and output or nil)

   -- if details are requested, loop over each row and create a new row for each scenario
   -- within each scenario set
   if details_flg == "Y" then

      local result = {vars = init_result.vars}

      -- Loop over each row
      for _, row in ipairs(init_result) do

         -- Make sure this row has scenarios
         if row.scenarios ~= nil then

            -- row.scenarios is a JSON table stored as a string: need to decode it
            local scenarioInfo = json:decode(row.scenarios)

            -- Loop over each scenario and add it to a new row
            for i, scen in pairs(scenarioInfo) do

               -- Clone the current row and add the new scenario vars to the new row
               local newRow = clone(row)
               for var, _ in pairs(scen) do
                  newRow[var] = scen[var]
               end

               -- Add new row to the result table
               table.insert(result, newRow)

            end
         end
      end

      -- If output requested, create the SAS dataset with the scenario details
      if output ~= nil then
         if #result == 0 then
            -- Make sure there is at least one record
            result[1] = {id = -1}
            -- Write table
            sas.write_ds(result, output)
            -- Now remove the empty record
            sas.submit([[data @output@; set @output@(obs = 0); run;]])
         else
            -- Write result
            sas.write_ds(result, output)
         end
      end

   end

end
M.rsmRestScenarioSet = rsmRestScenarioSet

return M