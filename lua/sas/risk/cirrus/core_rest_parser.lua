--[[
/* Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA */

/*!
\file

\brief   Module provides JSON parsing functionality for interacting with the SAS Risk Cirrus Core Objects REST API

\details
   The following functions are available:
   - parseJSONFile Parse a simple (one level) JSON structure
   - ...

\section ..

   Returns the list of workgroups defined in SAS Model Implementation Platform <br>

   <b>Syntax:</b> <i>(\<filename�fileref\>, output)</i>

   <b>Sample JSON input structure: </b>
   \code

   \endcode

   <b>Sample Output: </b>


\ingroup commonAnalytics

\author  SAS Institute Inc.

\date    2022

*/

]]


local filesys = require 'sas.risk.utils.filesys'
local stringutils = require 'sas.risk.utils.stringutils'
local args = require 'sas.risk.utils.args'
local errors = require 'sas.risk.utils.errors'
local sas_msg = require 'sas.risk.utils.sas_msg'
local tableutils = require 'sas.risk.utils.tableutils'
local json = require 'sas.risk.utils.json'

json.strictTypes = true

local M = {}

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

-- declare function
local parseJSONFile

rowCount = function(ds)
   local dsid = sas.open(ds)
   local cnt = sas.nobs(dsid)
   sas.close(dsid)
   return cnt
end

getJsonString = function(filename)
   -- Create a temporary table by reading the input file 1024 characters at the time
   local tmpDs = "__tmpJSON__"
   sas.set_quiet(true)
   sas.submit[[
      data @tmpDs@;
         infile @filename@ recfm = F lrecl = 1024 length = len;
         length str $1024.;
         input str $varying1024. len;
      run;
   ]]

   -- Load data set
   local data = sas.load_ds(tmpDs)
   -- Load content of each row into a new table
   local rows = {}
   for i = 1, rowCount("__tmpJSON__") do
      -- Make sure blanks are not truncated if they happen to be at the character position 1024
      rows[i] = data[i].str..string.rep(' ', 1024 - #data[i].str)
   end

   -- Cleanup
   sas.submit[[
      proc datasets library = work nolist nodetails;
         delete @tmpDs@;
      quit;
   ]]
   sas.set_quiet(false)
   -- Concatenate the table to get a full JSON string
   local str = table.concat(rows)
   return str
end;


parseJSONFile = function(filename)
   local jsonString = getJsonString(filename)
   local jsonTable, pos, err
   jsonTable = nil
   if jsonString ~= "" then
      jsonTable, pos, err = json:decode(jsonString)
   else
      print("WARNING: Input filename is empty!")
   end
   return jsonTable
end
M.parseJSONFile = parseJSONFile


processBoolean = function(tbl)
   for i, t in pairs(tbl) do
      if type(t) == "table" then
         tbl[i] = processBoolean(t)
      elseif type(t) == "boolean" then
         if t then
            tbl[i] = "true"
         else
            tbl[i] = "false"
         end
      end
   end
   return tbl
end;
M.processBoolean = processBoolean


function string:split(sep)
   local sep, fields = sep or ":", {}
   local pattern = string.format("([^%s]+)", sep)
   self:gsub(pattern, function(c) fields[#fields+1] = c end)
   return fields
end

-- Converts tabs to spaces
function detab(text, tab_width)
   tab_width = tab_width or 3
   local function rep(match)
      local spaces = tab_width - match:len() % tab_width
      return match .. string.rep(" ", spaces)
   end
   text = text:gsub("([^\n]-)\t", rep)
   return text
end

local checkError = function(jsonTable)
   local result = {}
   if jsonTable ~= nil and jsonTable.errorCode ~= nil then
      result.row = {
         errorCode = jsonTable.errorCode,
         message = jsonTable.message,
         remediation = jsonTable.remediation,
         httpStatusCode = jsonTable.httpStatusCode
      }

      -- Print error info to the log
      sas.print("%1z" .. result.row.message .. ". " .. result.row.remediation)
   end
   return result
end

local createSASTable = function(result, outputNm)
   if type(outputNm) == "string" and sas.mvalid('WORK', outputNm, 'data') then
      sas.print("Creating " .. outputNm)
      if #result == 0 then
         -- Make sure there is at least one record
         result[1] = {key = -1}
         -- Write table
         sas.write_ds(result, outputNm)
         -- Now remove the empty record
         sas.submit([[data @outputNm@ ; set @outputNm@ (obs = 0); run;]])
      else
         -- Write result
         sas.write_ds(result, outputNm)
      end
   else sas.print("ERROR: Invalid SAS output dataset name") end
end

local safeGet = function(args, default)
   local val = default
   for i, v in ipairs(args) do
      if i == 1 then do val = v end
      elseif type(val) == "table" then val = val[v] else val = nil end
      if val == nil then return default end
   end
   return val
end

local isArray = function(v)
   if type(v) ~= "table" then return false
   elseif tostring(v) ~= "JSON array" then return false
   else return true end
end

local isObject = function(v)
   if type(v) ~= "table" then return false
   elseif tostring(v) ~= "JSON object" then return false
   else return true end
end

local isEmpty = function(t)
   local next = next
   return next(t) == nil
end

local insertCp = function(t, e)
   if type(e) == "table" then
      local newEl = {}
      for k, v in pairs(e) do
         newEl[k] = v
      end
      table.insert(t, newEl)
   else table.insert(t, e) end
end

local simpleItemGet = function(jt, result)
   local items = safeGet({jt, "items"})
   if isArray(items) and not isEmpty(items) then
      for i, item in ipairs(items) do
         local row = {}
         for var, _ in pairs(result.vars) do
            row[var] = item[var]
         end
         table.insert(result, row)
      end
   end
end

getItem = function(obj, item)
   local levels = item:split('.')
   local value = obj
   for i, level in pairs(levels) do
      if (type(value) == "table") then
         value = value[level]
      else
        value = nil
      end
   end
   if value ~= nil and type(value) ~= "table" then
      local temp = value
      value = {}
      value[levels[#levels]] = temp
   end
   return value
end

coreRestPlain = function(filename, output, item, defaultLength)
   local jsonTable = parseJSONFile(filename)
   if jsonTable ~= nil then
      -- Check if we have to retrieve a specific item from the json object
      if item ~= nil then
         -- Retrieve the specified item
         jsonTable = getItem(jsonTable, item)
         -- Throw an error if the item could not be found
         if jsonTable == nil then
            sas.submit([[%put ERROR: Could not find JSON object matching the query: @item@;]])
         end
      end
      -- Need to check again in case the item search returned nil
      if jsonTable ~= nil then
         vars = {}
         jsonTable = processBoolean(jsonTable)
         if #jsonTable == 0 then
            jsonTable = {jsonTable}
         end

         -- Loop through all rows
         for i, row in ipairs(jsonTable) do
            -- Loop through all columns
            for key, value in pairs(row) do
               -- Convert into a flat string if the attribute is a table
               if type(value) == "table" then
                  row[key] = json:encode(value)
               end

               -- Check if the defaultLength has been specified
               if defaultLength ~= nil then
                  -- Check if this is a numeric column
                  if type(row[key]) == "number" then
                     varType = "N"
                     varLen = 8
                  else -- This is a character column
                     varType = "C"
                     -- Set the specified default lenght (unless the value of the column is larger)
                     varLen = math.max(string.len(row[key]), defaultLength)
                  end
                  -- Add the column definition to the vars array
                  vars[key] = {type = varType, length = varLen}
               end
            end
         end

         if defaultLength ~= nil then
            -- Set the vars definition
            jsonTable.vars = vars
         end

         sas.write_ds(jsonTable, output)
      end
   end
end;

M.coreRestPlain = coreRestPlain


-- Return standard Core Object attributes
coreObjectAttr = function()

   local result = {
      vars = {
         -- Standard fields
         key = {type = "C"}
         , objectId = {type = "C"}
         , sourceSystemCd = {type = "C"}
         , name = {type = "C"}
         , description = {type = "C"}
         , creationTimeStamp = {type = "C"}
         , modifiedTimeStamp = {type = "C"}
         , createdBy = {type = "C"}
         , modifiedBy = {type = "C"}
         , createdInTag = {type = "C"}
         , sharedWithTags = {type = "C"}
         , customFields = {type = "C", length = 32000}
         , classification = {type = "C", length = 32000}
         , objectLinks = {type = "C", length = 32000}
         , links = {type = "C", length = 32000}
         , mediaTypeVersion = {type = "N"}
         , itemsCount = {type = "N"}
      }
   }

   -- Return table structure
   return result
end

-- Process Core item attributes
coreProcessAttr = function(item, func)

   -- Initialize output row
   local row = {}

   for attrName, attrValue in pairs(item) do
      -- Process customFields (if available)
      if(attrName == "customFields") then
         -- Loop through all customFields
         for key, value in pairs(attrValue) do
            if(type(value) == "table") then
               -- Convert table into plain string
               row[key] = json:encode(value)
            else
               row[key] = value
            end
         end
      else -- Process all other attributes
         if(type(attrValue) == "table") then
            -- Convert table into plain string
            row[attrName] = json:encode(attrValue)
         else
            row[attrName] = attrValue
         end

         -- Parse Dimensional Points (grab only the first)
         if(attrName == "classification" and #attrValue > 0) then
            row.dimensionalPoint = attrValue[1]
         end
      end
   end

   -- Use custom function if provided
   if(func ~= nil) then
      row = func(item, row)
   end

   -- Return the processed row
   return row
end


-- General wrapper for REST calls of type objects/<CustomObject>
coreProcessItems = function(schema, jsonTable, output, func)
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
         row.httpStatusCode = jsonTable.httpStatusCodecoreProcessItems
         table.insert(result, row)
         -- Print error info to the log
         sas.print("%1z" .. row.message .. ". " .. row.remediation)
      else
         -- Check if this is a collection of items
         if jsonTable.items ~= nil then
            -- Loop through the items
            for i, row in pairs(jsonTable.items) do
               row.itemsCount = jsonTable.count
               -- Process current row
               local processedRecord = coreProcessAttr(row, func)
               -- Check if the above call returned a single record or multiple records
               if #processedRecord > 0 then
                  -- It's a table with multiple records: Loop through all records
                  for j = 1, #processedRecord do
                     -- Insert current record to the result
                     table.insert(result, processedRecord[j])
                  end
               else
                  -- It's a single record: add it to the output table
                  table.insert(result, processedRecord)
               end
            end
            -- Process boolean attributes
            result = processBoolean(result)
         else
            -- Check if this is a single item
            if jsonTable.key ~= nil or jsonTable.id ~= nil then

               -- Process current row
               local processedRecord = coreProcessAttr(jsonTable, func)
               -- Check if the above call returned a single record or multiple records
               if #processedRecord > 0 then
                  -- It's a table with multiple records: Loop through all records
                  for j = 1, #processedRecord do
                     processedRecord[j].itemsCount = 1
                     -- Insert current record to the result
                     table.insert(result, processedRecord[j])
                  end
               else
                  processedRecord.itemsCount = 1
                  -- It's a single record: add it to the output table
                  table.insert(result, processedRecord)
               end

               -- Process boolean attributes
               result = processBoolean(result)
            end
         end
      end

      if output ~= nil then
         if #result == 0 then
            -- Make sure there is at least one record
            result[1] = {key = -1}
            -- Write table
            sas.write_ds(result, output)
            -- Now remove the empty record
            sas.submit([[data @output@ ; set @output@ (obs = 0); run;]])
         else
            -- Write result
            sas.write_ds(result, output)
         end
      end
   end

   return result

end

local objectTypeSchemaMap = {
   configurationTables="coreSchemaConfigTable",
   configurationSets="coreSchemaConfigSet",
   dataDefinitions="coreSchemaDataDefinition",
   ruleSets="coreSchemaRuleSet",
   analysisData="coreSchemaAnalysisData",
   analysisRuns="coreSchemaAnalysisRun",
   attributionTemplates="coreSchemaAttributionTemplate",
   cycles="coreSchemaCycle",
   businessEvolutionPlans="coreSchemaBusinessEvolution",
   allocationSchemes="coreSchemaAllocationScheme",
   scripts="coreSchemaScript",
   models="coreSchemaModel"
}

-----------------------------
-- ConfigTables
-----------------------------
-- Return configuration table schema info
coreSchemaConfigTable = function()

   -- Define Standard Core Attributes
   local schema = coreObjectAttr()

   -- Add Custom Fields for the configurationTable object
   schema.vars.type = {type = "C"}
   schema.vars.typeData = {type = "C", length = 32000}

   -- Return schema
   return schema

end

-- Parser for riskCirrusObjects/objects/configurationTables/<configTableKey>
coreRestConfigTable = function(filename, output, output_data)
   -- Parse JSON
   local jsonTable = parseJSONFile(filename)

   -- Define ConfigTable Attributes
   local schema = coreSchemaConfigTable()

   -- Process all items - configset table info
   local result = coreProcessItems(schema, jsonTable, output)

   -- Lua table with the configTable data --
   local confResult = {}

   -- Lua array with the list of columns --
   local columns_list = {}

   -- Loop through the Lua table with all the items --
   for key, value in pairs(result) do
      -- Verify the data store format type --
      local array_of_arrays = 0
      for typeData_key, typeData_value in pairs(value) do
         if typeData_key == "dataStorageFormat" and typeData_value == "ARRAY_OF_ARRAYS" then
            array_of_arrays = 1
         end
      end
      -- Process the metadata --
      for result_key, result_value in pairs(value) do
         -- The configSet data is stored in the typeData property as a JSON string --
         if result_key == "typeData" and type(result_value) == "string" then
            -- Transform the JSON string in Lua table --
            local tdResult = json:decode(result_value)
            -- Loop through the typeData information --
            for td_key, td_value in pairs(tdResult) do
               if td_key == "table" then
                     -- Table JSON Object --
                  for table_key, table_value in pairs(td_value) do
                     if table_key == "columns" then
                        local vars = {}
                           -- Columns JSON Array --
                        for column_key, column_value in ipairs(table_value) do
                           local meta = {}
                           local var_id
                           -- Columns JSON Object --
                           for column_meta_key, column_meta_value in pairs(column_value) do
                              -- Process the column id --
                              if column_meta_key == "options" then
                                 for var_key, var_value in pairs(column_meta_value) do
                                    if var_key == "id" then
                                       var_id = var_value
                                    end
                                 end
                              end
                              -- Process the metdata inormation --
                              if column_meta_key == "cellOptions" then
                                 for meta_key, meta_value in pairs(column_meta_value) do
                                    if meta_key == "cellType" then
                                       if meta_value == "string" then
                                             meta["type"] = "C"
                                       elseif meta_value == "boolean" then do
                                             meta["type"] = "C"
                                             meta["length"] = 5
                                          end
                                          else
                                             meta["type"] = "N"
                                       end
                                    end
                                    if meta_key == "size" then
                                       meta["length"] = meta_value
                                    end
                                 end
                              end
                           end
                           -- Add the metadata to the vars table --
                           vars[var_id] = meta
                           -- Add the column to the array --
                           table.insert(columns_list, var_id)
                        end
                           -- After processing all the columns add the vars to the final table output --
                        confResult["vars"] = vars
                     end
                  end
               end
            end
         end
      end
      -- Process the data --
      for result_key, result_value in pairs(value) do
         -- The configSet data is stored in the typeData property as a JSON string --
         if result_key == "typeData" and type(result_value) == "string" then
            -- Transform the JSON string in Lua table --
            local tdResult = json:decode(result_value)
            -- Loop through the typeData information --
            for td_key, td_value in pairs(tdResult) do
               -- Process the data --
               if td_key == "row" then
                  -- Verify if the JSON structure has items property --
                  local has_items = 0
                  for row_key, row_value in pairs(td_value) do
                     if row_key == "items" then
                        has_items = 1
                     end
                  end
                  if has_items == 1 then
                     -- has items --
                     for row_key, row_value in pairs(td_value) do
                        if row_key == "items" then
                           -- The data value is a JSON Array (rows) of JSON objects (data) --
                           for item_key, item_value in pairs(row_value) do
                              -- Lua table to store the row data --
                              local row_data = {}
                              if array_of_arrays == 0 then
                                 for data_key, data_value in pairs(item_value) do
                                    -- Store each variable data in the row_data table --
                                    row_data[data_key] = data_value
                                 end
                              else
                                 for data_key, data_value in ipairs(columns_list) do
                                    -- Store each variable data in the row_data table --
                                    row_data[columns_list[data_key]] = item_value[data_key]
                                 end
                              end
                              -- Process boolean attributes
                              row_data = processBoolean(row_data)
                              -- Insert the row with data in the final table --
                              table.insert(confResult, row_data)
                           end
                        end
                     end
                  else
                     -- no items --
                     -- The data value is a JSON Array (rows) of JSON objects (data) --
                     for item_key, item_value in pairs(td_value) do
                        -- Lua table to store the row data --
                        local row_data = {}
                        if array_of_arrays == 0 then
                           for data_key, data_value in pairs(item_value) do
                              -- Store each variable data in the row_data table --
                              row_data[data_key] = data_value
                           end
                        else
                           -- If it is arrays of arrays loop through the list of columns and add the data value --
                           for data_key, data_value in ipairs(columns_list) do
                              -- Store each variable data in the row_data table --
                              row_data[columns_list[data_key]] = item_value[data_key]
                           end
                        end
                        -- Process boolean attributes
                        row_data = processBoolean(row_data)
                        -- Insert the row with data in the final table --
                        table.insert(confResult, row_data)
                     end
                  end
               end
            end
         end
      end
   end
   if #confResult == 0 then
      -- Make sure there is at least one record
      confResult[1] = {key = -1}
      -- Write table
      sas.write_ds(confResult, output_data)
      -- Now remove the empty record
      sas.submit([[data @output_data@; set @output_data@(obs = 0); run;]])
   else
      -- print("Output: " .. table.tostring(confResult))
      sas.write_ds(confResult, output_data)
   end
end

M.coreRestConfigTable = coreRestConfigTable


-----------------------------
-- ConfigSets
-----------------------------
-- Return configuration set schema info
coreSchemaConfigSet = function()

   -- Define Standard Core Attributes
   local schema = coreObjectAttr()

   -- Return schema
   return schema

end

-- Parser for riskCirrusObjects/objects/configurationSets/<configSetKey>
coreRestConfigSet = function(filename, output)
   -- Parse JSON
   local jsonTable = parseJSONFile(filename)
   -- Define ConfigSet Attributes
   local schema = coreSchemaConfigSet()
   -- Process all items
   coreProcessItems(schema, jsonTable, output)
end

M.coreRestConfigSet = coreRestConfigSet


-----------------------------
-- DataDefinitions
-----------------------------
-- Attributes of the Core dataDefinition custom object
coreSchemaDataDefinition = function()

   -- Define Standard Core Attributes
   local schema = coreObjectAttr()

   -- Add Custom Fields for the dataDefinition object
   schema.vars.baseDttm = {type = "C"}
   schema.vars.schemaName = {type = "C"}
   schema.vars.schemaVersion = {type = "C"}
   schema.vars.martLibraryNm = {type = "C"}
   schema.vars.martTableNm = {type = "C"}
   schema.vars.libref = {type = "C"}
   schema.vars.engine = {type = "C"}
   schema.vars.schemaTypeCd = {type = "C"}
   schema.vars.dataCategoryCd = {type = "C"}
   schema.vars.dataSubCategoryCd = {type = "C"}
   schema.vars.businessCategoryCd = {type = "C"}
   schema.vars.riskTypeCd = {type = "C"}
   schema.vars.dataType = {type = "C"}

   -- Return schema
   return schema
end

-- Parser for riskCirrusObjects/objects/<contentId>/dataDefinitions
coreRestDataDefinition = function(filename, outputSummary, outputColInfo, outputAggregationConfig)
   -- Parse JSON
   local jsonTable = parseJSONFile(filename)
   -- Define dataDefinition Attributes
   local schema = coreSchemaDataDefinition()
   -- Process all items
   local result = coreProcessItems(schema, jsonTable, outputSummary)

   -- Process columnInfo field
   if outputColInfo ~= nil then
      local columnInfoTable = {vars = {}}
      columnInfoTable.vars.dataDefKey = result.vars.key
      columnInfoTable.vars.dataDefName = result.vars.name
      columnInfoTable.vars.key = {type = "C"}
      columnInfoTable.vars.name = {type = "C"}
      columnInfoTable.vars.config_readOnly = {type = "C"}
      columnInfoTable.vars.label = {type = "C"}
      columnInfoTable.vars.type = {type = "C"}
      columnInfoTable.vars.size = {type = "N"}
      columnInfoTable.vars.format = {type = "C"}
      columnInfoTable.vars.informat = {type = "C"}
      columnInfoTable.vars.primaryKeyFlag = {type = "C"}
      columnInfoTable.vars.partitionFlag = {type = "C"}
      columnInfoTable.vars.filterable = {type = "C"}
      columnInfoTable.vars.classification = {type = "C"}
      columnInfoTable.vars.attributable = {type = "C"}
      columnInfoTable.vars.mandatory_segmentation = {type = "C"}
      columnInfoTable.vars.segmentationFlag = {type = "C"}
      columnInfoTable.vars.projectionFlag = {type = "C"}
      columnInfoTable.vars.projection = {type = "C"}
      columnInfoTable.vars.fx_var = {type = "C"}
      -- Loop through all data definitions
      for i, row in ipairs(result) do
         if(row.columnInfo ~= nil) then
            -- row.columnInfo is a JSON table stored as a string: need to decode it
            local cols = json:decode(row.columnInfo)
         -- print(table.tostring(cols))
            -- Loop through all columnInfo
            for j, item in ipairs(cols) do
               local col = {}
               col.dataDefKey = row.key
               col.dataDefName = row.name
               -- Loop through all attributes of the column
               for attrName, attrValue in pairs(item) do
                  if type(attrValue) == "table" then
                     -- It is a complex parameter. Need to encode it
                     col[attrName] = json:encode(attrValue)
                  elseif type(attrValue) == "boolean" then
                     if attrValue then
                        col[attrName] = "true"
                     else
                        col[attrName] = "false"
                     end
                  else
                     col[attrName] = attrValue
                  end
               end
               table.insert(columnInfoTable, col)
            end
         end
      end
      if #columnInfoTable == 0 then
         -- Make sure there is at least one record
         columnInfoTable[1] = {dataDefKey = -1}
         -- Write table
         sas.write_ds(columnInfoTable, outputColInfo)
         -- Now remove the empty record
         sas.submit([[data @outputColInfo@; set @outputColInfo@(obs = 0); run;]])
      else
         -- Write table
         sas.write_ds(columnInfoTable, outputColInfo)
      end
   end -- if outputColInfo ~= nil

   -- Process martAggregationConfig field
   if outputAggregationConfig ~= nil then
      local aggregationConfigTable = {vars = {}}
      aggregationConfigTable.vars.dataDefKey = result.vars.key
      aggregationConfigTable.vars.dataDefName = result.vars.name
      aggregationConfigTable.vars.var_name = {type = "C"}
      aggregationConfigTable.vars.var_scope = {type = "C"}
      aggregationConfigTable.vars.weight_var = {type = "C"}
      aggregationConfigTable.vars.alias = {type = "C"}

      -- Loop through all data definitions
      for i, row in ipairs(result) do
         if(row.martAggregationConfig ~= nil) then
            -- row.martAggregationConfig is a JSON table stored as a string: need to decode it
            local cols = json:decode(row.martAggregationConfig)
            -- Loop through all columnInfo
            for j, item in ipairs(cols.items) do
               local col = {}
               col.dataDefKey = row.key
               col.dataDefName = row.name
               -- Loop through all attributes of the column
               for attrName, attrValue in pairs(item) do
                  if type(attrValue) == "table" then
                     -- It is a complex parameter. Need to encode it
                     col[attrName] = json:encode(attrValue)
                  else
                     col[attrName] = attrValue
                  end
               end
               table.insert(aggregationConfigTable, col)
            end
         end
      end
      if #aggregationConfigTable == 0 then
         -- Make sure there is at least one record
         aggregationConfigTable[1] = {dataDefKey = -1}
         -- Write table
         sas.write_ds(aggregationConfigTable, outputAggregationConfig)
         -- Now remove the empty record
         sas.submit([[data @outputAggregationConfig@; set @outputAggregationConfig@(obs = 0); run;]])
      else
         -- Write table
         sas.write_ds(aggregationConfigTable, outputAggregationConfig)
      end
   end -- if outputAggregationConfig ~= nil

end
M.coreRestDataDefinition = coreRestDataDefinition


-----------------------------
-- ruleSet
-----------------------------
-- Attributes of the Core ruleSet custom object
coreSchemaRuleSet = function()

   -- Define Standard Core Attributes
   local schema = coreObjectAttr()

   -- Add Custom Fields for the ruleSet object
   schema.vars.typeCd = {type = "C"}
   schema.vars.ruleData = {type = "C", length = 32000}
   schema.vars.ruleMeta = {type = "C", length = 32000}

   return schema
end

coreRestRuleSet = function(filename, output, output_rules)
   -- Parse JSON
   local jsonTable = parseJSONFile(filename)

   -- Define RuleSet  Attributes
   local schema = coreSchemaRuleSet()

   -- Process all items - rule set table info
   local result = coreProcessItems(schema, jsonTable, output)

   if output_rules~=nil then
      -- Process ruleData field
      local rules = {vars ={}}
      rules.vars.ruleSetKey = {type = "C"}
      rules.vars.ruleSetType = {type = "C"}
      rules.vars.objectId = {type = "C"}
      rules.vars.name = {type = "C"}
      rules.vars.description = {type = "C"}
      -- added for Cirrus hierarchical spreadsheet
      rules.vars.type = {type = "C", length = 100}
      -- Loop through all RuleSets
      for i, ruleSet in ipairs(result) do
         if ruleSet.typeCd == 'BUSINESS_RULES' then
            rules.vars.primary_key = {type = "C", length = 10000}
            rules.vars.key = {type = "C", length = 40}
            rules.vars.target_table = {type = "C", length = 40}
            rules.vars.rule_id  = {type = "C", length = 100}
            rules.vars.rule_name = {type = "C", length = 100}
            rules.vars.rule_desc = {type = "C", length = 100}
            rules.vars.rule_component = {type = "C", length = 32}
            rules.vars.operator = {type = "C", length = 32}
            rules.vars.parenthesis = {type = "C", length = 3}
            rules.vars.column_nm = {type = "C", length = 32}
            rules.vars.rule_type = {type = "C", length = 100}
            rules.vars.rule_details = {type = "C", length = 4000}
            rules.vars.message_txt = {type = "C", length = 4096}
            rules.vars.lookup_table = {type = "C", length = 1024}
            rules.vars.lookup_key = {type = "C", length = 10000}
            rules.vars.lookup_data = {type = "C", length = 10000}
            rules.vars.aggr_var_nm = {type = "C", length = 32}
            rules.vars.aggr_expression_txt = {type = "C", length = 10000}
            rules.vars.aggr_group_by_vars = {type = "C", length = 10000}
            rules.vars.aggregated_rule_flg = {type = "C", length = 3}
            rules.vars.rule_reporting_lev1 = {type = "C", length = 1024}
            rules.vars.rule_reporting_lev2 = {type = "C", length = 1024}
            rules.vars.rule_reporting_lev3 = {type = "C", length = 1024}
            rules.vars.rule_weight = {type = "N"}
         elseif ruleSet.typeCd == 'ALLOCATION_RULES' then
            rules.vars.rule_id  = {type = "C", length = 32}
            rules.vars.rule_desc = {type = "C", length = 4096}
            rules.vars.record_id = {type = "C", length = 100}
            rules.vars.rule_method = {type = "C", length = 10}
            rules.vars.adjustment_value = {type = "C", length = 32000}
            rules.vars.measure_var_nm = {type = "C", length = 150}
            rules.vars.adjustment_type = {type = "C", length = 150}
            rules.vars.allocation_method = {type = "C", length = 150}
            rules.vars.aggregation_method = {type = "C", length = 32}
            rules.vars.weight_var_nm = {type = "C", length = 150}
            rules.vars.weighted_aggregation_flg = {type = "C", length = 3}
            rules.vars.filter_exp = {type = "C", length = 10000}
         else
            rules.vars.rule_id  = {type = "C", length = 32}
            rules.vars.rule_desc = {type = "C", length = 4096}
            rules.vars.record_id = {type = "C", length = 100}
            rules.vars.rule_method = {type = "C", length = 10}
            rules.vars.adjustment_value = {type = "C", length = 32000}
            rules.vars.measure_var_nm = {type = "C", length = 150}
            rules.vars.adjustment_type = {type = "C", length = 150}
            rules.vars.allocation_method = {type = "C", length = 150}
            rules.vars.aggregation_method = {type = "C", length = 32}
            rules.vars.weight_var_nm = {type = "C", length = 150}
            rules.vars.weighted_aggregation_flg = {type = "C", length = 3}
            rules.vars.filter_exp = {type = "C", length = 10000}
         end
         if (ruleSet.ruleData ~= nil) then
            -- Parse the embedded JSON string into a Lua table
            local ruleSetData = json:decode(ruleSet.ruleData)
            for j, row in pairs(ruleSetData.items) do
               -- Loop through all ites in the rule
               for item, value in pairs(row) do
                  -- Convert table values to string
                  if type(value) == "table" then
                     if item:lower() == 'adjustment_value' and value.rawExpression ~= nil then
                        row[item] = value.rawExpression
                     else
                        -- ignore hierarchical members array
                        if item:lower() ~= 'members' then
                           row[item] = json:encode(value)
                        end
                     end
                  end
                  -- Convert empty string to missing in case of numeric fields
                  if rules.vars[item:lower()] ~= nil then
                     if rules.vars[item:lower()].type == "N" and row[item] == "" then
                        row[item] = nil
                     end
                  end
               end
               row['ruleSetKey'] = ruleSet.key
               row['ruleSetType'] = ruleSet.typeCd
               row['objectId'] = ruleSet.objectId
               row['name'] = ruleSet.name
               row['description'] = ruleSet.description

               -- Add record to the output table
               table.insert(rules, row)
            end
         end
      end
      -- Write result
      if #rules == 0 then
         -- Make sure there is at least one record
         rules[1] = {ruleSetKey = -1}
         -- Write table
         sas.write_ds(rules, output_rules)
         -- Now remove the empty record
         sas.submit([[data @output_rules@; set @output_rules@(obs = 0); run;]])
      else
         -- Write table
         sas.write_ds(rules, output_rules)
      end
   end
end
M.coreRestRuleSet = coreRestRuleSet


-----------------------------
-- analysisData
-----------------------------
-- Attributes of the Core analysisData custom object
coreSchemaAnalysisData = function()

   -- Define Standard Core Attributes
   local schema = coreObjectAttr()

   -- Add Custom Fields for the analysisData object
   schema.vars.baseDt = {type = "C"}
   schema.vars.baseDttm = {type = "C"}
   schema.vars.statusCd = {type = "C"}
   schema.vars.importJobId = {type = "C"}
   schema.vars.importJobURI = {type = "C"}
   schema.vars.physicalTableNm = {type = "C"}
   schema.vars.indexList = {type = "C", length = 5000}
   schema.vars.strategyType = {type = "C"}
   schema.vars.rowsPerPartition = {type = "N"}
   schema.vars.partitionCount = {type = "N"}
   schema.vars.rowCount = {type = "N"}
   schema.vars.columnCount = {type = "N"}

   -- Return schema
   return schema
end

-- Parser for riskCirrusObjects/object/<contentId>/AnalysisData
coreRestAnalysisData = function(filename, output)
   -- Parse JSON
   local jsonTable = parseJSONFile(filename)
   -- Define dataDefinition Attributes
   local schema = coreSchemaAnalysisData()
   -- Process all items
   coreProcessItems(schema, jsonTable, output)
end
M.coreRestAnalysisData = coreRestAnalysisData


-----------------------------
-- analysisRun
-----------------------------
-- Attributes of the Core analysisRun custom object
coreSchemaAnalysisRun = function()

   -- Define Standard Core Attributes
   local schema = coreObjectAttr()

   -- Add Custom Fields for the analysisRun object
   schema.vars.baseDt = {type = "C"}
   schema.vars.baseDttm = {type = "C"}
   schema.vars.statusCd = {type = "C"}
   schema.vars.scriptParameters = {type = "C", length = 32000}
   schema.vars.scriptParametersUI = {type = "C", length = 32000}
   schema.vars.userTaskName = {type = "C"}
   schema.vars.jobKey = {type = "C"}
   schema.vars.productionFlg = {type = "C"}

   return schema
end

coreSchemaAnalysisRunParams = function()

   local schema = {vars = {}}
   schema.vars.analysisRunKey = {type = "C"}
   schema.vars.analysisRunName = {type = "C"}
   schema.vars.name = {type = "C", length = 200}
   schema.vars.type = {type = "C"}
   schema.vars.value = {type = "C", length = 32000}
   schema.vars.objectKey = {type = "C"}
   schema.vars.objectId = {type = "C"}
   schema.vars.objectName = {type = "C"}
   schema.vars.objectRestPath = {type = "C"}
   schema.vars.objectCustomFields = {type = "C", length = 32000}
   schema.vars.isValid = {type = "C"}
   schema.vars.isNested = {type = "C"}
   schema.vars.parentScriptParameter = {type = "C"}

   return schema
end


-- Parser for riskCirrusObjects/object/<contentId>/AnalysisRun
coreRestAnalysisRun = function(filename, output, outputParms, foutCode, objectOutputConfigDs)

   -- Parse JSON
   local jsonTable = parseJSONFile(filename)

   -- Define Analysis Run Attributes
   local schema = coreSchemaAnalysisRun()

   -- Process all items - analysis run table info
   local result = coreProcessItems(schema, jsonTable, output)

   -- Create the output parameters table, if requested
   if outputParms ~= nil then

      local outParmsTable = coreSchemaAnalysisRunParams()

      -- Define requested object parameter type attributes
      local objOutputTables = {}
      local objOutputConfig = {}
      if objectOutputConfigDs ~= nil then
         objOutputConfig = sas.read_ds(objectOutputConfigDs)
         for _, objConfig in ipairs(objOutputConfig) do
            objOutputTables["objSchema_"..objConfig.objecttype] = {}
         end
      end

      for i, value in ipairs(result) do
         for jsonKey, jsonValue in pairs(value) do
            -- The resolved parameters are stored in the resolvedExpressions property as a JSON string
            if jsonKey == "resolvedExpressions" and type(jsonValue) == "string" then

               -- Transform the JSON string in Lua table
               local resolvedExpressions = json:decode(jsonValue)

               -- Loop through the resolved script parameters
               for j, param in ipairs(resolvedExpressions.scriptParameterResults) do

                  -- Lua table to store the row data
                  local row = {}

                  -- Store each variable data in the row table
                  row.analysisRunKey = value.key
                  row.analysisRunName = value.name
                  row.name = tostring(param.parameterName)
                  row.isValid = tostring(param.isValid)
                  row.isNested = tostring(param.isNested or false)
                  row.parentScriptParameter = tostring(param.parentScriptParameter or "")
                  if param.originalScriptParam then
                     row.type = tostring(param.originalScriptParam.type)
                     row.objectRestPath = tostring(param.originalScriptParam.restPath or "")
                  end

                  -- Handle parameters that are tables (JSON arrays/ or objects).
                  -- Examples: cirrus object parameters, dual selector parameters, risk scenario set parameters, etc.
                  if type(param.expandedValue) == "table" then

                     -- if this is a table with elements, output a row for each element
                     -- if this is a table without elements, it's likely a dual selector or other array parameter that has formatted values (formattedValues)
                        -- if it doesn't have formattedValues, just encode the entire parameter as the value
                     if param.expandedValue[1] then
                        for k, expValue in ipairs(param.expandedValue) do

                           row.value = json:encode(expValue)

                           -- Special processing for Cirrus Objects parameters
                           if row.type == "cirrusObjectArray" then
                              row.objectKey = expValue.key
                              row.objectId = expValue.objectId
                              row.objectName = expValue.name
                              row.objectCustomFields = json:encode(expValue.customFields)

                           -- if any object parameter attributes have been requested, add that object's info (at analysis run submission time) to
                           -- the requested output table
                              for _, objConfig in ipairs(objOutputConfig) do
                                 if row.objectRestPath == objConfig.objecttype then
                                    local objSchema = _G[objectTypeSchemaMap[objConfig.objecttype]](objConfig.arg1, objConfig.arg2, objConfig.arg3)
                                    local objResult = coreProcessItems(objSchema, param.expandedValue[1])
                                    objOutputTables["objSchema_"..objConfig.objecttype].vars=objResult.vars
                                    table.insert(objOutputTables["objSchema_"..objConfig.objecttype],objResult[1])
                                 end
                              end

                           end
                           table.insert(outParmsTable,row)

                        end
                     else
                        row.value = param.expandedValue.formattedValues or json:encode(param.expandedValue)
                        table.insert(outParmsTable,row)
                     end

                  else
                     row.value = param.expandedValue
                     table.insert(outParmsTable,row)
                  end

               end --end loop over resolvedExpressions.scriptParameterResults
            end --end if key is resolvedExpressions
         end --end loop over response JSON keys
      end --end loop over result table

      -- Create the requested analysis run parameters SAS dataset
      createSASTable(outParmsTable, outputParms)

      -- Create any requested object parameter attributes SAS datasets
      for _, objConfig in ipairs(objOutputConfig) do
         createSASTable(objOutputTables["objSchema_"..objConfig.objecttype], objConfig.outputds)
      end

   end --end if output parameters table requested

   if foutCode ~= nil then
      if #result == 1  and result[1].sasCode ~= nil then
         -- Assign fileref
         local fileref = sasxx.new(foutCode)
         -- Open fileref for writing
         local f = fileref:open("wb")
         -- Write the code
         f:write(result[1].sasCode)
         -- Close file
         f:close()
         -- Deassign the fileref
         fileref:deassign()
      end
   end
end
M.coreRestAnalysisRun = coreRestAnalysisRun


-----------------------------
-- attributionTemplate
-----------------------------
-- Attributes of the Core attributionTemplate custom object
coreSchemaAttributionTemplate = function(details_flg)

   -- Process details_flg parameter
   details_flg = details_flg or "N"
   details_flg = string.upper(details_flg)

   -- Define Standard Core Attributes
   local schema = coreObjectAttr()

   -- Add Custom Fields for the attributionTemplate object
   if details_flg == "Y" then
      -- Add details about each scenario associated with this master risk scenario
      schema.vars.attributionGroupNo = {type = "N"}
      schema.vars.attributionGroupName = {type = "C", length = 100}
      schema.vars.attributeName = {type = "C", length = 100}
      schema.vars.attributeDescription = {type = "C", length = 256}
      schema.vars.attributionType = {type = "C", length = 100}
      schema.vars.attributionVars = {type = "C", length = 32000}
   else
      schema.vars.attFactors = {type = "C", length = 100000}
      schema.vars.outputVars = {type = "C", length = 100000}
   end

   schema.vars.transferFromLabel = {type = "C", length = 100}
   schema.vars.transferToLabel = {type = "C", length = 100}

   -- Return schema
   return schema
end

-- Parser for riskCirrusObjects/object/<contentId>/attributionTemplates
coreRestAttributionAnalysis = function(filename, output, details_flg)

   -- Process details_flg parameter
   details_flg = details_flg or "N"
   details_flg = string.upper(details_flg)

   -- Parse JSON
   local jsonTable = parseJSONFile(filename)

   -- Define attributionTemplate Attributes
   local schema = coreSchemaAttributionTemplate(details_flg)

      -- Local function for expanding outputVars and attFactors
   local expandAttribution = function(item, row)

      -- Get AttributionAnalysis Attributes (including details)
      local schema = coreSchemaAttributionTemplate("Y")

      local attributionTable = {}
      local attributionVars = ""

      if row.outputVars ~= nil then
         -- row.outputVars is a JSON table stored as a string: need to decode it
         local outputVarsData = json:decode(row.outputVars)
         -- Loop through all attribution variables
         for i, outputVar in pairs(outputVarsData.data) do
            attributionVars = sas.catx(" ", attributionVars, outputVar.value)
         end
      end

      -- Process the attribution factors associated with the attribution analysis (if any)
      if row.attFactors ~= nil then
         -- row.attFactors is a JSON table stored as a string: need to decode it
         local attFactorsData = json:decode(row.attFactors)
         -- Loop through all attribution factors
         for i, attFactor in pairs(attFactorsData) do
            -- Loop through all properties of the schema variable
            for var, options in pairs(schema.vars) do
               -- Copy the value of the property from row to attFactor
               if row[var] ~= nil then
                  attFactor[var] = row[var]
               end
            end
            -- Set attribution variables
            attFactor.attributionVars = attributionVars
            -- Add current attFactor to the result
            table.insert(attributionTable, attFactor)
         end
      end
      return attributionTable
   end

   -- Process all items
   coreProcessItems(schema, jsonTable, output, details_flg == "Y" and expandAttribution or nil)

end
M.coreRestAttributionAnalysis = coreRestAttributionAnalysis


-----------------------------
-- Attachments
-----------------------------
-- Return standard Core Attachment structure
coreAttachmentAttr = function()
   -- Declare table structure
   local result = {
      vars = {
         -- Standard fields
         key = {type = "C"}
         , tempUploadKey = {type = "C"}
         , parentObjectKey = {type = "C"}
         , objectId = {type = "C"}
         , sourceSystemCd = {type = "C"}
         , name = {type = "C"}
         , displayName = {type = "C"}
         , description = {type = "C"}
         , comment = {type = "C"}
         , grouping = {type = "C"}
         , fileSize = {type = "N"}
         , fileExtension = {type = "C"}
         , fileMimeType = {type = "C"}
         , creationTimeStamp = {type = "C"}
         , modifiedTimeStamp = {type = "C"}
         , createdBy = {type = "C"}
         , modifiedBy = {type = "C"}
         , customFields = {type = "C", length = 32000}
         , links = {type = "C", length = 32000}
         , mediaTypeVersion = {type = "N"}
      }
   }

   -- Return table structure
   return result

end

-- Parser for objects/attachments/<object_uri>/<object_key>
coreRestFileAttachments = function(filename, output, outAttachments)

   -- Parse JSON
   local jsonTable = parseJSONFile(filename)

   -- Define Object Attributes
   local schema = coreObjectAttr()

   -- Process all items - analysis run table info
   local result = coreProcessItems(schema, jsonTable, output)

   -- Create the output attachments table, if requested
   if outAttachments ~= nil then

      -- Define File Attachment Attributes
      local outAttachmentsTable = coreAttachmentAttr()

      for i, value in ipairs(result) do
         for jsonKey, jsonValue in pairs(value) do

            -- The resolved parameters are stored in the resolvedExpressions property as a JSON string --
            if jsonKey == "fileAttachments" and type(jsonValue) == "string" then

               -- Transform the JSON string in Lua table --
               local fileAttachments = json:decode(jsonValue)

               for attachKey, attachValue in ipairs(fileAttachments) do
                  attach_row = {}
                  for key, value in pairs(attachValue) do
                     if(type(value) == "table") then
                        attach_row[key] = json:encode(value)
                     else
                        attach_row[key] = value
                     end
                  end
                  table.insert(outAttachmentsTable, attach_row)
               end
            end
         end
      end

      if #outAttachmentsTable == 0 then
         -- Make sure there is at least one record
         outAttachmentsTable[1] = {key = -1}
         -- Write table
         sas.write_ds(outAttachmentsTable, outAttachments)
         -- Now remove the empty record
         sas.submit([[data @outAttachments@ ; set @outAttachments@ (obs = 0); run;]])
      else
         -- Write outParmsTable
         sas.write_ds(outAttachmentsTable, outAttachments)
      end

   end

end
M.coreRestFileAttachments = coreRestFileAttachments


-----------------------------
-- linkInstances
-----------------------------
-- Return standard Core LinkInstances structure
coreLinkInstancesAttr = function()
   -- Declare table structure
   local result = {
      vars = {
         -- Standard fields
         key = {type = "C"}
         , objectId = {type = "C"}
         , sourceSystemCd = {type = "C"}
         , linkType = {type = "C"}
         , businessObject1 = {type = "C"}
         , businessObject2 = {type = "C"}
         , description = {type = "C"}
         , createdBy = {type = "C"}
         , modifiedBy = {type = "C"}
         , creationTimeStamp = {type = "C"}
         , modifiedTimeStamp = {type = "C"}
         , itemsCount = {type = "N"}
      }
   }

   -- Return table structure
   return result
end

-- Parser for objects/<objectType>/<objectKey>/linkInstances/<linkTypeKey>
coreRestLinkInstances = function(filename, output)
   -- Parse JSON
   local jsonTable = parseJSONFile(filename)
   -- Define LinkTypes Attributes
   local schema = coreLinkInstancesAttr()
   -- Process all items
   coreProcessItems(schema, jsonTable, output)
end
M.coreRestLinkInstances = coreRestLinkInstances


-----------------------------
-- linkTypes
-----------------------------
-- Return Core LinkTypes Attributes
coreLinkTypesAttr = function()
   -- Declare table structure
   local result = {
      vars = {
         -- Standard fields
         key = {type = "C"}
         , objectId = {type = "C"}
         , sourceSystemCd = {type = "C"}
         , side1Type = {type = "C"}
         , side1ObjectTypeKey = {type = "C"}
         , side2Type = {type = "C"}
         , side2ObjectTypeKey = {type = "C"}
         , itemsCount = {type = "N"}
      }
   }

   -- Return table structure
   return result
end

-- Parser for objects/linkTypes
coreRestLinkTypes = function(filename, output)
   -- Parse JSON
   local jsonTable = parseJSONFile(filename)
   -- Define LinkTypes Attributes
   local schema = coreLinkTypesAttr()

   -- Local function for expanding the side1 and side2 keys
   local linkSideFunc = function(item, row)
      row=item
      row["side1Type"]=item.side1.type
      row["side1ObjectTypeKey"]=item.side1.typeKey
      row["side2Type"]=item.side2.type
      row["side2ObjectTypeKey"]=item.side2.typeKey
      return row
   end

   -- Process all items
   coreProcessItems(schema, jsonTable, output, linkSideFunc)

end
M.coreRestLinkTypes = coreRestLinkTypes


-----------------------------
-- jobId
-----------------------------
-- Return standard Core Object attributes
coreObjectjobIdAttr = function()
   -- Declare table structure
   local result = {
      vars = {
         -- Standard fields
         id = {type = "C"}
         , state = {type = "C"}
         , submittedByApplication = {type = "C"}
         , logLocation = {type = "C"}
         , results = {type = "C"}
         }
   }
   -- Return table structure
   return result
end

-- Parser for jobExecution/jobs
coreRestJobId = function(filename, output, outputParms)
   -- Parse JSON
   local jsonTable = parseJSONFile(filename)

   -- Define ConfigTable Attributes
   local schema = coreObjectjobIdAttr()

   -- Process all items - configset table info
   local result = coreProcessItems(schema, jsonTable, output)
   --print("Result info: " .. table.tostring(result))

   -- Lua table with the configTable data --
   local confResult = {}
   if outputParms ~= nil then
      for key, value in pairs(result) do
         for result_key, result_value in pairs(value) do
            
            if result_key == "error" and type(result_value) == "string" then
               local tdError = json:decode(result_value)
               for row_k, row_v in pairs(tdError) do
                  local row = {}
                  row.dataParamKey = tostring(row_k)
                  row.dataParamValues = tostring(row_v)
                  table.insert(confResult,row)
               end
            end
            -- The parameters data is stored in the scriptParameters property as a JSON string --
            if result_key == "results" and type(result_value) == "string" then
               -- Transform the JSON string in Lua table --
               local tdResult = json:decode(result_value)

               for row_k, row_v in pairs(tdResult) do
                  local row = {}
                  -- Store each variable data in the row table --
                  row.dataParamKey = tostring(row_k)
                  row.dataParamValues = tostring(row_v)
                  table.insert(confResult,row)
               end
            end
         end
      end
   end

   -- Insert the row with data in the final table --
   --print("confResult info: " .. table.tostring(confResult))

   if #confResult == 0 then
      -- Make sure there is at least one record
      confResult[1] = {key = -1}
      -- Write table
      sas.write_ds(confResult, outputParms)
      -- Now remove the empty record
      sas.submit([[data @outputParms@ ; set @outputParms@ (obs = 0); run;]])
   else
      -- Write confResult
      sas.write_ds(confResult, outputParms)
   end
end
M.coreRestJobId = coreRestJobId

-----------------------------------
-- Dimensional Points - Positions
-----------------------------------
-- Return standard Core dimensional points attributes
coreDimPointAttr = function()
   -- Declare table structure
   local result = {
      vars = {
         -- Standard fields
         key = {type = "C"}
         , objectId = {type = "C"}
         , sourceSystemCd = {type = "C"}
         , name = {type = "C"}
         , description = {type = "C"}
         , createdIntag = {type = "C"}
         , classification = {type = "C"}
         , mediaTypeVersion = {type = "N"}
      }
   }

   -- Return table structure
   return result
end

-- Parser for riskCirrusObjects/objects/<restPath>
coreRestDimPoints = function(filename, output)
   -- Parse JSON
   local jsonTable = parseJSONFile(filename)
   -- Define dimensional points Attributes
   local schema = coreDimPointAttr()
   -- Process all items
   coreProcessItems(schema, jsonTable, output)

end
M.coreRestDimPoints = coreRestDimPoints


-----------------------------------
-- Points Paths - Positions
-----------------------------------
-- Return standard Core Points Positions attributes
corePointPathsAttr = function()
   -- Declare table structure
   local result = {
      vars = {
         -- Standard fields
         key = {type = "C"}
         , objectId = {type = "C"}
         , sourceSystemCd = {type = "C"}
         , pointPaths = {type = "C", length = 2000}
         , mediaTypeVersion = {type = "N"}
      }
   }

   -- Return table structure
   return result
end

-- Parser for riskCirrusObjects/classifications/points
coreRestPointPaths = function(filename, output, output_data)
   -- Parse JSON
   local jsonTable = parseJSONFile(filename)
   -- Define dimensional points Attributes
   local schema = corePointPathsAttr()
   -- Process all items
   coreProcessItems(schema, jsonTable, output)

end
M.coreRestPointPaths = coreRestPointPaths


----------------
-- Cas Sources
----------------
-- Return CAS sources attributes
coreCasSourcesAttr = function()
   -- Declare table structure
   local result = {
      vars = {
         -- Standard fields
         id = {type = "C"}
         , name = {type = "C"}
         , type = {type = "C"}
         , host = {type = "C"}
         , port = {type = "N"}
         , providerId = {type = "C"}
         , description = {type = "C"}
         , hasTables = {type = "C"}
         , hasEngines = {type = "C"}
      }
   }

   -- Return table structure
   return result
end

-- Parser for casManagement/providers/cas/sources
coreRestCasSources = function(filename, output)
   -- Parse JSON
   local jsonTable = parseJSONFile(filename)
   -- Define LinkTypes Attributes
   local schema = coreCasSourcesAttr()

   -- Local function for expanding the cas source attributes
   local sourceAttrFunc = function(item, row)
      row=item
      row["host"]=item.attributes.host
      row["port"]=item.attributes.port
      return row
   end

   -- Process all items
   coreProcessItems(schema, jsonTable, output, sourceAttrFunc)
end
M.coreRestCasSources = coreRestCasSources


-- General wrapper for REST calls to get Classification points from riskCirrusObjects/objects/<object>
coreRestClassifications = function(filename, output, output_classif)

   -- Parse JSON
   local jsonTable = parseJSONFile(filename)

   -- Define ConfigTable Attributes
   local schema = coreObjectAttr()

   -- Process all items - configset table info
   local result = coreProcessItems(schema, jsonTable, output)

   -- Lua table with the configTable data --
   local classResult = {vars = {}}
      classResult.vars.classificationField = result.vars.classification
      classResult.vars.objectKey = result.vars.key
      classResult.vars.objectName = result.vars.name
      classResult.vars.objectDescription = result.vars.description
      classResult.vars.objectId = result.vars.objectId
      classResult.vars.sourceSystemCd = result.vars.sourceSystemCd
      classResult.vars.createdInTag = result.vars.createdInTag
      classResult.vars.classificationSeq = {type = "C"}
      classResult.vars.classificationKey = {type = "C"}

   -- Create the output parameters table, if requested
   if jsonTable ~= nil then
      for key, value in ipairs(result) do
         for jsonKey, jsonValue in pairs(value) do
            -- The resolved parameters are stored in the default property as a JSON string --
            if jsonKey == "classification" and type(jsonValue) == "string" then
               -- Transform the JSON string in Lua table --
               local defaultResult = json:decode(jsonValue)
               -- print("defaultResult info: " .. table.tostring(defaultResult))
               -- Loop through the classification keys
               for row_k, row_v in pairs(defaultResult) do
                  -- Second loop through the JSON object (data) --
                  for row_key, row_value in pairs(row_v) do
                     -- Lua table to store the row data --
                     local row = {}
                     row.classificationField = row_k
                     row.objectKey = value.key
                     row.objectName = value.name
                     row.objectDescription = value.description
                     row.objectId = value.objectId
                     row.sourceSystemCd = value.sourceSystemCd
                     row.createdInTag = value.createdInTag
                     -- Store each variable data in the row table --
                     row.classificationSeq = row_key
                     row.classificationKey = row_value
                     -- Insert the row with data in the final table --
                     table.insert(classResult,row)
                  end
               end
            end
         end
      end
   end

   -- Process boolean attributes
   classResult = processBoolean(classResult)

   if output ~= nil then
      if #classResult == 0 then
         -- Make sure there is at least one record
         classResult[1] = {key = -1}
         -- Write table
         sas.write_ds(classResult, output)
         -- Now remove the empty record
         sas.submit([[data @output@ ; set @output@ (obs = 0); run;]])
      else
         -- Write classResult
         sas.write_ds(classResult, output)
      end
   end  -- if output ~= nil
end
M.coreRestClassifications = coreRestClassifications


local coreRestProject = function(filename, output)

   local jt = parseJSONFile(filename)
   local result = checkError(jt)

   if isEmpty(result) then
      result.vars = {
         key = {type = "C", length = 36},
      }
      simpleItemGet(jt, result)
   end
   createSASTable(result, output)
end
M.coreRestProject = coreRestProject

local coreRestNamedTreeInfo = function(filename, output)

   local jt = parseJSONFile(filename)
   local result = checkError(jt)

   if isEmpty(result) then
      result.vars = {
         key = {type = "C", length = 36},
         objectId = {type = "C", length = 50},
         sourceSystemCd = {type = "C", length = 3},
         name = {type = "C", length = 200},
         classTypeKey = {type = "C", length = 36}
      }
      simpleItemGet(jt, result)
   end
   createSASTable(result, output)
end
M.coreRestNamedTreeInfo = coreRestNamedTreeInfo

local coreRestRegistrationClass = function(filename, output)

   local jt = parseJSONFile(filename)
   local result = checkError(jt)

   if isEmpty(result) then
      result.vars = {
         key = {type = "C", length = 36},
         objectId = {type = "C", length = 50},
         sourceSystemCd = {type = "C", length = 3},
         name = {type = "C", length = 200},
         context = {type = "C", length = 200},
         namedTreeKey = {type = "C", length = 36}
      }
      local items = safeGet({jt, "items"})
      if isArray(items) and not isEmpty(items) then
         for i, item in ipairs(items) do
            local row = {
               key = item.key,
               objectId = item.objectId,
               sourceSystemCd = item.sourceSystemCd,
               name = item.name
            }
            local class = safeGet({item, "classification"})
            if isObject(class) and not isEmpty(class) then
               for contextId, context in pairs(class) do
                  row.context = contextId
                  local dims = safeGet({context, "dimensions"})
                  if isArray(dims) and not isEmpty(dims) then
                     for _, d in ipairs(dims) do
                        row.namedTreeKey = d.namedTreeKey
                        insertCp(result, row)
                     end
                  else insertCp(result, row) end
               end
            else insertCp(result, row) end
         end
      end
   end
   createSASTable(result, output)
end
M.coreRestRegistrationClass = coreRestRegistrationClass

local coreRestRoles = function(filename, output, getPermissions)

   local jt = parseJSONFile(filename)
   local result = checkError(jt)

   if isEmpty(result) then
      local procPerm = (getPermissions == "Y")
      result.vars = {
         key = { type = "C", length = 36 },
         objectId = { type = "C", length = 50 },
         sourceSystemCd = { type = "C", length = 3 },
         name = { type = "C", length = 200 },
         description = { type = "C", length = 4000 }
      }
      if procPerm then
         result.vars.objectType = {type = "C", length = 36}
         result.vars.capability = {type = "C", length = 32}
      end
      local items = safeGet({jt, "items"})
      if isArray(items) and not isEmpty(items) then
         for i, item in ipairs(items) do
            local row = {
               key = item.key,
               objectId = item.objectId,
               sourceSystemCd = item.sourceSystemCd,
               name = item.name,
               description = item.description
            }
            if procPerm then
               local perms = safeGet({item, "permissions"})
               if isArray(perms) and not isEmpty(perms) then
                  for _, p in ipairs(perms) do
                     row.objectType = p.objectType
                     row.capability = p.capability
                     insertCp(result, row)
                  end
               else insertCp(result, row) end
            else insertCp(result, row) end
         end
      end
   end
   createSASTable(result, output)
end
M.coreRestRoles = coreRestRoles

local coreRestObjectWithUsers = function(filename, output, outputChunkData)

   local jt = parseJSONFile(filename)
   local result = checkError(jt)

   local chunkResult = {}
   chunkResult.vars = {
      start = { type = "N" },
      count = { type = "N" },
      limit = { type = "N" }
   }

   if isEmpty(result) then
      table.insert(chunkResult, {
         start = jt.start,
         count = jt.count,
         limit = jt.limit
      })
      result.vars = {
         key = { type = "C", length = 36 },
         objectId = { type = "C", length = 50 },
         sourceSystemCd = { type = "C", length = 3 },
         name = { type = "C", length = 200 },
         description = { type = "C", length = 4000 },
         linkType = { type = "C", length = 50 },
         userId = { type = "C", length = 50 }
      }

      local items = safeGet({jt, "items"})
      if isArray(items) and not isEmpty(items) then
         for i, item in ipairs(items) do
            local row = {
               key = item.key,
               objectId = item.objectId,
               sourceSystemCd = item.sourceSystemCd,
               name = item.name,
               description = item.description
            }
            local objectLinks = safeGet({item, "objectLinks"})
            if isArray(objectLinks) and not isEmpty(objectLinks) then
               for _, link in ipairs(objectLinks) do
                  row.linkType = link.linkType
                  row.userId = link.user1 or link.user2
                  if row.userId then insertCp(result, row) end
               end
            else insertCp(result, row) end
         end
      end
   else chunkResult.row = {start = 0, count = 0, limit = 0} end
   createSASTable(chunkResult, outputChunkData)
   createSASTable(result, output)
end
M.coreRestObjectWithUsers = coreRestObjectWithUsers

local coreRestPositions = function(filename, output, outputChunkData)

   local jt = parseJSONFile(filename)
   local result = checkError(jt)

   local chunkResult = {}
   chunkResult.vars = {
      start = { type = "N" },
      count = { type = "N" },
      limit = { type = "N" }
   }

   if isEmpty(result) then
      table.insert(chunkResult, {
         start = jt.start,
         count = jt.count,
         limit = jt.limit
      })
      result.vars = {
         ident = { type = "C", length = 50 },
         point = { type = "C", length = 36 },
         role = { type = "C", length = 36 }
      }

      local items = safeGet({jt, "items"})
      if isArray(items) and not isEmpty(items) then
         for i, item in ipairs(items) do
            local row = {
               ident = item.group or item.user,
               role = item.role,
            }
            local points = safeGet({item, "points"})
            if isArray(points) and not isEmpty(points) then
               for _, point in ipairs(points) do
                  row.point = point
                  insertCp(result, row)
               end
            else insertCp(result, row) end
         end
      end
   else chunkResult.row = {start = 0, count = 0, limit = 0} end
   createSASTable(chunkResult, outputChunkData)
   createSASTable(result, output)
end
M.coreRestPositions = coreRestPositions

local coreRestPoints = function(filename, output, outputChunkData)

   local jt = parseJSONFile(filename)
   local result = checkError(jt)

   local chunkResult = {}
   chunkResult.vars = {
      start = { type = "N" },
      count = { type = "N" },
      limit = { type = "N" }
   }

   if isEmpty(result) then
      table.insert(chunkResult, {
         start = jt.start,
         count = jt.count,
         limit = jt.limit
      })
      result.vars = {
         key = { type = "C", length = 36 },
         namedTreeKey = { type = "C", length = 36 },
         path = { type = "C", length = 10000 }
      }

      local items = safeGet({jt, "items"})
      if isArray(items) and not isEmpty(items) then
         for i, item in ipairs(items) do
            local row = {key = item.key}
            local points = safeGet({item, "localizedPointPaths"})
            if isObject(points) and not isEmpty(points) then
               for namedTreeKey, point in pairs(points) do
                  if isObject(point) and not isEmpty(point) then
                     row.path = point.path
                     row.namedTreeKey = namedTreeKey
                     insertCp(result, row)
                  end
               end
            end
         end
      end
   else chunkResult.row = {start = 0, count = 0, limit = 0} end
   createSASTable(chunkResult, outputChunkData)
   createSASTable(result, output)
end
M.coreRestPoints = coreRestPoints

-----------------------------
-- Cycle
-----------------------------

coreSchemaCycle = function()
   -- Declare table structure
   local result = {
      vars = {
         -- Standard fields
         key = {type = "C"}
         , objectId = {type = "C"}
         , name = {type = "C"}
         , sourceSystemCd = {type = "C"}
         , createdInTag = {type = "C"}
         , baseDttm = {type = "C"}
         , baseDT = {type = "C"}
         , statusCd = {type = "C"}
         , runTypeCd = {type = "C"}
         }
      }
   -- Return table structure
   return result
end

-- Parser for riskCirrusObjects/object/Cycle
coreRestCycle = function(filename, output, output_classification)
   -- Parse JSON
   local jsonTable = parseJSONFile(filename)
   -- print("jsonTable info: " .. table.tostring(jsonTable))

   -- Define dataDefinition Attributes
   local schema = coreSchemaCycle()

   local result = coreProcessItems(schema, jsonTable, output)
   -- print("result info: " .. table.tostring(result))

   -- Create the output parameters table, if requested
   if output_classification ~= nil then

      -- Lua table with the configTable data --
      local confResult = {vars = {}}
         confResult.vars.cycleKey = result.vars.key
         confResult.vars.cycleName = result.vars.name
         confResult.vars.classification_seq = {type = "C"}
         confResult.vars.classification = {type = "C"}

      for i, value in ipairs(result) do
         for jsonKey, jsonValue in pairs(value) do
            -- The resolved parameters are stored in the default property as a JSON string --
            if jsonKey == "classification" and type(jsonValue) == "string" then
               -- Transform the JSON string in Lua table --
               local defaultResult = json:decode(jsonValue)
               -- print("defaultResult info: " .. table.tostring(defaultResult))
               -- Loop through the classification keys
               for row_k, row_v in pairs(defaultResult) do
                  -- Second loop through the JSON object (data) --
                  -- Lua table to store the row data --
                  local row = {}
                  for row_key, row_value in pairs(row_v) do
                     -- Store each variable data in the row table --
                     row.cycleKey = value.key
                     row.cycleName = value.name
                     row.classification_seq = row_key
                     row.classification = row_value
                  end
                  -- Insert the row with data in the final table --
                  table.insert(confResult,row)

                  if #confResult == 0 then
                     -- Make sure there is at least one record
                     confResult[1] = {key = -1}
                     -- Write table
                     sas.write_ds(confResult, output_classification)
                     -- Now remove the empty record
                     sas.submit([[data @output_classification@ ; set @output_classification@ (obs = 0); run;]])
                  else
                     -- Write outputClassif
                     sas.write_ds(confResult, output_classification)
                  end
               end
            end
         end
      end
   end
end
M.coreRestCycle = coreRestCycle

-----------------------------
-- Business Evolution Plans
-----------------------------
-- Attributes of the Cirrus Core businessEvolution custom object
coreSchemaBusinessEvolution = function()

   -- Define Standard Core Attributes
   local schema = coreObjectAttr()

   -- Add Custom Fields for the BusinessEvolution object
   schema.vars.intervalType = {type = "C", length = 32}
   schema.vars.intervalCount = {type = "N"}
   schema.vars.planningCurrency = {type = "C", length = 16}
   schema.vars.initType = {type = "C", length = 12}
   schema.vars.planningDataKey = {type = "C"}
   schema.vars.hierarchyDataKey = {type = "C"}

   -- Return schema
   return schema
end

-- Parser for riskCirrusObjects/objects/businessEvolutionPlans
coreRestBusinessEvolution = function(filename, outputSummary, bepLinkTypesDs, outputEvolution)

   -- Parse JSON
   local jsonTable = parseJSONFile(filename)

   -- Define BEP Attributes
   local schema = coreSchemaBusinessEvolution()

   -- Process all items - bep outputSummary table info
   local result = coreProcessItems(schema, jsonTable)

   if #result > 0 then

      -- If link type keys are given, add the linked object keys (planning data key, hierarhcy data key) to the outputSummary table
      if bepLinkTypesDs ~= nil then

         local linkTypeId_key_map = {}
         for _, inputRow in ipairs(sas.read_ds(bepLinkTypesDs)) do
            linkTypeId_key_map[inputRow.objectid] = inputRow.key
         end

         for _, item in ipairs(result) do
            for jsonKey, jsonValue in pairs(item) do
               if jsonKey == "objectLinks" then
                  for _, link in ipairs(json:decode(jsonValue)) do
                     if link.linkType == linkTypeId_key_map.businessEvolutionPlan_planningData then
                        item.planningDataKey = link.businessObject2
                     elseif link.linkType == linkTypeId_key_map.businessEvolutionPlan_hierarchyData then
                        item.hierarchyDataKey = link.businessObject2
                     end
                  end
               end
            end
         end
      end

      sas.write_ds(result, outputSummary)
   else
      result[1] = {key = -1}
      sas.write_ds(result, outputSummary)
      sas.submit([[data @outputSummary@ ; set @outputSummary@ (obs = 0); run;]])
   end

   -- Create the output business evolution details table, if requested
   if outputEvolution ~= nil then

      local outEvolutionTable = {vars = {}}
      outEvolutionTable.vars.bepKey = result.vars.key
      outEvolutionTable.vars.bepName = result.vars.name
      outEvolutionTable.vars.segment = {type = "C"}
      outEvolutionTable.vars.level = {type = "N"}
      outEvolutionTable.vars.hierarchy = {type = "C", length = 32000}
      outEvolutionTable.vars.key = {type = "C"}
      outEvolutionTable.vars.segmentFilter = {type = "C", length = 32000}
      outEvolutionTable.vars.segmentFilterSas = {type = "C", length = 32000}
      outEvolutionTable.vars.targetVar = {type = "C"}
      outEvolutionTable.vars.intervalType = {type = "C"}
      outEvolutionTable.vars.intervalName = {type = "C"}
      outEvolutionTable.vars.activationHorizonInterval = {type = "C"}
      outEvolutionTable.vars.horizon0 = {type = "N"}
      outEvolutionTable.vars.hasChildren = {type = "C"}
      outEvolutionTable.vars.targetDefinition = {type = "C"}
      outEvolutionTable.vars.targetType = {type = "C"}
      outEvolutionTable.vars.activationHorizon = {type = "N"}
      outEvolutionTable.vars.relativeValue = {type = "N"}
      outEvolutionTable.vars.relativePct = {type = "N"}
      outEvolutionTable.vars.absoluteValue = {type = "N"}
      outEvolutionTable.vars.segVar1 = {type = "C"}
      outEvolutionTable.vars.segVar2 = {type = "C"}
      outEvolutionTable.vars.segVar3 = {type = "C"}
      outEvolutionTable.vars.segVar4 = {type = "C"}
      outEvolutionTable.vars.segVar5 = {type = "C"}
      outEvolutionTable.vars.segVar1Value = {type = "C"}
      outEvolutionTable.vars.segVar2Value = {type = "C"}
      outEvolutionTable.vars.segVar3Value = {type = "C"}
      outEvolutionTable.vars.segVar4Value = {type = "C"}
      outEvolutionTable.vars.segVar5Value = {type = "C"}

      --for intervalType=IRRBB, the intervals are:
      --    OVERNIGHT 1M 3M 6M 9M 12M 18M 2Y 3Y 4Y 5Y 6Y 7Y 8Y 9Y 10Y 15Y 20Y 20YEARPLUS
      --for ease of use in the output table, all intervals are converted to month
      local irrbbIntervals = {
         {name="OVERNIGHT", months=0},
         {name="MONTH_1", months=1},   {name="MONTH_3", months=3},   {name="MONTH_6", months=6},
         {name="MONTH_9", months=9},   {name="MONTH_12", months=12}, {name="MONTH_18", months=18},
         {name="YEAR_2", months=24},   {name="YEAR_3", months=36},   {name="YEAR_4", months=48},
         {name="YEAR_5", months=60},   {name="YEAR_6", months=72},   {name="YEAR_7", months=84},
         {name="YEAR_8", months=96},   {name="YEAR_9", months=108},  {name="YEAR_10", months=120},
         {name="YEAR_15", months=180}, {name="YEAR_20", months=240}, {name="YEARPLUS_20", months=241}
      }

      --loop over each row in the BEP growth spreadsheet (bepData.data)
      local segVars = {}
      local segVarMap = {}
      for _, value in ipairs(result) do
         for jsonKey, jsonValue in pairs(value) do
            if jsonKey == "bepData" then

               local bepData = json:decode(jsonValue)
               for _, bepRow in ipairs(bepData.data) do

                  local row = {}
                  row.bepKey = value.key
                  row.bepName = value.name
                  row.segment = bepRow.segment
                  row.level = bepRow.level
                  row.hierarchy = json:encode(bepRow.hierarchy)
                  row.key = bepRow.key
                  row.segmentFilter = bepRow.segmentFilter
                  row.targetVar = bepRow.targetVar
                  row.intervalType = value.intervalType
                  row.horizon0 = bepRow.horizon0
                  row.hasChildren = tostring(bepRow.hasChildren)
                  row.targetDefinition = bepRow.targetDefinition or bepRow.growthInputParam
                  row.targetType = bepRow.targetType or bepRow.growthInputType

                  -- set the segVar variables if this is the top-level (level=0) of a new target variable
                  if bepRow["level"] == 0 then
                     segVars = {}
                     segVarMap = {}
                     local i=1
                     while bepRow["segVar"..i] do
                        segVars[i] = bepRow["segVar"..i]
                        segVarMap[segVars[i]]="segVar"..i
                        i = i + 1
                     end
                  end
                  for i, segVar in ipairs(segVars) do
                     row["segVar"..i] = segVar
                  end

                  --convert the JSON segment filter to a SAS code segment filter
                  if bepRow.segmentFilter then
                     row.segmentFilterSas = ""
                     local segmentFilterJson = json:decode(bepRow.segmentFilter)
                     for segName, segValue in pairs(segmentFilterJson) do
                        segName = string.upper(segName)
                        row.segmentFilterSas = row.segmentFilterSas..segName.." eq '"..segValue.."' and "
                        if segVarMap[segName] then
                           row[segVarMap[segName].."Value"] = segValue
                        end
                     end
                     row.segmentFilterSas = string.gsub(row.segmentFilterSas, " and $", "")
                  end

                  --activationHorizon is reported as MONTH for all horizons if intervalType=IRRBB.
                  row.activationHorizonInterval = value.intervalType
                  if value.intervalType == "IRRBB" then
                     row.activationHorizonInterval = "MONTH"
                  end

                  --if this BEP row is a leaf-level row, output a row for each horizon column (MONTH_1, MONTH_2, ...)
                  --otherwise, just output 1 row for this BEP row
                  if bepRow.hasChildren == false then

                     local intervalCount = value.intervalCount
                     if value.intervalType == "IRRBB" then
                        intervalCount = #irrbbIntervals-1
                     end

                     local absoluteValue = bepRow.horizon0  --running tracker of calculated value at each horizon for this segment
                     for h=0, intervalCount do

                        local horizonRow = {}
                        local horizonRow = clone(row)
                        horizonRow.horizon0 = bepRow.horizon0

                        if value.intervalType == "IRRBB" then
                           horizonRow.intervalName = irrbbIntervals[h+1].name
                           horizonRow.activationHorizon=irrbbIntervals[h+1].months
                        else
                           horizonRow.intervalName = value.intervalType.."_"..h
                           horizonRow.activationHorizon=h
                        end

                        -- only calculate relativeValue/relativePct/absoluteValue if this horizon has a specified value
                        local rowHorizonValue = tonumber(bepRow[horizonRow.intervalName])
                        if rowHorizonValue then

                           --calculate relativeValue and relativePct
                           if row.targetType == "PREVIOUS" or row.targetType == "FIXED" then
                              horizonRow.relativeValue = rowHorizonValue - absoluteValue
                              horizonRow.relativePct = horizonRow.relativeValue / absoluteValue
                           else
                              if row.targetDefinition == "PERCENTAGE" then
                                 horizonRow.relativePct = rowHorizonValue
                                 horizonRow.relativeValue = absoluteValue * horizonRow.relativePct
                              else
                                 horizonRow.relativeValue = rowHorizonValue
                                 horizonRow.relativePct = horizonRow.relativeValue / absoluteValue
                              end
                           end

                           --calculate absoluteValue
                           absoluteValue = absoluteValue + horizonRow.relativeValue
                           horizonRow.absoluteValue = absoluteValue

                        end

                        table.insert(outEvolutionTable,horizonRow)

                     end --end if bepRow does not have children (is a leaf-level row)
                  else
                     table.insert(outEvolutionTable,row)
                  end

               end --end loop over bepData.data

               --Write the outputEvolution SAS dataset
               if #outEvolutionTable == 0 then
                  outEvolutionTable[1] = {key = -1}
                  sas.write_ds(outEvolutionTable, outputEvolution)
                  sas.submit([[data @outputEvolution@ ; set @outputEvolution@ (obs = 0); run;]])
               else
                  sas.write_ds(outEvolutionTable, outputEvolution)
               end

            end--end if key is bepData
         end --end loop over response JSON keys
      end --end loop over result table
   end --end if outputEvolution table requested

end
M.coreRestBusinessEvolution = coreRestBusinessEvolution

-----------------------------------------

-----------------------------
-- Allocation Schemes
-----------------------------
-- Attributes of the Cirrus Core allocationSchemes custom object
coreSchemaAllocationScheme = function()

   -- Define Standard Core Attributes
   local schema = coreObjectAttr()

   -- Add Custom Fields for the AllocationScheme object
   schema.vars.defaultAllocationMethod = {type = "C"}
   schema.vars.defaultAllocationType = {type = "C"}
   schema.vars.targetVariable = {type = "C"}
   schema.vars.defaultModelParameters = {type = "C", length = 32000}
   schema.vars.statusCd = {type = "C"}
   schema.vars.bepDataKey = {type = "C"}
   schema.vars.defaultModelDataKey = {type = "C"}
   schema.vars.modelDataKey = {type = "C"}
   schema.vars.cycleDataKey = {type = "C"}
   schema.vars.analysisRunDataKey = {type = "C"}

   -- Return schema
   return schema
end

-- Parser for riskCirrusObjects/objects/allocationSchemes
coreRestAllocationScheme = function(filename, outputSummary, allocSchemeLinkTypesDs, outputWeights)

   -- Parse JSON
   local jsonTable = parseJSONFile(filename)

   -- Define allocationSchemes Attributes
   local schema = coreSchemaAllocationScheme()

   -- Process all items - allocationSchemes outputSummary table info
   local result = coreProcessItems(schema, jsonTable)


   if #result > 0 then

      -- If link type keys are given, add the linked object keys to the outputSummary table
      if allocSchemeLinkTypesDs ~= nil then

         local linkTypeId_key_map = {}
         for _, inputRow in ipairs(sas.read_ds(allocSchemeLinkTypesDs)) do
            linkTypeId_key_map[inputRow.objectid] = inputRow.key
         end

         for _, item in ipairs(result) do
            for jsonKey, jsonValue in pairs(item) do
               if jsonKey == "objectLinks" then
                  for _, link in ipairs(json:decode(jsonValue)) do
                     if link.linkType == linkTypeId_key_map.allocationScheme_bep then
                        item.bepDataKey = link.businessObject2
                     elseif link.linkType == linkTypeId_key_map.allocationScheme_defaultModel then
                        item.defaultModelDataKey = link.businessObject2
                     elseif link.linkType == linkTypeId_key_map.allocationScheme_model then
                        item.modelDataKey = link.businessObject2
                     elseif link.linkType == linkTypeId_key_map.allocationScheme_cycle then
                        item.cycleDataKey = link.businessObject2
                     elseif link.linkType == linkTypeId_key_map.allocationScheme_analysisRun then
                        item.analysisRunDataKey = link.businessObject2
                     end
                  end
               end
            end
         end
      end

      sas.write_ds(result, outputSummary)
   else
      result[1] = {key = -1}
      sas.write_ds(result, outputSummary)
      sas.submit([[data @outputSummary@ ; set @outputSummary@ (obs = 0); run;]])
   end

   -- Create the output allocation scheme details table, if requested
   if outputWeights ~= nil then

      -- Lua table with the weightsTable data --
      local outWeightsTable = {}

      -- Loop through the Lua table with all the items --
      for key, value in pairs(result) do
         for result_key, result_value in pairs(value) do
            -- The configSet data is stored in the allocationData property as a JSON string --
            if result_key == "allocationData" and type(result_value) == "string" then
               -- Transform the JSON string in Lua table --
               local tdResult = json:decode(result_value)

               -- Make sure we process "meta" first
               -- Loop through the allocationData information --
               for td_key, td_value in pairs(tdResult) do
                  if td_key == "meta" then
                     -- Table JSON Object --
                     for table_key, table_value in pairs(td_value) do
                        if table_key == "columns" then
                           local vars = {}
                           -- Columns JSON Array --
                           for column_key, column_value in pairs(table_value) do
                              local meta = {}
                              local var_id
                              -- Columns JSON Object --
                              for column_meta_key, column_meta_value in pairs(column_value) do
                                 -- Process the column id --
                                 if column_meta_key == "options" then
                                    for var_key, var_value in pairs(column_meta_value) do
                                       if var_key == "id" then
                                          var_id = var_value
                                       end
                                    end
                                 end
                                 -- Process the metadata information --
                                 if column_meta_key == "cellOptions" then
                                    for meta_key, meta_value in pairs(column_meta_value) do
                                       if meta_key == "cellType" then
                                          if meta_value == "number" then
                                             meta["type"] = "N"
                                          end
                                       end

                                       if meta_key == "size" then
                                          meta["length"] = meta_value
                                       end
                                    end
                                 end
                                 if meta["type"] == nil or meta["type"] == "" then
                                    meta["type"] = "C"
                                 end
                              end

                              -- Column type overrides
                              -- NOTE: Processing the incoming metadata information may lead to errors when handling dates and timestamps, as they should be treated as characters rather than numbers
                              if var_id == "reporting_dt" then
                                 meta["type"] = "C"
                              end

                              -- Add the metadata to the vars table --
                              vars[var_id] = meta
                           end
                           -- After processing all the columns add the vars to the table output --
                           outWeightsTable["vars"] = vars

                           -- Add custom columns for allocation scheme details table
                           outWeightsTable.vars.allocationSchemeKey = result.vars.key
                           outWeightsTable.vars.allocationSchemeName = result.vars.name
                           outWeightsTable.vars.segmentFilterSas = {type = "C", length = 32000}

                        end
                     end

                  end -- end if td_key == "meta" ..
               end -- end for td_key, td_value in pairs(tdResult) ..

               -- Loop through the allocationData information --
               for td_key, td_value in pairs(tdResult) do
                  if td_key == "data" then
                     local has_items = 0
                     for row_key, row_value in pairs(td_value) do
                        if row_key == "items" then
                           has_items = 1
                        end
                     end
                     if has_items == 1 then
                        for row_key, row_value in pairs(td_value) do
                           if row_key == "items" then
                              -- The data value is a JSON Array (rows) of JSON objects (data) --
                              for item_key, item_value in pairs(row_value) do
                                 local row_data = {}
                                 -- Lua table to store the item data --
                                 for data_key, data_value in pairs(item_value) do
                                    -- Store each variable data in the row_data table --
                                    row_data[data_key] = data_value

                                    --convert the JSON segment filter to a SAS code segment filter
                                    if data_key == "segmentFilter" then
                                       row_data.segmentFilterSas = ""
                                       local segmentFilterJson = json:decode(row_data.segmentFilter)
                                       for segName, segValue in pairs(segmentFilterJson) do
                                          row_data.segmentFilterSas = row_data.segmentFilterSas..segName.." eq '"..segValue.."' and "
                                       end
                                       row_data.segmentFilterSas = string.gsub(row_data.segmentFilterSas, " and $", "")
                                    end
                                 end

                                 row_data.allocationSchemeKey = value.key
                                 row_data.allocationSchemeName = value.name

                                 -- Insert the row with data in the final table --
                                 table.insert(outWeightsTable, row_data)
                              end
                           end
                        end
                     else
                        -- The data value is a JSON Array (rows) of JSON objects (data) --
                        for item_key, item_value in pairs(td_value) do
                           local row_data = {}
                           -- Lua table to store the item data --
                           for data_key, data_value in pairs(item_value) do
                              -- Store each variable data in the row_data table --
                              row_data[data_key] = data_value

                              --convert the JSON segment filter to a SAS code segment filter
                              if data_key == "segmentFilter" then
                                 row_data.segmentFilterSas = ""
                                 local segmentFilterJson = json:decode(row_data.segmentFilter)
                                 for segName, segValue in pairs(segmentFilterJson) do
                                    row_data.segmentFilterSas = row_data.segmentFilterSas..segName.." eq '"..segValue.."' and "
                                 end
                                 row_data.segmentFilterSas = string.gsub(row_data.segmentFilterSas, " and $", "")
                              end

                           end

                           row_data.allocationSchemeKey = value.key
                           row_data.allocationSchemeName = value.name

                           -- Insert the row with data in the final table --
                           table.insert(outWeightsTable, row_data)
                        end
                     end

                     -- Process boolean attributes
                     outWeightsTable = processBoolean(outWeightsTable)

                     -- print("Output info: " .. table.tostring(outWeightsTable))
                     sas.write_ds(outWeightsTable, outputWeights)

                  end -- end if td_key == "data" ..

               end -- end for td_key, td_value in pairs(tdResult) ..
            end -- end if result_key == "allocationData" ..
         end -- end for result_key, result_value in pairs(value) ..
      end -- end for key, value in pairs(result) ..
   end --end if outputWeights table requested

end
M.coreRestAllocationScheme = coreRestAllocationScheme


-----------------------------
-- Scripts
-----------------------------
-- Return standard Core Script structure
coreSchemaScript = function()

   -- Define Standard Core Attributes
   local schema = coreObjectAttr()

   -- Add Custom Fields for the analysisRun object
   schema.vars.key = {type = "C"}
   schema.vars.engineCd = {type = "C"}
   schema.vars.typeCd = {type = "C"}
   schema.vars.statusCd = {type = "C"}
   schema.vars.codeEditor = {type = "C", length = 32000}

   return schema
end

-- Parser for objects/scripts/<object_uri>/<object_key>
coreRestScripts = function(filename, output, output_data)

   -- Parse JSON
   local jsonTable = parseJSONFile(filename)

   -- Define Script Attributes
   local schema = coreSchemaScript()

   -- Process all items - analysis run table info
   local result = coreProcessItems(schema, jsonTable, output)
   -- print("result: " .. table.tostring(result))

  local confResult = {vars = {}}
     confResult.vars.scriptKey = result.vars.key
     confResult.vars.language = result.vars.name
     confResult.vars.code = {type = "C",  length = 32000}

   -- Create the output parameters table, if requested
   if jsonTable ~= nil then
      for key, value in ipairs(result) do
         for jsonKey, jsonValue in pairs(value) do
            -- The resolved parameters are stored in the default property as a JSON string --
            if jsonKey == "codeEditor" and type(jsonValue) == "string" then
               -- Transform the JSON string in Lua table --
               local defaultResult = json:decode(jsonValue)
               -- print("defaultResult info: " .. table.tostring(defaultResult))
               local row = {}
               row.scriptKey = value.key
               row.language = defaultResult.language
               row.code = defaultResult.value

               -- Insert the row with data in the final table --
               table.insert(confResult,row)
            end
         end
      end
   end
   if #confResult == 0 then
      -- Make sure there is at least one record
      confResult[1] = {key = -1}
      -- Write table
      sas.write_ds(confResult, output_data)
      -- Now remove the empty record
      sas.submit([[data @output_data@ ; set @output_data@ (obs = 0); run;]])
   else
      -- Write output_data
      sas.write_ds(confResult, output_data)
   end

end
M.coreRestScripts = coreRestScripts


-----------------------------
-- Models
-----------------------------
-- Attributes of the Cirrus Core models custom object
coreSchemaModel = function()

   -- Define Standard Core Attributes
   local schema = coreObjectAttr()

   -- Add Custom Fields for the AllocationScheme object
   schema.vars.engineCd = {type = "C"}
   schema.vars.engineModelUri = {type = "C"}

   -- execTypeCd=ANA, Engine=SAS code editor
   schema.vars.modelCode = {type = "C"}

   -- execTypeCd=ANA, Engine=RE code editors
   schema.vars.preModelCode = {type = "C"}
   schema.vars.preReActionsCode = {type = "C"}
   schema.vars.postReActionsCode = {type = "C"}
   schema.vars.postModelCode = {type = "C"}

   --execTypeCd=ALL (Allocation model run type) code editors
   schema.vars.genCode = {type = "C"}
   schema.vars.postGenCode = {type = "C"}
   schema.vars.postRiskCode = {type = "C"}

   schema.vars.version = {type = "C"}
   schema.vars.modelStatus = {type = "C"}
   schema.vars.statusCd = {type = "C"}
   schema.vars.execTypeCd = {type = "C"}

   -- Return schema
   return schema
end

-- Parser for riskCirrusObjects/objects/models
coreRestModel = function(filename, output, inDsFileRefs)

   -- Parse JSON
   local jsonTable = parseJSONFile(filename)

   -- Define Model Attributes
   local schema = coreSchemaModel()

   -- Process all items - model table info
   local result = coreProcessItems(schema, jsonTable, output)

   if inDsFileRefs ~= nil then
      -- Loop through the list
      for i, inputRow in ipairs(sas.read_ds(inDsFileRefs)) do
         if inputRow.fref ~= nil then
            if #result == 1 and result[1][inputRow.codeeditorfield] ~= nil then
               -- Assign fileref
               local fileref = sasxx.new(inputRow.fref)
               -- Open fileref for writing
               local f = fileref:open("wb")
               -- Write the code (convert tabs to spaces)
               f:write(detab(json:decode(result[1][inputRow.codeeditorfield]).value))
               -- Close file
               f:close()
               -- Deassign the fileref
               fileref:deassign()
            end
         end
      end
   end

end
M.coreRestModel = coreRestModel

return M