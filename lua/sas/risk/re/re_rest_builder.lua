--[[
/* Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA */

/*!
\file    re_rest_builder.lua

\brief   Risk Engine integration

\ingroup commonAnalytics

\author  SAS Institute Inc.

\date    2022

\details Risk Engine integration

*/

]]

local json_utils = require 'sas.risk.cirrus.core_rest_parser'
local json = require 'sas.risk.utils.json'
local fileutils = require 'sas.risk.utils.fileutils'

json.strictTypes = true

local M = {}

saveEnvironmentNodeJson = function(saveEnvInputs)
   local saveEnvNodeJson = {}
   saveEnvNodeJson.category="result"
   saveEnvNodeJson.categoryName="Output"
   saveEnvNodeJson.execution={results={}}
   saveEnvNodeJson.id="saveEnvironment"
   saveEnvNodeJson.label="Save Environment"
   saveEnvNodeJson.type="saveEnvironment"
   saveEnvNodeJson.typeName="Save Environment"

   saveEnvNodeJson.outputOptions={}
   saveEnvNodeJson.outputOptions["save"] = saveEnvInputs.reSaveTables ~= "" and saveEnvInputs.reSaveTables or "false"
   saveEnvNodeJson.outputOptions["promote"] = saveEnvInputs.rePromoteTables ~= "" and saveEnvInputs.rePromoteTables or "true"
   if (saveEnvInputs.rePromoteTablesLifetime ~= "") then
      saveEnvNodeJson.outputOptions["lifetime"] = saveEnvInputs.rePromoteTablesLifetime
   end

   saveEnvNodeJson.register = saveEnvInputs.reRegisterEnv ~= "" and saveEnvInputs.reRegisterEnv or "false"
   --If we're registering the Risk Environment, store it in the default RE location:
   --   Path: /Products/SAS Risk Engine/Projects/<project>/Risk Pipelines/<pipeline>
   --   Name: <pipeline>_<project>_env.rskenv
   if (saveEnvNodeJson.register ~= "") then
      saveEnvNodeJson.registrationPath = nil
   end

   saveEnvNodeJson.advancedOptionsEnabled=false
   if (saveEnvInputs.reAdvancedOptions ~= "") then
      saveEnvNodeJson.advancedOptionsEnabled=true
      saveEnvNodeJson.advancedOptions = advancedOptionsJson(saveEnvInputs.reAdvancedOptions)
   end

   return saveEnvNodeJson
end

casTableJson = function(inputCaslib, casTable)
   local casTableArr = string.split(casTable, '.')
   local casTableJson = {}
   casTableJson.caslibName=casTableArr[2] and casTableArr[1] or inputCaslib
   casTableJson.tableName=casTableArr[2] and casTableArr[2] or casTableArr[1]
   casTableJson.version=1
   return casTableJson
end

blankExecutionJson = function()
   local blankExecJson = {}
   blankExecJson.canceled=false
   blankExecJson.results={}
   blankExecJson.warningCount=0
   return blankExecJson
end

customCodeNodeJson = function(nodeId, nodeCodeFileRef, nodeCodeLanguage)
   local codeNodeJson = {}
   codeNodeJson['version'] = 1
   codeNodeJson['execution'] = blankExecutionJson()
   codeNodeJson['id'] = nodeId
   codeNodeJson['category'] = "action"
   codeNodeJson['categoryName'] = "Actions"
   codeNodeJson['type'] = "customCode"
   codeNodeJson['typeName'] = "Execute Custom Code"
   codeNodeJson['label'] = "Execute Custom Code"
   codeNodeJson['code'] = string.gsub(fileutils.read_fileref(nodeCodeFileRef), "\t", "   ")
   if nodeCodeLanguage then
      codeNodeJson['language'] = nodeCodeLanguage ~= "" and string.lower(nodeCodeLanguage) or "sas"
   end
   return codeNodeJson
end

advancedOptionsJson = function(advancedOptions)
   local advancedOptionsJson = {}
   local keyValueSeparator, optionName, optionValue
   for _, advOpt in pairs(string.split(advancedOptions,"|")) do
      keyValueSeparator = string.find(advOpt, ":", 1)
      optionName = string.sub(advOpt, 1, keyValueSeparator-1)
      optionName = optionName:gsub("^%s*(.-)%s*$", "%1")
      optionValue = string.sub(advOpt, keyValueSeparator+1)
      optionValue = optionValue:gsub("^%s*(.-)%s*$", "%1")
      table.insert(advancedOptionsJson, {name=optionName, value=optionValue})
   end
   return advancedOptionsJson
end

writeJsonToSasDs = function(rawJsonText, rawJsonOutDs, rawJsonOutDsColName, maxLength)

   local raw_json_text_table = { vars = {}}
   raw_json_text_table.vars[rawJsonOutDsColName] = {type = "C", length = maxLength}

   for i=1,#rawJsonText/maxLength+1 do
      local row = {}
      row[rawJsonOutDsColName]=string.sub(rawJsonText,maxLength*(i-1)+1,maxLength*i)
      table.insert(raw_json_text_table, row)
   end

   sas.write_ds(raw_json_text_table, rawJsonOutDs)

end

toboolean = function(str)
   local bool = false
   if str == "true" then
      bool = true
   end
   return bool
end


-- Main function to build new pipeline JSON from existing pipeline JSON
buildPipelineBody = function(pipelineJsonFile
                            , newPipelineName
                            , actionNodeInputDs
                            , dataNodeInputDs
                            , configurationInputDs
                            , saveEnvironment
                            , rawJsonOutDs
                            , rawJsonOutDsCol
                            , rawJsonOutDsMaxLength
                            , debug)

   debug = toboolean(debug)
   ------------------------------
   -- Read in the input tables --
   ------------------------------

   --Action node modifiers
   local queryNodeInputs = {}
   local evaluatePortfolioInputs = {}
   local scoreCounterpartiesInputs = {}
   local saveEnvInputs = {}
   local coreCustomCodeNodeInputs = {}
   local userCustomCodeNodeInputs = {}
   for i, inputRow in ipairs(sas.read_ds(actionNodeInputDs)) do
      if (inputRow.type == "query") then
         queryNodeInputs[inputRow.parameter]=inputRow.value
      elseif (inputRow.type == "evaluatePortfolio") then
         evaluatePortfolioInputs[inputRow.parameter]=inputRow.value
      elseif (inputRow.type == "scoreCounterparties") then
         scoreCounterpartiesInputs[inputRow.parameter]=inputRow.value
      elseif (inputRow.type == "saveEnvironment") then
         saveEnvInputs[inputRow.parameter]=inputRow.value
      elseif (inputRow.type == "customCode") then
         if (inputRow.parameter == "rePreCode" or inputRow.parameter == "rePostCode"
            or inputRow.parameter == "rePreCodeLanguage" or inputRow.parameter == "rePostCodeLanguage") then
            coreCustomCodeNodeInputs[inputRow.parameter]=inputRow.value
         else
            userCustomCodeNodeInputs[inputRow.parameter]=inputRow.value
         end
      end
   end

   if (debug) then
      print(table.tostring(queryNodeInputs))
      print(table.tostring(evaluatePortfolioInputs))
      print(table.tostring(scoreCounterpartiesInputs))
      print(table.tostring(saveEnvInputs))
      print(table.tostring(coreCustomCodeNodeInputs))
      print(table.tostring(userCustomCodeNodeInputs))
   end

   --Data node modifiers
   local portfolioDataNodeTableInputs = {}
   local marketDataNodeTableInputs = {}
   local marketDataNodeInputs = {}
   for i, inputRow in ipairs(sas.read_ds(dataNodeInputDs)) do
      if (inputRow.type == "portfolioDataTable") then
         portfolioDataNodeTableInputs[inputRow.parameter]=inputRow.value
      elseif (inputRow.type == "marketDataTable") then
         marketDataNodeTableInputs[inputRow.parameter]=inputRow.value
      elseif (inputRow.type == "marketData") then
         marketDataNodeInputs[inputRow.parameter]=inputRow.value
      end
   end

   if (debug) then
      print(table.tostring(portfolioDataNodeTableInputs))
      print(table.tostring(marketDataNodeTableInputs))
      print(table.tostring(marketDataNodeInputs))
   end

   --Configuration modifiers
   local configurationInputs = {}
   local functionSetInputs = {}
   local haveFunctionSets = false
   local pmxInputs = {}
   local cashflowInputs = {}
   for i, inputRow in ipairs(sas.read_ds(configurationInputDs)) do
      if (inputRow.type == "configuration") then
         configurationInputs[inputRow.parameter]=inputRow.value
      elseif (inputRow.type == "functionSet") then
         functionSetInputs[inputRow.parameter]=inputRow.value
         haveFunctionSets = true
      elseif (inputRow.type == "pmx") then
         pmxInputs[inputRow.parameter]=inputRow.value
      elseif (inputRow.type == "cashflow") then
         cashflowInputs[inputRow.parameter]=inputRow.value
      end
   end

   if (debug) then
      print(table.tostring(configurationInputs))
      print(table.tostring(functionSetInputs))
      print(table.tostring(pmxInputs))
      print(table.tostring(cashflowInputs))
   end

   ----------------------------------------------------------
   -- Read the pipeline JSON file to a table and modify it --
   ----------------------------------------------------------

   --read the project's default pipeline JSON to a table
   local pipelineTable = json_utils.parseJSONFile(pipelineJsonFile)
   local hasSaveEnvirionmentNode = false

   --iterate over the pipeline table, modifying as needed for the new pipeline
   for pipelineKey, pipelineVal in pairs(pipelineTable) do

      --Modify action nodes
      if (pipelineKey == "actionNodes") then

         for i, actionNode in ipairs(pipelineVal) do

            actionNode["execution"] = blankExecutionJson()

            --Modify the query action node
            if (actionNode.type == "query") then

               if (queryNodeInputs.reHorizons ~= "") then
                  actionNode["horizons"] = {}
                  for i, horizons in pairs(string.split(queryNodeInputs.reHorizons,"|")) do
                     if string.upper(horizons) ~= "ALL" then
                        table.insert(actionNode["horizons"], {horizons=horizons})
                     end
                  end
               end
               actionNode["outputTypes"] = queryNodeInputs.reOutputTables ~= "" and string.split(queryNodeInputs.reOutputTables,", ") or actionNode["outputTypes"]
               actionNode["riskMethods"] = queryNodeInputs.reRiskMethods ~= "" and string.split(queryNodeInputs.reRiskMethods,", ") or actionNode["riskMethods"]
               actionNode["filters"] = {}
               if (queryNodeInputs.reFilter ~= "") then
                  for _, filter in pairs(string.split(queryNodeInputs.reFilter,"|")) do
                     table.insert(actionNode["filters"], {whereClause=filter})
                  end
               end

               if (string.upper(queryNodeInputs.reOutputVariables) == "") then
                  actionNode["allOutputVariables"] = true
                  actionNode["outputVariables"] = string.split(queryNodeInputs.reOutputVariables,", ")
               else
                  actionNode["allOutputVariables"] = false
                  actionNode["outputVariables"] = string.split(queryNodeInputs.reOutputVariables,", ")
               end

               actionNode["queryType"] = queryNodeInputs.reQueryType ~= "" and string.lower(queryNodeInputs.reQueryType) or actionNode["queryType"]

               --if noaggregate is being used, don't specify any aggregationLevels in order to avoid warnings
               if (actionNode["queryType"] == "noaggregate") then
                  actionNode["aggregationLevels"] = {}
               elseif (queryNodeInputs.reAggregationLevels ~= "") then
                  actionNode["aggregationLevels"] = {}
                  for _, ccVars in pairs(string.split(queryNodeInputs.reAggregationLevels,"|")) do
                     if (ccVars == "_TOP_LEVEL_") then
                        table.insert(actionNode["aggregationLevels"], {crossClassVariables={}})
                     else
                        table.insert(actionNode["aggregationLevels"], {crossClassVariables=string.split(ccVars,", ")})
                     end
                  end
               end

               actionNode["statistics"] = queryNodeInputs.reStatistics ~= "" and string.split(queryNodeInputs.reStatistics,", ") or actionNode["statistics"]

               if (queryNodeInputs.reAdvancedOptions ~= "") then
                  actionNode["advancedOptionsEnabled"] = true
                  actionNode["advancedOptions"] = advancedOptionsJson(queryNodeInputs.reAdvancedOptions)
               end

            --Modify the evaluatePortfolio action node
            elseif (actionNode.type == "evaluatePortfolio") then

               actionNode["methodTrace"] = evaluatePortfolioInputs.reMethodTrace ~= "" and evaluatePortfolioInputs.reMethodTrace or actionNode["methodTrace"]

               local scenarioDrawCount = tonumber(evaluatePortfolioInputs.reScenarioDrawCount) or actionNode["scenarioDrawCount"]
               actionNode["scenarioDrawCount"] = (scenarioDrawCount>0) and scenarioDrawCount or 0

               if (evaluatePortfolioInputs.reAdvancedOptions ~= "") then
                  actionNode["advancedOptionsEnabled"] = true
                  actionNode["advancedOptions"] = advancedOptionsJson(evaluatePortfolioInputs.reAdvancedOptions)
               end

            --Modify the scoreCounterparties action node
            elseif (actionNode.type == "scoreCounterparties") then

               actionNode["methodTrace"] = scoreCounterpartiesInputs.reMethodTrace ~= "" and scoreCounterpartiesInputs.reMethodTrace or actionNode["methodTrace"]

               local scenarioDrawCount = tonumber(scoreCounterpartiesInputs.reScenarioDrawCount) or actionNode["scenarioDrawCount"]
               actionNode["scenarioDrawCount"] = (scenarioDrawCount>0) and scenarioDrawCount or 0

               if (scoreCounterpartiesInputs.reAdvancedOptions ~= "") then
                  actionNode["advancedOptionsEnabled"] = true
                  actionNode["advancedOptions"] = advancedOptionsJson(scoreCounterpartiesInputs.reAdvancedOptions)
               end

            --Modify the saveEnvironment action node
            elseif (actionNode.type == "saveEnvironment") then
               hasSaveEnvirionmentNode = true

               actionNode["outputOptions"] = actionNode["outputOptions"] or {}
               actionNode.outputOptions["save"] = saveEnvInputs.reSaveTables ~= "" and saveEnvInputs.reSaveTables or actionNode.outputOptions["save"]
               actionNode.outputOptions["promote"] = saveEnvInputs.rePromoteTables ~= "" and saveEnvInputs.rePromoteTables or actionNode.outputOptions["promote"]
               actionNode.outputOptions["lifetime"] = saveEnvInputs.rePromoteTablesLifetime ~= "" and tonumber(saveEnvInputs.rePromoteTablesLifetime) or actionNode.outputOptions["lifetime"]
               actionNode["register"] = saveEnvInputs.reRegisterEnv ~= "" and saveEnvInputs.reRegisterEnv or actionNode["register"]
               --If we're registering the Risk Environment, store it in the default RE location:
               --   Path: /Products/SAS Risk Engine/Projects/<project>/Risk Pipelines/<pipeline>
               --   Name: <pipeline>_<project>_env.rskenv
               if (actionNode["register"]) then
                  actionNode["registrationPath"] = nil
               end

               if (saveEnvInputs.reAdvancedOptions ~= "") then
                  actionNode["advancedOptionsEnabled"] = true
                  actionNode["advancedOptions"] = advancedOptionsJson(saveEnvInputs.reAdvancedOptions)
               end

            --Modify the custom code node.  This is separate from the special pre/post custom code nodes we add
            --In case the customer needs to inject any extra custom code they can this way
            elseif (actionNode.type == "customCode") then

               --Modify customer custom nodes - inject the customer's custom code into their custom code node with the corresponding ID
               for customCodeNodeId, customCodeFileRef in pairs(userCustomCodeNodeInputs) do
                  if (actionNode.id == customCodeNodeId) then
                     actionNode["code"] = string.gsub(fileutils.read_fileref(customCodeFileRef), "\t", "   ")
                     if (not actionNode['code']) then
                        sas.submit([[%put ERROR: Failed to read custom code from fileref: %sysfunc(pathname(@customCodeFileRef@));]])
                     end
                  end
               end

               --Modify special Core custom nodes (preCustomCodeFromCore, postCustomCodeFromCore) in case they exist in the project's pipeline we're building off of
               --if these special nodes already exist (by ID), inject the code directly into the existing node instead of into a new custom code node
               if (actionNode.id == "preCustomCodeFromCore") then
                  if (coreCustomCodeNodeInputs.rePreCode ~= "") then
                     actionNode['code'] = string.gsub(fileutils.read_fileref(coreCustomCodeNodeInputs.rePreCode), "\t", "   ")
                     coreCustomCodeNodeInputs.rePreCode = ""
                  end
               elseif (actionNode.id == "postCustomCodeFromCore") then
                  if (coreCustomCodeNodeInputs.rePostCode ~= "") then
                     actionNode['code'] = string.gsub(fileutils.read_fileref(coreCustomCodeNodeInputs.rePostCode), "\t", "   ")
                     coreCustomCodeNodeInputs.rePostCode = ""
                  end
               end

            end

         end

         --Add in the saveEnvironment action node, if needed
         if (not hasSaveEnvirionmentNode and saveEnvironment) then
            table.insert(pipelineVal, saveEnvironmentNodeJson(saveEnvInputs))
         end

      end --end if actionNodes

      --Modify data nodes
      if (pipelineKey == "dataNodes") then

         for _, dataNode in ipairs(pipelineVal) do

            --reset the execution JSON for all nodes
            dataNode.execution = blankExecutionJson()

            --Modify the portfolio data node
            if (dataNode.type == "portfolioData") then
               for inputType, inputName in pairs(portfolioDataNodeTableInputs) do
                  if inputName ~= "" then
                     dataNode[inputType.."Table"] = casTableJson(configurationInputs.inputCaslib, inputName)
                  end
               end
            end

            --Modify the market data node
            if (dataNode.type == "marketData") then

               dataNode["dataType"] = marketDataNodeInputs.reMarketDataType ~= "" and string.lower(marketDataNodeInputs.reMarketDataType) or dataNode["dataType"]

               if (dataNode["dataType"] == "combined") then
                  dataNode["combinedData"] = dataNode["combinedData"] or {}
                  dataNode.combinedData["interval"] = marketDataNodeInputs.interval ~= "" and string.lower(marketDataNodeInputs.interval) or dataNode.combinedData["interval"]
                  dataNode.combinedData["intervalAlignment"] = marketDataNodeInputs.intervalAlignment ~= "" and string.lower(marketDataNodeInputs.intervalAlignment) or dataNode.combinedData["intervalAlignment"]
                  if next(marketDataNodeTableInputs) then
                     dataNode.combinedData["economicDataTables"] = {}
                  end
               end

               for inputType, inputName in pairs(marketDataNodeTableInputs) do
                  if inputName ~= "" then
                     if (dataNode["dataType"] == "combined") then
                        table.insert(dataNode.combinedData["economicDataTables"], casTableJson(configurationInputs.inputCaslib, inputName))
                     else
                        dataNode[inputType.."Table"] = casTableJson(configurationInputs.inputCaslib, inputName)
                     end
                  end
               end
            end

         end

      end --end if dataNodes

      --Modify pipeline configuration
      if (pipelineKey == "configuration") then

         pipelineVal.runAsOfDate = configurationInputs.asOfDate
         pipelineVal.crossClassVariables = configurationInputs.crossClassVariables ~= "" and string.split(configurationInputs.crossClassVariables,", ") or pipelineVal.crossClassVariables
         pipelineVal.currency = configurationInputs.outputCurrency ~= "" and {id=configurationInputs.outputCurrency,label=configurationInputs.outputCurrency,version=1} or pipelineVal.currency
         pipelineVal.outputCaslib = configurationInputs.outputCaslib ~= "" and configurationInputs.outputCaslib or pipelineVal.outputCaslib
         pipelineVal["outputOptions"] = pipelineVal["outputOptions"] or {}
         pipelineVal.outputOptions["save"] = configurationInputs.reSaveTables ~= "" and configurationInputs.reSaveTables or pipelineVal.outputOptions["save"]
         pipelineVal.outputOptions["promote"] = configurationInputs.rePromoteTables ~= "" and configurationInputs.rePromoteTables or pipelineVal.outputOptions["promote"]
         pipelineVal.outputOptions["lifetime"] = configurationInputs.rePromoteTablesLifetime ~= "" and tonumber(configurationInputs.rePromoteTablesLifetime) or pipelineVal.outputOptions["lifetime"]

         --Add parameter matrix mappings
         pipelineVal["riskDataObjectMappings"] = {}
         for pmxName, pmxTable in pairs(pmxInputs) do
            local pmxDataObjectMapping = {riskDataObject={name=pmxName, type="pmxData"}, table=casTableJson(configurationInputs.inputCaslib, pmxTable)}
            table.insert(pipelineVal["riskDataObjectMappings"], pmxDataObjectMapping )
         end

         --Add cashflow data mappings
         for cfName, cfTable in pairs(cashflowInputs) do
            local cfObjectMapping = {riskDataObject={name=cfName, type="cashflow"}, table=casTableJson(configurationInputs.inputCaslib, cfTable)}
            table.insert(pipelineVal["riskDataObjectMappings"], cfObjectMapping )
         end

         --Add function sets
         if (haveFunctionSets) then
            pipelineVal["functionSetOrder"] = {}
            for fsName, fsId in pairs(functionSetInputs) do
               local functionSet = {name=fsName, id=fsId, predefined=false, version=2}
               table.insert(pipelineVal["functionSetOrder"], functionSet )
            end
         end

      end -- end if configuration

   end --end loop over pipeline JSON table

   --Add pre/post execution custom code nodes (action nodes)
   --Pre-execution code goes before any action nodes
   if (coreCustomCodeNodeInputs.rePreCode ~= "") then
      table.insert(pipelineTable.actionNodes, 1, customCodeNodeJson("preCustomCodeFromCore", coreCustomCodeNodeInputs.rePreCode, coreCustomCodeNodeInputs.rePreCodeLanguage))
   end

   --Post-execution code goes after any action nodes (output nodes are considered action nodes)
   if (coreCustomCodeNodeInputs.rePostCode ~= "") then
      table.insert(pipelineTable.actionNodes, customCodeNodeJson("postCustomCodeFromCore", coreCustomCodeNodeInputs.rePostCode, coreCustomCodeNodeInputs.rePostCodeLanguage))
   end

   --Modify pipeline metadata
   pipelineTable.id = nil
   pipelineTable.execution = blankExecutionJson()
   pipelineTable.execution.state="none"
   pipelineTable.name = newPipelineName
   pipelineTable.type = 'unlocked'
   pipelineTable.isEditable = false
   pipelineTable.isRestricted = false

   ---------------------------------------------------------------
   -- Encode the new pipeline table as JSON and write to a file --
   ---------------------------------------------------------------
   local raw_json_text = json:encode(pipelineTable)
   if (debug) then
      print(table.tostring(pipelineTable))
   end
   return writeJsonToSasDs(raw_json_text, rawJsonOutDs, rawJsonOutDsCol, rawJsonOutDsMaxLength)

end
M.buildPipelineBody = buildPipelineBody

return M