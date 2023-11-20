--[[
/* Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA */

/*!
\file

\brief   Module provides functions needed for creating data partitions in PostgreSQL.
\details
   The following functions are available:
   - generatePartitions : Common function that handles data partition.
   - partionByNumRows : Helps detecting number of partitions for rows based strategy .  Default rowsPerPartition = 1000000 (1 million)
   - partionByVars : Helps detecting number of partitions for BYVARS based strategy . Defaul maxPartition = 99
   - partitionStrategy : Function that support following partition strategy :  ROWS (routes to partionByNumRows) , BYVARS (routes to partionByVars)
                        , FIXED (forced number of partition), NONE (No Partition)
   - generatePostgresSQLPartition : generatePartitions based upon DBTYPE (POSTGRESQL) routes partion creation job to this function.  Default in case of DBTYPE absence we will assume 'POSTGRESQL'

\section ..

   generatePartitions Returns 0 if partition creation if successful else nil. <br>

   <br> For PostgreSQL primary key vars must include all partition vars as well. This is required cause PostgreSQL wants to ensure primary key constraints on partitions as well  <br>


   <b>Syntax:</b> <i>(sourceLibname,sourceDSName,targetLibname,targetDSName,parameters,replace,dbType)</i>
                  <i>sourceLibname: Source LibName</i>
                  <i>sourceDSName: Source Dataset</i>
                  <i>targetLibname: Target DB Lib . Will be provided by DataService</i>
                  <i>targetDSName: Target DatasetName . This will be generated unique based on sequence from dataService</i>
                  <i>Parameters : Target DatasetName . This will be generated unique based on sequence from dataService</i>


   <b>Sample function calls : </b>
   \code

            data work.cars;
            obs=_N_;
            set sashelp.cars;
            run;

            libname rd postgres server="sas-crunchy-data-test" port=5432
            user=owner password='<pass>' database=SharedServices
            schema="public";


            Lua submission :
            local ds = require(sas.risk.cirrus.core_data_service)
            //Default parition strategy is ROWS. Per partition 1 million rows be default.
            ds.generatePartitions("work","cars","rd","carsNewRows",
                                    { partitionByVars = "type origin" ,
                                     primaryKey = "obs type origin" ,
                                     indexList="type make,origin" )


            ds.generatePartitions("work","cars","rd","carsNewRows",
                                    { partitionByVars = "type origin" ,
                                     primaryKey = "obs type origin" ,
                                    strategyType = "ROWS" ,
                                    rowsPerPartition = 100
                                    } ,true)



            ds.generatePartitions("work","cars","rd","carsNewByVars",
                                    { partitionByVars = "type origin" ,
                                        primaryKey = "obs type origin" ,
                                        strategyType = "BYVARS" ,
                                    } ,true)





            ds.generatePartitions("work","cars","rd","carsNewFixed",
                                    { partitionByVars = "type origin" ,
                                        primaryKey = "obs type origin" ,
                                        strategyType = "FIXED" ,
                                        partitionCount= 5
                                    } ,true)




            ds.generatePartitions("work","cars","rd","carsNewNone",
                                    { partitionByVars = "type origin" ,
                                        primaryKey = "obs type origin" ,
                                        strategyType = "NONE" ,
                                    } ,true)


   \endcode


\ingroup commonAnalytics

\author  SAS Institute Inc.

\date    2022

*/

--]]
local json_utils = require 'sas.risk.cirrus.core_rest_parser'

local M = {}

---Function trim spaces from right and left.
---@param s string
---@return string
M.trimAll = function(s)
    return (s:gsub("^%s+", ''):gsub("%s+$", ''))
end

---Function counts number of rows in a give sas dataset.
---@param ds string : <libName.DataSetName>
---@return number : <Number of rows>
M.sasDSRowCount = function(ds)
    if sas.exists(ds) then
        local dsid = sas.open(ds)
        local cnt = sas.nobs(dsid)
        sas.close(dsid)
        if cnt < 0 then
            sas.print("%1zNot able to find number of rows. Not a Base SAS table")
        else
            sas.print("%3zNumber of Rows are:" .. cnt)

        end
        return cnt
    else
        sas.print("%1zCannot find Dataset:" .. ds)
        return nil
    end
end

--- Function uses sas.print for all args and then aborts SAS session.
---@param ... string : Takes any number of strings to print using sas.print.
M.printWithAbort = function(...)
    local arg = { ... }
    for i, v in ipairs(arg) do
        sas.print(v)
    end
    sas.submit("data _null_;abort return;run;")
end


---sasDeleteTable deletes table. Needed if we want to clean up target table before creating new with same name.
--Does not complain if dataset not found.
---@param fullDSName string : <libName.DataSetName>
---@return number 0 or nil
M.sasDeleteTable = function(fullDSName, sqlOptions)
    sqlOptions = sqlOptions or ''
    if sas.exists(fullDSName) then
        local code = [[
           proc sql @sqlOptions@;
           drop table @fullDSName@;
           quit;
       ]]
        rc = sas.submit(code, { fullDSName = fullDSName, sqlOptions = sqlOptions })
        if rc ~= 0 then
            sas.print("%1zFailed in deleting dataset." .. fullDSName)
            return nil
        end

        return 0

    else
        --If we dont find dataset we just print it as NOTE
        sas.print("%2zCannot find Dataset nothing to delete:" .. string.format("%s", fullDSName))
        return 0
    end

end

---sasDeleteView deletes a SAS view.
--Does not complain if view not found. Same can be achieved with NOERRORSTOP but here we return status code for caller to take action.
-- If we user NOERRORSTOP return code would always be 0.
---@param fullViewName string :  <libName.Viewname>
---@param sqlOptions string :  SQL options
---@return number
M.sasDeleteView = function(fullViewName, sqlOptions)
    sqlOptions = sqlOptions or ''
    local code = [[
           proc sql @sqlOptions@;
           drop view @fullViewName@;
           quit;
       ]]

    local rc = sas.submit(code, { fullViewName = fullViewName, sqlOptions = sqlOptions }, nil, 2000)

    return rc

end

---TODO
---@param uuid any
M.shortenUUID = function(uuid)

    local b16 = { ["0"] = "0000", ["1"] = "0001", ["2"] = "0010", ["3"] = "0011", ["4"] = "0100", ["5"] = "0101", ["6"] = "0110", ["7"] = "0111", ["8"] = "1000", ["9"] = "1001", ["a"] = "1010", ["b"] = "1011", ["c"] = "1100", ["d"] = "1101", ["e"] = "1110", ["f"] = "1111" }

    local b64 = {
        ["A"] = "000000", ["B"] = "000001", ["C"] = "000010", ["D"] = "000011", ["E"] = "000100", ["F"] = "000101", ["G"] = "000110", ["H"] = "000111", ["I"] = "001000", ["J"] = "001001",
        ["K"] = "001010", ["L"] = "001011", ["M"] = "001100", ["N"] = "001101", ["P"] = "001111", ["Q"] = "010000", ["R"] = "010001", ["S"] = "010010", ["T"] = "010011", ["U"] = "010100",
        ["V"] = "010101", ["W"] = "010110", ["X"] = "010111", ["Y"] = "011000", ["Z"] = "011001",
        ["a"] = "011010", ["b"] = "011011", ["c"] = "011100", ["d"] = "011101", ["e"] = "011110", ["f"] = "011111", ["g"] = "100000", ["h"] = "100001", ["i"] = "100010",
        ["j"] = "100011", ["k"] = "100011", ["l"] = "100101", ["m"] = "100110", ["n"] = "100111", ["o"] = "101000", ["p"] = "101001", ["q"] = "101010", ["r"] = "101011",
        ["s"] = "101100", ["t"] = "101101", ["u"] = "101110", ["v"] = "101111", ["w"] = "110000", ["x"] = "110001", ["y"] = "110010", ["z"] = "110011",
        ["0"] = "110100", ["1"] = "110101", ["2"] = "110110", ["3"] = "110111", ["4"] = "111000", ["5"] = "111001", ["6"] = "111010", ["7"] = "111011", ["8"] = "111100", ["9"] = "111101",
        ["+"] = "111110", ["/"] = "111111"
    }

end

---This  function will return unique name for Dataset. There is more todo her once DataService API endpoint gets ready.
---@param schema string
---@param counter number
---@param schemaLen number
---@return string
M.getUniqueDatasetName = function(schema, counter, schemaLen)
    schemaLen = schemaLen or 21
    if (schema ~= nil and counter ~= nil and counter > 0 and string.len(schema) <= schemaLen) then
        local hexCntr = string.format("%07x", counter)
        local uniqueName = schema .. '_' .. hexCntr
        sas.print("%3zUnique DataSet Name::" .. uniqueName)
        return uniqueName
    else
        sas.print("%1zNo value provided for schema or counter or schema name length is not less than or equals to" .. schemaLen)
        sas.print("%1zSchema value:" .. schema .. ". Schema Length:" .. string.len(schema))
        sas.print("%1zCounter value:" .. string.format("%s", counter))
        return nil
    end

end

---Returns number if partition for row based strategy.
---@param fullDSName string
---@param rowsPerPartition number
---@return number
M.partionByNumRows = function(fullDSName, rowsPerPartition)
    local partsCount = tonumber(string.format("%d", M.sasDSRowCount(fullDSName) / rowsPerPartition))
    sas.print("%3zNumber of partitions calculated::" .. partsCount)
    return partsCount
end

---Function calculates partition count by applying distincts byVars on fullDSName.
---@param fullDSName string  : <libName.DataSetName>
---@param partitionByVars string : space seperated list of partition vars.
---@return number
M.partitionByVars = function(fullDSName, partitionByVars)
    --Replacing comma with pipes
    --Expected partitionByVars as space seperated in dataset.
    partitionByVars = M.trimAll(partitionByVars)
    partitionByVars = string.gsub(partitionByVars, " +", ",")
    local parts_cnt = nil
    sas.print("%3zExtracting source lib from full ds name")
    slib = string.split(fullDSName, ".")
    local CASDSParameters = M.getCASDSParameters(slib[1])
    local code = [[
    proc sql noprint;
     select count(*) into : parts_cnt from (select distinct @partitionByVars@ from @fullDSName@ @CASDSParameters@);
    quit;
   ]]
    local rc = sas.submit(code)
    parts_cnt = sas.symget("parts_cnt")
    sas.print("%3zNumber of partitions calculated::" .. parts_cnt)
    --If partition count is 1 . We don't need partition.
    if parts_cnt == 1 then
        parts_cnt = 0
    end

    if rc ~= 0 then
        sas.print("%1zFailed to get partition count.")
        return nil
    end

    return parts_cnt

end

---This function handles detection partition count by routing calls appropriately.
---@param fullDSName string : <libName.DataSetName>
---@param strategy table : {strategyType = 'ROWS|BYVARS|FIXED|NONE' , rowsPerPartition = <number. Only applicable when strategyType is ROWS>
---                        , partitionByVars = <space seperated byvars. Applicable to strategyType=BYVARS,ROWS,FIXED
---                        , maxPartitions=<number> , partitionCount=<number it is required for strategyType FIXED>  }
---@return number
M.partitionStrategy = function(fullDSName, strategy)
    strategy.maxPartitions = strategy.maxPartitions or 99
    strategy.strategyType = strategy.strategyType or 'ROWS'
    local strategyMap = { ROWS = M.partionByNumRows, BYVARS = M.partitionByVars }
    local res
    if strategy.strategyType == 'FIXED' then
        res = strategy.partitionCount
    elseif strategy.strategyType == 'NONE' then
        res = 0
    elseif strategy.strategyType == 'ROWS' then
        res = strategyMap.ROWS(fullDSName, strategy.rowsPerPartition or 1000000)
    elseif strategy.strategyType == 'BYVARS' then
        res = strategyMap.BYVARS(fullDSName, strategy.partitionByVars)
    end

    if res > strategy.maxPartitions or res == nil then
        sas.print("%1zFailed to get partition count or number of partitions greater than maximum.")
        sas.print("%1zNumber of calculated partitions::" .. string.format("%d", res))
        sas.print("%1zMaximum allowed partitions:" .. strategy.maxPartitions)
        return nil
    end

    return res

end

---Function specific to postgres for data partition(hash based) , applying primary keys and indexes.
---@param sourceLibname string
---@param sourceDSName string
---@param targetLibname string
---@param targetDSName string
---@param parameters table { partitionByVars = <By vars space seperated> ,
---                          primaryKey = <vars space seperated>,
---                          indexList = "index2 vars space separated, index2 vars space separated"
---                         }
---@param schemaName string
---
M.generatePostgresSQLPartition = function(sourceLibname, sourceDSName, targetLibname, targetDSName, parameters, schemaName)
    local partitionCreateStr = ';'
    local partitionStr = ';'
    local primaryKeyStr = ';'
    local indexStr = ';'
    local dbType = ''
    local keepStmt = ''
    local sasAttribStmt = ''
    local fullSrcDS = sourceLibname .. "." .. sourceDSName
    local srcDSTemp = "set "..fullSrcDS.." ;"
    local fullTargetDS = targetLibname .. "." .. targetDSName
    --This is required for passthru in datastep
    local schemaWithTargetDS = schemaName .. '.' .. targetDSName
    local sasPartitionCode=''
    local createTablePG=''

    --get buildSelectList
    local sourceSelectList,missingList = M.buildColumnListFromSource(fullSrcDS, parameters.columnMap)
    if sourceSelectList ~= "" then
        sourceSelectList = "keep=" .. sourceSelectList
    else
        M.printWithAbort("%1zAtleast one matching column with Data Definition must be present in source data.")
    end


    if parameters.partitionCount > 0 and (parameters.partitionByVars ~= nil and parameters.partitionByVars ~= '') then
        --Trimming before we use gsub
        parameters.partitionByVars = M.trimAll(parameters.partitionByVars)
        partitionCreateStr = "partition by hash(" .. string.gsub(parameters.partitionByVars, " +", ",") .. ");"
        --partition count starts at 0 based on remainder . If you have 5 partition your remainders will 0,1,2,3,4
        for i = 0, parameters.partitionCount - 1, 1 do
            partitionStr = partitionStr .. "create table " .. schemaWithTargetDS .. "_" .. i .. " partition of " .. schemaWithTargetDS .. " for values  with (modulus " .. parameters.partitionCount .. " , remainder " .. i .. ");\n"
        end

        print(partitionStr)

    end

    if parameters.primaryKey ~= nil and parameters.primaryKey ~= '' then
        --Trimming before we use gsub
        parameters.primaryKey = M.trimAll(parameters.primaryKey)
        local _tmp_ = string.gsub(parameters.primaryKey, " +", ",")
        primaryKeyStr = "Alter table " .. schemaWithTargetDS .. " add primary key (" .. _tmp_ .. ");"

    end
    -- The value of index list can be like =Make Type, Origin : Once we split by comma we have insert comma for all spaces so that index becomes
    -- (Make,Type)  |  (Origin)
    -- Extra space in from Origin can cause value like ",Origin"
    if parameters.indexList ~= nil and parameters.indexList ~= '' then
        --Trimming before we use gsub
        parameters.indexList = M.trimAll(parameters.indexList)
        for i, v in ipairs(string.split(parameters.indexList, ",")) do
            -- trimAll would eliminate any leading and trailing space so that extra comma doesnt go in.
            local _tmp_ = "create index on " .. schemaWithTargetDS .. " (" .. string.gsub(M.trimAll(v), " +", ",") .. ");"
            indexStr = indexStr .. " " .. _tmp_

        end
        print(indexStr)
    end

    --Get parameter if the source is CAS other wise we get empty string.
    local CASDSParameters = M.getCASDSParametersNoBrackets(sourceLibname)

    --Get srcWhereStmt
    -- local srcWhereStmt=M.processDSWhereClause(parameters.srcWhereStmt)

    local srcWhereStmt = M.processDSAppendWhereClause(parameters.srcWhereStmt)

    if parameters.keepStmt ~= nil and parameters.keepStmt ~= '' then
        keepStmt = "keep=" .. parameters.keepStmt
    end

    -- First import will not have createTablePGStmt.
    if #parameters.createTablePGStmt == 0 then
        -- We add length and format only when first time import and column missing.
        if parameters.sasAttribStmt ~= nil and parameters.sasAttribStmt ~= "" and missingList ~= "" then
            print("Missing list of columns:", missingList)
            sasAttribStmt = parameters.sasAttribStmt .. "\n"
        end

        sasPartitionCode = [[
            data @fullTargetDS@ (@dbType@ @keepStmt@ DBCREATE_TABLE_OPTS = '
                     @partitionCreateStr@
                     @primaryKeyStr@
                     @indexStr@
                     @partitionStr@
                     ') ;
            @sasAttribStmt@
            set @fullSrcDS@(obs=0);
            run;
    ]]

    end


    --Second import will have createTablePGStmt set to something
    if #parameters.createTablePGStmt > 0  then
        createTablePG=table.concat(parameters.createTablePGStmt,", ")

        sasPartitionCode = [[
            proc sql;
                connect using @targetLibname@;
                execute (create table @schemaWithTargetDS@ ( @createTablePG@ ) @partitionCreateStr@ ) by @targetLibname@;
                execute (@primaryKeyStr@) by @targetLibname@;
                execute (@indexStr@) by @targetLibname@;
                execute (@partitionStr@) by @targetLibname@;
                disconnect from @targetLibname@;
            quit;

        ]]

    end


    -- local commentStr="comments on "..fullTargetDS.." is ".."'Source data for this table is "..fullSrcDS.."';"
    -- We could always load data by including attrib statement and just using dataset or CAS save (once allow unquoted names implemented).
    -- Using append logic makes it bit different but would bring more consistent tables.
    -- options sastrace=',,,d' sastraceloc=saslog;
    local sasAppendCode = [[
            proc append
                 data=@fullSrcDS@ (@srcWhereStmt@ @sourceSelectList@ @CASDSParameters@)
                 base=@fullTargetDS@ (bulkload=YES bl_options="ERRORS=0, PARALLEL=TRUE"  bl_delete_datafile=YES) force nowarn;
            run;

            data _null_;
                 if &SYSCC. = 4 then do;
                    call symput('SYSCC', '0');
                 end;
            run;

        ]]


    local codeMap = { fullTargetDS = fullTargetDS
    , fullSrcDS = fullSrcDS
    , srcDSTemp=srcDSTemp
    , partitionStr = partitionStr
    , partitionCreateStr = partitionCreateStr
    , primaryKeyStr = primaryKeyStr
    , indexStr = indexStr
    , CASDSParameters = CASDSParameters
    , srcWhereStmt = srcWhereStmt
    , dbType = dbType
    , keepStmt = keepStmt
    , sasAttribStmt = sasAttribStmt
    , sourceSelectList = sourceSelectList
    , createTablePG=createTablePG
    , schemaWithTargetDS=schemaWithTargetDS
    }

    local rc = sas.submit(sasPartitionCode,codeMap,nil, 2000)
    print("PG Partition return code is:",rc)
    if rc ~= 0 or rc == nil then
        return rc
    end

    --Not failing on error from SAS so control can go back to lua and we can fail and rollback.
    return sas.submit(sasAppendCode, codeMap , nil, 2000)


end

---Main function that will be called by DataService.
---@param sourceLibname string
---@param sourceDSName string
---@param targetLibname string
---@param targetDSName string
---@param parameters table   { partitionByVars = <By vars space seperated> ,
---                          primaryKey = <vars space seperated>,
---                          indexList = "index2 vars space separated, index2 vars space separated" ,
---                          strategyType = 'ROWS|BYVARS|FIXED|NONE' , rowsPerPartition = <number. Only applicable when strategyType is ROWS>
---                          partitionByVars = <space seperated byvars. Only Applicable to strategyType=BYVARS ,
---                          maxPartitions=number defaults to 99
---                          rowsPerPartition = defaults to 1000000
---                          }
---@param replace boolean if True will try to delete exisiting target table. Default is false.
---@param dbType string Defaults to 'POSTGRESQL'
---@param schemaName string
---@return number 0 or nil
M.generatePartitions = function(sourceLibname, sourceDSName, targetLibname, targetDSName, parameters, replace, dbType, schemaName)
    local rc = nil
    local status = nil
    local defaultDBType = 'POSTGRESQL'
    dbType = dbType or defaultDBType
    local fullSrcDS = sourceLibname .. "." .. sourceDSName
    local fullTargetDS = targetLibname .. '.' .. targetDSName
    replace = replace or false

    -- Not checking midtier already checks
    -- if sas.exists(fullSrcDS) == false then
    --     M.printWithAbort("%1zSource Dataset not found:"..fullSrcDS)
    -- end

    if replace == false and sas.exists(fullTargetDS) then
        M.printWithAbort("%1zReplace flag is false and target dataset exists.")
    end

    if replace and sas.exists(fullTargetDS) then
        rc = M.sasDeleteTable(fullTargetDS)
        if rc == nil then
            M.printWithAbort("%1zAborting.")
        end
    end

    parameters.partitionCount = M.partitionStrategy(fullSrcDS, parameters)

    if parameters.partitionCount == nil then
        M.printWithAbort("%1zAborting..")
    end

    if dbType == defaultDBType then
        rc = M.generatePostgresSQLPartition(sourceLibname, sourceDSName, targetLibname, targetDSName, parameters, schemaName)
    else
        rc = nil
    end

    --Not sure if we will ever need to ignore warning in this case
    if rc ~= 0 or rc == nil then
        sas.print("%1zPartition Creation failed and may be incomplete. Rolling back if applicable.")
        --If we are here most likely table is created but with errors creation
        M.sasDeleteTable(fullTargetDS, 'NOERRORSTOP')
        return M.printWithAbort("%1zAborting")
    end


end

M.rdsTransformFrefToJsonTable = function(jsonFileRef)
    local jsonDSTable = json_utils.parseJSONFile(jsonFileRef)
    assert(type(jsonDSTable) == "table", "Cannot covert json filref to table")
    return jsonDSTable
end

M.rdsGetParamFromJsonTable = function(jsonTable)
    local def = jsonTable["dataDefinition"] or {}
    local dataDef = def['customFields'] or jsonTable['customFields']
    local columnInfo = dataDef["columnInfo"] or {}
    local aTemp = jsonTable["analysisData"] or {}
    local analysisData = aTemp['customFields'] or {}
    local configProperties = jsonTable["configProperties"] or {}


    --Exploring analysis Data fields for parameters->
    --physicalTableNm , indexList , strategyType ,rowsPerPartition , partitionCount
    if analysisData['indexList'] == "" then
        analysisData['indexList'] = nil
    end

    if dataDef['defaultIndexList'] == "" then
        dataDef['defaultIndexList'] = nil
    end

    paramTable = { indexList = analysisData['indexList'] or dataDef['defaultIndexList'] or nil,
                   strategyType = analysisData['strategyType'] or "BYVARS",
                   primaryKey = nil,
                   partitionByVars = nil,
                   partitionCount = analysisData['partitionCount'] or nil,
                   srcWhereStmt = analysisData['srcWhereStmt'] or nil,
                   createTablePGStmt = {},
                   configProperties = configProperties,
                   keepStmt = nil,
                   sasAttribStmt = nil,
                   sasAttribStmtNoFormat=nil,
                   columnMap = {}


    }
    --Running AND if true else nil
    paramTable['rowsPerPartition'] = ((paramTable['strategyType'] == "ROWS" and (analysisData['rowsPerPartition'] or 1000000)) or nil)

    -- local cmap = {}
    -- local createTablePG = {}
    --Retrieving primaryKey and partitionFlag from data definition
    for i, v in pairs(columnInfo) do
        local low_name = string.lower(v['name'])
        --gathering all primary keys
        if v['primaryKeyFlag'] == true then
            paramTable['primaryKey'] = (paramTable['primaryKey'] or '') .. v['name'] .. ' '
        end
        --gathering all partitionvars
        if v['partitionFlag'] == true then
            paramTable['partitionByVars'] = (paramTable['partitionByVars'] or '') .. v['name'] .. ' '
        end

        -- keep statement
        paramTable['keepStmt'] = (paramTable['keepStmt'] or '') .. low_name .. ' '
        -- columnMap for checking columns
        paramTable['columnMap'][low_name] = true

        --Generate sasAttribStmt
        local strLen = tostring(v['size'])
        local lowertype = string.lower(v['type'])

        -- Check the type and modify strLen accordingly
        if lowertype == "char" then
            strLen = low_name .. " $" .. strLen
        elseif lowertype == "varchar" then
            strLen = low_name .. " varchar(" .. strLen .. ")"
        else
            -- everything else if numeric then.
            strLen = low_name .. " " .. strLen
        end

        local format = ""
        if v['format'] ~= nil and v['format'] ~= "" then
            format =  "format " ..low_name.." "..v['format'].."; \n"
        end
        local sasAttr = "length "..strLen.."; \n"..format
        paramTable['sasAttribStmt'] = (paramTable['sasAttribStmt'] or '') .. sasAttr

        -- db type stmt
        if v['sqlType'] ~= nil and v['sqlType'] ~= "" then
            paramTable['createTablePGStmt'][#paramTable['createTablePGStmt'] + 1] = low_name .. " " .. v['sqlType']
            -- local dbTableStmt = low_name .. " " .. v['sqlType'] .. ","
            -- paramTable['createTablePGStmt'] = (paramTable['createTablePGStmt'] or '') .. dbTableStmt .. "\n"
        end
    end


    if paramTable['strategyType'] ~= 'NONE' and paramTable['partitionByVars'] == nil then
        sas.print("%2zFor strategyType " .. paramTable['strategyType'] .. " you should have partitionByVars defined in your analysisData.")
        sas.print("%2zResetting Partition Strategy to NONE")
        paramTable['strategyType'] = 'NONE'
    end

    --If strategyType is NONE we make partitionByVar as nil
    if paramTable['strategyType'] == 'NONE' then
        sas.print("%2zIgnoring partitionByVars as strategyType is:" .. paramTable['strategyType'])
        paramTable['partitionByVars'] = nil
        paramTable['partitionCount'] = nil
    end

    if paramTable['strategyType'] == 'FIXED' and (paramTable['partitionCount'] == nil or paramTable['partitionCount'] == 0) then
        error("For strategyType " .. paramTable['strategyType'] .. " you must have partitionCount set in your analysisData.")
    end

    --trimming all string parameters
    for i, v in pairs(paramTable) do
        if type(v) == 'string' then
            paramTable[i] = M.trimAll(v)
        end
    end

    return paramTable
end

M.getLibNameEngine = function(libname)
    local code = [[
    %let engine=;
    proc sql noprint;
     select engine into : engine from SASHELP.VLIBNAM where libname=upcase("@libname@");
    quit;
   ]]
    local rc = sas.submit(code, { libname = libname }, nil, 2000)
    if rc ~= 0 then
        sas.print("%1zFailed to get engine.")
        return nil
    end
    local engine = sas.symget("engine")

    engine = engine:gsub("^%s+", ''):gsub("%s+$", '')
    if engine == "" then
        return nil
    end

    return engine


end

M.isLibEngineCAS = function(libname)
    libType = M.getLibNameEngine(libname)
    print("Lib type identified as:", libType)
    if libType == 'CAS' then
        return true
    end
    return false
end

M.getCASDSParameters = function(libname)
    sas.print("%3zChecking if we need to pass CAS dataset parameter")
    dsParam = "(datalimit=ALL READTRANSFERSIZE=500M)"
    local flg = M.isLibEngineCAS(libname)
    if flg then
        sas.print("%3zDetected CAS lib. Returning CAS Parameters:" .. dsParam)
        return dsParam
    end
    return ""
end

M.getCASDSParametersNoBrackets = function(libname)
    sas.print("%3zChecking if we need to pass CAS dataset parameter")
    dsParam = "datalimit=ALL READTRANSFERSIZE=500M"
    local flg = M.isLibEngineCAS(libname)
    if flg then
        sas.print("%3zDetected CAS lib. Returning CAS Parameters:" .. dsParam)
        return dsParam
    end
    return ""
end


M.isEmpty = function(val)
    return val == nil or val == ''
end

M.processDSWhereClause = function(srcWhereStmt)
    --Processing whereClause
    if M.isEmpty(srcWhereStmt) then
        return ""
    else
        return "where " .. srcWhereStmt .. " ;"
    end
end

M.processDSAppendWhereClause = function(srcWhereStmt)
    --Processing whereClause
    if M.isEmpty(srcWhereStmt) then
        return ""
    else
        return "where=(" .. srcWhereStmt .. ")"
    end
end

---buildColumnListFromSource checks and return columns which are present in table matching in map.
---it also sends columns not present in dataset.
---This is helpful to build select list.
M.buildColumnListFromSource = function(tableName, columnMap)
    local scols = ""
    local mcols = ""
    local slist = {}
    local missing_list = {}
    local varMap = {}
    local dsid = sas.open(tableName)

    -- gather all dataset columns in a map.
    for var in sas.vars(dsid) do
        local lname = string.lower(var.name)
        varMap[lname]= true
    end
    sas.close(dsid)
    for cname,_ in pairs(columnMap) do
        -- name is always in lowcase
        if varMap[cname] then
            slist[#slist + 1] = cname
        else
            missing_list[#missing_list+1] = cname
        end
    end
    -- if none of the columns are matching we will get blank .
    if #slist > 0 then
        scols = table.concat(slist, " ")
    end

    -- if missing
    if #missing_list > 0 then
        mcols = table.concat(missing_list, " ")
    end
    -- returning select list and missing column list.
    return scols,mcols

end



return M
