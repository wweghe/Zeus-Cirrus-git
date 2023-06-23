--
-- This module provides function to create data loaders for configSet , configuration table and linkInstances
--
--

local M = {}

local json = require "sas.risk.utils.json"
json.strictTypes = true

---
-- @param libref : Libref where config datasets are present
-- @param tableName : list of configuration table names.
M.dataSetToTable = function(libref, tableName)
    mapToLoad = { data = {}, name = "", vars = {} }
    fullTabName = libref .. "." .. tableName
    mapToLoad.name = string.lower(tableName)

    --Getting data
    if sas.exists(fullTabName) then
        local dsid = sas.open(fullTabName)

        for row in sas.rows(dsid) do
            _tempRow = {}
            for i, v in pairs(row) do
                if type(i) == "string" then
                    --if var is missing make it "." An issue for numeric missing.
                    if v == sas.MISSING then
                        v = "."
                    end
                    print(i, v)
                    _tempRow[i] = v
                end
            end
            table.insert(mapToLoad.data, _tempRow)
        end

        -- --Getting vars
        for var in sas.vars(dsid) do
            lowVarName = string.lower(var.name)
            mapToLoad.vars[lowVarName] = {}
            table.insert(mapToLoad.vars[lowVarName], var)
        end

        sas.close(dsid)
    end

    return mapToLoad
end


M.createConfigTables = function(libref, tableNames, dsName, sourceSystemCd, versionNm, statusCd, applySuffix)
    applySuffix = applySuffix or true
    local version_suffix = ''

    if applySuffix == true then
        version_suffix = '__' .. string.gsub(versionNm, "%.", "_")
    end

    sas.new_table(dsName, {
        { name = "sourceSystemCd", type = "C", length = 3, label = "Configuration Table.sourceSystemCd" },
        { name = "id", type = "C", length = 50, label = "Configuration Table.id" },
        { name = "name", type = "C", length = 100, label = "Configuration Table.name" },
        { name = "description", type = "C", length = 100, label = "Configuration Table.description" },
        { name = "type", type = "C", length = 100, label = "Configuration Table.type" },
        { name = "statusCd", type = "C", length = 50, label = "Configuration Table.statusCd" },
        { name = "versionNm", type = "C", length = 50, label = "Configuration Table.versionNm" },
        { name = "typeData", type = "C", length = 32767, label = "Configuration Table.typeData" }
    })


    local dsid = sas.open(dsName, "u")
    for k, v in pairs(tableNames) do
        tableData = M.dataSetToTable(libref, v)
        sas.append(dsid)
        sas.put_value(dsid, "sourceSystemCd", sourceSystemCd)
        sas.put_value(dsid, "id", v .. version_suffix)
        sas.put_value(dsid, "name", v .. version_suffix)
        sas.put_value(dsid, "description", v .. version_suffix)
        sas.put_value(dsid, "type", tableData.name)
        sas.put_value(dsid, "statusCd", statusCd)
        sas.put_value(dsid, "versionNm", versionNm)
        sas.put_value(dsid, "typeData", json:encode(tableData))

        sas.update(dsid)
    end

    sas.close(dsid)
end



M.createConfigSet = function(dsName, config_set)
    local version_suffix = '__' .. string.gsub(config_set.versionNm, "%.", "_")
    sas.new_table(dsName, {
        { name = "sourceSystemCd", type = "C", length = 3, label = "Configuration Set.sourceSystemCd" },
        { name = "id", type = "C", length = 50, label = "Configuration Set.id" },
        { name = "name", type = "C", length = 100, label = "Configuration Set.name" },
        { name = "description", type = "C", length = 100, label = "Configuration Set.description" },
        { name = "versionNm", type = "C", length = 32, label = "Configuration Set.versionNm" },
        { name = "statusCd", type = "C", length = 32, label = "Configuration Set.statusCd" }
    })

    if config_set.applySuffix == false then
        version_suffix = ''
    end

    local dsid = sas.open(dsName, "u")
    sas.append(dsid)
    sas.put_value(dsid, "sourceSystemCd", config_set.sourceSystemCd)
    sas.put_value(dsid, "id", config_set.idPrefix .. version_suffix)
    sas.put_value(dsid, "name", config_set.namePrefix .. version_suffix)
    sas.put_value(dsid, "description", config_set.descriptionPrefix .. version_suffix)
    sas.put_value(dsid, "statusCd", config_set.statusCd)
    sas.put_value(dsid, "versionNm", config_set.versionNm)

    sas.update(dsid)
    sas.close(dsid)
end



--This function works many to many . For each record in side1 table all elements of side2 tables will be linked.
M.createConfigTableLinkInstance = function(dsName, linkSide1Table, linkSide2Table, linkDetails)
    --linkDetails would include:   { 'linkTypeId' = <> ,sourceSystemCd=, obj1RegName = 'objRegName' , obj2RegName = 'obj2RegName' , versionNm=<>, prefixLinkInstance='' }

    local version_suffix = '__' .. string.gsub(linkDetails.versionNm, "%.", "_")
    linkDetails.prefixLinkInstance = linkDetails.prefixLinkInstance or 'ConfigSet_'
    sas.new_table(dsName, {
        { name = "sourceSystemCd", type = "C", length = 3, label = "linkInstances.sourceSystemCd" },
        { name = "id", type = "C", length = 50, label = "linkInstances.id" },
        { name = "linkTypeSourceSystemCd", type = "C", length = 3, label = "linkInstances.linkTypeSourceSystemCd" },
        { name = "linkTypeId", type = "C", length = 50, label = "linkInstances.linkTypeId" },
        { name = "obj1RegName", type = "C", length = 100, label = "linkInstances.obj1RegName" },
        { name = "obj1SourceSystemCd", type = "C", length = 3, label = "linkInstances.obj1SourceSystemCd" },
        { name = "obj1Id", type = "C", length = 50, label = "linkInstances.obj1Id" },
        { name = "obj2RegName", type = "C", length = 100, label = "linkInstances.obj2RegName" },
        { name = "obj2SourceSystemCd", type = "C", length = 3, label = "linkInstances.obj2SourceSystemCd" },
        { name = "obj2Id", type = "C", length = 50, label = "linkInstances.obj2Id" },
        { name = "description", type = "C", length = 100, label = "linkInstances.description" }
    })

    local link1dsid = sas.open(linkSide1Table)
    local link2dsid = sas.open(linkSide2Table)
    local dsid = sas.open(dsName, "u")
    --SQL cartesian would produce same complexity
    for row1 in sas.rows(link1dsid) do
        for row2 in sas.rows(link2dsid) do
            sas.append(dsid)
            sas.put_value(dsid, "sourceSystemCd", linkDetails.sourceSystemCd)
            sas.put_value(dsid, "id", row2.id)
            sas.put_value(dsid, "linkTypeSourceSystemCd", linkDetails.sourceSystemCd)
            sas.put_value(dsid, "linkTypeId", linkDetails.linkTypeId)
            sas.put_value(dsid, "obj1RegName", linkDetails.obj1RegName)
            sas.put_value(dsid, "obj1SourceSystemCd", row1.sourcesystemcd)
            sas.put_value(dsid, "obj1Id", row1.id)
            sas.put_value(dsid, "obj2RegName", linkDetails.obj2RegName)
            sas.put_value(dsid, "obj2SourceSystemCd", row2.sourcesystemcd)
            sas.put_value(dsid, "obj2Id", row2.id)
            sas.put_value(dsid, "description", row1.id .. '#' .. row2.id)

            sas.update(dsid)

            print(linkDetails.sourceSystemCd, linkDetails.prefixLinkInstance .. version_suffix,
                linkDetails.sourceSystemCd, linkDetails.linkTypeId, linkDetails.obj1RegName,
                row1.sourcesystemcd, row1.id, linkDetails.obj2regname, row2.sourcesystemcd, row2.id, linkDetails.prefixLinkInstance .. version_suffix)
        end
    end

    sas.close(link1dsid)
    sas.close(link2dsid)
    sas.close(dsid)
end

---
-- @param xlsxFile  : /local/a.xlsx
-- @param ... : You can pass list of tables where each table has this format : { { sheetName = "<>" ,dsName = <>}, { sheetName = "<>" ,dsName = <>} }
--
M.pushToXLSX = function(xlsxFile, ...)
    args = { ... }
    for i, v in ipairs(args) do
        print(i, v.sheetName, v.dsName)
        local replace = ''
        local code = [[
           proc export data=@dsName@ outfile="@xlsxFile@"
      dbms=xlsx label replace ; sheet="@sheetName@" ;run;
       ]]
        rc = sas.submit(code, { dsName = v.dsName, sheetName = v.sheetName, xslxFile = xlsxFile, replace = replace })
        print(rc)
    end
end

return M