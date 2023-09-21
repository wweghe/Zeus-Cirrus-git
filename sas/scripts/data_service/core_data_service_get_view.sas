/*
 *  This is a SAS file called by risk Data service end point riskData/objects/<analysisObjectKey?/view
 *  The code here contains only deal with single data view creation. This code also works as glue code picking values
 *  from midtier and injecting into SAS
 *  Macro variables provided from midtier are :
 *  RDS_VIEW_SOURCE_TABLE_NAME
 *  RDS_VIEW_DESTINATION_LIBNAME
 *  RDS_VIEW_DESTINATION_TABLE_NAME
 *  RDS_VIEW_DATA_SRC_FILEREF
 *  RDS_VIEW_WHERE_CLAUSE
 *  RDS_VIEW_VALID_DAYS
 *
 */


proc lua restart;
submit;
    local ds = require "sas.risk.cirrus.core_data_service"

    --Reading SAS macro vars
    --fileref source lib , source ds amd targetlib from SAS env var.
    local sasVarTab = {
        RDS_VIEW_SOURCE_TABLE_NAME = "",
        RDS_VIEW_DESTINATION_LIBNAME = "",
        RDS_VIEW_DESTINATION_TABLE_NAME = "",
        RDS_VIEW_DATA_SRC_FILEREF = "",
        RDS_VIEW_WHERE_CLAUSE = "",
        RDS_VIEW_VALID_DAYS = "",
        RDS_VIEW_SELECT_COLUMN_LIST = "",
        RDS_DATA_DEF_FILEREF_NAME = ""
    }

    --validating that none of macro var is empty or nil
    for i, v in pairs(sasVarTab) do
        sasVarTab[i] = sas.symget(i)
        if sasVarTab[i] == nil or sasVarTab[i] == "" then
            error("Following macro variable is not set::" .. i)
        end
    end

    print("****************************************************")
    print("RDSMACROVARS BEFORE", table.tostring(sasVarTab))
    print("****************************************************")

    --Getting custom code if any. This can be blank.
    sasVarTab.RDS_VIEW_CUSTOM_CODE = sas.symget("RDS_VIEW_CUSTOM_CODE")

    -- Getting datamap stmt if provided use or set it to blank
    sasVarTab.RDS_VIEW_DATAMAP_CODE_STMTS = sas.symget("RDS_VIEW_DATAMAP_CODE_STMTS")
    if sasVarTab.RDS_VIEW_DATAMAP_CODE_STMTS == nil then
           sasVarTab.RDS_VIEW_DATAMAP_CODE_STMTS=""
    end
    if string.len(sasVarTab.RDS_VIEW_DATAMAP_CODE_STMTS) > 0 then
    sasVarTab.RDS_VIEW_DATAMAP_CODE_STMTS= ", "..sasVarTab.RDS_VIEW_DATAMAP_CODE_STMTS
    end



    --if where clause value is 1 then making 1=1
    if sasVarTab.RDS_VIEW_WHERE_CLAUSE == 1 then
        sasVarTab.RDS_VIEW_WHERE_CLAUSE = "1=1"
    end

    print("****************************************************")
    print("RDSMACROVARS AFTER", table.tostring(sasVarTab))
    print("****************************************************")

    --Reading DB details from fileref
    local dbParams = ds.rdsTransformFrefToJsonTable(sasVarTab.RDS_VIEW_DATA_SRC_FILEREF)


    --Converting json file to lua json object.
    jsonObjectDataDef=ds.rdsTransformFrefToJsonTable(sasVarTab.RDS_DATA_DEF_FILEREF_NAME)
    print("****************************************************")
    print("configProperties", table.tostring(jsonObjectDataDef.configProperties))
    print("****************************************************")

    --Setting table for DB vars
    local dbVarsTab = {
        port = "",
        server = "",
        user = "",
        schema = "",
        engine = "",
        database = "",
        password = ""
    }

    --Assigning values to DB vars. IF not found or nil we fail
    for k, v in pairs(dbVarsTab) do
        dbVarsTab[k] = dbParams[k]
        if dbVarsTab[k] == nil or dbVarsTab[k] == "" then
            error("Following variable are not set::" .. k)
        end
    end

    --Building source connection lib
    local source_lib_connection_options1 =
        'server ="' ..
        dbVarsTab.server ..
            '" port=' .. dbVarsTab.port .. " user=" .. dbVarsTab.user .. " password='" .. dbVarsTab.password .. "'"

    local source_lib_connection_options =
        source_lib_connection_options1 .. " database=" .. dbVarsTab.database .. " schema=" .. dbVarsTab.schema

    --We are storing source connection as macro var so that we dont show up in substitutions.
        sas.symput("source_lib_connection_options", source_lib_connection_options)

    -- Setting fullviewname
    local fullViewName=sasVarTab.RDS_VIEW_DESTINATION_LIBNAME .. "." .. sasVarTab.RDS_VIEW_DESTINATION_TABLE_NAME


    print("****************************************************")
    -- print("RDSMACROVARS", source_lib_connection_options)
    print("****************************************************")

    --Template SAS code to run for creating view
    --We will be using select list instead of keep and drop cause if just want to use where clause on column you dont want to keep will fail.
    --Best would be for midtier to resolve the select list.
    local sasCode =
        [[

    proc sql noprint ;
    create view @out_view@
           as select @select_column_list@ @datamap_stmts@
           from @dbschema@.@in_ds@ (BULKUNLOAD=YES) @where_clause@
           using libname @dbschema@ @engine@ &source_lib_connection_options.;
    quit;


    ]]

    local rc =
        sas.submit(
        sasCode,
        {
            engine = dbVarsTab.engine,
            in_ds = sasVarTab.RDS_VIEW_SOURCE_TABLE_NAME,
            out_view = fullViewName,
            where_clause = "where " .. sasVarTab.RDS_VIEW_WHERE_CLAUSE,
            custom_code = sasVarTab.RDS_VIEW_CUSTOM_CODE,
            valid_days = sasVarTab.RDS_VIEW_VALID_DAYS,
            select_column_list = sasVarTab.RDS_VIEW_SELECT_COLUMN_LIST,
            datamap_stmts=sasVarTab.RDS_VIEW_DATAMAP_CODE_STMTS,
            dbschema = dbVarsTab.schema
        },
        nil,
        2000
    )

    if rc ~= 0 or rc == nil then
        ds.sasDeleteView(fullViewName)
        ds.printWithAbort("%1zRisk Data Service view creation failed and may be incomplete. Please check service logs.")
    end

endsubmit;
run;