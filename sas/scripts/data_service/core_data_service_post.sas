/*
*  This is a SAS file called by risk Data service end point POST riskData/objects/
*  The code here contains deals with uploading of source data into riskdata.
*/

proc lua restart;
submit;

local ds = require "sas.risk.cirrus.core_data_service"

--Reading SAS macro vars
--fileref source lib , source ds amd targetlib from SAS env var.
local sasVarTab= { RDS_DATA_DEF_FILEREF_NAME = "",
                   RDS_INPUT_LIBREF_NAME = "",
                   RDS_INPUT_TABLE_NAME = "" ,
                   RDS_POSTGRES_LIBREF_NAME = "",
                   RDS_ANALYSIS_DATA_TABLE_NAME= "",
                   RDS_POSTGRES_SCHEMA_NAME= ""
                   }

for i,v in pairs(sasVarTab) do
  sasVarTab[i]=sas.symget(i)
  if sasVarTab[i] == nil  or sasVarTab[i] == '' then
     error("Following macro variable is not set::"..i )
  end

end

--Converting json file to lua json object.
jsonObjectFromService=ds.rdsTransformFrefToJsonTable(sasVarTab.RDS_DATA_DEF_FILEREF_NAME)

--Reading table to get targetTableNm and partitions params.
local params=ds.rdsGetParamFromJsonTable(jsonObjectFromService)

print("****************************************************")
print("SourceLibNm",sasVarTab.RDS_INPUT_LIBREF_NAME)
print("SourceTableNm",sasVarTab.RDS_INPUT_TABLE_NAME)
print("TargetLibNm",sasVarTab.RDS_POSTGRES_LIBREF_NAME)
print("TargetTableNm",sasVarTab.RDS_POSTGRES_LIBREF_NAME)
print("TargetDBSchemaNm",sasVarTab.RDS_POSTGRES_SCHEMA_NAME)
print("PartitionParams",table.tostring(params))
print("****************************************************")

local res= ds.generatePartitions(sasVarTab.RDS_INPUT_LIBREF_NAME
                                ,sasVarTab.RDS_INPUT_TABLE_NAME
                                ,sasVarTab.RDS_POSTGRES_LIBREF_NAME
                                ,sasVarTab.RDS_ANALYSIS_DATA_TABLE_NAME
                                ,params
                                ,false
                                ,'POSTGRESQL'
                                ,sasVarTab.RDS_POSTGRES_SCHEMA_NAME)
print("Partition Return Value::")
print(res)


endsubmit;
run;