/*
   This script will create dataload xlsx compatible with riskCirrusObjects for loading configurationSets and configurationTables.
   Can be used for all stratum 94 solutions.
   TODO : We have not added any solution dimension in script. PLan is to automate that as well in future.

Instructions :
   Gather all datasets from sas9.4 stratum solution and create a libref.
   You will need to set below vars in proc lua block:

    --Libref pointing to 9.4 config datasets
    local config_source_sas_lib="work"

    --Core version
    local version='2022.10'

    --out put loader xslx file
    local loader_file='/risk_cirrus_core/configLoader.xlsx'

    --config datasets to pull
    local source_config_dataset_name={ "allocation_config","control_option ", "data_extraction_config", "datastore_config", "dq_config", "enrichment_config", "execution_config","report_config","report_option", "report_parameters", "reportmart_config","rule_set_config", "run_option"}

*/

%macro setup();
%let core_root_path=%sysget(SAS_RISK_CIRRUS_CODE_LIB_BASE_PATH_CORE);
%let core_code_path=&core_root_path./%sysget(SAS_RISK_CIRRUS_CODE_LIB_LATEST_VERSION_DIR_CORE);
option insert = ( SASAUTOS = ("&core_code_path./sas/ucmacros"));
filename LUAPATH ("&core_code_path./lua"  "&core_root_path."  "&core_code_path.");
%put &=core_root_path;
%put &=core_code_path;
%mend;

/* including macros and lua */
%setup();


proc lua restart;
submit;

--Libref pointing to 9.4 config datasets
local config_source_sas_lib="work"

--Core version
local version='2022.1.4'

--out put loader xslx file
local loader_file='/risk_cirrus_core/configLoader.xlsx'

--config datasets to pull
local source_config_dataset_name={ "allocation_config","control_option ", "data_extraction_config", "datastore_config", "dq_config", "enrichment_config", "execution_config","report_config","report_option", "report_parameters", "reportmart_config","rule_set_config", "run_option"}


--Below code may not need change--
--Creating config set
local cfs= require "utils.migration.configset.core_configuration"

local config_set = { sourceSystemCd='RCC', idPrefix='ConfigSet' , versionNm = version,	namePrefix = 'ConfigSet',	descriptionPrefix='Config Set desc',	statusCd='TEST'}
cfs.createConfigSet('work.configSet',config_set )

--Creating config tables : Here we convert datasets to json create loader like DS
cfs.createConfigTables(config_source_sas_lib
    ,source_config_dataset_name
    ,"work.configtables"
    ,"RCC"
    ,version
    ,'TEST'

)

--Creating linkInstance relating to configset --> config tables
local link_inst_details={ linkTypeId = "configurationSet_configurationTable"
                         ,sourceSystemCd='RCC'
                         , obj1RegName = 'Configuration Set'
                         , obj2RegName = 'Configuration Table'
                         , versionNm=version, prefixLinkInstance='ConfigSet_'
                         }
cfs.createConfigTableLinkInstance("work.config_links",'work.configSet',"work.configtables",link_inst_details)

--Pushing all datasets to xlsx. You can pass any number of args.
cfs.pushToXLSX(loader_file,{ sheetName= 'Configuration Set' , dsName ='work.configSet'}, { sheetName= 'Configuration Table' , dsName ="work.configtables"}, { sheetName= 'LinkInstances' , dsName ="work.config_links"} )

endsubmit;
run;
