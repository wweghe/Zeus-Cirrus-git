/* ********************************************* */
/*        Init Script: Input Parameters          */
/* ********************************************* */

/* Cycle Id */
%let cycle_key = ${Cycle.key}; 

/* Cycle Name */
%let cycle_name = %nrbquote(${Cycle.name});

/* Analysis Run Key - required to resolve SASAUTOS and LUAPATH paths below */
%let analysis_run_key = ${AnalysisRun.key};

/* Analysis Run Solution - required to resolve SASAUTOS and LUAPATH paths below */
%let analysis_run_solution = %lowcase(${AnalysisRun.createdInTag});

/* Log Level: 1-4 */
%let log_level = ${AnalysisRun.customFields.scriptParameters.LOG_LEVEL}; 

/* Get the cycle's code library paths */
%let cycle_code_lib_paths = '${params='{"objectRestPath":"cycles","objectKey":"'+Cycle.key+'"}'; function:RunRequest("/riskCirrusCore/codeLibraryActions/getCodeLibFolders",Var.params).codeLibPaths}';
%let cycle_code_lib_paths=%sysfunc(prxchange(s/\\u0026/%nrstr(&)/, -1, %superq(cycle_code_lib_paths)));
%let cycle_code_lib_paths = %sysfunc(prxchange(s/(\s*(\[|\])\s*)|(%str(,))/ /, -1, %superq(cycle_code_lib_paths)));

%let unresolved_new_sasautos = %sysfunc(prxchange(s/"([^%bquote(")]+)"/"$1\/sas\/ucmacros" "$1\/sas\/nodes"/i, -1, %superq(cycle_code_lib_paths)));
%let unresolved_new_lua = %sysfunc(prxchange(s/"([^%bquote(")]+)"/"$1\/lua"/i, -1, %superq(cycle_code_lib_paths)));

/* Set SASAUTOS */
options insert=sasautos = (%sysfunc(dequote(&unresolved_new_sasautos.)));
%put %sysfunc(getoption(SASAUTOS));

/* Set LUAPATH */
%let existing_lua = %sysfunc(prxchange(s/[()]//, -1, %sysget(SASLUA)));
filename LUAPATH (%sysfunc(dequote(&unresolved_new_lua.)) &existing_lua.);
%put %sysfunc(pathname(LUAPATH));

/* Set logging options (based on the value of LOG_LEVEL macro variable) */
%rsk_set_logging_options ();


/* ************************************************ */
/*    Start creation of init.sas script file        */
/* ************************************************ */
filename tmp temp;
data _null_;
	file tmp lrecl = 32000 termstr = nl;
   
   length str $10000.;
   str = cat("/*  - Cycle Name: &cycle_name.", repeat(" ", %sysfunc(max(1, 45 - %length(%superq(cycle_name))))), "          */");

   put '/**************************************************************************/';
   put '/* Initialization Script                                                  */';
   put "/*  - Cycle Key: &cycle_key.                     */";
   put str;
   put "/*  - Creation Date: %sysfunc(date(), yymmddd10.) %sysfunc(time(), tod8.)                                  */";
   put "/*  - Analysis Run Key: &analysis_run_key.              */";
   put '/**************************************************************************/';
   put;
   put '%let code_lib_paths = ' "%superq(cycle_code_lib_paths);";
   put;
   put '%if %sysevalf(%superq(analysis_run_code_lib_paths) ne, boolean) %then %do;';
   put '   %let analysis_run_code_lib_paths=%sysfunc(prxchange(s/\\u0026/%nrstr(&)/, -1, %superq(analysis_run_code_lib_paths)));';
   put '   %let code_lib_paths = %sysfunc(prxchange(s/(\s*(\[|\])\s*)|(%str(,))/ /, -1, %superq(analysis_run_code_lib_paths)));';
   put '%end;';
   put;
   put '%let unresolved_new_sasautos=%sysfunc(prxchange(s/"([^%bquote(")]+)"/"$1\/sas\/nodes" "$1\/sas\/ucmacros"/i, -1, %superq(code_lib_paths)));';
   put '%let unresolved_new_lua=%sysfunc(prxchange(s/"([^%bquote(")]+)"/"$1\/lua"/i, -1, %superq(code_lib_paths)));';
   put;
   put '/* Set SASAUTOS */';
   put 'options insert = (sasautos = ( %sysfunc(dequote(&unresolved_new_sasautos.)) ) );';
   put;
   put '/* Set LUAPATH */';
   put 'filename LUAPATH ( %sysfunc(dequote(&unresolved_new_lua.)) %sysfunc(prxchange(s/[()]//, -1, %sysget(SASLUA))) );';
   put;
run;


/* ******************************************* */
/*    Attach init.sas file to the cycle        */
/* ******************************************* */
%let httpSuccess=0;
%core_rest_create_file_attachment(objectKey = &cycle_key.
                      , objectType = cycles
                      , file = %sysfunc(pathname(tmp))
                      , attachmentName = init.sas
                      , attachmentDisplayName = Initialize Script
                      , attachmentDesc = Initialization script to set up the SAS environment for cycle &cycle_key.
                      , attachmentGrouping = documentation_attachments
                      , replace = Y
                      );

%if &httpSuccess ne 1 %then %do;
   %put Response status is: &responseStatus.;
   %rsk_terminate(msg=Failed to attach initialization file to cycle);
%end;