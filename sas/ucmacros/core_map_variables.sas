/*
 Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file 
\anchor core_map_variables

   \brief   Apply a set of mapping rules to an input table

   \param[in] ds_in Input table to be processed
   \param[in] map_ds Input table containing the mapping logic
   \param[in] map_type Type of mapping rules.
   \param[in] include_wildcard_flg Flag (Y/N). Controls whether wildcard entries are always included or just used as a fallback option. (Default: N)
   \param[out] ds_out Name of the output table
   \param[out] ds_out_map (Optional) Name of the output table containing details of the mapping rules that were processed.
   \param[out] fout (Optional) Output fileref. If provided, the mapping code will be written to the fileref.

   \details

   This macro applies a set of mapping rules to an input dataset.

   The expected structure of the input mapping data set is as follows:

   |PK            |Variable                  |Type              |Label                                |Description                                                                                                                |
   |--------------|--------------------------|------------------|-------------------------------------|---------------------------------------------------------------------------------------------------------------------------|
   |![ ](pk.jpg)  | MAP_TYPE                 | CHARACTER(100)   |  Map Type                           | Type of mapping rules to be applied.                                                                                      |
   |![ ](pk.jpg)  | TARGET_VAR_NAME          | CHARACTER(32)    |  Target Variable Name               | Name of the variable to be created in the output dataset.                                                                 |
   |              | EXPRESSION_TXT           | CHARACTER(4096)  |  Expression                         | The expression can be any of the following: <br> - &lt;Variable Name&gt;: it will result in the variable being renamed. <br> - =&lt;assignment&gt; (i.e.: = Var1 + Var2): any valid SAS statement. <br> - &lt;Blank&gt;: This record will be ignored. |
   |              | TARGET_VAR_LENGTH        | CHARACTER(32)    |  Target Variable Length             | (Optional) Set the length of the target variable (i.e.: $32. for characters or 8. for numeric variables).                 |
   |              | TARGET_VAR_LABEL         | CHARACTER(150)   |  Target Variable Label              | (Optional) Set the label for the target variable (Do not include quotes!).                                                |
   |              | TARGET_VAR_FMT           | CHARACTER(32)    |  Target Variable Format             | (Optional) Set the format of the target variable.                                                                         |

   A wildcard symbol * can be used for either columns WORKGROUP or MODELING_SYSTEM to indicate that the corresponding records of the VARIABLE_NAME column are applicable to any value of the WORKGROUP or MODELING_SYSTEM parameter.

   For each value of the input parameter MAP_TYPE this macro will query the &MAP_DS table and process the records that match one of the following conditions:
   1) MAP_TYPE = &lt;Current value of MAP_TYPES&gt;

   \note
      When INCLUDE_WILDCARD_FLG = N, <b>Only one</b> of the above conditions is applied: as soon as one of the filter conditions returns any record, all other conditions are ignored.

   \ingroup coreRestUtils

   \author  SAS Institute Inc.
   \date    2024
*/
%macro core_map_variables(ds_in =
                         , map_ds =
                         , map_type =
                         , include_wildcard_flg = N
                         , ds_out =
                         , ds_out_map =
                         , fout =
                         );

   %local
      totRenameVars
      totExpressions
      totAttrib
      quote_option
      fout_flg
      i
   ;

   /* ************************************** */
   /* Check input parameters                 */
   /* ************************************** */

   /* Check if the fout parameter has been provided */
   %let fout_flg = N;
   %if %sysevalf(%superq(fout) ne, boolean) %then %do;
      %let fout_flg = Y;
      /* Check if the fileref has been assigned */
      %if(%sysfunc(fileref(&fout.)) ne 0) %then %do;
         /* Assign a temporary fileref */
         filename &fout. temp;
      %end;
   %end;

   /* Check parameter map_type */
   %if(%sysevalf(%superq(map_type) =, boolean)) %then %do;
      %put ERROR: Input parameter MAP_TYPE is missing. Skipping execution.;
      %return;
   %end;

   /* Check parameter out_vars */
   %if(%sysevalf(%superq(ds_out) =, boolean) and %sysevalf(%superq(fout) =, boolean)) %then %do;
      %put ERROR: Both output parameters DS_OUT and FOUT are missing. At least one must be provided. Skipping execution.;
      %return;
   %end;

   /* Check parameter map_ds */
   %if(%sysevalf(%superq(map_ds) =, boolean)) %then %do;
      %put ERROR: Input parameter MAP_DS is missing. Skipping execution.;
      %return;
   %end;
   %else %if(not %rsk_dsexist(&map_ds.)) %then %do;
      %put ERROR: Input dataset &map_ds. does not exist. Skipping execution.;
      %return;
   %end;

   /* Assign default value to parameter workgroup if missing */
   %if(%sysevalf(%superq(include_wildcard_flg) =, boolean)) %then
      %let include_wildcard_flg = N;
   %else
      %let include_wildcard_flg = %upcase(&include_wildcard_flg.);

   %let totRenameVars = 0;
   %let totExpressions = 0;
   %let totAttrib = 0;

   /* Get the current system option */
   %let quote_option = %sysfunc(getoption(quotelenmax));
   /* Avoid annoying message aobut unbalanced quotes */
   option noquotelenmax;

      data __tmp_map_subset__;
      set &map_ds.;
      where
         upcase(map_type) = upcase("&map_type.")
         and expression_txt is not missing
         and target_var_name ne expression_txt
      ;
   run;

   data
      %if %sysevalf(%superq(ds_out_map) ne, boolean) %then
         &ds_out_map.;
      %else
         _null_;
      ;
      length
         map_type $100.
         target_var_name $32.
         expression_txt $4096.
         target_var_length $32.
         target_var_label $150.
         target_var_fmt $32.
         rename_flg $1.
         renameCount 8.
         expressionCount 8.
         attribCount 8.
      ;

      drop
         rename_flg
         renameCount
         expressionCount
         attribCount
         rx rc
      ;

      /* Regex to identify variable rename */
      rx = prxparse("/^\w+$/");
      %if (&fout_flg. = Y and %sysevalf(%superq(ds_out) ne, boolean)) %then %do;
         drop rx2;
         rx2 = prxparse("/%sysfunc(prxchange(s/\s+/|/i, -1, %rsk_getvarlist(&ds_out.)))/i");
      %end;

      /* Load map configuration table into a lookup table */
      declare hash hList (dataset: "__tmp_map_subset__", multidata: "yes");

      hList.defineKey("map_type");
      hList.defineData("target_var_name", "expression_txt", "target_var_length", "target_var_label", "target_var_fmt");
      hList.defineDone();

      put;
      put "NOTE: ----------------------------------------";
      map_type = upcase("&map_type.");

      /* The hList.check() method above has moved the iterator to the next item. Need to reset the iterator to the beginning of the list */
      rc = hList.reset_dup();
      renameCount = 0;
      expressionCount = 0;
      attribCount = 0;

      put "NOTE: Retrieving list of variables for:";
      put "NOTE:  - map_type: " map_type;

      /* Loop through all variables */
      do while(hList.do_over() eq 0);
         rename_flg = "N";

         %if (&fout_flg. = Y and %sysevalf(%superq(ds_out) ne, boolean)) %then %do;
            /* Convert rename to assignment in case the mapping code is executed outside this macro call */
            if (prxmatch(rx, strip(expression_txt)) and prxmatch(rx2, strip(target_var_name))) then do;
               expression_txt = cats("=", expression_txt);
            end;
         %end;

         /* Check if this is a rename statement */
         if (prxmatch(rx, strip(expression_txt))) then do;
            rename_flg = "Y";
            renameCount = renameCount + 1;
            call symputx(cats("rename_stmt_", put(renameCount, 8.)), catx(" = ", expression_txt, target_var_name), "L");
         end;
         else do;
            /* It is an expression */
            expressionCount = expressionCount + 1;
            call symputx(cats("tgt_var_name_", put(expressionCount, 8.)), target_var_name, "L");
            call symputx(cats("expression_", put(expressionCount, 8.)), expression_txt, "L");
         end;
         if(not missing(target_var_name)
            %if (&fout_flg. = Y) %then %do;
               /* if the rename statement code is executed outside of this macro we have to skip the attrib statement for this variable */
               and rename_flg = "N"
            %end;
            and (not missing(target_var_length)
                 or not missing(target_var_label)
                 or not missing(target_var_fmt))
            ) then do;
            attribCount = attribCount + 1;
            call symputx(cats("tgt_var_nm_", put(attribCount, 8.)), target_var_name, "L");
            call symputx(cats("tgt_var_length_", put(attribCount, 8.)), target_var_length, "L");
            call symputx(cats("tgt_var_label_", put(attribCount, 8.)), target_var_label, "L");
            call symputx(cats("tgt_var_fmt_", put(attribCount, 8.)), target_var_fmt, "L");
         end;

         %if %sysevalf(%superq(ds_out_map) ne, boolean) %then %do;
            output;
         %end;
      end;
      put "NOTE:  --> Number of variables to be renamed: " renameCount;
      put "NOTE:  --> Number of expressions retrieved: " expressionCount;
      put "NOTE:  --> Total number of attributes retrieved: " attribCount;
      put "NOTE: ----------------------------------------";

      call symputx("totRenameVars", renameCount, "L");
      call symputx("totExpressions", expressionCount, "L");
      call symputx("totAttrib", attribCount, "L");
   run;

   /* Restore system options */
   option &quote_option.;

   /* Finalize output */
   %if (&fout_flg. = Y) %then %do;
      data _null_;
         file &fout. lrecl = 32000;
         /* Set the column attributes */
         %do i = 1 %to &totAttrib.;
            put "attrib";
            put "   &&tgt_var_nm_&i..";
               %if %sysevalf(%superq(tgt_var_length_&i.) ne, boolean) %then %do;
                  put "      length = &&tgt_var_length_&i..";
               %end;
               %if %sysevalf(%superq(tgt_var_label_&i.) ne, boolean) %then %do;
                  put "      label = '%sysfunc(prxchange(s/['']/''/i, -1, %superq(tgt_var_label_&i.)))'";
               %end;
               %if %sysevalf(%superq(tgt_var_fmt_&i.) ne, boolean) %then %do;
                  put "      format = &&tgt_var_fmt_&i..";
               %end;
            put ";";
         %end;

         put;

         /* Apply the rename statements (if any) */
         %if(&totRenameVars. > 0) %then %do;
            put "rename";
            %do i = 1 %to &totRenameVars.;
               put "   &&rename_stmt_&i..";
            %end;
            put ";";
         %end;

         put;

         /* Apply the expressions */
         %do i = 1 %to &totExpressions.;
            put "&&tgt_var_name_&i.. %sysfunc(prxchange(s/[""]/""/i, -1, %superq(expression_&i.)));";
         %end;

      run;
   %end;
   %else %do;
      data &ds_out.;
         /* Set the column attributes */
         %do i = 1 %to &TotAttrib.;
            attrib
               &&tgt_var_nm_&i..
               %if %sysevalf(%superq(tgt_var_length_&i.) ne, boolean) %then
                  length = &&tgt_var_length_&i..;
               %if %sysevalf(%superq(tgt_var_label_&i.) ne, boolean) %then
                  label = "&&tgt_var_label_&i..";
               %if %sysevalf(%superq(tgt_var_fmt_&i.) ne, boolean) %then
                  format = &&tgt_var_fmt_&i..;
            ;
         %end;

         set
            &ds_in.
               /* Apply the rename statements (if any) */
               %if(&totRenameVars. > 0) %then %do;
                  (rename = (%do i = 1 %to &totRenameVars.;
                                &&rename_stmt_&i..
                             %end;
                             )
                   )
               %end;
         ;

         /* Apply the expressions */
         %do i = 1 %to &totExpressions.;
            &&tgt_var_name_&i.. &&expression_&i..;
         %end;
      run;
   %end;

%mend core_map_variables;