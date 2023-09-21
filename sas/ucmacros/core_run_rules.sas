/*
 Copyright (C) 2018 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file 
\anchor core_run_rules

   \brief   Apply a set of rules and report on the results

   \param [in] ds_rule_def Input dataset containing rules definition.
   \param [out] ds_out_summary Output table containing summary information about the number of records that fired each rule.
   \param [out] ds_out_details Output table containing detailed information about the records that fired each rule.

   \details

   Input table <i>DS_RULE_DEF</> is expected to have the following structure:

   | Variable            | Type             | Not Null * | Label                      | Description                                                                                                                                                                                 |
   |---------------------|------------------|------------|----------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
   | SOURCE_TABLE        | CHARACTER(40)    | Y          | Source Table               | Source table name. Format: [<libref>.]<table_name>                                                                                                                                          |
   | SOURCE_TABLE_DESC   | CHARACTER(256)   | Y          | Source Table Description   | Source table description                                                                                                                                                                    |
   | PRIMARY_KEY         | CHARACTER(10000) | N          | Primary Key                | (Optional) List of primary key variables of the source table. Only used if parameter <i>DS_OUT_DETAILS</i> is not blank                                                                     |
   | TARGET_TABLE        | CHARACTER(40)    | N          | Target Table               | (Optional) Target table name. See below for details. Format: [<libref>.]<table_name>                                                                                                        |
   | RULE_ID             | CHARACTER(32)    | Y          | Rule Id                    | Rule Identifier                                                                                                                                                                             |
   | RULE_NAME           | CHARACTER(100)   | N          | Rule Name                  | Rule Name                                                                                                                                                                                   |
   | RULE_DESC           | CHARACTER(100)   | N          | Rule Description           | Rule Description                                                                                                                                                                            |
   | RULE_COMPONENT      | CHARACTER(10)    | Y          | Rule Component             | Specifes if the current record relates to the Condition or the Action portion of the rule. See details below. <br>Valid Values (case insensitive):<br> - Condition <br> - Action            |
   | OPERATOR            | CHARACTER(10)    | N          | Operator                   | Boolean Operator. It can be used to combine multiple conditions within a rule.<br>Valid Values (case insensitive): <br> - <blank> <br> - And <br> - Or <br> -Not <br> -And Not <br> -Or Not |
   | PARENTHESIS         | CHARACTER(1)     | N          | Parenthesis                | Opening/Closing parenthesis. It can be used to create more complex boolean conditions.<br>Valid Values: <br> - <blank> <br> - ( <br> - )                                                    |
   | COLUMN_NM           | CHARACTER(32)    | Y          | Column Name                | Required unless RULE_TYPE = "CUSTOM". Name of the variable used to build the condition expression                                                                                           |
   | RULE_TYPE           | CHARACTER(100)   | Y          | Rule Type                  | Controls how the expression for evaluating the condition/action is built. See below for details.                                                                                            |
   | RULE_DETAILS        | CHARACTER(4000)  | Y          | Rule Details               | Provides additional details needed to build the expression for evaluating the condition/action. See below for details                                                                       |
   | MESSAGE_TXT         | CHARACTER(4096)  | N          | Message Text               | Provides a custom message for reporting the results of rules evaluation. Only one message per rule can be specified: for any given rule id, the first non-blank value that is used.         |
   | LOOKUP_TABLE        | CHARACTER(40)    | N          | Lookup Table               | Required if RULE_TYPE in  ("LOOKUP", "NOT_LOOKUP"). Used to specify an additional lookup table to be used for evaluating the condition/action                                               |
   | LOOKUP_KEY          | CHARACTER(10000) | N          | Lookup key                 | Space separated list of lookup key variables. These variables must be found in both the SOURCE_TABLE and the LOOKUP table                                                                   |
   | LOOKUP_DATA         | CHARACTER(10000) | N          | Lookup Data                | Space separated list of lookup data variables to retrieve. These variables must be found in the LOOKUP table                                                                                |
   | AGGR_VAR_NM         | CHARACTER(32)    | N          | Aggregated Variable Name   | Name (Alias) given to the result of the aggregated expression                                                                                                                               |
   | AGGR_EXPRESSION_TXT | CHARACTER(10000) | N          | Aggregated Expression Text | Summary-type of expression (i.e. SUM(<varname>). <br>Orthogonal query expressions can be used to perform filtered aggregation (i.e. SUM(<varname> * (<other_varname> = "some value"))       |
   | AGGR_GROUP_BY_VARS  | CHARACTER(10000) | N          | Aggregated Group By Vars   | (Optional) Space separated list of group-by variables for the aggregation.                                                                                                                  |
   | AGGREGATED_RULE_FLG | CHARACTER(1)     | Y          | Aggregated Rule Flag       | Flag (Y/N). Specifies if the rule operates at the detail level (for each record of the SOURCE_TABLE) or at aggregated level                                                                 |
   | RULE_REPORTING_LEV1 | CHARACTER(1024)  | N          | Rule Reporting Level 1     | This field is used to classify rules for reporting purpose                                                                                                                                  |
   | RULE_REPORTING_LEV2 | CHARACTER(1024)  | N          | Rule Reporting Level 2     | This field is used to classify rules for reporting purpose                                                                                                                                  |
   | RULE_REPORTING_LEV3 | CHARACTER(1024)  | N          | Rule Reporting Level 3     | This field is used to classify rules for reporting purpose                                                                                                                                  |
   | RULE_WEIGHT         | NUMERIC(8)       | Y          | Rule Weight                | A weight assigned to the rule. Used for reporting purpose (weighted aggregation for building performance indicators)                                                                        |


   This macro executes a number of rules against the specified input table/tables (<i>SOURCE_TABLE</i>) and produces a number of output tables with details of the execution. <br>
   Each rule takes the following form:
   \code
      IF <Conditions> THEN <Actions>
   \endcode

   Multiple <i>Conditions</i> and <i>Actions</i> can be specified for each rule by providing a separate record for each condition/action in the input table <i>DS_RULE_DEF</i>

   Rules can operate either at detail level or on aggregated data.
   If a rule is marked as operating at aggregate level (<i>AGGREGATED_RULE_FLG = Y</i>) then each condition associated to this rule must specify an aggregtion expression (AGGR_EXPRESSION_TXT).


   The following table shows details of how each rule type is evaluated.

   | RULE_COMPONENT | RULE_TYPE                                        | RULE_DETAILS                                                                          | Evaluated expression                                                                                     |
   |----------------|--------------------------------------------------|---------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------|
   | Condition      | MISSING                                          |                                                                                       | missing(<COLUMN_NM>)                                                                                     |
   | Condition      | NOT_MISSING                                      |                                                                                       | not missing(<COLUMN_NM>)                                                                                 |
   | Condition      | FIXED_LENGTH                                     | <Number>                                                                              | klength(<COLUMN_NM>) = <Number>                                                                          |
   | Condition      | NOT_FIXED_LENGTH                                 | <Number>                                                                              | klength(<COLUMN_NM>) ne <Number>                                                                         |
   | Condition      | MAX_LENGTH                                       | <Number>                                                                              | klength(<COLUMN_NM>) <= <Number>                                                                         |
   | Condition      | MIN_LENGTH                                       | <Number>                                                                              | klength(<COLUMN_NM>) >= <Number>                                                                         |
   | Condition      | <                                                | <Number>                                                                              | <COLUMN_NM> < <Number>                                                                                   |
   | Condition      | <=                                               | <Number>                                                                              | <COLUMN_NM> <= <Number>                                                                                  |
   | Condition      | =                                                | <Number> \| "<String>"                                                                | <COLUMN_NM> = <Number> \| "<String>"                                                                     |
   | Condition      | NE                                               | <Number> \| "<String>"                                                                | <COLUMN_NM> ne <Number> \| "<String>"                                                                    |
   | Condition      | >=                                               | <Number>                                                                              | <COLUMN_NM> >= <Number>                                                                                  |
   | Condition      | >                                                | <Number>                                                                              | <COLUMN_NM> > <Number>                                                                                   |
   | Condition      | BETWEEN                                          | <Number1>, <Number2>                                                                  | <Number1> <= <COLUMN_NM> <= <Number2>                                                                    |
   | Condition      | BETWEEN_LEFT_EXCLUDED                            | <Number1>, <Number2>                                                                  | <Number1> < <COLUMN_NM> <= <Number2>                                                                     |
   | Condition      | BETWEEN_RIGHT_EXCLUDED                           | <Number1>, <Number2>                                                                  | <Number1> <= <COLUMN_NM> < <Number2>                                                                     |
   | Condition      | BETWEEN_EXCLUDED \| BETWEEN_BOTH_EXCLUDED        | <Number1>, <Number2>                                                                  | <Number1> < <COLUMN_NM> < <Number2>                                                                      |
   | Condition      | NOT_BETWEEN                                      | <Number1>, <Number2>                                                                  | not (<Number1> <= <COLUMN_NM> <= <Number2>)                                                              |
   | Condition      | NOT_BETWEEN_LEFT_EXCLUDED                        | <Number1>, <Number2>                                                                  | not (<Number1> < <COLUMN_NM> <= <Number2>)                                                               |
   | Condition      | NOT_BETWEEN_RIGHT_EXCLUDED                       | <Number1>, <Number2>                                                                  | not (<Number1> <= <COLUMN_NM> < <Number2>)                                                               |
   | Condition      | NOT_BETWEEN_EXCLUDED \| NOT_BETWEEN_BOTH_EXCLUDED| <Number1>, <Number2>                                                                  | not (<Number1> < <COLUMN_NM> < <Number2>)                                                                |
   | Condition      | LIST                                             | <Number1>, <Number2>, ... , <NumberN> \| "<String1>", "<String2>", ... , "<StringN>"  | <COLUMN_NM> in (<Number1>, <Number2>, ... , <NumberN> \| "<String1>", "<String2>", ... , "<StringN>")    |
   | Condition      | NOT_LIST                                         | <Number1>, <Number2>, ... , <NumberN> \| "<String1>", "<String2>", ... , "<StringN>"  | <COLUMN_NM> not in (<Number1>, <Number2>, ... , <NumberN> \| "<String1>", "<String2>", ... , "<StringN>")|
   | Condition      | CUSTOM                                           | <Custom Expression>                                                                   | <Custom Expression>                                                                                      |
   | Action         | MISSING \| SET_MISSING                           |                                                                                       | call missing(<COLUMN_NM>);                                                                               |
   | Action         | ERROR                                            |                                                                                       | put "ERROR: <MESSAGE_TXT>";                                                                              |
   | Action         | STOP                                             |                                                                                       | stop;                                                                                                    |
   | Action         | SET \| SET_VALUE                                 | <Number> \| "<String>"                                                                | <COLUMN_NM> = <Number> \| <"String">;                                                                    |
   | Action         | CUSTOM                                           | <Custom Expression>                                                                   | <Custom Expression>;                                                                                     |


   Output table <DS_OUT_SUMMARY> has the following structure:

   | Variable            | Type            | Not Null * | Label                    | Description                                                                                                            |
   |---------------------|-----------------|------------|--------------------------|------------------------------------------------------------------------------------------------------------------------|
   | EXECUTION_DTTM      | DATETIME        | Y          | Execution Datetime       | Execution datetime                                                                                                     |
   | SOURCE_TABLE        | CHARACTER(40)   | Y          | Source Table             | Source table name. Format: [<libref>.]<table_name>                                                                     |
   | SOURCE_TABLE_DESC   | CHARACTER(256)  | Y          | Source Table Description | Source table Description                                                                                               |
   | RULE_ID             | CHARACTER(32)   | Y          | Rule Id                  | Rule Identifier                                                                                                        |
   | RULE_NAME           | CHARACTER(100)  | N          | Rule Name                | Rule Name                                                                                                              |
   | RULE_DESC           | CHARACTER(100)  | N          | Rule Description         | Rule Description                                                                                                       |
   | RULE_REPORTING_LEV1 | CHARACTER(1024) | N          | Rule Reporting Level 1   | This field is used to classify rules for reporting purpose                                                             |
   | RULE_REPORTING_LEV2 | CHARACTER(1024) | N          | Rule Reporting Level 2   | This field is used to classify rules for reporting purpose                                                             |
   | RULE_REPORTING_LEV3 | CHARACTER(1024) | N          | Rule Reporting Level 3   | This field is used to classify rules for reporting purpose                                                             |
   | RULE_MESSAGE_TXT    | CHARACTER(4096) | N          | Message Text             | Custom message associated to this rule                                                                                 |
   | RULE_WEIGHT         | NUMERIC(8)      | Y          | Rule Weight              | A weight assigned to the rule. Used for reporting purpose (weighted aggregation for building performance indicators)   |
   | RULE_MATCH_CNT      | NUMERIC(8)      | Y          | Rule Match Count         | Number of records that matched the rule condition                                                                      |
   | TOTAL_ROW_CNT       | NUMERIC(8)      | Y          | Total Row Count          | Total number of records processed                                                                                      |


   Output table <DS_OUT_DETAILS> has the following structure:

   | Variable            | Type             | Not Null * | Label                    | Description                                                                      |
   |---------------------|------------------|------------|--------------------------|----------------------------------------------------------------------------------|
   | EXECUTION_DTTM      | DATETIME         | Y          | Execution Datetime       | Execution datetime                                                               |
   | SOURCE_TABLE        | CHARACTER(40)    | Y          | Source Table             | Source table name. Format: [<libref>.]<table_name>                               |
   | SOURCE_TABLE_DESC   | CHARACTER(256)   | Y          | Source Table Description | Source table Description                                                         |
   | RULE_ID             | CHARACTER(32)    | Y          | Rule Id                  | Rule Identifier                                                                  |
   | RULE_NAME           | CHARACTER(100)   | N          | Rule Name                | Rule Name                                                                        |
   | RULE_DESC           | CHARACTER(100)   | N          | Rule Description         | Rule Description                                                                 |
   | RULE_REPORTING_LEV1 | CHARACTER(1024)  | N          | Rule Reporting Level 1   | This field is used to classify rules for reporting purpose                       |
   | RULE_REPORTING_LEV2 | CHARACTER(1024)  | N          | Rule Reporting Level 2   | This field is used to classify rules for reporting purpose                       |
   | RULE_REPORTING_LEV3 | CHARACTER(1024)  | N          | Rule Reporting Level 3   | This field is used to classify rules for reporting purpose                       |
   | RULE_PRIMARY_KEY    | CHARACTER(4096)  | Y          | Rule Primary Key         | Provides the primary key values for the records that matched the rule conodition |
   | RULE_CONDITION_TXT  | CHARACTER(10000) | Y          | Rule Condition Text      | The evaluated condition expression                                               |
   | RULE_MESSAGE_TXT    | CHARACTER(4096)  | N          | Message Text             | Custom message associated to this rule                                           |

\author  SAS Institute Inc.
\date    2018

*/
%macro core_run_rules(ds_rule_def =
                     , ds_out_summary =
                     , ds_out_details =
                     ) / minoperator;

   %local
      tot_sources
      source_key
      rule_aggr_key
      multi_aggr_key
      single_aggr_key
      rule_detail_key
      quoted_var_list
      lookup_cnt
      action_key
      current_var
      current_dttm
      i
   ;

   %let current_dttm = %sysfunc(datetime(), datetime21.);


   /* Create empty output structure */
   data
      &ds_out_summary. (drop = rule_primary_key rule_primary_key)
      %if(%sysevalf(%superq(ds_out_details) ne, boolean)) %then %do;
         &ds_out_details. (drop = rule_weight rule_match_cnt total_row_cnt)
      %end;
      ;

      attrib
         execution_dttm       length = 8.        label = "Execution Datetime"         format = datetime21.
         source_table         length = $100.     label = "Source Table"
         source_table_desc    length = $256.     label = "Source Table Description"
         rule_id              length = $100.     label = "Rule Id"
         rule_name            length = $100.     label = "Rule Name"
         rule_desc            length = $256.     label = "Rule Description"
         rule_reporting_lev1  length = $1024.    label = "Rule Reporting Level 1"
         rule_reporting_lev2  length = $1024.    label = "Rule Reporting Level 2"
         rule_reporting_lev3  length = $1024.    label = "Rule Reporting Level 3"
         rule_primary_key     length = $4096.    label = "Rule Primary Key"
         rule_condition_txt   length = $10000.   label = "Rule Condition"
         rule_message_txt     length = $4096.    label = "Rule Message"
         rule_weight          length = 8.        label = "Rule Weight"
         rule_match_cnt       length = 8.        label = "Match Count"
         total_row_cnt        length = 8.        label = "Total Row Count"
      ;
      stop;
   run;

   /* Keep track of the relative order of each record */
   data _tmp_rule_def_ / view = _tmp_rule_def_;
      length rownum 8.;
      set &ds_rule_def.;
      rownum = _N_;
   run;

   /* Sort data */
   proc sql;
      create table _tmp_rule_def_srt_ as
      select
         t1.*
         /* if any record is marked with aggregated_rule_flg = "Y" then the entire rule is assumed to operate on aggregate level */
         , coalescec(max(aggregated_rule_flg), "N") as max_aggregated_rule_flg
      from
         _tmp_rule_def_ as t1
      group by
         source_table
         , rule_id
      order by
         source_table
         , rule_id
         , rule_component desc
         , rownum
      ;
   quit;


   %let tot_sources = 0;
   data _null_;
      set _tmp_rule_def_srt_ end = last;
      by
         source_table
         rule_id
         descending rule_component
         rownum
      ;

      length
         rule_message_txt $10000.
         condition_txt $10000.
         current_condition_txt $10000.
         action_txt $10000.
         multi_aggr_key_list $10000.
         any_simple_aggr_flg $3.
         source_pkey $10000.
      ;

      retain
         source_key 0
         multi_aggr_key 0
         single_aggr_key 0
         lookup_cnt 0
         rule_key 0
         rule_detail_key 0
         rule_aggr_key 0
         action_key 0
         rule_message_txt ""
         condition_txt ""
         multi_aggr_key_list ""
         any_simple_aggr_flg "N"
         source_pkey ""
         retained_rule_weight
      ;

      if first.source_table then do;
         /* Increment the source table key counter */
         source_key + 1;

         /* Source and Target tables */
         call symputx(catx("_", "source_table", source_key), source_table, "L");
         call symputx(catx("_", "source_table_desc", source_key), source_table_desc, "L");
         call symputx(catx("_", "target_table", source_key), target_table, "L");

         /* Reset the related counters for this source table */
         multi_aggr_key = 0;
         single_aggr_key = 0;
         lookup_cnt = 0;
         rule_key = 0;
         rule_detail_key = 0;
         rule_aggr_key = 0;
         call missing(source_pkey);
      end;

      source_pkey = coalescec(source_pkey, primary_key);

      /* Keep track of the total number of source tables */
      call symputx("tot_sources", source_key, "L");

      if first.rule_id then do;
         /* Increment the rule key counter */
         rule_key + 1;
         if(max_aggregated_rule_flg = "Y") then
            rule_aggr_key + 1;
         else
            rule_detail_key + 1;
         /* Reset the action key counter */
         action_key = 0;
         /* Reset retained variables */
         call missing(rule_message_txt, condition_txt, multi_aggr_key_list, retained_rule_weight);
         any_simple_aggr_flg = "N";
      end;

      /* Get the rule message (no need to specify the message for each record, the first non missing value of message_txt related to this rule will be used) */
      rule_message_txt = coalescec(rule_message_txt, message_txt);
      /* Get the rule weight (no need to specify the weight for each record, the first non missing value of rule_weight related to this rule will be used) */
      retained_rule_weight = coalesce(retained_rule_weight, rule_weight);

      /* ********************************************************** */
      /* Process Aggregation Info                                   */
      /* ********************************************************** */
      if not missing(aggr_expression_txt) then do;
         if not missing(aggr_group_by_vars) then do;
            /* This record requires a group by aggregation */
            multi_aggr_key + 1;
            call symputx(catx("_", "maggr_column_nm", source_key, multi_aggr_key), aggr_var_nm, "L");
            call symputx(catx("_", "maggr_expression_txt", source_key, multi_aggr_key), aggr_expression_txt, "L");
            call symputx(catx("_", "aggr_group_by_vars", source_key, multi_aggr_key), aggr_group_by_vars, "L");
            /* Keep Track of all the aggregation tables associated to this rule */
            if(max_aggregated_rule_flg = "Y") then
               multi_aggr_key_list = catx(" ", multi_aggr_key_list, multi_aggr_key);
         end;
         else do;
            /* This record requires a top level aggregation */
            single_aggr_key + 1;
            call symputx(catx("_", "saggr_column_nm", source_key, single_aggr_key), aggr_var_nm, "L");
            call symputx(catx("_", "saggr_expression_txt", source_key, single_aggr_key), aggr_expression_txt, "L");
            if(max_aggregated_rule_flg = "Y") then
               any_simple_aggr_flg = "Y";
         end;
      end;

      /* ********************************************************** */
      /* Process Lookup Info                                        */
      /* ********************************************************** */

      /* Implicit lookups are required for rules that operate at detail level (max_aggregated_rule_flg = "N") and consume group-by aggregated data */
      if max_aggregated_rule_flg = "N" and not missing(aggr_group_by_vars) then do;
         /* Increment the lookup counter */
         lookup_cnt + 1;
         /* Internal lookup table (group by aggregation): _tmp_maggr_<source_key>_<multi_aggr_key> */
         call symputx(catx("_", "lookup_table", source_key, lookup_cnt), catx("_", "_tmp_maggr", source_key, multi_aggr_key), "L");
         /* Lookup key: the group by variables */
         call symputx(catx("_", "lookup_key", source_key, lookup_cnt), aggr_group_by_vars, "L");
         /* Lookup data: the alias (aggr_var_nm) associated with the aggretation expression */
         call symputx(catx("_", "lookup_data", source_key, lookup_cnt), aggr_var_nm, "L");
      end;

      /* Explicit lookups */
      if not missing(lookup_table) then do;
         /* Increment the lookup counter */
         lookup_cnt + 1;
         call symputx(catx("_", "lookup_table", source_key, lookup_cnt), lookup_table, "L");
         call symputx(catx("_", "lookup_key", source_key, lookup_cnt), coalescec(lookup_key, column_nm), "L");
         call symputx(catx("_", "lookup_data", source_key, lookup_cnt), coalescec(lookup_data, column_nm), "L");
      end;

      /* ********************************************************** */
      /* Build Condition expression                                 */
      /* ********************************************************** */
      if(upcase(rule_component) = "CONDITION") then do;
         /* If a rule operates at aggregated level then all conditions must specify aggregation expressions */
         if(max_aggregated_rule_flg = "Y" and missing(aggr_expression_txt)) then do;
            put "ERROR: Rule Id " rule_id " has been set to operate on aggregated data, however not all conditions define aggregation functions.";
            call symputx("tot_sources", 0, "L");
            stop;
         end;

         select (upcase(rule_type));
            when("MISSING")
               current_condition_txt = cats("missing(", column_nm, ")");
            when("NOT_MISSING")
               current_condition_txt = cats("not missing(", column_nm, ")");
            when("FIXED_LENGTH")
               current_condition_txt = catx(" ", cats("klength(", column_nm, ")"), "=", rule_details);
            when("NOT_FIXED_LENGTH")
               current_condition_txt = catx(" ", cats("klength(", column_nm, ")"), "ne", rule_details);
            when("MAX_LENGTH")
               current_condition_txt = catx(" ", cats("klength(", column_nm, ")"), "<=", rule_details);
            when("MIN_LENGTH")
               current_condition_txt = catx(" ", cats("klength(", column_nm, ")"), ">=", rule_details);
            when("<", "LT", "LESS_THAN")
               current_condition_txt = catx(" ", column_nm, "<", rule_details);
            when("<=", "LEQ", "LESS_EQUAL_THAN")
               current_condition_txt = catx(" ", column_nm, "<=", rule_details);
            when("=", "EQ", "EQUAL_TO")
               current_condition_txt = catx(" ", column_nm, "=", rule_details);
            when("<>", "NE", "NOT_EQUAL_TO")
               current_condition_txt = catx(" ", column_nm, "ne", rule_details);
            when(">=", "GE", "GREATER_EQUAL_THAN")
               current_condition_txt = catx(" ", column_nm, ">=", rule_details);
            when(">", "GT", "GREATER_THAN")
               current_condition_txt = catx(" ", column_nm, ">", rule_details);
            when("BETWEEN")
               current_condition_txt = catx(" ", scan(rule_details, 1, ","), "<=", column_nm, "<=", scan(rule_details, 2, ","));
            when("BETWEEN_LEFT_EXCLUDED")
               current_condition_txt = catx(" ", scan(rule_details, 1, ","), "<", column_nm, "<=", scan(rule_details, 2, ","));
            when("BETWEEN_RIGHT_EXCLUDED")
               current_condition_txt = catx(" ", scan(rule_details, 1, ","), "<=", column_nm, "<", scan(rule_details, 2, ","));
            when("BETWEEN_EXCLUDED", "BETWEEN_BOTH_EXCLUDED")
               current_condition_txt = catx(" ", scan(rule_details, 1, ","), "<", column_nm, "<", scan(rule_details, 2, ","));
            when("NOT_BETWEEN")
               current_condition_txt = catx(" ", "not (", scan(rule_details, 1, ","), "<=", column_nm, "<=", scan(rule_details, 2, ","), ")");
            when("NOT_BETWEEN_LEFT_EXCLUDED")
               current_condition_txt = catx(" ", "not (", scan(rule_details, 1, ","), "<", column_nm, "<=", scan(rule_details, 2, ","), ")");
            when("NOT_BETWEEN_RIGHT_EXCLUDED")
               current_condition_txt = catx(" ", "not (", scan(rule_details, 1, ","), "<=", column_nm, "<", scan(rule_details, 2, ","), ")");
            when("NOT_BETWEEN_EXCLUDED", "NOT_BETWEEN_BOTH_EXCLUDED")
               current_condition_txt = catx(" ", "not (", scan(rule_details, 1, ","), "<", column_nm, "<", scan(rule_details, 2, ","), ")");
            when("LIST")
               current_condition_txt = catx(" ", column_nm, "in (", rule_details, ")");
            when("NOT_LIST")
               current_condition_txt = catx(" ", column_nm, "not in (", rule_details, ")");
            when("LOOKUP")
               current_condition_txt = catx(" ", cats("__rc_", lookup_cnt), "= 0");
            when("NOT_LOOKUP")
               current_condition_txt = catx(" ", cats("__rc_", lookup_cnt), "ne 0");
            when("CUSTOM")
               current_condition_txt = rule_details;
            otherwise do;
               /* Invalid rule type: stop execution */
               put "ERROR: Invalid rule type: " rule_type " (Rule Id: " rule_id ")";
               call symputx("tot_sources", 0, "L");
               stop;
            end;
         end; /* select(upcase(rule_type)) */

         /* Update the condition expression */
         condition_txt = catx(" ", condition_txt
                              /* And / Or / Not / And Not / Or Not */
                              , operator
                              /* Open parenthesis (if any) */
                              , ifc(parenthesis = "(", parenthesis, "")
                              /* Current condition */
                              , "(" , current_condition_txt, ")"
                              /* Close parenthesis (if any) */
                              , ifc(parenthesis = ")", parenthesis, "")
                              );

      end; /* if(upcase(rule_component) = "CONDITION") */

      /* ********************************************************** */
      /* Build Action expression                                    */
      /* ********************************************************** */
      else if(upcase(rule_component) = "ACTION") then do;
         /* Increment action counter */
         action_key + 1;
         select (upcase(rule_type));
            when("MISSING", "SET_MISSING")
               action_txt = cats("call missing(", column_nm, ");");
            when("SET", "SET_VALUE")
               action_txt = catx(" ", column_nm, "=", cats(rule_details, ";"));
            when("CUSTOM")
               action_txt = cats(rule_details, ";");
            when("ERROR")
               action_txt = catx(" ", 'put "ERROR:', rule_message_txt, '";');
            when("STOP")
               action_txt = "stop;";
            otherwise do;
               /* Invalid rule type: stop execution */
               put "ERROR: Invalid rule type: " rule_type " (Rule Id: " rule_id ")";
               call symputx("tot_sources", 0, "L");
               stop;
            end;
         end;

         if(max_aggregated_rule_flg = "Y") then
            /* Store the action expression inside macro variable array aggr_action_txt_<source_key>_<rule_aggr_key>_<action_key> */
            call symputx(catx("_", "aggr_action_txt", source_key, rule_aggr_key, action_key), action_txt, "L");
         else
            /* Store the action expression inside macro variable array dtl_action_txt_<source_key>_<rule_detail_key>_<action_key> */
            call symputx(catx("_", "dtl_action_txt", source_key, rule_detail_key, action_key), action_txt, "L");

      end; /* if(upcase(rule_component) = "ACTION") */
      else do;
         /* Invalid rule component: stop execution */
         put "ERROR: Invalid rule component: " rule_component " (Rule Id: " rule_id ")";
         call symputx("tot_sources", 0, "L");
         stop;
      end;

      if last.rule_id then do;
         if(max_aggregated_rule_flg = "Y") then do;
            /* Keep track of all the aggregation tables to be merged in order to check this aggregated rule */
            call symputx(catx("_", "multi_aggr_key_list", source_key, rule_aggr_key), multi_aggr_key_list, "L");
            /* Flag used to determine if the top level aggregation table is required for this aggregated rule */
            call symputx(catx("_", "any_simple_aggr_flg", source_key, rule_aggr_key), any_simple_aggr_flg, "L");
            /* Store the condition expression inside macro variable array aggr_condition_txt_<source_key>_<rule_aggr_key> */
            call symputx(catx("_", "aggr_condition_txt", source_key, rule_aggr_key), condition_txt, "L");
            /* Keep track of how many actions are associated with this rule */
            call symputx(catx("_", "tot_aggr_actions", source_key, rule_aggr_key), action_key, "L");
            /* Rule attributes */
            call symputx(catx("_", "aggr_rule_id", source_key, rule_aggr_key), rule_id, "L");
            call symputx(catx("_", "aggr_rule_name", source_key, rule_aggr_key), rule_name, "L");
            call symputx(catx("_", "aggr_rule_desc", source_key, rule_aggr_key), rule_desc, "L");
            call symputx(catx("_", "aggr_rule_rpt_lev1", source_key, rule_aggr_key), rule_reporting_lev1, "L");
            call symputx(catx("_", "aggr_rule_rpt_lev2", source_key, rule_aggr_key), rule_reporting_lev2, "L");
            call symputx(catx("_", "aggr_rule_rpt_lev3", source_key, rule_aggr_key), rule_reporting_lev3, "L");
            call symputx(catx("_", "aggr_rule_message_txt", source_key, rule_aggr_key), rule_message_txt, "L");
            call symputx(catx("_", "aggr_rule_weight", source_key, rule_aggr_key), retained_rule_weight, "L");
         end;
         else do;
            /* Store the condition expression inside macro variable array dtl_condition_txt_<source_key>_<rule_detail_key> */
            call symputx(catx("_", "dtl_condition_txt", source_key, rule_detail_key), condition_txt, "L");
            /* Keep track of how many actions are associated with this rule */
            call symputx(catx("_", "tot_dtl_actions", source_key, rule_detail_key), action_key, "L");
            /* Rule attributes */
            call symputx(catx("_", "dtl_rule_id", source_key, rule_detail_key), rule_id, "L");
            call symputx(catx("_", "dtl_rule_name", source_key, rule_detail_key), rule_name, "L");
            call symputx(catx("_", "dtl_rule_desc", source_key, rule_detail_key), rule_desc, "L");
            call symputx(catx("_", "dtl_rule_rpt_lev1", source_key, rule_detail_key), rule_reporting_lev1, "L");
            call symputx(catx("_", "dtl_rule_rpt_lev2", source_key, rule_detail_key), rule_reporting_lev2, "L");
            call symputx(catx("_", "dtl_rule_rpt_lev3", source_key, rule_detail_key), rule_reporting_lev3, "L");
            call symputx(catx("_", "dtl_rule_message_txt", source_key, rule_detail_key), rule_message_txt, "L");
            call symputx(catx("_", "dtl_rule_weight", source_key, rule_detail_key), retained_rule_weight, "L");
         end;
      end;

      if last.source_table then do;
         /* Keep track of how many aggregation queries we need to run for the current source table */
         call symputx(catx("_", "tot_multi_aggr", source_key), multi_aggr_key, "L");
         call symputx(catx("_", "tot_single_aggr", source_key), single_aggr_key, "L");
         /* Keep track of how many lookups must be performed for the current source table */
         call symputx(catx("_", "tot_lookups", source_key), lookup_cnt, "L");
         /* Keep track of how many aggregated rules are defined for the current source table */
         call symputx(catx("_", "tot_aggr_rules", source_key), rule_aggr_key, "L");
         /* Keep track of how many detail rules are defined for the current source table */
         call symputx(catx("_", "tot_detail_rules", source_key), rule_detail_key, "L");
         /* List of primary key variables for the current source table */
         call symputx(catx("_", "source_pkey", source_key), source_pkey, "L");
      end;

   run;

   /* Loop through all sources */
   %do source_key = 1 %to &tot_sources.;

      /* ********************************************************** */
      /* Create a separate table for each group by aggregation      */
      /* ********************************************************** */

      /* Loop through all group-by aggregations */
      %do multi_aggr_key = 1 %to &&tot_multi_aggr_&source_key..;
         /* Create a separate table for each group-by aggregation */
         proc sql;
            create table _tmp_maggr_&source_key._&multi_aggr_key. as
            select
               /* Group-By variables (comma separated) */
               %sysfunc(prxchange(s/\s+/%str(,) /i, -1, %superq(aggr_group_by_vars_&source_key._&multi_aggr_key.)))
               /* <Expression> as <Column> */
               , &&maggr_expression_txt_&source_key._&multi_aggr_key.. as &&maggr_column_nm_&source_key._&multi_aggr_key..
            from
               &&source_table_&source_key..
            group by
               /* Group-By variables (comma separated) */
               %sysfunc(prxchange(s/\s+/%str(,) /i, -1, %superq(aggr_group_by_vars_&source_key._&multi_aggr_key.)))
            ;
         quit;
      %end;

      /* ********************************************************** */
      /* Create a single table for all top-level aggregations       */
      /* ********************************************************** */

      %if(&&tot_single_aggr_&source_key.. > 0) %then %do;
         proc sql;
            create table _tmp_saggr_&source_key. as
            select
               /* Loop through all group-by aggregations */
               %do single_aggr_key = 1 %to &&tot_single_aggr_&source_key..;
                  %if &single_aggr_key. > 1 %then ,;
                  &&saggr_expression_txt_&source_key._&single_aggr_key.. as &&saggr_column_nm_&source_key._&single_aggr_key..
               %end;
            from
               &&source_table_&source_key..
            ;
         quit;
      %end;

      /* ********************************************************** */
      /* Determine if the source table is a CAS table               */
      /* If it is then make tmplib point to the caslib              */
      /* otherwise use work                                         */
      /* ********************************************************** */
      %let sourceLibname=%scan(&&source_table_&source_key..,1,%str(.));
      proc SQL noprint;
         select distinct(engine) into:engine from sashelp.vlibnam where libname=upcase("&sourceLibname");
      quit;
      %if &engine = CAS %then
         %do;
            %let tmplib=&sourceLibname..;
         %end;
      %else
         %do;
            %let tmplib=;
         %end;
      /* Check if there are any rules that operate at the detail level */
      %if(&&tot_detail_rules_&source_key.. > 0) %then %do;

         data __tmp_source_ds__;
            /* Read source table (only first record) */
            set &&source_table_&source_key.. (obs = 1);
         run;


         data
            &tmplib._tmp_dtl_rules_summary_&source_key.
               (keep = __execution_dttm
                       __source_table
                       __source_table_desc
                       __rule_id
                       __rule_name
                       __rule_desc
                       __rule_condition_txt
                       __rule_message_txt
                       __rule_reporting_lev1
                       __rule_reporting_lev2
                       __rule_reporting_lev3
                       __rule_weight
                       __rule_match_cnt
                       __total_row_cnt

                rename = (__execution_dttm = execution_dttm
                          __source_table = source_table
                          __source_table_desc = source_table_desc
                          __rule_id = rule_id
                          __rule_name = rule_name
                          __rule_desc = rule_desc
                          __rule_condition_txt = rule_condition_txt
                          __rule_message_txt = rule_message_txt
                          __rule_reporting_lev1 = rule_reporting_lev1
                          __rule_reporting_lev2 = rule_reporting_lev2
                          __rule_reporting_lev3 = rule_reporting_lev3
                          __rule_weight = rule_weight
                          __rule_match_cnt = rule_match_cnt
                          __total_row_cnt = total_row_cnt
                        )
               )

            %if(%sysevalf(%superq(ds_out_details) ne, boolean)) %then %do;
               &tmplib._tmp_dtl_rules_details_&source_key.
                  (keep = __execution_dttm
                          __source_table
                          __source_table_desc
                          __rule_id
                          __rule_name
                          __rule_desc
                          __rule_reporting_lev1
                          __rule_reporting_lev2
                          __rule_reporting_lev3
                          __rule_primary_key
                          __rule_condition_txt
                          __rule_message_txt

                   rename = (__execution_dttm = execution_dttm
                             __source_table = source_table
                             __source_table_desc = source_table_desc
                             __rule_id = rule_id
                             __rule_name = rule_name
                             __rule_desc = rule_desc
                             __rule_reporting_lev1 = rule_reporting_lev1
                             __rule_reporting_lev2 = rule_reporting_lev2
                             __rule_reporting_lev3 = rule_reporting_lev3
                             __rule_primary_key = rule_primary_key
                             __rule_condition_txt = rule_condition_txt
                             __rule_message_txt = rule_message_txt
                           )
                  )
            %end;
            %if(%sysevalf(%superq(target_table_&source_key.) ne, boolean)) %then %do;
               &&target_table_&source_key..
                  (drop = __execution_dttm
                          __source_table
                          __source_table_desc
                          __rule_id
                          __rule_name
                          __rule_desc
                          __rule_reporting_lev1
                          __rule_reporting_lev2
                          __rule_reporting_lev3
                          __rule_primary_key
                          __rule_condition_txt
                          __rule_message_txt
                          __rule_weight
                          __rule_match_cnt
                          __total_row_cnt
                  )
            %end;
            ;

            length
               __execution_dttm 8.
               __source_table $100.
               __source_table_desc $256.
               __rule_id $100.
               __rule_name $100.
               __rule_desc $256.
               __rule_reporting_lev1 $1024.
               __rule_reporting_lev2 $1024.
               __rule_reporting_lev3 $1024.
               __rule_primary_key $4096.
               __rule_condition_txt $10000.
               __rule_message_txt $4096.
               __rule_weight 8.
               __rule_match_cnt 8.
               __total_row_cnt 8.
            ;
            format __execution_dttm datetime21.;
            retain
               __source_table "&&source_table_&source_key.."
               __source_table_desc "&&source_table_desc_&source_key.."
            ;
            retain __execution_dttm "&current_dttm."dt;

            /* Check if the source table has any record */
            %if(%rsk_attrn(__tmp_source_ds__, nobs)) %then %do;
               /* Read source table */
               set &&source_table_&source_key.. end = last;
            %end;
            %else %do;
               /* The source table has no records */

               if _N_ = 0 then
                  /* Declare all variables */
                  set &&source_table_&source_key..;

               /* Set the last variable for downstream processing */
               last = 1;
               drop last;
            %end;

            /* Array to keep track of how many records match the rule */
            array __rule_match {&&tot_detail_rules_&source_key..} _temporary_ (&&tot_detail_rules_&source_key.. * 0);

            %if(&&tot_single_aggr_&source_key.. > 0) %then %do;
               %if(%rsk_attrn(__tmp_source_ds__, nobs)) %then %do;
                  if _N_ = 1 then
               %end;
                  set _tmp_saggr_&source_key.;
            %end;

            /* Declare all lookup tables */
            %if(&&tot_lookups_&source_key.. > 0) %then %do;

               /* Make sure all lookup variables are declared */
               if _N_ = 0 then do;
                  %do lookup_cnt = 1 %to &&tot_lookups_&source_key..;
                     set &&lookup_table_&source_key._&lookup_cnt..;
                  %end;
               end;

               /* Setup the lookup tables */
               if _N_ = 1 then do;
                  %do lookup_cnt = 1 %to &&tot_lookups_&source_key..;
                     declare hash _hLookup_&lookup_cnt. (dataset: "&&lookup_table_&source_key._&lookup_cnt..");
                     %let quoted_var_list = %sysfunc(prxchange(s/\s+/%str(, )/i, -1, &&lookup_key_&source_key._&lookup_cnt..));
                     %let quoted_var_list = %sysfunc(prxchange(s/(\w+)/"$1"/i, -1, %bquote(&quoted_var_list.)));
                     _hLookup_&lookup_cnt..defineKey(&quoted_var_list.);
                     %let quoted_var_list = %sysfunc(prxchange(s/\s+/%str(, )/i, -1, &&lookup_data_&source_key._&lookup_cnt..));
                     %let quoted_var_list = %sysfunc(prxchange(s/(\w+)/"$1"/i, -1, %bquote(&quoted_var_list.)));
                     _hLookup_&lookup_cnt..defineData(&quoted_var_list.);
                     _hLookup_&lookup_cnt..defineDone();
                  %end;
               end;

               /* Perform the lookups */
               %do lookup_cnt = 1 %to &&tot_lookups_&source_key..;
                  /* Reset any variable from in the lookup data list that is not defined as a lookup key */
                  %do i = 1 %to %sysfunc(countw(&&lookup_data_&source_key._&lookup_cnt.., %str( )));
                     %let current_var = %scan(&&lookup_data_&source_key._&lookup_cnt.., &i., %str( ));
                     %if(not (&current_var. in (&&lookup_key_&source_key._&lookup_cnt..))) %then %do;
                        call missing(&current_var.);
                     %end;
                  %end;
                  /* Perform the lookup */
                  drop __rc_&lookup_cnt.;
                  __rc_&lookup_cnt. = _hLookup_&lookup_cnt..find();
               %end;
            %end;

            /* Loop through all rules that operate at the detail level */
            %do rule_detail_key = 1 %to &&tot_detail_rules_&source_key..;
               if(&&dtl_condition_txt_&source_key._&rule_detail_key..) then do;
                  /* Update the counter of records that match the condition */
                  __rule_match[&rule_detail_key.] + 1;

                  /* Report details of the record that matches the condition */
                  %if(%sysevalf(%superq(ds_out_details) ne, boolean)) %then %do;

                     /* Set rule attributes */
                     __rule_id = "&&dtl_rule_id_&source_key._&rule_detail_key..";
                     __rule_name = "&&dtl_rule_name_&source_key._&rule_detail_key..";
                     __rule_desc = "%sysfunc(prxchange(s/[""]/""/i, -1, %superq(dtl_rule_desc_&source_key._&rule_detail_key.)))";
                     __rule_reporting_lev1 = "&&dtl_rule_rpt_lev1_&source_key._&rule_detail_key..";
                     __rule_reporting_lev2 = "&&dtl_rule_rpt_lev2_&source_key._&rule_detail_key..";
                     __rule_reporting_lev3 = "&&dtl_rule_rpt_lev3_&source_key._&rule_detail_key..";
                     __rule_message_txt = "%sysfunc(prxchange(s/[""]/""/i, -1, %superq(dtl_rule_message_txt_&source_key._&rule_detail_key.)))";
                     __rule_condition_txt = "%sysfunc(prxchange(s/[""]/""/i, -1, %superq(dtl_condition_txt_&source_key._&rule_detail_key.)))";
                     __rule_condition_txt = prxchange('s/__rc_[0-9]+ ne 0/No match with lookup/i', -1, __rule_condition_txt);
                     __rule_condition_txt = prxchange('s/__rc_[0-9]+ = 0/Match with lookup/i', -1, __rule_condition_txt);

                     __rule_primary_key = catx("; "
                                               , catx(" ", "Row No:", _N_)
                                               %do i = 1 %to %sysfunc(countw(&&source_pkey_&source_key.., %str( )));
                                                   %let current_var = %scan(&&source_pkey_&source_key.., &i., %str( ));
                                                   , catx(" ", "&current_var.:", &current_var.)
                                               %end;
                                              );

                     /* Write record to output detail report table */
                     output &tmplib._tmp_dtl_rules_details_&source_key.;
                  %end;

                  /* Apply all related actions */
                  %do action_key = 1 %to &&tot_dtl_actions_&source_key._&rule_detail_key..;
                     &&dtl_action_txt_&source_key._&rule_detail_key._&action_key..
                  %end;
               end; /* if(<condition>) */
            %end; /* Loop through all rules that operate at the detail level */

            /* Apply any action and write to target table */
            %if(%sysevalf(%superq(target_table_&source_key.) ne, boolean)) %then %do;
               /* Write record to the target table */
               output &&target_table_&source_key..;
            %end;

            if last then do;
               %do rule_detail_key = 1 %to &&tot_detail_rules_&source_key..;
                  __rule_id = "&&dtl_rule_id_&source_key._&rule_detail_key..";
                  __rule_name = "&&dtl_rule_name_&source_key._&rule_detail_key..";
                  __rule_desc = "%sysfunc(prxchange(s/[""]/""/i, -1, %superq(dtl_rule_desc_&source_key._&rule_detail_key.)))"; 
                  __rule_condition_txt = "%sysfunc(prxchange(s/[""]/""/i, -1, %superq(dtl_condition_txt_&source_key._&rule_detail_key.)))";
                  __rule_condition_txt = prxchange('s/__rc_[0-9]+ ne 0/No match with lookup/i', -1, __rule_condition_txt);
                  __rule_condition_txt = prxchange('s/__rc_[0-9]+ = 0/Match with lookup/i', -1, __rule_condition_txt);
                  __rule_message_txt = "%sysfunc(prxchange(s/[""]/""/i, -1, %superq(dtl_rule_message_txt_&source_key._&rule_detail_key.)))";
                  __rule_reporting_lev1 = "&&dtl_rule_rpt_lev1_&source_key._&rule_detail_key..";
                  __rule_reporting_lev2 = "&&dtl_rule_rpt_lev2_&source_key._&rule_detail_key..";
                  __rule_reporting_lev3 = "&&dtl_rule_rpt_lev3_&source_key._&rule_detail_key..";
                  __rule_weight = &&dtl_rule_weight_&source_key._&rule_detail_key..;
                  __rule_match_cnt = __rule_match[&rule_detail_key.];
                  __total_row_cnt = _N_;
                  output &tmplib._tmp_dtl_rules_summary_&source_key.;
               %end;
            end;
         run;

         /* Append summary results to output */
         proc append data = &tmplib._tmp_dtl_rules_summary_&source_key.
                     base = &ds_out_summary. force nowarn;
         run;

         %if(%sysevalf(%superq(ds_out_details) ne, boolean)) %then %do;
            /* Append detailed results to output */
            proc append data = &tmplib._tmp_dtl_rules_details_&source_key.
                        base = &ds_out_details. force nowarn;
            run;
         %end;

      %end; /* Check if there are any rules that operate at the detail level */

      /* Loop through all rules that operate at the aggregated level */
      %do rule_aggr_key = 1 %to &&tot_aggr_rules_&source_key..;

         data _tmp_aggr_rules_summary_&source_key._&rule_aggr_key.
               (keep = __execution_dttm
                       __source_table
                       __source_table_desc
                       __rule_id
                       __rule_name
                       __rule_desc
                       __rule_condition_txt
                       __rule_message_txt
                       __rule_reporting_lev1
                       __rule_reporting_lev2
                       __rule_reporting_lev3
                       __rule_weight
                       __rule_match_cnt
                       __total_row_cnt

                rename = (__execution_dttm = execution_dttm
                          __source_table = source_table
                          __source_table_desc = source_table_desc
                          __rule_id = rule_id
                          __rule_name = rule_name
                          __rule_desc = rule_desc
                          __rule_condition_txt = rule_condition_txt
                          __rule_message_txt = rule_message_txt
                          __rule_reporting_lev1 = rule_reporting_lev1
                          __rule_reporting_lev2 = rule_reporting_lev2
                          __rule_reporting_lev3 = rule_reporting_lev3
                          __rule_weight = rule_weight
                          __rule_match_cnt = rule_match_cnt
                          __total_row_cnt = total_row_cnt
                        )
               )

            %if(%sysevalf(%superq(ds_out_details) ne, boolean)) %then %do;
               _tmp_aggr_rules_details_&source_key._&rule_aggr_key.
                  (keep = __execution_dttm
                          __source_table
                          __source_table_desc
                          __rule_id
                          __rule_name
                          __rule_desc
                          __rule_reporting_lev1
                          __rule_reporting_lev2
                          __rule_reporting_lev3
                          __rule_primary_key
                          __rule_condition_txt
                          __rule_message_txt

                   rename = (__execution_dttm = execution_dttm
                             __source_table = source_table
                             __source_table_desc = source_table_desc
                             __rule_id = rule_id
                             __rule_name = rule_name
                             __rule_desc = rule_desc
                             __rule_reporting_lev1 = rule_reporting_lev1
                             __rule_reporting_lev2 = rule_reporting_lev2
                             __rule_reporting_lev3 = rule_reporting_lev3
                             __rule_primary_key = rule_primary_key
                             __rule_condition_txt = rule_condition_txt
                             __rule_message_txt = rule_message_txt
                           )
                  )
            %end;
            ;

            length
               __execution_dttm 8.
               __source_table $100.
               __source_table_desc $256.
               __rule_id $100.
               __rule_name $100.
               __rule_desc $256.
               __rule_reporting_lev1 $1024.
               __rule_reporting_lev2 $1024.
               __rule_reporting_lev3 $1024.
               __rule_primary_key $4096.
               __rule_condition_txt $10000.
               __rule_message_txt $4096.
               __rule_weight 8.
               __rule_match_cnt 8.
               __total_row_cnt 8.
            ;
            format __execution_dttm datetime21.;

            /* Check if there is any aggregated table */
            %if(%sysfunc(countw(&&multi_aggr_key_list_&source_key._&rule_aggr_key.., %str( ))) > 0) %then %do;
               /* Read the first aggregated table */
               %let multi_aggr_key = %scan(&&multi_aggr_key_list_&source_key._&rule_aggr_key.., 1, %str( ));

               /* Read the table only if there are any records */
               %if(%rsk_attrn(_tmp_maggr_&source_key._&multi_aggr_key., nobs)) %then %do;
                  set _tmp_maggr_&source_key._&multi_aggr_key. end = last;
               %end;
               %else %do;
                  /* There are no records, just declare all columns */
                  if _N_ = 0 then
                     set _tmp_maggr_&source_key._&multi_aggr_key.;

                  /* Set the last variable for downstream processing */
                  last = 1;
                  drop last;
               %end;

               /* Make sure all columns are defined */
               if _N_ = 0 then do;
                  %do i = 2 %to %sysfunc(countw(&&multi_aggr_key_list_&source_key._&rule_aggr_key.., %str( )));
                     %let multi_aggr_key = %scan(&&multi_aggr_key_list_&source_key._&rule_aggr_key..,&i., %str( ));
                     set _tmp_maggr_&source_key._&multi_aggr_key.;
                  %end;
               end;

               /* Load all other aggregated tables as lookups */
               if _N_ = 1 then do;
                  %do i = 2 %to %sysfunc(countw(&&multi_aggr_key_list_&source_key._&rule_aggr_key.., %str( )));
                     %let multi_aggr_key = %scan(&&multi_aggr_key_list_&source_key._&rule_aggr_key..,&i., %str( ));
                     declare hash hAggr_&i. (dataset: "_tmp_maggr_&source_key._&multi_aggr_key.");
                     %let quoted_var_list = %sysfunc(prxchange(s/\s+/%str(, )/i, -1, &&aggr_group_by_vars_&source_key._&multi_aggr_key..));
                     %let quoted_var_list = %sysfunc(prxchange(s/(\w+)/"$1"/i, -1, %bquote(&quoted_var_list.)));
                     hAggr_&i..defineKey(&quoted_var_list.);
                     hAggr_&i..defineData("&&maggr_column_nm_&source_key._&multi_aggr_key..");
                     hAggr_&i..defineDone();

                     %if(&&any_simple_aggr_flg_&source_key._&rule_aggr_key.. = Y) %then %do;
                        %if(%rsk_attrn(_tmp_saggr_&source_key., nobs)) %then %do;
                           set _tmp_saggr_&source_key.;
                        %end;
                        %else %do;
                           if _N_ = 0 then
                              set _tmp_saggr_&source_key.;
                        %end;
                     %end;
                  %end;
               end;

               /* Perform lookups */
               %do i = 2 %to %sysfunc(countw(&&multi_aggr_key_list_&source_key._&rule_aggr_key.., %str( )));
                  %let multi_aggr_key = %scan(&&multi_aggr_key_list_&source_key._&rule_aggr_key..,&i., %str( ));
                  call missing(&&maggr_column_nm_&source_key._&multi_aggr_key..);
                  __rc_&i. = hAggr_&i..find();
               %end;
            %end;
            %else %do;
               /* Read the table only if there are any records */
               %if(%rsk_attrn(_tmp_saggr_&source_key., nobs)) %then %do;
                  set _tmp_saggr_&source_key. end = last;
               %end;
               %else %do;
                  /* There are no records, just declare all columns */
                  if _N_ = 0 then
                     set _tmp_saggr_&source_key.;

                  /* Set the last variable for downstream processing */
                  last = 1;
                  drop last;
               %end;
            %end;


            /* Set rule attributes */
            __execution_dttm = "&current_dttm."dt;
            __source_table = "&&source_table_&source_key..";
            __source_table_desc = "&&source_table_desc_&source_key..";
            __rule_id = "&&aggr_rule_id_&source_key._&rule_aggr_key..";
            __rule_name = "&&aggr_rule_name_&source_key._&rule_aggr_key..";
            __rule_desc = "%sysfunc(prxchange(s/[""]/""/i, -1, %superq(aggr_rule_desc_&source_key._&rule_aggr_key.)))";
            __rule_reporting_lev1 = "&&aggr_rule_rpt_lev1_&source_key._&rule_aggr_key..";
            __rule_reporting_lev2 = "&&aggr_rule_rpt_lev2_&source_key._&rule_aggr_key..";
            __rule_reporting_lev3 = "&&aggr_rule_rpt_lev3_&source_key._&rule_aggr_key..";
            __rule_message_txt = "%sysfunc(prxchange(s/[""]/""/i, -1, %superq(aggr_rule_message_txt_&source_key._&rule_aggr_key.)))";
            __rule_condition_txt = "%sysfunc(prxchange(s/[""]/""/i, -1, %superq(aggr_condition_txt_&source_key._&rule_aggr_key.)))";
            __rule_condition_txt = prxchange('s/__rc_[0-9]+ ne 0/No match with lookup/i', -1, __rule_condition_txt);
            __rule_condition_txt = prxchange('s/__rc_[0-9]+ = 0/Match with lookup/i', -1, __rule_condition_txt);
            __rule_weight = &&aggr_rule_weight_&source_key._&rule_aggr_key..;

            if(&&aggr_condition_txt_&source_key._&rule_aggr_key..) then do;
               /* Update the counter of records that match the condition */
               __rule_match_cnt + 1;

               /* Report details of the record that matches the condition */
               %if(%sysevalf(%superq(ds_out_details) ne, boolean)) %then %do;

                  __rule_primary_key = catx("; "
                                            , catx(" ", "Aggregated Row No:", _N_)
                                            %if(%sysfunc(countw(&&multi_aggr_key_list_&source_key._&rule_aggr_key.., %str( ))) > 0) %then %do;
                                               %let multi_aggr_key = %scan(&&multi_aggr_key_list_&source_key._&rule_aggr_key.., 1, %str( ));
                                               %do i = 1 %to %sysfunc(countw(&&aggr_group_by_vars_&source_key._&multi_aggr_key.., %str( )));
                                                   %let current_var = %scan(&&aggr_group_by_vars_&source_key._&multi_aggr_key.., &i., %str( ));
                                                   , catx(" ", "&current_var.:", &current_var.)
                                               %end;
                                            %end;
                                           );

                  /* Write record to output detail report table */
                  output &tmplib._tmp_aggr_rules_details_&source_key._&rule_aggr_key.;
               %end;

               /* Apply all related actions */
               %do action_key = 1 %to &&tot_aggr_actions_&source_key._&rule_aggr_key..;
                  &&aggr_action_txt_&source_key._&rule_aggr_key._&action_key..
               %end;
            end;

            if last then do;
               __total_row_cnt = _N_;
               output &tmplib._tmp_aggr_rules_summary_&source_key._&rule_aggr_key.;
            end;
         run;

         /* Append summary results to output */
         proc append data = &tmplib._tmp_aggr_rules_summary_&source_key._&rule_aggr_key.
                     base = &ds_out_summary. force nowarn;
         run;

         %if(%sysevalf(%superq(ds_out_details) ne, boolean)) %then %do;
            /* Append detailed results to output */
            proc append data = &tmplib._tmp_aggr_rules_details_&source_key._&rule_aggr_key.
                        base = &ds_out_details. force nowarn;
            run;
         %end;

      %end; /* Loop through all rules that operate at the aggregated level */


   %end; /* Loop through all sources */

%mend;